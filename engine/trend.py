"""
Trend Risk Cap (Layer 2)
------------------------
VOO 200 日均線趨勢層，作為 regime / allocation 的 risk cap。

判定規則：
  history_length < 220           → TREND_WARMUP  (資料不足，bypass cap，不報錯)
  close >= SMA_200               → TREND_OK
  close < SMA_200, slope > 0     → TREND_CAUTION (SMA 仍上升)
  close < SMA_200, slope <= 0    → TREND_RISK_CAP (SMA 已平坦/下彎)

SMA_200_slope = SMA_200(today) - SMA_200(20 trading days ago)

設計原則：
  - compute_trend_status()：純函式，可不依賴 DB 進行單元測試
  - TrendLayer.run()：從 DB 讀 VOO raw close（直接讀 raw_market_data，
                       不走 CORE_ASSET_PROXIES 的 VOO→SPY 代理機制）
  - 任何失敗（DB 讀取失敗 / 資料不足）→ TREND_WARMUP，不拋例外，不中斷流程
  - 不寫 DB，不改 Snapshot，不修改任何現有 public interface
  - Layer 2 只作為 risk cap，不作為 hard veto
  - 不直接清倉，不直接改寫 Macro Regime，不直接把 summary 改成 RED
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import List, Optional

logger = logging.getLogger(__name__)

# ── 常數 ───────────────────────────────────────────────────────────────────────

# 計算 SMA_200 所需最少資料筆數（200 均線 + 20 slope buffer）
MIN_HISTORY: int = 220
# SMA slope 計算：今日 vs. N 個交易日前的 SMA
SLOPE_WINDOW: int = 20
# DB 查詢 buffer（多取 30 筆以確保 rolling 計算穩定）
FETCH_BUFFER: int = 30

VOO_SYMBOL: str = "VOO"


# ── Status Enum ────────────────────────────────────────────────────────────────

class TrendStatus(str, Enum):
    WARMUP   = "TREND_WARMUP"    # 歷史資料不足，bypass cap
    OK       = "TREND_OK"        # 收盤 >= SMA_200
    CAUTION  = "TREND_CAUTION"   # 收盤 < SMA_200，SMA 仍上升
    RISK_CAP = "TREND_RISK_CAP"  # 收盤 < SMA_200，SMA 平坦或下彎


# ── Output ─────────────────────────────────────────────────────────────────────

@dataclass
class TrendResult:
    status:        TrendStatus
    close:         Optional[float]      # 最新收盤（None = 無資料）
    sma_200:       Optional[float]      # 200 日均線（None = 資料不足）
    sma_200_slope: Optional[float]      # SMA_200 slope（None = 資料不足）
    history_len:   int                  # 可用交易日筆數
    rationale:     str                  # 判定說明


# ── 純計算函式（可單元測試，不依賴 DB）─────────────────────────────────────────

def compute_trend_status(closes: List[float]) -> TrendResult:
    """
    依收盤價序列計算 Trend 狀態。

    Parameters
    ----------
    closes : 收盤價序列（由舊到新，float list），長度任意

    Returns
    -------
    TrendResult
      - WARMUP  : len(closes) < MIN_HISTORY
      - OK      : close[-1] >= SMA_200
      - CAUTION : close[-1] < SMA_200, slope > 0
      - RISK_CAP: close[-1] < SMA_200, slope <= 0
    """
    n = len(closes)

    if n < MIN_HISTORY:
        logger.warning(
            "[TREND] WARMUP — history=%d < %d, "
            "Warning: Insufficient data for 200DMA calculation, bypassing Trend Risk Cap.",
            n, MIN_HISTORY,
        )
        return TrendResult(
            status        = TrendStatus.WARMUP,
            close         = float(closes[-1]) if closes else None,
            sma_200       = None,
            sma_200_slope = None,
            history_len   = n,
            rationale     = (
                f"Insufficient data for 200DMA calculation ({n} < {MIN_HISTORY}), "
                "bypassing Trend Risk Cap."
            ),
        )

    # ── 計算 SMA_200 ──────────────────────────────────────────────────────────
    # 使用最後 n 筆（已有 >= MIN_HISTORY 筆），逐日 rolling mean
    # SMA_today   = mean(closes[-200:])
    # SMA_20_ago  = mean(closes[-220:-20])
    sma_today  = sum(closes[-200:]) / 200.0
    # 20 交易日前的 SMA_200：往前移 20 格
    idx_20ago  = n - 1 - SLOPE_WINDOW          # = n - 21
    if idx_20ago >= 199:                        # 確保有足夠資料計算舊 SMA
        sma_20ago = sum(closes[idx_20ago - 199 : idx_20ago + 1]) / 200.0
    else:
        sma_20ago = sma_today   # 不夠就平設為今日值 → slope = 0

    slope       = sma_today - sma_20ago
    close_today = float(closes[-1])

    if close_today >= sma_today:
        status = TrendStatus.OK
        note   = (
            f"close={close_today:.4f} >= SMA_200={sma_today:.4f}"
        )
    elif slope > 0:
        status = TrendStatus.CAUTION
        note   = (
            f"close={close_today:.4f} < SMA_200={sma_today:.4f}, "
            f"slope={slope:+.4f} > 0 (SMA rising)"
        )
    else:
        status = TrendStatus.RISK_CAP
        note   = (
            f"close={close_today:.4f} < SMA_200={sma_today:.4f}, "
            f"slope={slope:+.4f} <= 0 (SMA flat/falling)"
        )

    logger.info("[TREND] %s — %s", status.value, note)
    return TrendResult(
        status        = status,
        close         = close_today,
        sma_200       = round(sma_today, 4),
        sma_200_slope = round(slope, 4),
        history_len   = n,
        rationale     = note,
    )


# ── DB Reader ──────────────────────────────────────────────────────────────────

class TrendLayer:
    """
    從 DB 讀取 VOO raw close，計算 Trend 狀態。

    直接讀 raw_market_data（不走 CORE_ASSET_PROXIES VOO→SPY 代理機制），
    確保 Trend Layer 使用 VOO 自身的價格。

    任何 DB 失敗或資料不足 → TREND_WARMUP，不拋例外，不中斷流程。
    """

    def __init__(
        self,
        symbol:      str = VOO_SYMBOL,
        min_history: int = MIN_HISTORY,
    ) -> None:
        self.symbol      = symbol
        self.min_history = min_history
        self._fetch_n    = min_history + FETCH_BUFFER

    def run(self, conn, as_of: date) -> TrendResult:
        """
        讀 VOO close 並回傳 TrendResult。
        任何 DB 失敗 → TREND_WARMUP（不中斷流程）。
        """
        try:
            closes = self._fetch_closes(conn, as_of)
        except Exception as exc:
            logger.warning("[TREND] DB read failed (%s) → TREND_WARMUP", exc)
            return TrendResult(
                status        = TrendStatus.WARMUP,
                close         = None,
                sma_200       = None,
                sma_200_slope = None,
                history_len   = 0,
                rationale     = f"DB read failed: {exc}",
            )

        return compute_trend_status(closes)

    def _fetch_closes(self, conn, as_of: date) -> List[float]:
        """
        取 VOO 最近 self._fetch_n 筆 close（由舊到新）。
        回傳 float list；空 list 代表無資料。
        查詢為 DESC，最後 reversed() 轉為由舊到新。
        """
        query = """
            SELECT rmd.close
            FROM raw_market_data rmd
            JOIN assets a ON a.asset_id = rmd.asset_id
            WHERE a.symbol = %s
              AND rmd.frequency = '1d'
              AND rmd.close IS NOT NULL
              AND rmd.time::date <= %s
            ORDER BY rmd.time DESC
            LIMIT %s
        """
        with conn.cursor() as cur:
            cur.execute(query, (self.symbol, as_of, self._fetch_n))
            rows = cur.fetchall()

        # rows 為 DESC 順序，reverse 轉為由舊到新
        return [float(r[0]) for r in reversed(rows)]
