"""
Market Snapshot
---------------
從 DB 讀取最新可用數值，填入 Snapshot dataclass。
任何欄位缺失一律設為 None，不中斷流程。
confidence_score 依可用關鍵指標數量決定：High / Medium / Low。

Phase 1 新增：
- ism_pmi_date / hy_oas_date：記錄月資料 / 日資料的實際來源日期
- _latest_macro_with_dates()：同時取值與日期
- 載入時呼叫 check_staleness() 記錄新鮮度
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional, Tuple

import pandas as pd
from psycopg2.extensions import connection as PgConnection

from etl.cleaner import (
    MAX_DAILY_STALENESS_DAYS,
    MAX_MONTHLY_STALENESS_DAYS,
    check_staleness,
)

# Z-Score 指標 → DB 中的 asset symbol（用於查詢 derived_indicators）
# "market" 來源直接用 yfinance symbol；"macro" 來源用 indicator_code（合成資產）
ZSCORE_ASSET_MAP: Dict[str, str] = {
    "VIX_Z_252":                "^VIX",
    "HY_OAS_Z_252":             "HY_OAS",               # synthetic asset
    "US_10Y_YIELD_Z_252":       "US_10Y_YIELD",          # synthetic asset
    "US_2Y_YIELD_Z_252":        "US_2Y_YIELD",           # synthetic asset
    "YIELD_SPREAD_10Y2Y_Z_252": "US_YIELD_SPREAD_10Y2Y",
    "ISM_PMI_MFG_Z_60M":        "ISM_PMI_MFG",           # synthetic asset（月資料）
    "DXY_Z_252":                "DX-Y.NYB",
    "OIL_Z_252":                "CL=F",
    "VVIX_Z_252":               "^VVIX",
}

logger = logging.getLogger(__name__)

# 核心資產 proxy（DB 中實際的 symbol）
# PRIMARY_ASSETS → DB proxy symbol（無代理則直接用自身）
CORE_ASSET_PROXIES: Dict[str, str] = {
    "VOO":     "SPY",      # Core：VOO ≈ SPY（同追蹤 S&P 500）
    "QQQM":    "QQQ",      # Tactical：QQQM ≈ QQQ（同追蹤 NASDAQ 100）
    "SMH":     "SOXX",     # Tactical：SMH ≈ SOXX（半導體）
    "2330.TW": "2330.TW",  # Tactical：台積電，直接使用
}


# ── Per-asset data ─────────────────────────────────────────────────────────────

@dataclass
class AssetData:
    symbol:         str
    asset_id:       Optional[int]
    close:          Optional[float]
    sma_5:          Optional[float]
    chg_1w_pct:     Optional[float]   # %
    chg_1m_pct:     Optional[float]   # %


# ── Main snapshot ──────────────────────────────────────────────────────────────

@dataclass
class Snapshot:
    as_of: date

    # Macro（可缺失）
    hy_oas:        Optional[float] = None   # HY OAS %
    ism_pmi:       Optional[float] = None   # ISM PMI (MISSING → None, OK)
    spread_10y2y:  Optional[float] = None   # 10Y-2Y %

    # 資料日期（Phase 1：用於報告標示與新鮮度 log）
    hy_oas_date:   Optional[date] = None    # HY OAS 實際資料日期
    ism_pmi_date:  Optional[date] = None    # ISM PMI 實際資料日期（月資料）

    # Volatility
    vix:           Optional[float] = None
    vix_pct_rank:  Optional[float] = None   # 0.0~1.0，過去 252 日百分位
    vix_mean_20:   Optional[float] = None

    # Per-asset
    assets: Dict[str, AssetData] = field(default_factory=dict)

    # Z-Score 標準化風險座標（Phase 1 新增，只進入報表，不影響 Scenario 判定）
    # key = 指標名（e.g. "VIX_Z_252"），value = 最新 z-score 或 None
    z_scores: Dict[str, Optional[float]] = field(default_factory=dict)

    # ── Confidence ─────────────────────────────────────────────────────────────

    @property
    def confidence_score(self) -> str:
        """
        High   : 所有關鍵指標（含 ISM_PMI）都有值
        Medium : ISM_PMI 缺失，其他關鍵指標OK
        Low    : 多個關鍵指標缺失
        """
        key_indicators = [self.hy_oas, self.vix, self.vix_pct_rank, self.spread_10y2y]
        available = sum(1 for v in key_indicators if v is not None)

        if self.ism_pmi is not None and available == len(key_indicators):
            return "High"
        if available >= 3:
            return "Medium"   # ISM_PMI 或一個指標缺失
        return "Low"

    @property
    def missing_indicators(self) -> List[str]:
        checks = {
            "ISM_PMI_MFG":       self.ism_pmi,
            "HY_OAS":            self.hy_oas,
            "VIX":               self.vix,
            "VIX_PCT_RANK_252":  self.vix_pct_rank,
            "YIELD_SPREAD_10Y2Y":self.spread_10y2y,
        }
        return [k for k, v in checks.items() if v is None]


# ── Loader ─────────────────────────────────────────────────────────────────────

class SnapshotLoader:

    def __init__(self, conn: PgConnection) -> None:
        self.conn = conn

    def load(self, as_of: date) -> Snapshot:
        snap = Snapshot(as_of=as_of)
        self._load_macro(snap, as_of)
        self._load_vix(snap, as_of)
        self._load_vix_derived(snap, as_of)
        self._load_spread(snap, as_of)
        self._load_assets(snap, as_of)
        self._load_zscores(snap, as_of)   # Phase 1：標準化風險座標

        logger.info(
            "Snapshot loaded as_of=%s  confidence=%s  missing=%s  z_scores_loaded=%d",
            as_of,
            snap.confidence_score,
            snap.missing_indicators,
            sum(1 for v in snap.z_scores.values() if v is not None),
        )
        return snap

    # ── Macro ──────────────────────────────────────────────────────────────────

    def _load_macro(self, snap: Snapshot, as_of: date) -> None:
        rows = self._latest_macro_with_dates(
            ["HY_OAS", "ISM_PMI_MFG"], as_of, lookback=None
        )

        # HY OAS（日資料）
        if "HY_OAS" in rows:
            snap.hy_oas, snap.hy_oas_date = rows["HY_OAS"]
            check_staleness(snap.hy_oas_date, as_of, MAX_DAILY_STALENESS_DAYS, "HY_OAS")
        else:
            logger.warning("[N/A]   HY_OAS: 過去 45 天無資料")

        # ISM PMI（月資料）
        if "ISM_PMI_MFG" in rows:
            snap.ism_pmi, snap.ism_pmi_date = rows["ISM_PMI_MFG"]
            check_staleness(
                snap.ism_pmi_date, as_of, MAX_MONTHLY_STALENESS_DAYS, "ISM_PMI_MFG"
            )
            logger.info(
                "[MACRO] ISM_PMI_MFG=%.1f，資料日期=%s",
                snap.ism_pmi, snap.ism_pmi_date,
            )
        else:
            logger.info(
                "[N/A]   ISM_PMI_MFG: 過去 45 天無資料 — regime 將繼續執行（降低 Confidence）"
            )

    # ── VIX price ─────────────────────────────────────────────────────────────

    def _load_vix(self, snap: Snapshot, as_of: date) -> None:
        vix_id = self._asset_id("^VIX")
        if vix_id is None:
            return
        row = self._scalar("""
            SELECT close FROM raw_market_data
            WHERE asset_id = %s AND frequency = '1d' AND time::date <= %s
            ORDER BY time DESC LIMIT 1
        """, (vix_id, as_of))
        snap.vix = float(row) if row is not None else None

    # ── VIX derived indicators ─────────────────────────────────────────────────

    def _load_vix_derived(self, snap: Snapshot, as_of: date) -> None:
        vix_id = self._asset_id("^VIX")
        if vix_id is None:
            return
        for indicator, attr in [
            ("VIX_PCT_RANK_252", "vix_pct_rank"),
            ("VIX_ROLLING_MEAN_20", "vix_mean_20"),
        ]:
            row = self._scalar("""
                SELECT value FROM derived_indicators
                WHERE indicator = %s AND asset_id = %s
                  AND time::date <= %s
                ORDER BY time DESC LIMIT 1
            """, (indicator, vix_id, as_of))
            setattr(snap, attr, float(row) if row is not None else None)

    # ── 10Y-2Y spread ──────────────────────────────────────────────────────────

    def _load_spread(self, snap: Snapshot, as_of: date) -> None:
        spread_id = self._asset_id("US_YIELD_SPREAD_10Y2Y")
        if spread_id is None:
            return
        row = self._scalar("""
            SELECT value FROM derived_indicators
            WHERE indicator = 'YIELD_SPREAD_10Y2Y' AND asset_id = %s
              AND time::date <= %s
            ORDER BY time DESC LIMIT 1
        """, (spread_id, as_of))
        snap.spread_10y2y = float(row) if row is not None else None

    # ── Per-asset prices + derived ─────────────────────────────────────────────

    def _load_assets(self, snap: Snapshot, as_of: date) -> None:
        for target, proxy in CORE_ASSET_PROXIES.items():
            asset_id = self._asset_id(proxy)
            ad = AssetData(
                symbol=target, asset_id=asset_id,
                close=None, sma_5=None, chg_1w_pct=None, chg_1m_pct=None,
            )

            if asset_id is not None:
                # close price
                row = self._scalar("""
                    SELECT close FROM raw_market_data
                    WHERE asset_id = %s AND frequency = '1d' AND time::date <= %s
                    ORDER BY time DESC LIMIT 1
                """, (asset_id, as_of))
                ad.close = float(row) if row is not None else None

                # derived indicators
                for indicator, attr in [
                    ("SMA_5",             "sma_5"),
                    ("PRICE_CHG_PCT_1W",  "chg_1w_pct"),
                    ("PRICE_CHG_PCT_1M",  "chg_1m_pct"),
                ]:
                    row = self._scalar("""
                        SELECT value FROM derived_indicators
                        WHERE indicator = %s AND asset_id = %s
                          AND time::date <= %s
                        ORDER BY time DESC LIMIT 1
                    """, (indicator, asset_id, as_of))
                    setattr(ad, attr, float(row) if row is not None else None)

            snap.assets[target] = ad

    # ── Z-Score（Phase 1）─────────────────────────────────────────────────────

    def _load_zscores(self, snap: Snapshot, as_of: date) -> None:
        """
        載入各宏觀風險指標的最新 rolling z-score（來自 derived_indicators）。

        設計原則：
        - 任何單一 z-score 失敗 → 設為 None，不中斷流程
        - z-score 只進入報表，不影響 Scenario / Signal baseline 邏輯
        - 找不到資產或無資料 → None（debug log），不 warning（可能尚未計算）
        """
        for z_name, symbol in ZSCORE_ASSET_MAP.items():
            try:
                asset_id = self._asset_id(symbol)
                if asset_id is None:
                    snap.z_scores[z_name] = None
                    logger.debug("[ZSCORE_LOAD] %s: symbol %r 不在 DB", z_name, symbol)
                    continue

                val = self._scalar("""
                    SELECT value FROM derived_indicators
                    WHERE indicator = %s AND asset_id = %s
                      AND time::date <= %s
                    ORDER BY time DESC LIMIT 1
                """, (z_name, asset_id, as_of))

                snap.z_scores[z_name] = float(val) if val is not None else None

                if val is None:
                    logger.debug("[ZSCORE_LOAD] %s: 無資料（可能尚未執行 zscore indicator）", z_name)
                else:
                    logger.debug("[ZSCORE_LOAD] %s = %.3f", z_name, float(val))

            except Exception as exc:
                snap.z_scores[z_name] = None
                logger.warning("[ZSCORE_LOAD] %s 載入失敗: %s", z_name, exc)

        loaded = sum(1 for v in snap.z_scores.values() if v is not None)
        total  = len(ZSCORE_ASSET_MAP)
        if loaded < total:
            missing = [k for k, v in snap.z_scores.items() if v is None]
            logger.info(
                "[ZSCORE_LOAD] 載入 %d/%d 個 z-score，缺失：%s",
                loaded, total, missing,
            )

    # ── DB helpers ─────────────────────────────────────────────────────────────

    def _asset_id(self, symbol: str) -> Optional[int]:
        row = self._scalar(
            "SELECT asset_id FROM assets WHERE symbol = %s", (symbol,)
        )
        return int(row) if row is not None else None

    def _latest_macro(
        self, indicators: List[str], as_of: date, lookback: Optional[int] = 45
    ) -> Dict[str, float]:
        """取每個 indicator 最新一筆（過去 lookback 天內）。僅回傳值。"""
        rows = self._latest_macro_with_dates(indicators, as_of, lookback)
        return {k: v for k, (v, _) in rows.items()}

    def _latest_macro_with_dates(
        self, indicators: List[str], as_of: date, lookback: Optional[int] = 45
    ) -> Dict[str, Tuple[float, date]]:
        """
        取每個 indicator 最新一筆（過去 lookback 天內）。
        回傳 {indicator: (value, data_date)}，缺失的 indicator 不在 dict 中。
        """
        try:
            with self.conn.cursor() as cur:
                if lookback is None:
                    cur.execute("""
                        SELECT DISTINCT ON (indicator)
                            indicator, value, time::date
                        FROM macro_data
                        WHERE indicator = ANY(%s)
                          AND value IS NOT NULL
                          AND time::date <= %s
                        ORDER BY indicator, time DESC
                    """, (indicators, as_of))
                else:
                    start = pd.Timestamp(as_of) - pd.Timedelta(days=lookback)
                    cur.execute("""
                        SELECT DISTINCT ON (indicator)
                            indicator, value, time::date
                        FROM macro_data
                        WHERE indicator = ANY(%s)
                          AND value IS NOT NULL
                          AND time >= %s AND time::date <= %s
                        ORDER BY indicator, time DESC
                    """, (indicators, start.to_pydatetime(), as_of))
                result = {}
                for row in cur.fetchall():
                    indicator, value, data_date = row
                    if value is not None:
                        result[indicator] = (float(value), data_date)
                return result
        except Exception as exc:
            logger.error("_latest_macro_with_dates failed: %s", exc)
            self.conn.rollback()
            return {}

    def _scalar(self, sql: str, params: tuple) -> Optional[any]:
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
            return row[0] if row and row[0] is not None else None
        except Exception as exc:
            logger.error("scalar query failed: %s — %s", sql[:60], exc)
            self.conn.rollback()
            return None
