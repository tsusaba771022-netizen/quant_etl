"""
Rolling Z-Score Indicators
---------------------------
針對宏觀風險指標計算 rolling z-score，
讓不同單位的指標可放在同一標準化座標系中比較。

輸出指標（寫入 derived_indicators）：
  VIX_Z_252                 — VIX 波動率（日資料，window=252d）
  HY_OAS_Z_252              — 高收益信用利差（日資料，window=252d）
  US_10Y_YIELD_Z_252        — 美國 10 年期公債殖利率（日資料，window=252d）
  US_2Y_YIELD_Z_252         — 美國 2 年期公債殖利率（日資料，window=252d）
  YIELD_SPREAD_10Y2Y_Z_252  — 10Y-2Y 利差（日資料，window=252d）
  ISM_PMI_MFG_Z_60M         — ISM PMI 製造業（月資料，window=60M，約 5 年）
  DXY_Z_252                 — 美元指數（optional）
  OIL_Z_252                 — 原油（optional）
  VVIX_Z_252                — 波動率的波動率（optional）

設計原則：
- 向量化計算，不使用逐列 for-loop
- 分析層 forward fill（audit_and_fill）；原始資料不受影響
- optional 指標不在 DB → 靜默跳過，不中斷 pipeline
- rolling std ≈ 0 / 資料不足 → NaN，不寫入 DB
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd
from psycopg2.extensions import connection as PgConnection

from .base import BaseIndicator, IndicatorRow
from .loader import (
    ensure_synthetic_asset,
    fetch_close_prices,
    fetch_derived_indicators,
    fetch_macro_series,
    get_asset_id,
)
from etl.cleaner import (
    audit_and_fill,
    compute_rolling_zscore,
    summarize_missingness,
)

logger = logging.getLogger(__name__)

# ── 常數 ──────────────────────────────────────────────────────────────────────

WINDOW: int = 252   # rolling window（交易日）

# Z-Score 指標設定表
# source 類型：
#   "market"     → raw_market_data（yfinance）
#   "macro"      → macro_data（FRED）
#   "derived"    → derived_indicators（已計算的衍生指標）
#   "macro_diff" → macro_data 兩欄相減（例如 10Y - 2Y 利差）
#                  欄位：indicator_a, indicator_b, symbol（合成資產 symbol）
#
# optional = True → 資料不存在時靜默 skip，不算錯誤
ZSCORE_TARGETS: Dict[str, Dict] = {
    "VIX_Z_252": {
        "source":      "market",
        "symbol":      "^VIX",
        "unit":        "z-score",
        "optional":    False,
        "label_zh":    "VIX（波動率）",
        "risk_dir":    "up",        # z 越高 → 風險越高
    },
    "HY_OAS_Z_252": {
        "source":      "macro",
        "indicator":   "HY_OAS",
        "synth_name":  "ICE BofA US HY OAS",
        "unit":        "z-score",
        "optional":    False,
        "label_zh":    "HY OAS（信用利差）",
        "risk_dir":    "up",
    },
    "US_10Y_YIELD_Z_252": {
        "source":      "macro",
        "indicator":   "US_10Y_YIELD",
        "synth_name":  "US 10Y Treasury Yield",
        "unit":        "z-score",
        "optional":    False,
        "label_zh":    "10 年期公債殖利率",
        "risk_dir":    "neutral",   # 高低各有含義
    },
    "US_2Y_YIELD_Z_252": {
        "source":      "macro",
        "indicator":   "US_2Y_YIELD",
        "synth_name":  "US 2Y Treasury Yield",
        "unit":        "z-score",
        "optional":    False,
        "label_zh":    "2 年期公債殖利率",
        "risk_dir":    "neutral",
    },
    "YIELD_SPREAD_10Y2Y_Z_252": {
        # 直接從 macro_data 計算 10Y - 2Y，不依賴 spread indicator 是否已跑
        # 這樣不管 run_spread() 是否在本次執行，z-score 都能獨立完成
        "source":        "macro_diff",
        "indicator_a":   "US_10Y_YIELD",
        "indicator_b":   "US_2Y_YIELD",
        "symbol":        "US_YIELD_SPREAD_10Y2Y",  # 合成資產（已存或自動建立）
        "synth_name":    "US 10Y-2Y Yield Spread (Z-Score Input)",
        "unit":          "z-score",
        "optional":      False,
        "label_zh":      "10Y-2Y 利差",
        "risk_dir":      "down",   # z 越低 → 越接近倒掛，風險越高
    },
    "DXY_Z_252": {
        "source":      "market",
        "symbol":      "DX-Y.NYB",
        "unit":        "z-score",
        "optional":    True,        # 尚未接入資料層時靜默跳過
        "label_zh":    "美元指數（DXY）",
        "risk_dir":    "neutral",
    },
    "OIL_Z_252": {
        "source":      "market",
        "symbol":      "CL=F",
        "unit":        "z-score",
        "optional":    True,
        "label_zh":    "原油價格（WTI）",
        "risk_dir":    "neutral",
    },
    "VVIX_Z_252": {
        "source":      "market",
        "symbol":      "^VVIX",
        "unit":        "z-score",
        "optional":    True,
        "label_zh":    "VVIX（波動率的波動率）",
        "risk_dir":    "up",
    },
    # ── 月資料 z-score（用於 Regime Matrix Growth 軸）──────────────────────────
    "ISM_PMI_MFG_Z_60M": {
        "source":      "macro",
        "indicator":   "ISM_PMI_MFG",
        "synth_name":  "CFNAI (Chicago Fed National Activity Index)",
        "window":      60,       # 60 個月觀測值（≈ 5 年）
        "min_periods": 12,       # 最少 1 年月資料才輸出
        "frequency":   "monthly",
        "unit":        "z-score",
        "optional":    False,
        "label_zh":    "ISM PMI（製造業，60M）",
        "window_label":"60M",    # 顯示於報告表頭
        "risk_dir":    "down",   # z 越低 → 成長越疲弱 → 風險越高
    },
}

# snapshot / report 顯示順序
ZSCORE_DISPLAY_ORDER: List[str] = [
    "VIX_Z_252",
    "HY_OAS_Z_252",
    "YIELD_SPREAD_10Y2Y_Z_252",
    "US_10Y_YIELD_Z_252",
    "US_2Y_YIELD_Z_252",
    "ISM_PMI_MFG_Z_60M",
    "DXY_Z_252",
    "OIL_Z_252",
    "VVIX_Z_252",
]


# ── 主計算函式 ──────────────────────────────────────────────────────────────────

def compute_all_zscores(
    conn: PgConnection,
    buf_start: pd.Timestamp,
    end_ts: pd.Timestamp,
    output_start: pd.Timestamp,
) -> List[IndicatorRow]:
    """
    計算所有宏觀風險指標的 rolling z-score。

    Parameters
    ----------
    conn         : DB connection
    buf_start    : 資料載入起始（含 252 日 buffer）
    end_ts       : 資料載入結束
    output_start : 輸出起始（裁剪 buffer 期，僅保留此日期之後的結果）

    Returns
    -------
    List[IndicatorRow] — 可直接傳入 upsert_indicators()
    """
    all_rows: List[IndicatorRow] = []

    for z_name, cfg in ZSCORE_TARGETS.items():
        optional = cfg.get("optional", False)
        try:
            rows = _compute_one(conn, z_name, cfg, buf_start, end_ts, output_start)
            all_rows.extend(rows)
            logger.info("[ZSCORE] %-35s: %d rows", z_name, len(rows))
        except _SkipIndicator as e:
            # optional 指標未接入 → info；required 指標缺資料 → warning
            log_fn = logger.info if optional else logger.warning
            log_fn("[SKIP]  %s: %s", z_name, e)
        except Exception:
            if optional:
                logger.info("[SKIP]  %s (optional): 未預期錯誤，略過", z_name)
            else:
                logger.exception("[ERROR] %s: 計算失敗，略過並記錄錯誤", z_name)

    return all_rows


# ── 單一指標計算 ────────────────────────────────────────────────────────────────

class _SkipIndicator(Exception):
    """內部信號：此指標應 skip（資料不存在或不完整），非系統錯誤。"""


def _compute_one(
    conn: PgConnection,
    z_name: str,
    cfg: Dict,
    buf_start: pd.Timestamp,
    end_ts: pd.Timestamp,
    output_start: pd.Timestamp,
) -> List[IndicatorRow]:
    """
    計算單一 z-score 指標的完整流程：
    1. 依來源類型 fetch 原始時間序列 + 解析 write_asset_id
    2. 記錄並統計缺值（summarize_missingness）
    3. 分析層 forward fill（audit_and_fill，不修改 raw DB 資料）
    4. 計算 rolling z-score（compute_rolling_zscore，全向量化）
    5. 裁剪 buffer 期間，轉為 IndicatorRow list（跳過 NaN）

    支援 per-target 設定：
    - window      : 預設 WINDOW(252)，月資料可設 60
    - min_periods : 預設 = window，月資料可設較小值（12）
    - frequency   : 預設 "1d"，月資料設 "monthly"
    """
    source      = cfg["source"]
    window      = cfg.get("window",      WINDOW)
    min_periods = cfg.get("min_periods", window)
    frequency   = cfg.get("frequency",  "1d")

    # ── Step 1: 計算 per-indicator 有效 buf_start ─────────────────────────────
    # 月資料需要自行計算更長的 buffer（window 個月 + 1 年）
    if frequency == "monthly":
        effective_buf = output_start - pd.DateOffset(months=window + 14)
    else:
        effective_buf = buf_start   # 使用呼叫端提供的全域 buffer

    # ── Step 2: Fetch + resolve write_asset_id ────────────────────────────────
    if source == "market":
        raw_series, write_asset_id = _fetch_market(conn, cfg, effective_buf, end_ts)
    elif source == "macro":
        raw_series, write_asset_id = _fetch_macro(conn, cfg, effective_buf, end_ts)
    elif source == "macro_diff":
        raw_series, write_asset_id = _fetch_macro_diff(conn, cfg, effective_buf, end_ts)
    elif source == "derived":
        raw_series, write_asset_id = _fetch_derived(conn, cfg, effective_buf, end_ts)
    else:
        raise ValueError(f"Unknown source type: {source!r}")

    if raw_series.empty:
        raise _SkipIndicator(f"原始資料為空（source={source}）")

    # ── Step 3: 缺值統計（log，不修改資料）───────────────────────────────────
    # 取來源指標名稱作為 log label
    src_name = cfg.get("indicator", cfg.get("symbol", z_name))
    summarize_missingness(raw_series, name=src_name)

    # ── Step 4: 分析層 forward fill ───────────────────────────────────────────
    # 原則：只在計算層做 ffill，不回寫到 raw_market_data / macro_data
    filled_series, _ = audit_and_fill(raw_series, name=src_name)

    if filled_series.empty or filled_series.notna().sum() == 0:
        raise _SkipIndicator("ffill 後仍無有效資料")

    # ── Step 5: Rolling z-score（向量化，使用 per-target window）────────────
    z_series = compute_rolling_zscore(
        filled_series,
        window=window,
        min_periods=min_periods,
        name=z_name,
    )

    # ── Step 6: 裁剪 buffer，轉為 IndicatorRow（跳過 NaN）────────────────────
    z_trimmed = z_series[z_series.index >= output_start]
    if z_trimmed.empty:
        raise _SkipIndicator(f"裁剪 buffer 後無資料（output_start={output_start.date()}）")

    rows = BaseIndicator._series_to_rows(
        z_trimmed,
        indicator=z_name,
        asset_id=write_asset_id,
        frequency=frequency,      # 月資料用 "monthly"，日資料用 "1d"
        unit=cfg.get("unit", "z-score"),
    )
    return rows


# ── Source-specific fetch helpers ─────────────────────────────────────────────

def _fetch_macro_diff(
    conn: PgConnection,
    cfg: Dict,
    buf_start: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> Tuple[pd.Series, int]:
    """
    從 macro_data 取兩個指標序列，計算 (indicator_a - indicator_b) 差值。

    用途：YIELD_SPREAD_10Y2Y = US_10Y_YIELD - US_2Y_YIELD。
    直接從 macro_data 計算，不依賴 derived_indicators 中的 spread 是否已存在，
    避免 spread indicator 尚未跑過時造成的循環依賴問題。

    缺值處理：
    - 各欄位分別 dropna，取兩欄共同非空的交集時間點
    - 差值計算後仍可能有 NaN（分析層 audit_and_fill 會再 ffill）
    """
    ind_a  = cfg["indicator_a"]   # "US_10Y_YIELD"
    ind_b  = cfg["indicator_b"]   # "US_2Y_YIELD"
    symbol = cfg["symbol"]        # "US_YIELD_SPREAD_10Y2Y"

    macro_df = fetch_macro_series(conn, [ind_a, ind_b], buf_start, end_ts)

    if macro_df.empty:
        raise _SkipIndicator(
            f"macro_diff [{ind_a} - {ind_b}]：macro_data 無資料"
            f"（請先執行 ETL 確認 FRED 資料已入庫）"
        )

    missing = [c for c in (ind_a, ind_b) if c not in macro_df.columns]
    if missing:
        raise _SkipIndicator(
            f"macro_diff：{missing} 欄位不存在於 macro_data"
            f"（請先執行 ETL）"
        )

    diff_series = (macro_df[ind_a] - macro_df[ind_b]).dropna()
    if diff_series.empty:
        raise _SkipIndicator(
            f"macro_diff [{ind_a} - {ind_b}]：差值計算後無有效資料"
        )

    diff_series.name = f"{ind_a}_minus_{ind_b}"
    logger.info(
        "[macro_diff] %s - %s：%d 筆有效資料（%s → %s）",
        ind_a, ind_b, len(diff_series),
        diff_series.index.min().date(), diff_series.index.max().date(),
    )

    # 確保合成資產存在（derived_indicators 需要 FK）
    asset_id = ensure_synthetic_asset(
        conn,
        symbol     = symbol,
        name       = cfg.get("synth_name", symbol),
        asset_type = "Index",
        exchange   = "SYNTHETIC",
    )
    conn.commit()   # FK 必須先 commit

    return diff_series, asset_id


def _fetch_market(
    conn: PgConnection,
    cfg: Dict,
    buf_start: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> Tuple[pd.Series, int]:
    """從 raw_market_data 取收盤價序列。"""
    symbol   = cfg["symbol"]
    asset_id = get_asset_id(conn, symbol)
    if asset_id is None:
        raise _SkipIndicator(f"Symbol {symbol!r} 不在 assets 表（尚未執行 ETL）")

    close_df = fetch_close_prices(conn, [asset_id], buf_start, end_ts)
    if close_df.empty or asset_id not in close_df.columns:
        raise _SkipIndicator(f"Symbol {symbol!r} 在 raw_market_data 無收盤價資料")

    series      = close_df[asset_id]
    series.name = symbol
    return series, asset_id


def _fetch_macro(
    conn: PgConnection,
    cfg: Dict,
    buf_start: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> Tuple[pd.Series, int]:
    """
    從 macro_data 取指標序列，並確保對應合成資產存在。
    合成資產以 indicator_code 為 symbol（e.g. "HY_OAS"）。
    """
    indicator = cfg["indicator"]
    macro_df  = fetch_macro_series(conn, [indicator], buf_start, end_ts)
    if macro_df.empty or indicator not in macro_df.columns:
        raise _SkipIndicator(
            f"Macro indicator {indicator!r} 無資料（請執行 ETL 確認 FRED 資料已入庫）"
        )

    series      = macro_df[indicator]
    series.name = indicator

    # 建立合成資產（derived_indicators 的 asset_id FK 需要對應資產）
    asset_id = ensure_synthetic_asset(
        conn,
        symbol    = indicator,
        name      = cfg.get("synth_name", indicator),
        asset_type= "Index",
        exchange  = "SYNTHETIC",
    )
    conn.commit()   # 合成資產需先 commit，才能當 FK 使用
    return series, asset_id


def _fetch_derived(
    conn: PgConnection,
    cfg: Dict,
    buf_start: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> Tuple[pd.Series, int]:
    """
    從 derived_indicators 取已計算的衍生指標序列。
    例如 YIELD_SPREAD_10Y2Y（10Y-2Y 利差）。
    """
    indicator = cfg["indicator"]
    symbol    = cfg["symbol"]      # 合成資產的 symbol

    asset_id = get_asset_id(conn, symbol)
    if asset_id is None:
        raise _SkipIndicator(
            f"Synthetic asset {symbol!r} 不存在"
            f"（請先執行 'spread' indicator 再執行 zscore）"
        )

    series = fetch_derived_indicators(conn, indicator, asset_id, buf_start, end_ts)
    if series.empty:
        raise _SkipIndicator(
            f"Derived indicator {indicator!r} 無資料"
            f"（請先執行 'spread' indicator）"
        )

    return series, asset_id
