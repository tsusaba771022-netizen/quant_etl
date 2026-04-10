"""
Sanity Checks — 物理邊界檢查
-----------------------------
在資料寫入 DB 之前，驗證數值是否在合理物理範圍內。
超出範圍的值設為 NaN（不寫入 DB），並記錄 [SANITY_WARN] log。
分析層的 audit_and_fill 可在事後 forward-fill 填補這些空值。

設計原則：
- check_series_bounds()：主要入口，向量化，不使用 for-loop（除了 log 前幾筆）
- 邊界設定故意保守（寬鬆），只擋明顯的資料錯誤，不過濾正常的極端市況
- 找不到名稱對應 → 靜默返回原序列，不中斷 pipeline
- 所有 log 使用 [SANITY_WARN] / [SANITY_OK] tag，便於 grep 搜尋
"""
from __future__ import annotations

import logging
from typing import Dict, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# ── 邊界設定（lo, hi） ──────────────────────────────────────────────────────────
# 原則：寬鬆保守邊界，只擋明顯資料錯誤；極端市況（如 VIX=80）仍應通過
#
# market symbols（yfinance ticker）
# macro indicator codes（cleaner 層）
# FRED series IDs（fetch_macro 層）
SANITY_BOUNDS: Dict[str, Tuple[float, float]] = {
    # ── 波動率 ───────────────────────────────────────────────────────────────
    "^VIX":          (0.5,   200.0),  # VIX：歷史最高 ~90 (2008)；<0.5 為資料異常
    "^VVIX":         (40.0,  400.0),  # VVIX：波動率的波動率，正常 80-140

    # ── 美元與原油 ────────────────────────────────────────────────────────────
    "DX-Y.NYB":      (50.0,  130.0),  # DXY：歷史範圍約 70-165；<50 或 >130 明顯異常
    "CL=F":          (-10.0, 250.0),  # WTI 原油：2020-04 負油價事件，上限 250

    # ── 信用利差（macro indicator codes）────────────────────────────────────
    "HY_OAS":        (0.5,   30.0),   # HY OAS %：<0 或 >30 為資料錯誤
    "BAMLH0A0HYM2EY":(0.5,   30.0),  # 同上（FRED series ID）

    # ── 公債殖利率（macro indicator codes）──────────────────────────────────
    "US_10Y_YIELD":  (-2.0,  25.0),   # 歷史最高 ~16%；負殖利率曾出現但不應低於 -2
    "US_2Y_YIELD":   (-2.0,  25.0),
    "DGS10":         (-2.0,  25.0),   # FRED series IDs
    "DGS2":          (-2.0,  25.0),

    # ── Macro Growth Proxy（Plan B: CFNAI 取代 NAPM）──────────────────────────
    "ISM_PMI_MFG":   (-5.0,   5.0),   # CFNAI 範圍：正常 ±1；極端 ±3~5；超過 ±5 異常
    "CFNAI":         (-5.0,   5.0),   # FRED series ID（Plan B，取代已退役的 NAPM）

    # ── 10Y-2Y Yield Spread（derived）──────────────────────────────────────
    "YIELD_SPREAD_10Y2Y": (-6.0, 10.0),  # 利差範圍；<-6 或 >10 為異常
}


# ── 主要入口 ───────────────────────────────────────────────────────────────────

def check_series_bounds(
    series: pd.Series,
    name: str,
) -> Tuple[pd.Series, int]:
    """
    向量化邊界檢查：超出範圍的值設為 NaN。

    Parameters
    ----------
    series : 要檢查的時間序列
    name   : 指標名稱（用於查詢 SANITY_BOUNDS 和 log）

    Returns
    -------
    (cleaned_series, n_rejected)
    - cleaned_series：NaN 已替換超界值的副本
    - n_rejected：被替換的筆數（0 表示全部正常）
    """
    # 標準化 name（去除前後空白）
    bounds = SANITY_BOUNDS.get(name.strip())
    if bounds is None:
        return series, 0   # 無邊界設定 → 直接通過，不警告

    lo, hi = bounds
    # 只檢查非 NaN 的值
    valid_mask = series.notna()
    bad_mask   = valid_mask & ((series < lo) | (series > hi))
    n_bad      = int(bad_mask.sum())

    if n_bad == 0:
        return series, 0

    # ── 記錄異常 ──────────────────────────────────────────────────────────────
    logger.warning(
        "[SANITY_WARN] %-25s: %d 筆超出合理範圍 [%.2f, %.2f]，已設為 NaN（不寫入 DB）",
        name, n_bad, lo, hi,
    )
    # 列出前 5 筆明細（日期 + 數值）
    for ts, val in series[bad_mask].head(5).items():
        logger.warning("[SANITY_WARN]   %-25s  %s = %.4f", name, ts, val)
    if n_bad > 5:
        logger.warning("[SANITY_WARN]   %-25s  ...（共 %d 筆，只顯示前 5）", name, n_bad)

    # ── 替換為 NaN（不修改原始 Series）───────────────────────────────────────
    cleaned = series.copy()
    cleaned[bad_mask] = float("nan")
    return cleaned, n_bad


def check_dataframe_close(
    df: pd.DataFrame,
    symbol: str,
) -> Tuple[pd.DataFrame, int]:
    """
    針對市場 OHLCV DataFrame 的 close / adj_close 欄位執行邊界檢查。
    若 close 值超界，同步將 adj_close 設為 NaN（避免衍生列計算時使用髒值）。

    Returns
    -------
    (cleaned_df, n_rejected)
    """
    if "close" not in df.columns:
        return df, 0

    cleaned_close, n_bad = check_series_bounds(df["close"], symbol)
    if n_bad == 0:
        return df, 0

    df = df.copy()
    df["close"] = cleaned_close

    # close 超界 → adj_close 也一併 NaN（adj_close = close × adj_factor，同樣無效）
    if "adj_close" in df.columns:
        bad_idx = cleaned_close[cleaned_close.isna() & df["adj_close"].notna()].index
        if len(bad_idx) > 0:
            df.loc[bad_idx, "adj_close"] = float("nan")

    return df, n_bad
