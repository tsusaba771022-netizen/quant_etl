"""
統一資料清洗與缺值處理工具
--------------------------
集中管理所有缺值填補邏輯，避免散落在各模組。

三大功能：
1. audit_and_fill()         — 審計缺值 → ffill → 記錄哪些欄位被填補、哪些仍為 N/A
2. align_monthly_to_daily() — 月資料以 merge_asof 對齊到日交易日索引
3. check_staleness()        — 檢查資料距今天數，超閾值發出 warning

設計原則：
- raw 資料表保留原始值（ETL 入庫前盡量不 fill OHLC 等核心欄位）
- 分析層（engine/snapshot 讀取後）可使用 ffill
- 所有 fill 動作均 log：哪些欄位填了幾個值、最終仍有幾個 N/A
- 向量化操作，不用 for 迴圈逐筆處理
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Dict, Optional, Tuple, Union

import pandas as pd

logger = logging.getLogger(__name__)

MIN_ZSCORE_DENOMINATOR: float = 0.01

# ── 閾值設定（可由呼叫端覆寫） ──────────────────────────────────────────────

# 月資料（CFNAI 等月頻資料）：超過此天數視為過舊，降低 Confidence
# CFNAI 正常發布週期最長 ≈ 55 天（含假期延遲），60 天為真正異常門檻
MAX_MONTHLY_STALENESS_DAYS: int = 60
# 日資料（HY OAS、VIX 等）：超過此天數視為過舊（例如長假期）
MAX_DAILY_STALENESS_DAYS: int = 5
# 衍生指標（rolling stats）：超過此天數視為過舊
MAX_INDICATOR_STALENESS_DAYS: int = 7


# ── 核心工具：缺值審計 + 填補 ─────────────────────────────────────────────────

def audit_and_fill(
    data: Union[pd.DataFrame, pd.Series],
    name: str,
    method: str = "ffill",
    limit: Optional[int] = None,
) -> Tuple[Union[pd.DataFrame, pd.Series], Dict]:
    """
    審計缺值 → 執行填補 → 回傳結果與審計報告。

    Parameters
    ----------
    data   : DataFrame 或 Series（原始資料，不會被 in-place 修改）
    name   : 用於 log 的資料名稱（e.g. "SPY", "HY_OAS"）
    method : 填補方法（"ffill" 或 "bfill"）
    limit  : 最多連續填補幾格（None = 不限制）

    Returns
    -------
    filled_data : 填補後的資料（新物件）
    report      : dict，含以下 key：
                    name, total_rows,
                    missing_before  : {col: int} 或 int,
                    filled          : {col: int} 或 int,
                    still_missing   : {col: int} 或 int
    """
    is_series = isinstance(data, pd.Series)

    # 統一轉為 DataFrame 操作，最後再轉回 Series
    if is_series:
        df = data.to_frame(name=str(data.name or name))
    else:
        df = data.copy()

    total_rows = len(df)

    # 填補前缺值統計（向量化）
    missing_before: Dict[str, int] = df.isnull().sum().to_dict()

    # 執行填補
    if method == "ffill":
        filled_df = df.ffill(limit=limit)
    elif method == "bfill":
        filled_df = df.bfill(limit=limit)
    else:
        raise ValueError(f"Unsupported fill method: {method!r}. Use 'ffill' or 'bfill'.")

    # 填補後缺值統計
    missing_after: Dict[str, int] = filled_df.isnull().sum().to_dict()

    # 計算實際填補數（向量化差值）
    fill_counts: Dict[str, int] = {
        col: missing_before[col] - missing_after[col]
        for col in df.columns
    }

    report = {
        "name":          name,
        "total_rows":    total_rows,
        "missing_before": missing_before,
        "filled":         fill_counts,
        "still_missing":  missing_after,
    }

    _log_fill_report(report)

    if is_series:
        result_series = filled_df.iloc[:, 0]
        result_series.name = data.name
        return result_series, report

    return filled_df, report


def _log_fill_report(report: Dict) -> None:
    """將 audit 報告逐欄輸出到 logger。"""
    name       = report["name"]
    total      = report["total_rows"]
    before     = report["missing_before"]
    filled     = report["filled"]
    still_miss = report["still_missing"]

    total_missing_before = sum(before.values())
    total_filled         = sum(filled.values())
    total_still_missing  = sum(still_miss.values())

    if total_missing_before == 0:
        logger.debug("[CLEAN] %s: 無缺值（rows=%d）", name, total)
        return

    logger.info(
        "[FILL]  %s: rows=%d  缺值=%d  已填補=%d  仍缺失=%d",
        name, total, total_missing_before, total_filled, total_still_missing,
    )

    # 逐欄詳細輸出
    for col, n_before in before.items():
        if n_before == 0:
            continue
        n_filled = filled.get(col, 0)
        n_after  = still_miss.get(col, 0)
        col_tag  = col if col != name else ""  # Series 只有一欄時不重複標名

        if n_after > 0:
            logger.warning(
                "[N/A]   %s%s: ffill 補了 %d 筆，仍有 %d 筆無法填補（資料真缺口）",
                name, f".{col_tag}" if col_tag else "", n_filled, n_after,
            )
        else:
            logger.info(
                "[FFIL]  %s%s: ffill 補齊 %d 筆缺值",
                name, f".{col_tag}" if col_tag else "", n_filled,
            )


# ── 月資料對齊日索引 ──────────────────────────────────────────────────────────

def align_monthly_to_daily(
    monthly: pd.Series,
    daily_index: pd.DatetimeIndex,
    name: str = "",
    max_staleness_days: int = MAX_MONTHLY_STALENESS_DAYS,
) -> pd.Series:
    """
    將月頻率資料（e.g. ISM PMI）以 merge_asof 對齊到日交易日索引。

    每個交易日取「≤ 該日」的最近一期月資料（backward fill）。
    若最近一期距 daily_index 末尾超過 max_staleness_days → warning。

    Parameters
    ----------
    monthly            : 月資料 Series，任意 DatetimeIndex（需 sorted）
    daily_index        : 交易日 DatetimeIndex（主索引）
    name               : 用於 log 的指標名稱
    max_staleness_days : 超過此天數發 warning

    Returns
    -------
    aligned : pd.Series，index = daily_index，未能對齊則為 NaN
    """
    if monthly.empty:
        logger.warning("[ALIGN] %s: 月資料為空，全部對齊為 NaN", name)
        return pd.Series(index=daily_index, dtype=float, name=name)

    # 建立工作 DataFrame（不修改原始）
    # reset_index() 產生 2 欄：索引欄 + 值欄，統一重命名為 time/value
    monthly_df = monthly.reset_index()
    monthly_df.columns = ["time", "value"]
    monthly_df["time"] = pd.to_datetime(monthly_df["time"], utc=True)
    monthly_df = monthly_df.sort_values("time").reset_index(drop=True)

    daily_df = pd.DataFrame(
        {"time": pd.to_datetime(daily_index, utc=True)}
    ).sort_values("time").reset_index(drop=True)

    # merge_asof：每個交易日取最近一期（≤ 當日）的月資料
    merged = pd.merge_asof(
        daily_df,
        monthly_df[["time", "value"]],
        on="time",
        direction="backward",
    )

    result = pd.Series(
        merged["value"].values,
        index=daily_index,
        name=name,
    )

    # 新鮮度記錄
    latest_ts  = monthly_df["time"].max()
    latest_val = monthly_df.loc[monthly_df["time"] == latest_ts, "value"].iloc[0]
    latest_date = latest_ts.date() if hasattr(latest_ts, "date") else latest_ts

    if len(daily_index) > 0:
        as_of_ts   = pd.to_datetime(daily_index.max(), utc=True)
        as_of_date = as_of_ts.date()
        staleness  = (as_of_date - latest_date).days

        logger.info(
            "[ALIGN] %s: 最近一期=%s（值=%.2f），對齊至 %s，距今 %d 天",
            name, latest_date, latest_val, as_of_date, staleness,
        )

        if staleness > max_staleness_days:
            logger.warning(
                "[STALE] %s: 月資料距今 %d 天 > 閾值 %d 天，Confidence 可能受影響",
                name, staleness, max_staleness_days,
            )

    n_na = result.isna().sum()
    if n_na > 0:
        logger.warning("[N/A]   %s: align 後仍有 %d 筆 NaN（daily_index 早於所有月資料）", name, n_na)

    return result


# ── 新鮮度檢查 ────────────────────────────────────────────────────────────────

def align_to_trading_calendar(
    series: pd.Series,
    ref_index: pd.DatetimeIndex,
    name: str = "",
    max_gap: int = 5,
) -> pd.Series:
    """
    將 Series reindex 到參考交易日索引，並 forward fill 補假日空缺。

    用途：讓不同來源（FRED vs yfinance）的資料對齊到同一日曆，
    例如 FRED 週五資料填補到 yfinance 的週一。

    Parameters
    ----------
    series    : 原始 Series（任意 DatetimeIndex）
    ref_index : 目標交易日索引（主日曆）
    name      : 用於 log
    max_gap   : 最多連續填補幾個交易日（防止長假後過度延伸）

    Returns
    -------
    aligned : pd.Series，index = ref_index
    """
    reindexed   = series.reindex(ref_index)
    n_before    = int(reindexed.isna().sum())
    filled      = reindexed.ffill(limit=max_gap)
    n_after     = int(filled.isna().sum())
    n_filled    = n_before - n_after
    label       = name or str(series.name or "series")

    if n_filled > 0 or n_after > 0:
        logger.info(
            "[ALIGN_CAL] %s: reindex 後缺 %d 筆，ffill(max_gap=%d) 補 %d 筆，仍 NaN=%d",
            label, n_before, max_gap, n_filled, n_after,
        )
    else:
        logger.debug("[ALIGN_CAL] %s: 無缺值，對齊完成（rows=%d）", label, len(filled))

    return filled


def safe_forward_fill(
    data: Union[pd.DataFrame, pd.Series],
    name: str = "",
    max_gap: Optional[int] = None,
) -> Union[pd.DataFrame, pd.Series]:
    """
    帶 log 的 forward fill（audit_and_fill 的語意簡化版）。

    適用場景：只需填補，不需要 audit report dict 的呼叫端。

    Parameters
    ----------
    data    : 原始 DataFrame 或 Series
    name    : 用於 log
    max_gap : 最多連續填補幾格（None = 不限制）

    Returns
    -------
    填補後的資料（新物件）
    """
    filled, _ = audit_and_fill(data, name=name, method="ffill", limit=max_gap)
    return filled


def compute_rolling_zscore(
    series: pd.Series,
    window: int = 252,
    min_periods: Optional[int] = None,
    name: str = "",
) -> pd.Series:
    """
    計算 rolling z-score（全向量化，不用逐列 for-loop）。

    z = (x - rolling_mean) / rolling_std

    邊界處理：
    - rolling_std ≈ 0  → NaN（常數序列，z-score 無意義）
    - 資料 < min_periods → NaN（buffer 期間，正常現象）
    - 原始缺值（NaN）→ 傳播為 NaN（不插補，呼叫端應先 ffill）

    Parameters
    ----------
    series      : 輸入時間序列（建議已完成分析層 ffill）
    window      : rolling window（預設 252 交易日）
    min_periods : 最少有效觀測數（None → 等於 window）
    name        : 用於 log

    Returns
    -------
    z : pd.Series，index 同輸入，NaN = buffer 期或資料不足
    """
    if min_periods is None:
        min_periods = window

    label = name or str(series.name or "series")

    rolling_mean = series.rolling(window=window, min_periods=min_periods).mean()
    rolling_std  = series.rolling(window=window, min_periods=min_periods).std(ddof=1)

    # rolling_std ≈ 0（閾值 1e-9 防止浮點誤差）→ mask 為 NaN
    safe_denominator = rolling_std.clip(lower=MIN_ZSCORE_DENOMINATOR)
    z = (series - rolling_mean) / safe_denominator

    n_total = len(z)
    n_valid = int(z.notna().sum())
    n_nan   = n_total - n_valid

    if n_nan > 0:
        logger.info(
            "[ZSCORE] %s: window=%d  有效=%d/%d  NaN=%d（buffer 期或資料不足，正常）",
            label, window, n_valid, n_total, n_nan,
        )
    else:
        logger.debug(
            "[ZSCORE] %s: window=%d  全部 %d 筆有效",
            label, window, n_valid,
        )

    return z


def summarize_missingness(
    data: Union[pd.DataFrame, pd.Series],
    name: str = "",
) -> Dict[str, int]:
    """
    統計缺值比例並輸出 log，不修改 data。

    Returns
    -------
    dict: {col_name: n_missing}
    """
    label = name or (str(data.name) if isinstance(data, pd.Series) else "")
    total = len(data)

    if isinstance(data, pd.Series):
        n_miss = int(data.isna().sum())
        pct    = n_miss / total * 100 if total > 0 else 0.0
        result = {str(data.name or label): n_miss}
        if n_miss > 0:
            logger.info(
                "[MISSINGNESS] %s: %d/%d NaN (%.1f%%)",
                label, n_miss, total, pct,
            )
        else:
            logger.debug("[MISSINGNESS] %s: 無缺值（rows=%d）", label, total)
        return result

    # DataFrame
    missing_per_col = data.isnull().sum()
    result = {str(c): int(v) for c, v in missing_per_col.items()}
    any_missing = {k: v for k, v in result.items() if v > 0}

    if any_missing:
        logger.info("[MISSINGNESS] %s: %s", label, any_missing)
    else:
        logger.debug(
            "[MISSINGNESS] %s: 無缺值（rows=%d, cols=%d）",
            label, total, len(data.columns),
        )
    return result


def check_staleness(
    data_date: Optional[date],
    as_of: date,
    max_days: int,
    name: str,
) -> bool:
    """
    檢查資料是否過舊。

    Parameters
    ----------
    data_date : 資料的實際日期（None = 無資料，直接回傳 False）
    as_of     : 分析基準日
    max_days  : 最大容忍天數
    name      : 用於 log 的指標名稱

    Returns
    -------
    True  = 資料新鮮（在 max_days 內）
    False = 過舊或無資料
    """
    if data_date is None:
        logger.warning("[STALE] %s: 無資料日期，無法判斷新鮮度", name)
        return False

    staleness = (as_of - data_date).days

    if staleness > max_days:
        logger.warning(
            "[STALE] %s: 資料日期=%s，距今 %d 天 > 閾值 %d 天",
            name, data_date, staleness, max_days,
        )
        return False

    logger.debug(
        "[FRESH] %s: 資料日期=%s，距今 %d 天（OK，閾值=%d）",
        name, data_date, staleness, max_days,
    )
    return True
