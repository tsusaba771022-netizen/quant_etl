"""
Macro Data Fetcher (FRED via fredapi)
--------------------------------------
1. download_fred_series() : 下載單一 FRED 序列（使用 fredapi，穩定可靠）
2. build_macro_rows()     : 轉換成 macro_data INSERT tuple list

pandas_datareader 對 FRED 的請求常被封鎖並回傳 HTML，
改用官方 fredapi 套件（需在 .env 設定 FRED_API_KEY）。
FRED API Key 免費申請：https://fred.stlouisfed.org/docs/api/api_key.html
"""
import logging
from datetime import date
from typing import Any, Dict, List, Optional

import pandas as pd

from .db import MacroRow
from .cleaner import audit_and_fill
from .sanity import check_series_bounds

logger = logging.getLogger(__name__)


def download_fred_series(
    fred_id: str,
    start: date,
    end: date,
    api_key: str = "",
) -> Optional[pd.Series]:
    """
    透過 fredapi 取得 FRED 序列。
    回傳 UTC-aware DatetimeIndex 的 pd.Series。
    """
    try:
        from fredapi import Fred
    except ImportError:
        logger.error(
            "fredapi 未安裝。請執行：pip install fredapi>=0.5.1"
        )
        return None

    if not api_key:
        logger.error(
            "FRED_API_KEY 未設定。請在 .env 加入：FRED_API_KEY=your_key_here\n"
            "免費申請：https://fred.stlouisfed.org/docs/api/api_key.html"
        )
        return None

    logger.info("FRED download: %s  %s ~ %s", fred_id, start, end)
    try:
        fred   = Fred(api_key=api_key)
        series = fred.get_series(
            fred_id,
            observation_start=start.isoformat(),
            observation_end=end.isoformat(),
        )
    except Exception as exc:
        logger.error("FRED download failed for %s: %s", fred_id, exc)
        return None

    if series is None or series.empty:
        logger.warning("No data returned for FRED series %s", fred_id)
        return None

    # 統一 timezone → UTC（先做，方便後續審計時索引一致）
    if series.index.tz is None:
        series.index = series.index.tz_localize("UTC")
    else:
        series.index = series.index.tz_convert("UTC")

    # 記錄並丟棄 NaN（FRED 有時回傳尾端空行）
    raw_len = len(series)
    has_nan = series.isna().sum()
    if has_nan > 0:
        logger.info(
            "[DROP]  %s: 丟棄 %d 筆 NaN（FRED 空行），剩餘 %d 筆",
            fred_id, has_nan, raw_len - has_nan,
        )
    series = series.dropna()
    if series.empty:
        logger.warning("[EMPTY] %s: dropna 後無資料", fred_id)
        return None

    # ── Sanity Check（物理邊界）──────────────────────────────────────────────
    # 超出合理範圍的值設為 NaN，不寫入 DB；後續 ffill 可補前一筆有效值
    # 同時用 fred_id（原始序列代碼）查詢，作為 fallback
    series, n_sanity_bad = check_series_bounds(series, fred_id)

    # Forward fill 補缺值（假日停刊 + sanity 排除的異常值），並記錄填補狀況
    series, _ = audit_and_fill(series, name=fred_id, method="ffill")

    logger.info("FRED %s: %d rows ready", fred_id, len(series))
    return series


def build_macro_rows(
    series: pd.Series,
    indicator_code: str,
    config: Dict[str, Any],
) -> List[MacroRow]:
    """
    將 FRED Series 轉換為 macro_data INSERT tuple list。

    Tuple 欄位順序與 db.upsert_macro_data() 對應：
    (indicator, time, frequency, value, unit, source, source_code)
    """
    rows: List[MacroRow] = []
    for ts, val in series.items():
        if pd.isna(val):
            continue
        rows.append((
            indicator_code,              # indicator
            ts,                          # time
            config["frequency"],         # frequency
            float(val),                  # value
            config.get("unit"),          # unit
            config.get("source", "fred"),# source
            config.get("fred_id", ""),   # source_code (FRED series ID)
        ))
    return rows
