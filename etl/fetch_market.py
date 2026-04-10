"""
Market Data Fetcher
-------------------
1. download_ohlcv()   : yfinance 下載原始資料
2. build_market_rows(): 轉換成 raw_market_data 的 INSERT tuple list
"""
import logging
from datetime import date, timedelta
from typing import Dict, List, Optional

import pandas as pd
import yfinance as yf

from .db import MarketRow
from .cleaner import audit_and_fill
from .sanity import check_dataframe_close

logger = logging.getLogger(__name__)

# yfinance end date 為 exclusive，需加一天
_YF_END_OFFSET = timedelta(days=1)


def download_ohlcv(
    symbols: List[str],
    start: date,
    end: date,
) -> Dict[str, pd.DataFrame]:
    """
    下載多檔每日 OHLCV。
    回傳 {symbol: DataFrame}，columns 統一為：
        open, high, low, close, adj_close, volume
    index 為 UTC-aware DatetimeIndex。
    """
    end_yf = end + _YF_END_OFFSET

    logger.info("yfinance download: %s  %s ~ %s", symbols, start, end)
    raw = yf.download(
        tickers=symbols,
        start=start.isoformat(),
        end=end_yf.isoformat(),
        auto_adjust=False,
        group_by="ticker",
        threads=True,
        progress=False,
    )

    result: Dict[str, pd.DataFrame] = {}

    if len(symbols) == 1:
        df = _normalize(raw, symbols[0])
        if df is not None:
            result[symbols[0]] = df
    else:
        for sym in symbols:
            if sym not in raw.columns.get_level_values(0):
                logger.warning("Symbol %s missing from download result", sym)
                continue
            df = _normalize(raw[sym], sym)
            if df is not None:
                result[sym] = df

    return result


def _normalize(df: pd.DataFrame, symbol: str) -> Optional[pd.DataFrame]:
    """
    統一欄位名稱、去除全空行、補 forward-fill、標準化 timezone → UTC。
    """
    if df is None or df.empty:
        logger.warning("Empty DataFrame for %s", symbol)
        return None

    # yfinance 欄位名稱（不同版本可能有差異）
    col_map = {
        "Open":      "open",
        "High":      "high",
        "Low":       "low",
        "Close":     "close",
        "Adj Close": "adj_close",
        "Volume":    "volume",
    }
    df = df.rename(columns=col_map)

    # 只保留需要的欄位
    keep = [c for c in col_map.values() if c in df.columns]
    df = df[keep].copy()

    # 補齊缺少的欄位（e.g. ^VIX 無 volume）
    for col in ("open", "high", "low", "close", "adj_close", "volume"):
        if col not in df.columns:
            df[col] = None

    # 丟棄 OHLC 全為 NaN 的列（假日/停市）
    df = df.dropna(subset=["open", "high", "low", "close"], how="all")
    if df.empty:
        return None

    # ── Sanity Check（物理邊界，寫入 DB 前）─────────────────────────────────
    # 超出合理範圍的 close / adj_close 設為 NaN，不寫入 DB；
    # 後續 audit_and_fill 可用前一筆有效值 forward-fill
    df, n_sanity_bad = check_dataframe_close(df, symbol)
    if n_sanity_bad > 0:
        logger.warning(
            "[SANITY_WARN] %s: %d 筆 close 超界，已設 NaN（不寫入 DB）",
            symbol, n_sanity_bad,
        )

    # 記錄 OHLC 剩餘缺值（不填補；停市日已由 dropna 移除）
    ohlc_missing = df[["open", "high", "low", "close"]].isnull().sum()
    ohlc_missing = ohlc_missing[ohlc_missing > 0]
    if not ohlc_missing.empty:
        logger.warning(
            "[MISSING] %s OHLC 仍有缺值（不填補）: %s",
            symbol, ohlc_missing.to_dict(),
        )

    # Forward fill（僅針對非 OHLC 欄位，避免誤填停市價格），並記錄填補狀況
    filled_sub, _ = audit_and_fill(
        df[["adj_close", "volume"]], name=symbol, method="ffill"
    )
    df[["adj_close", "volume"]] = filled_sub

    # 統一 timezone → UTC
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")

    df.index.name = "time"
    logger.debug("%s: %d rows downloaded", symbol, len(df))
    return df


def build_market_rows(
    df: pd.DataFrame,
    asset_id: int,
    source: str = "yfinance",
    source_code: str = "",
) -> List[MarketRow]:
    """
    將單一 symbol 的 DataFrame 轉換為 INSERT tuple list。

    Tuple 欄位順序與 db.upsert_market_data() 對應：
    (asset_id, time, frequency, open, high, low, close, adj_close,
     volume, source, source_code)
    """
    rows: List[MarketRow] = []
    for ts, row in df.iterrows():
        rows.append((
            asset_id,                     # asset_id
            ts,                           # time  (UTC-aware)
            "1d",                         # frequency
            _safe_float(row.get("open")),
            _safe_float(row.get("high")),
            _safe_float(row.get("low")),
            _safe_float(row.get("close")),
            _safe_float(row.get("adj_close")),
            _safe_float(row.get("volume")),
            source,                       # source
            source_code,                  # source_code (ticker symbol)
        ))
    return rows


def _safe_float(val) -> Optional[float]:
    """NaN / None → None，保留 DB NULL 語意。"""
    if val is None:
        return None
    try:
        f = float(val)
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None
