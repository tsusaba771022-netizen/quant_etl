"""
Database I/O for Indicators
-----------------------------
Read:
  - fetch_close_prices()      : raw_market_data → wide DataFrame (col = asset_id)
  - fetch_macro_series()      : macro_data → wide DataFrame (col = indicator)
  - get_asset_id()            : symbol → asset_id（新 schema：UNIQUE on symbol）
  - ensure_synthetic_asset()  : 建立 macro 合成資產

Write:
  - upsert_indicators()       : List[IndicatorRow] → derived_indicators
"""
import logging
from typing import Dict, List, Optional

import pandas as pd
import psycopg2.extras
from psycopg2.extensions import connection as PgConnection

from .base import IndicatorRow

logger = logging.getLogger(__name__)


# ── Read ───────────────────────────────────────────────────────────────────────

def fetch_close_prices(
    conn: PgConnection,
    asset_ids: List[int],
    start: pd.Timestamp,
    end: pd.Timestamp,
    freq: str = "1d",
) -> pd.DataFrame:
    """
    從 raw_market_data 取出 close price。
    回傳：wide DataFrame，DatetimeIndex (UTC)，columns = asset_id (int)。
    """
    sql = """
        SELECT time, asset_id, close
        FROM raw_market_data
        WHERE asset_id = ANY(%s)
          AND frequency = %s
          AND time >= %s
          AND time <= %s
        ORDER BY time
    """
    with conn.cursor() as cur:
        cur.execute(sql, (asset_ids, freq, start, end))
        rows = cur.fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["time", "asset_id", "close"])
    df["close"]    = pd.to_numeric(df["close"], errors="coerce")
    df["time"]     = pd.to_datetime(df["time"], utc=True)

    wide = df.pivot(index="time", columns="asset_id", values="close")
    wide.columns.name = None
    return wide


def fetch_macro_series(
    conn: PgConnection,
    indicators: List[str],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    """
    從 macro_data 取出指標值。
    新 schema：欄位名稱為 indicator（無 revision 過濾）。
    回傳：wide DataFrame，DatetimeIndex (UTC)，columns = indicator (str)。
    """
    sql = """
        SELECT time, indicator, value
        FROM macro_data
        WHERE indicator = ANY(%s)
          AND time >= %s
          AND time <= %s
        ORDER BY time
    """
    with conn.cursor() as cur:
        cur.execute(sql, (indicators, start, end))
        rows = cur.fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["time", "indicator", "value"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["time"]  = pd.to_datetime(df["time"], utc=True)

    wide = df.pivot(index="time", columns="indicator", values="value")
    wide.columns.name = None
    return wide


def get_asset_id(
    conn: PgConnection,
    symbol: str,
) -> Optional[int]:
    """
    回傳 assets.asset_id。
    新 schema：symbol 為單欄 UNIQUE，不需要 exchange。
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT asset_id FROM assets WHERE symbol = %s",
            (symbol,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def ensure_synthetic_asset(
    conn: PgConnection,
    symbol: str,
    name: str,
    asset_type: str = "Index",
    exchange: str = "SYNTHETIC",
    currency: str = "USD",
) -> int:
    """
    建立合成資產（用於 macro-level 衍生指標，e.g. 10Y-2Y spread）。
    新 schema：ON CONFLICT (symbol)。
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO assets (symbol, name, asset_type, exchange, currency)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (symbol) DO NOTHING
            """,
            (symbol, name, asset_type, exchange, currency),
        )
        cur.execute(
            "SELECT asset_id FROM assets WHERE symbol = %s",
            (symbol,),
        )
        row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"Could not resolve synthetic asset_id for {symbol}")
    return row[0]


def fetch_derived_indicators(
    conn: PgConnection,
    indicator: str,
    asset_id: int,
    start: pd.Timestamp,
    end: pd.Timestamp,
    freq: str = "1d",
) -> pd.Series:
    """
    從 derived_indicators 取出單一指標時間序列。

    用途：供 zscore.py 讀取 YIELD_SPREAD_10Y2Y 等已計算的衍生指標。

    Returns
    -------
    pd.Series，DatetimeIndex (UTC)，name = indicator，
    若無資料則回傳 empty Series。
    """
    sql = """
        SELECT time, value
        FROM derived_indicators
        WHERE indicator = %s
          AND asset_id  = %s
          AND frequency = %s
          AND time >= %s
          AND time <= %s
        ORDER BY time
    """
    with conn.cursor() as cur:
        cur.execute(sql, (indicator, asset_id, freq, start, end))
        rows = cur.fetchall()

    if not rows:
        return pd.Series(dtype=float, name=indicator)

    df = pd.DataFrame(rows, columns=["time", "value"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["time"]  = pd.to_datetime(df["time"], utc=True)

    series = df.set_index("time")["value"]
    series.name = indicator
    return series


# ── Write ──────────────────────────────────────────────────────────────────────

_UPSERT_SQL = """
    INSERT INTO derived_indicators
        (indicator, asset_id, time, frequency, value, unit,
         calculation_method, source)
    VALUES %s
    ON CONFLICT (indicator, asset_id, time, frequency) DO UPDATE SET
        value              = EXCLUDED.value,
        calculation_method = EXCLUDED.calculation_method
"""


def upsert_indicators(conn: PgConnection, rows: List[IndicatorRow]) -> int:
    """批次寫入 derived_indicators。重複計算時 DO UPDATE 覆蓋舊值。"""
    if not rows:
        return 0
    tuples = [r.to_tuple() for r in rows]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, _UPSERT_SQL, tuples, page_size=500)
    logger.info("derived_indicators: upserted %d rows", len(tuples))
    return len(tuples)
