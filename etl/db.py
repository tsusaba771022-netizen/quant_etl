"""
Database Utilities
------------------
- get_connection()           : context manager，自動 commit / rollback
- ensure_idempotency_indexes : 確保所有冪等性 UNIQUE INDEX 存在（幕等，可安全重複執行）
- ensure_asset()             : upsert assets 並回傳 asset_id
- upsert_market_data()       : 批次寫入 raw_market_data（ON CONFLICT DO NOTHING）
- upsert_macro_data()        : 批次寫入 macro_data（ON CONFLICT DO NOTHING）

冪等性設計（各表 unique constraint）：
  assets             → UNIQUE (symbol)
  raw_market_data    → UNIQUE (asset_id, time, frequency)     DO NOTHING
  macro_data         → UNIQUE (indicator, time, frequency)    DO NOTHING
  derived_indicators → UNIQUE (indicator, asset_id, time,     DO UPDATE value
                                frequency)
  regimes            → UNIQUE (time)                          DO UPDATE all
  signals            → UNIQUE (asset_id, time)                DO UPDATE all
"""
import logging
from contextlib import contextmanager
from typing import Generator, List, Tuple

import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PgConnection

from .config import DB, MarketSymbol

logger = logging.getLogger(__name__)


# ── Connection ────────────────────────────────────────────────────────────────

@contextmanager
def get_connection() -> Generator[PgConnection, None, None]:
    conn = psycopg2.connect(DB.dsn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Assets ────────────────────────────────────────────────────────────────────

def ensure_asset(conn: PgConnection, sym: MarketSymbol) -> int:
    """
    Insert asset if not exists; return asset_id regardless.
    Uses INSERT ... ON CONFLICT DO NOTHING + separate SELECT for idempotency.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO assets (symbol, name, asset_type, exchange, currency)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (symbol) DO NOTHING
            """,
            (sym.symbol, sym.name, sym.asset_type, sym.exchange, sym.currency),
        )
        cur.execute(
            "SELECT asset_id FROM assets WHERE symbol = %s",
            (sym.symbol,),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(f"Could not resolve asset_id for {sym.symbol}")
        return row[0]


# ── raw_market_data ───────────────────────────────────────────────────────────

# Row tuple order must match the INSERT below:
# (asset_id, time, frequency, open, high, low, close, adj_close,
#  volume, source, source_code)
MarketRow = Tuple

_INSERT_MARKET = """
    INSERT INTO raw_market_data
        (asset_id, time, frequency, open, high, low, close, adj_close,
         volume, source, source_code)
    VALUES %s
    ON CONFLICT (asset_id, time, frequency) DO NOTHING
"""


def upsert_market_data(conn: PgConnection, rows: List[MarketRow]) -> int:
    """Bulk-insert market rows. Returns number of rows attempted."""
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, _INSERT_MARKET, rows, page_size=500)
    logger.info("market_data: upserted up to %d rows", len(rows))
    return len(rows)


# ── macro_data ────────────────────────────────────────────────────────────────

# Row tuple order:
# (indicator, time, frequency, value, unit, source, source_code)
MacroRow = Tuple

_INSERT_MACRO = """
    INSERT INTO macro_data
        (indicator, time, frequency, value, unit, source, source_code)
    VALUES %s
    ON CONFLICT (indicator, time, frequency) DO NOTHING
"""


def upsert_macro_data(conn: PgConnection, rows: List[MacroRow]) -> int:
    """Bulk-insert macro rows. Returns number of rows attempted."""
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, _INSERT_MACRO, rows, page_size=500)
    logger.info("macro_data: upserted up to %d rows", len(rows))
    return len(rows)


# ── Idempotency Index Guard ───────────────────────────────────────────────────

# 所有冪等性所依賴的 UNIQUE INDEX（IF NOT EXISTS → 幕等，安全重複執行）
_IDEMPOTENCY_INDEXES = [
    # (index_name, table, columns)
    ("uq_assets_symbol",            "assets",             "(symbol)"),
    ("uq_raw_market_data_key",      "raw_market_data",    "(asset_id, time, frequency)"),
    ("uq_macro_data_key",           "macro_data",         "(indicator, time, frequency)"),
    ("uq_derived_indicators_key",   "derived_indicators", "(indicator, asset_id, time, frequency)"),
    ("uq_regimes_time",             "regimes",            "(time)"),
    ("uq_signals_asset_time",       "signals",            "(asset_id, time)"),
]


def ensure_idempotency_indexes(conn: PgConnection) -> None:
    """
    確保所有冪等性 UNIQUE INDEX 存在。

    - 使用 CREATE UNIQUE INDEX IF NOT EXISTS（幕等，可安全重複執行）
    - 若 index 已存在 → no-op
    - 若 index 不存在但表中已有重複資料 → PostgreSQL 會拋出例外（這是對的）
    - 應在 ETL 啟動早期呼叫，確保後續的 ON CONFLICT 子句有效

    設計原則：不影響 TimescaleDB hypertable 已有的 chunk index，
    因為 CREATE UNIQUE INDEX IF NOT EXISTS 在 hypertable 上可能限制較多；
    若在 hypertable 上失敗，記錄 warning 但不中斷主流程。
    """
    for idx_name, table, columns in _IDEMPOTENCY_INDEXES:
        sql = f"CREATE UNIQUE INDEX IF NOT EXISTS {idx_name} ON {table} {columns}"
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
            logger.debug("[IDX] %-40s OK", idx_name)
        except Exception as exc:
            conn.rollback()
            # TimescaleDB hypertable 上建 UNIQUE INDEX 有限制（須包含 partition key）
            # 若已有重複資料也會失敗 → WARN 讓運維人員排查
            logger.warning(
                "[IDX] %-40s SKIP（%s）— 請執行 schema_main.sql 確認約束",
                idx_name, exc,
            )

    logger.info("[IDX] Idempotency index check complete")
