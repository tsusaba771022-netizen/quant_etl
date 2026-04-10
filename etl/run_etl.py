"""
ETL Entry Point
---------------
用法：
    python -m etl.run_etl                      # 預設抓最近 N 天
    python -m etl.run_etl --start 2024-01-01   # 指定起始日
    python -m etl.run_etl --start 2024-01-01 --end 2024-03-31
    python -m etl.run_etl --market-only        # 只跑行情
    python -m etl.run_etl --macro-only         # 只跑總經

每日排程範例（cron）：
    0 7 * * 1-5  cd /path/to/quant_etl && python -m etl.run_etl >> logs/etl.log 2>&1
"""
import argparse
import logging
import sys
from datetime import date, timedelta
from typing import Dict

from .config import (
    DB,
    ETL_LOOKBACK_DAYS,
    FRED_API_KEY,
    FRED_SERIES,
    MARKET_SYMBOLS,
)
from .db import (
    ensure_asset,
    get_connection,
    upsert_macro_data,
    upsert_market_data,
)
from .fetch_macro import build_macro_rows, download_fred_series
from .fetch_market import build_market_rows, download_ohlcv

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("etl")


# ── Market ETL ────────────────────────────────────────────────────────────────

def run_market_etl(start: date, end: date) -> None:
    symbols = list(MARKET_SYMBOLS.keys())
    ohlcv_map = download_ohlcv(symbols, start, end)

    if not ohlcv_map:
        logger.warning("No market data downloaded. Skipping DB write.")
        return

    with get_connection() as conn:
        # 確保所有 assets 存在並取得 asset_id
        asset_ids: Dict[str, int] = {}
        for sym_str, sym_cfg in MARKET_SYMBOLS.items():
            try:
                asset_ids[sym_str] = ensure_asset(conn, sym_cfg)
                logger.debug("asset_id[%s] = %d", sym_str, asset_ids[sym_str])
            except Exception as exc:
                logger.error("ensure_asset failed for %s: %s", sym_str, exc)

        conn.commit()  # commit asset inserts before market data

        # 批次建立並寫入 market rows
        all_rows = []
        for sym_str, df in ohlcv_map.items():
            if sym_str not in asset_ids:
                logger.warning("Skipping %s: no asset_id", sym_str)
                continue
            rows = build_market_rows(df, asset_ids[sym_str], source_code=sym_str)
            logger.info("%s: %d rows to upsert", sym_str, len(rows))
            all_rows.extend(rows)

        upsert_market_data(conn, all_rows)


# ── Macro ETL ─────────────────────────────────────────────────────────────────

def run_macro_etl(start: date, end: date) -> None:
    all_rows = []

    for indicator_code, cfg in FRED_SERIES.items():
        series = download_fred_series(
            fred_id=cfg["fred_id"],
            start=start,
            end=end,
            api_key=FRED_API_KEY,
        )
        if series is None:
            logger.warning("Skipping %s: no data", indicator_code)
            continue

        rows = build_macro_rows(series, indicator_code, cfg)
        logger.info("%s (%s): %d rows to upsert", indicator_code, cfg["fred_id"], len(rows))
        all_rows.extend(rows)

    if not all_rows:
        logger.warning("No macro data to write.")
        return

    with get_connection() as conn:
        upsert_macro_data(conn, all_rows)


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    today = date.today()
    default_start = today - timedelta(days=ETL_LOOKBACK_DAYS)

    parser = argparse.ArgumentParser(description="Quant ETL: yfinance + FRED → PostgreSQL")
    parser.add_argument(
        "--start",
        type=date.fromisoformat,
        default=default_start,
        help=f"Start date YYYY-MM-DD (default: today - {ETL_LOOKBACK_DAYS} days)",
    )
    parser.add_argument(
        "--end",
        type=date.fromisoformat,
        default=today,
        help="End date YYYY-MM-DD (default: today)",
    )
    parser.add_argument("--market-only", action="store_true", help="Only run market ETL")
    parser.add_argument("--macro-only",  action="store_true", help="Only run macro ETL")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start: date = args.start
    end:   date = args.end

    if start > end:
        logger.error("--start (%s) must be <= --end (%s)", start, end)
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("ETL start  %s → %s", start, end)
    logger.info("DB: %s@%s:%s/%s", DB.user, DB.host, DB.port, DB.dbname)
    logger.info("=" * 60)

    run_market = not args.macro_only
    run_macro  = not args.market_only

    if run_market:
        logger.info("── Market ETL ──")
        try:
            run_market_etl(start, end)
        except Exception:
            logger.exception("Market ETL failed")

    if run_macro:
        logger.info("── Macro ETL ──")
        try:
            run_macro_etl(start, end)
        except Exception:
            logger.exception("Macro ETL failed")

    logger.info("ETL complete.")


if __name__ == "__main__":
    main()
