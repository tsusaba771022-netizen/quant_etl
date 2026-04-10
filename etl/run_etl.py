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

# ── Backfill 常數 ─────────────────────────────────────────────────────────────
# Pass 1：今日往回 300 自然日（≈207 交易日），可能不足 220。
# Pass 2（自動觸發）：若 VOO 歷史 < BACKFILL_MIN_TRADING_DAYS，
#           改用固定起始日 BACKFILL_FIXED_START 確保足夠。
BACKFILL_DAYS: int            = 300
BACKFILL_FIXED_START: date    = date(2020, 1, 1)
BACKFILL_MIN_TRADING_DAYS: int = 220   # 必須與 engine/trend.MIN_HISTORY 保持一致

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


# ── Backfill Helpers ──────────────────────────────────────────────────────────

def _count_voo_history(end: date) -> int:
    """
    查詢 DB 中 VOO 在 end 日期前的日線筆數。
    任何 DB 失敗 → 回傳 0（呼叫端視為不足，觸發 Pass 2）。
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM raw_market_data rmd
                    JOIN assets a ON a.asset_id = rmd.asset_id
                    WHERE a.symbol = 'VOO'
                      AND rmd.frequency = '1d'
                      AND rmd.close IS NOT NULL
                      AND rmd.time::date <= %s
                    """,
                    (end,),
                )
                row = cur.fetchone()
                return int(row[0]) if row else 0
    except Exception as exc:
        logger.warning("[BACKFILL] Cannot count VOO history: %s", exc)
        return 0


def _run_backfill(end: date, run_market: bool, run_macro: bool) -> None:
    """
    兩階段 backfill：
      Pass 1：今日往回 BACKFILL_DAYS（300）自然日
      Pass 2：若 VOO 歷史 < BACKFILL_MIN_TRADING_DAYS，
              改用固定起始日 BACKFILL_FIXED_START（2020-01-01）補抓

    設計原則：
      - 兩次都走 run_market_etl / run_macro_etl（冪等，DO NOTHING 重複無害）
      - DB 計數失敗 → 視為不足，自動觸發 Pass 2（fail-safe）
      - 各 pass 的 ETL 異常各自捕捉，不中斷後續
    """
    today = date.today()
    start_pass1 = today - timedelta(days=BACKFILL_DAYS)

    logger.info(
        "[BACKFILL] Pass 1: %s → %s (today - %d days)",
        start_pass1, end, BACKFILL_DAYS,
    )
    if run_market:
        try:
            run_market_etl(start_pass1, end)
        except Exception:
            logger.exception("[BACKFILL] Pass 1 market ETL failed")
    if run_macro:
        try:
            run_macro_etl(start_pass1, end)
        except Exception:
            logger.exception("[BACKFILL] Pass 1 macro ETL failed")

    # ── 檢查 VOO 是否已達到 Trend Layer 最低門檻 ─────────────────────────────
    voo_count = _count_voo_history(end)
    if voo_count < BACKFILL_MIN_TRADING_DAYS:
        logger.warning(
            "[BACKFILL] initial backfill insufficient (%d < %d), "
            "retrying with fixed start date %s",
            voo_count, BACKFILL_MIN_TRADING_DAYS, BACKFILL_FIXED_START,
        )
        if run_market:
            try:
                run_market_etl(BACKFILL_FIXED_START, end)
            except Exception:
                logger.exception("[BACKFILL] Pass 2 market ETL failed")
        if run_macro:
            try:
                run_macro_etl(BACKFILL_FIXED_START, end)
            except Exception:
                logger.exception("[BACKFILL] Pass 2 macro ETL failed")
    else:
        logger.info(
            "[BACKFILL] VOO history sufficient: %d rows >= %d",
            voo_count, BACKFILL_MIN_TRADING_DAYS,
        )

    logger.info("[BACKFILL] complete.")


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
    parser.add_argument(
        "--backfill",
        action="store_true",
        help=(
            f"Backfill mode: override --start to today - {BACKFILL_DAYS} days "
            f"to ensure sufficient history for Trend Layer (needs >= 220 trading days of VOO)."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    today = date.today()
    end:   date = args.end

    run_market = not args.macro_only
    run_macro  = not args.market_only

    # --backfill：兩階段補抓（Pass 1: 300天；Pass 2: 2020-01-01 if insufficient）
    if args.backfill:
        logger.info("=" * 60)
        logger.info("ETL backfill mode  end=%s", end)
        logger.info("DB: %s@%s:%s/%s", DB.user, DB.host, DB.port, DB.dbname)
        logger.info("=" * 60)
        _run_backfill(end, run_market, run_macro)
        return

    start = args.start
    if start > end:
        logger.error("--start (%s) must be <= --end (%s)", start, end)
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("ETL start  %s → %s", start, end)
    logger.info("DB: %s@%s:%s/%s", DB.user, DB.host, DB.port, DB.dbname)
    logger.info("=" * 60)

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
