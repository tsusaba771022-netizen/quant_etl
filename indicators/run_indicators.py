"""
Derived Indicators Entry Point
-------------------------------
用法：
    python -m indicators.run_indicators
    python -m indicators.run_indicators --start 2018-01-01
    python -m indicators.run_indicators --only ma momentum
    python -m indicators.run_indicators --list

可用指標：
    ma        → SMA_5（全部核心資產）
    momentum  → PRICE_CHG_PCT_1W / PRICE_CHG_PCT_1M（全部核心資產）
    vix       → VIX_ROLLING_MEAN_20 / VIX_ROLLING_STD_20 / VIX_PCT_RANK_252
    spread    → YIELD_SPREAD_10Y2Y（需 US_10Y_YIELD + US_2Y_YIELD 在 macro_data）
    zscore    → VIX/HY_OAS/殖利率/利差 各 252 日 rolling z-score
                （需先完成 spread；DXY/OIL/VVIX 為 optional，未接入時自動略過）
"""
import argparse
import logging
import sys
from datetime import date, timedelta
from typing import Dict, List, Optional

import pandas as pd

from etl.config import DB, ETL_LOOKBACK_DAYS, INDICATOR_EQUITY_SYMBOLS
from etl.db import get_connection
from .base import IndicatorRow
from .loader import (
    ensure_synthetic_asset,
    fetch_close_prices,
    fetch_macro_series,
    get_asset_id,
    upsert_indicators,
)
from .ma import MovingAverage
from .momentum import PriceChangePct
from .spread import CODE_10Y, CODE_2Y, SYNTHETIC_SYMBOL, YieldSpread
from .vix_stats import VIXRollingStats
from .zscore import WINDOW as ZSCORE_WINDOW, compute_all_zscores

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("indicators")

# 需計算衍生指標的 equity symbols（從 config 統一管理）
EQUITY_SYMBOLS = INDICATOR_EQUITY_SYMBOLS   # ["SPY","QQQ","SOXX","VT","2330.TW"]
VIX_SYMBOL     = "^VIX"
AVAILABLE      = ["ma", "momentum", "vix", "spread", "zscore"]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ts(d: date) -> pd.Timestamp:
    return pd.Timestamp(d, tz="UTC")


def _resolve_ids(conn, symbols: List[str]) -> Dict[str, int]:
    """symbol → asset_id，找不到的 skip 並 warning。"""
    result = {}
    for sym in symbols:
        aid = get_asset_id(conn, sym)
        if aid is None:
            logger.warning("asset_id not found for %s (run ETL first)", sym)
        else:
            result[sym] = aid
    return result


# ── Individual runners ────────────────────────────────────────────────────────

def run_ma(conn, close_df: pd.DataFrame) -> List[IndicatorRow]:
    if close_df.empty:
        return []
    return MovingAverage(period=5).compute(close_df=close_df)


def run_momentum(conn, close_df: pd.DataFrame) -> List[IndicatorRow]:
    if close_df.empty:
        return []
    return PriceChangePct().compute(close_df=close_df)


def run_vix(conn, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> List[IndicatorRow]:
    vix_aid = get_asset_id(conn, VIX_SYMBOL)
    if vix_aid is None:
        logger.warning("VIXRollingStats: ^VIX not found, run ETL first")
        return []

    # pct_rank 需要 252 日 buffer
    buf_start = start_ts - pd.DateOffset(days=VIXRollingStats.lookback_days + 10)
    close_df  = fetch_close_prices(conn, [vix_aid], buf_start, end_ts)
    if close_df.empty or vix_aid not in close_df.columns:
        logger.warning("VIXRollingStats: no VIX price data")
        return []

    vix_series = close_df[vix_aid]
    all_rows   = VIXRollingStats(asset_id=vix_aid).compute(vix_series=vix_series)

    # 移除 buffer 期間（只保留 start_ts 之後）
    return [r for r in all_rows if r.time >= start_ts]


def run_spread(conn, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> List[IndicatorRow]:
    macro_df = fetch_macro_series(conn, [CODE_10Y, CODE_2Y], start_ts, end_ts)
    if macro_df.empty:
        logger.warning(
            "YieldSpread: US_10Y_YIELD / US_2Y_YIELD not in macro_data. "
            "Add DGS10 + DGS2 to etl/config.py FRED_SERIES and re-run ETL."
        )
        return []

    spread_aid = ensure_synthetic_asset(
        conn,
        symbol    = SYNTHETIC_SYMBOL,
        name      = "US Treasury 10Y-2Y Yield Spread",
        asset_type= "Index",
        exchange  = "SYNTHETIC",
    )
    conn.commit()   # commit 合成資產後再寫 derived_indicators（FK 約束）

    return YieldSpread(asset_id=spread_aid, frequency="daily").compute(macro_df=macro_df)


def run_zscore(conn, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> List[IndicatorRow]:
    """
    計算所有宏觀風險指標的 rolling z-score。

    需求：
    - spread indicator 需先執行（YIELD_SPREAD_10Y2Y 才有資料）
    - DXY / OIL / VVIX optional，若尚未接入資料層，自動略過

    Buffer（日資料）：252 + 30 曆日，用於計算 rolling window
    Buffer（月資料）：_compute_one 內部自動計算（window 個月 + 14 個月），
                     月資料 ISM_PMI_MFG_Z_60M 的 buf_start 由
                     output_start - DateOffset(months=74) 決定，不受此處影響
    """
    # z-score 需要 252 交易日 buffer（約 365 曆日，多取 30 天緩衝）
    buf_start = start_ts - pd.DateOffset(days=ZSCORE_WINDOW + 30 + 365)
    return compute_all_zscores(conn, buf_start, end_ts, output_start=start_ts)


# ── Main ──────────────────────────────────────────────────────────────────────

def run_all(start: date, end: date, only: Optional[List[str]] = None) -> None:
    targets = set(only) if only else set(AVAILABLE)

    start_ts = _ts(start)
    end_ts   = _ts(end)
    # MA / momentum 需要最多 21 日 buffer（交易日），多取 35 曆日
    buf_start = start_ts - pd.DateOffset(days=35)

    logger.info("=" * 60)
    logger.info("Indicators  %s → %s  targets=%s", start, end, sorted(targets))
    logger.info("=" * 60)

    with get_connection() as conn:

        # 取得 equity asset_ids
        equity_ids_map = _resolve_ids(conn, EQUITY_SYMBOLS)
        equity_ids     = list(equity_ids_map.values())

        # 預先抓取 equity close（MA + momentum 共用）
        close_df = pd.DataFrame()
        if equity_ids and ("ma" in targets or "momentum" in targets):
            close_df = fetch_close_prices(conn, equity_ids, buf_start, end_ts)

        all_rows: List[IndicatorRow] = []

        if "ma" in targets:
            logger.info("── MA5 ──")
            rows = run_ma(conn, close_df)
            rows = [r for r in rows if r.time >= start_ts]   # trim buffer
            all_rows.extend(rows)
            logger.info("MA5: %d rows", len(rows))

        if "momentum" in targets:
            logger.info("── Δ1W / Δ1M ──")
            rows = run_momentum(conn, close_df)
            rows = [r for r in rows if r.time >= start_ts]   # trim buffer
            all_rows.extend(rows)
            logger.info("PriceChangePct: %d rows", len(rows))

        if "vix" in targets:
            logger.info("── VIX Rolling Stats ──")
            rows = run_vix(conn, start_ts, end_ts)
            all_rows.extend(rows)
            logger.info("VIXRollingStats: %d rows", len(rows))

        if "spread" in targets:
            logger.info("── 10Y-2Y Spread ──")
            rows = run_spread(conn, start_ts, end_ts)
            all_rows.extend(rows)
            logger.info("YieldSpread: %d rows", len(rows))

        if "zscore" in targets:
            logger.info("── Rolling Z-Score (252d) ──")
            rows = run_zscore(conn, start_ts, end_ts)
            all_rows.extend(rows)
            logger.info("RollingZScore: %d rows total", len(rows))

        total = upsert_indicators(conn, all_rows)
        logger.info("Total rows written: %d", total)

    logger.info("Indicators complete.")


def parse_args():
    today = date.today()
    p = argparse.ArgumentParser(description="Derived Indicators Calculator")
    p.add_argument("--start", type=date.fromisoformat,
                   default=today - timedelta(days=ETL_LOOKBACK_DAYS))
    p.add_argument("--end",   type=date.fromisoformat, default=today)
    p.add_argument("--only",  nargs="+", choices=AVAILABLE, metavar="IND")
    p.add_argument("--list",  action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    if args.list:
        for name in AVAILABLE:
            print(f"  {name}")
        sys.exit(0)
    if args.start > args.end:
        logger.error("--start must be <= --end")
        sys.exit(1)
    run_all(start=args.start, end=args.end, only=args.only)


if __name__ == "__main__":
    main()
