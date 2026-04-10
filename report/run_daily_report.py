"""
Daily Report Entry Point
------------------------
用法：
    python -m report.run_daily_report
    python -m report.run_daily_report --date 2025-04-05
    python -m report.run_daily_report --no-db    # 只印報告，不寫 DB

輸出：
    output/daily_report_YYYY-MM-DD.md
    （同時也會更新 engine 的 regimes / signals 表，除非 --no-db）
"""
import argparse
import io
import logging
import sys
from datetime import date
from pathlib import Path

# Windows 終端機預設 CP950，強制 UTF-8 以正確顯示中文與 emoji
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from etl.db import get_connection
from engine.db_writer import write_regime, write_signals
from engine.regime import RegimeEngine
from engine.signals import SignalEngine
from engine.snapshot import SnapshotLoader
from backtest.strategy import blended_portfolio_positions
from report.daily_report import SCOUTING_MULT, build_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("daily_report")

OUTPUT_DIR = Path(__file__).parent.parent / "output"


def parse_args():
    p = argparse.ArgumentParser(description="Daily Portfolio Report")
    p.add_argument("--date",       type=date.fromisoformat, default=date.today())
    p.add_argument("--no-db",      action="store_true",
                   help="只輸出報告，不寫 regimes/signals 表")
    p.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    return p.parse_args()


def _write_regime_signals(conn, as_of: date, regime, signals) -> None:
    """
    冪等寫入 regimes + signals（委託 engine/db_writer.py）。

    冪等鍵：
      regimes → (time)
      signals → (asset_id, time)
    """
    write_regime(conn, as_of, regime)
    write_signals(
        conn, as_of, signals, regime,
        extra_meta={"scouting_mult": SCOUTING_MULT},
    )
    logger.info("DB: regimes + signals written for %s", as_of)


def main():
    args  = parse_args()
    as_of = args.date
    args.output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("Daily Report  as_of=%s  scouting_mult=%.2f", as_of, SCOUTING_MULT)
    logger.info("=" * 60)

    with get_connection() as conn:
        # 1. 載入 Snapshot
        snap = SnapshotLoader(conn).load(as_of)

        # 2. Regime 判定
        regime = RegimeEngine().run(snap)

        # 3. Signal 判定
        signals = SignalEngine().run(snap, regime)

        # 4. 目標配置（Baseline v1.0：scouting_mult=0.50）
        pos = blended_portfolio_positions(signals, scouting_mult=SCOUTING_MULT)

        # 5. 建立報告
        report = build_report(snap, regime, signals, pos)

        # 6. 存檔
        out_path = args.output_dir / f"daily_report_{as_of}.md"
        out_path.write_text(report, encoding="utf-8")
        logger.info("Report saved: %s", out_path)

        # 7. 印出到終端
        print("\n" + report)

        # 8. 寫 DB
        if not args.no_db:
            try:
                _write_regime_signals(conn, as_of, regime, signals)
            except Exception:
                logger.exception("DB write failed (report still saved)")

    logger.info("Daily report complete.")


if __name__ == "__main__":
    main()
