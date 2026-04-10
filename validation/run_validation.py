"""
Pre-Backtest Validation Entry Point
-------------------------------------
用法：
    python -m validation.run_validation
    python -m validation.run_validation --start 2020-01-01 --end 2024-12-31
    python -m validation.run_validation --start 2020-01-01 --json
    python -m validation.run_validation --fail-fast

退出碼：
    0 → PASS / WARN（可繼續回測）
    1 → FAIL（需修正）
    2 → 資料庫連線失敗

可作為 CI / 回測啟動前的 gate check：
    python -m validation.run_validation || exit 1
    python -m backtest.run_backtest
"""
import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

from etl.db import get_connection
from .checks import (
    check_derived_indicators,
    check_engine_outputs,
    check_macro_data,
    check_raw_market_data,
    check_time_alignment,
)
from .report import Reporter, summarize

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("validation")

OUTPUT_DIR = Path(__file__).parent.parent / "output"


# ── Main ──────────────────────────────────────────────────────────────────────

def run_all_checks(conn, start, end, fail_fast: bool = False):
    """
    依序執行所有 checks，回傳結果清單。
    fail_fast=True：第一個 FAIL 後立即停止（省時間）。
    """
    all_results = []

    check_groups = [
        ("raw_market_data",    lambda: check_raw_market_data(conn, start, end)),
        ("macro_data",         lambda: check_macro_data(conn, start, end)),
        ("derived_indicators", lambda: check_derived_indicators(conn, start, end)),
        ("engine_outputs",     lambda: check_engine_outputs(conn, start, end)),
        ("time_alignment",     lambda: check_time_alignment(conn, start, end)),
    ]

    for name, fn in check_groups:
        logger.info("Running: %s ...", name)
        results = fn()
        all_results.extend(results)

        fails = [r for r in results if r.status == "FAIL"]
        if fails:
            logger.warning("%s → %d FAIL(s)", name, len(fails))
            if fail_fast:
                logger.warning("--fail-fast: stopping after first FAIL group")
                break
        else:
            logger.info("%s → OK", name)

    return all_results


def parse_args():
    p = argparse.ArgumentParser(
        description="Pre-Backtest Data Quality Validation"
    )
    p.add_argument(
        "--start", type=date.fromisoformat, default=None,
        help="驗證起始日 YYYY-MM-DD（不指定則驗證所有歷史資料）",
    )
    p.add_argument(
        "--end", type=date.fromisoformat, default=None,
        help="驗證結束日 YYYY-MM-DD（不指定則驗證到最新）",
    )
    p.add_argument(
        "--fail-fast", action="store_true",
        help="第一個 FAIL 群組後停止，加快 CI 速度",
    )
    p.add_argument(
        "--json", action="store_true",
        help="額外輸出 JSON 格式結果（儲存至 output/）",
    )
    p.add_argument(
        "--output-dir", type=Path, default=OUTPUT_DIR,
        help="報告輸出目錄（預設 output/）",
    )
    p.add_argument(
        "--quiet", action="store_true",
        help="不印 Markdown 報告到 stdout（僅存檔）",
    )
    return p.parse_args()


def main():
    args = parse_args()
    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("Pre-Backtest Validation  start=%s  end=%s", args.start, args.end)
    logger.info("=" * 60)

    # ── Connect ───────────────────────────────────────────────────────────────
    try:
        with get_connection() as conn:
            results = run_all_checks(conn, args.start, args.end, args.fail_fast)
    except Exception as exc:
        logger.error("DB connection failed: %s", exc)
        sys.exit(2)

    # ── Summarize ─────────────────────────────────────────────────────────────
    summary = summarize(results)

    date_range_str = None
    if args.start or args.end:
        date_range_str = f"{args.start or '(all)'} ~ {args.end or '(latest)'}"

    # ── Markdown Report ───────────────────────────────────────────────────────
    report_md = Reporter().generate(
        results, summary,
        as_of=str(date.today()),
        date_range=date_range_str,
    )

    today_str = date.today().isoformat()
    md_path = output_dir / f"validation_{today_str}.md"
    md_path.write_text(report_md, encoding="utf-8")
    logger.info("Markdown report saved: %s", md_path)

    if not args.quiet:
        print(report_md)

    # ── JSON Report ───────────────────────────────────────────────────────────
    if args.json:
        json_payload = {
            "verdict":        summary.verdict,
            "can_backtest":   summary.can_backtest,
            "verdict_reason": summary.verdict_reason,
            "n_pass":  summary.n_pass,
            "n_warn":  summary.n_warn,
            "n_fail":  summary.n_fail,
            "n_info":  summary.n_info,
            "failed_checks": summary.failed_checks,
            "warned_checks": summary.warned_checks,
            "checks": [
                {
                    "name":    r.name,
                    "status":  r.status,
                    "message": r.message,
                    "value":   r.value if not callable(r.value) else str(r.value),
                    "details": r.details,
                }
                for r in results
            ],
        }
        json_path = output_dir / f"validation_{today_str}.json"
        json_path.write_text(
            json.dumps(json_payload, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        logger.info("JSON report saved: %s", json_path)

    # ── Final verdict ─────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info(
        "VERDICT: %s  (PASS=%d  WARN=%d  FAIL=%d)",
        summary.verdict, summary.n_pass, summary.n_warn, summary.n_fail,
    )
    logger.info("=" * 60)

    sys.exit(0 if summary.can_backtest else 1)


if __name__ == "__main__":
    main()
