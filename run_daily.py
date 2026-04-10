"""
每日執行主程式
--------------
執行順序：
  Step 1  ETL         — 抓取最新市場行情 + 總經數據（yfinance + FRED）
  Step 2  Indicators  — 計算衍生指標（SMA / Momentum / VIX Stats / Yield Spread）
  Step 3  Daily Report— Regime 判定 + Signal 計算 + 產出繁體中文日報

用法：
  python run_daily.py                    # 今日執行
  python run_daily.py --date 2026-04-05  # 指定日期的報告（ETL 仍抓最新）
  python run_daily.py --etl-only         # 只更新資料，不產報告
  python run_daily.py --report-only      # 跳過 ETL，只產報告（資料已是最新）
  python run_daily.py --no-db            # 不寫 DB（診斷用）

設計原則：
  - Step 1 失敗 → 記錄錯誤，但繼續嘗試 Step 2、3（使用舊資料）
  - Step 2 失敗 → 記錄錯誤，但繼續嘗試 Step 3
  - Step 3 失敗 → 記錄錯誤，結束
  - 所有 log 同時輸出到 logs/daily_YYYY-MM-DD.log

輸出位置：
  output/daily_report_YYYY-MM-DD.md
"""
from __future__ import annotations

import argparse
import io
import logging
import sys
import time
import traceback
from datetime import date, timedelta
from pathlib import Path

# ── Windows UTF-8 terminal fix ────────────────────────────────────────────────
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ── 目錄設定 ──────────────────────────────────────────────────────────────────
ROOT       = Path(__file__).parent
OUTPUT_DIR = ROOT / "output"
LOG_DIR    = ROOT / "logs"
OUTPUT_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# ── Logging（同時輸出終端 + 檔案）────────────────────────────────────────────

def _setup_logging(log_date: date) -> logging.Logger:
    log_path = LOG_DIR / f"daily_{log_date}.log"
    fmt      = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"
    datefmt  = "%Y-%m-%d %H:%M:%S"

    handlers: list = [logging.StreamHandler(sys.stdout)]
    try:
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))
    except Exception:
        pass   # 無法寫 log 檔也不中斷

    logging.basicConfig(level=logging.INFO, format=fmt, datefmt=datefmt,
                        handlers=handlers, force=True)
    return logging.getLogger("daily")


# ── Step runners ──────────────────────────────────────────────────────────────

def _step_ensure_indexes() -> bool:
    """Step 0：確保所有冪等性 UNIQUE INDEX 存在（幕等，安全重複執行）"""
    from etl.db import get_connection, ensure_idempotency_indexes
    logger = logging.getLogger("daily.indexes")
    try:
        with get_connection() as conn:
            ensure_idempotency_indexes(conn)
        return True
    except Exception as exc:
        logger.warning("  ⚠ Index check 失敗（不影響後續流程）：%s", exc)
        return False


def _step_etl(lookback_days: int = 7) -> bool:
    """Step 1：ETL（市場 + 總經）"""
    from etl.run_etl import run_market_etl, run_macro_etl
    today = date.today()
    start = today - timedelta(days=lookback_days)
    logger = logging.getLogger("daily.etl")

    ok = True
    logger.info("▶ ETL：%s → %s", start, today)

    logger.info("  [1/2] 市場行情（yfinance）")
    try:
        run_market_etl(start, today)
        logger.info("  ✓ 市場行情 OK")
    except Exception as exc:
        logger.error("  ✗ 市場行情失敗：%s", exc)
        ok = False

    logger.info("  [2/2] 總經數據（FRED）")
    try:
        run_macro_etl(start, today)
        logger.info("  ✓ 總經數據 OK")
    except Exception as exc:
        logger.error("  ✗ 總經數據失敗：%s", exc)
        ok = False

    return ok


def _step_indicators(lookback_days: int = 7) -> bool:
    """Step 2：衍生指標計算"""
    from indicators.run_indicators import run_all
    today = date.today()
    start = today - timedelta(days=lookback_days)
    logger = logging.getLogger("daily.indicators")

    logger.info("▶ 衍生指標：%s → %s", start, today)
    try:
        run_all(start=start, end=today)
        logger.info("  ✓ 衍生指標 OK")
        return True
    except Exception as exc:
        logger.error("  ✗ 衍生指標失敗：%s", exc)
        return False


def _step_email(report_path: Path, report_date: date) -> bool:
    """Step 4：發送 Email（失敗不中斷主流程）"""
    from report.send_email import send_report
    logger = logging.getLogger("daily.email")
    logger.info("▶ Email 發送：%s", report_path.name)
    try:
        ok = send_report(report_path, report_date)
    except Exception as exc:
        logger.error("EMAIL_FAILED  date=%s  error=UnexpectedException  detail=%s",
                     report_date, exc)
        ok = False
    if not ok:
        logger.warning("  ⚠ 報告已存檔，可手動補寄：python -m report.send_email --date %s",
                       report_date)
    return ok


def _step_line(report_path: Path, report_date: date) -> bool:
    """Step 5：LINE 推播（失敗不中斷主流程）"""
    from report.send_line import send_line_report
    logger = logging.getLogger("daily.line")
    logger.info("▶ LINE 推播：%s", report_path.name)
    try:
        ok = send_line_report(report_path, report_date)
    except Exception as exc:
        logger.error("LINE_FAILED  date=%s  error=UnexpectedException  detail=%s",
                     report_date, exc)
        ok = False
    if not ok:
        logger.warning(
            "  ⚠ LINE 推播失敗（報告已存檔，可手動補送）："
            "python -m report.send_line --date %s", report_date,
        )
    return ok


def _step_report(report_date: date, no_db: bool = False) -> tuple[bool, Path | None]:
    """Step 3：Daily Report（Regime + Signal + 日報輸出）"""
    from etl.db import get_connection
    from etl.config import TACTICAL_CAPS
    from engine.regime import RegimeEngine
    from engine.signals import SignalEngine
    from engine.snapshot import SnapshotLoader
    from backtest.strategy import blended_portfolio_positions, apply_macro_alloc_caps
    from report.daily_report import SCOUTING_MULT, build_report
    from report.run_daily_report import _write_regime_signals

    logger = logging.getLogger("daily.report")
    logger.info("▶ Daily Report：%s", report_date)

    try:
        with get_connection() as conn:
            snap    = SnapshotLoader(conn).load(report_date)
            regime  = RegimeEngine().run(snap)
            signals = SignalEngine().run(snap, regime)

            # 資料健康度檢查（缺值不中斷報告）
            health = None
            try:
                from monitor.data_health import check_indicator_health
                health = check_indicator_health(conn)
                logger.info("  ✓ 資料健康度：overall=%s", health.overall)
            except Exception as exc:
                logger.warning("  ⚠ 資料健康度檢查失敗（報告仍繼續）：%s", exc)

            # ── Layer 2：Trend Risk Cap（VOO 200DMA）──────────────────────────
            trend_result = None
            try:
                from engine.trend import TrendLayer, TrendStatus
                trend_result = TrendLayer().run(conn, report_date)
                logger.info("  ✓ Trend Layer: %s  (history=%d)",
                            trend_result.status.value, trend_result.history_len)
                # Phase 2-B：VOO DB 完全空白時 ERROR 提示（需執行 backfill）
                if (trend_result.status == TrendStatus.WARMUP
                        and trend_result.history_len == 0):
                    logger.error(
                        "[TREND] VOO history is empty in DB — "
                        "run: python -m etl.run_etl --backfill"
                    )
            except Exception as exc:
                logger.warning("  ⚠ Trend Layer 計算失敗（報告仍繼續）：%s", exc)

            # ── Layer 3：Macro Allocation Matrix ──────────────────────────────
            macro_alloc_result = None
            try:
                from engine.macro_alloc import classify_macro_alloc
                macro_alloc_result = classify_macro_alloc(
                    cfnai        = snap.ism_pmi,
                    spread       = snap.spread_10y2y,
                    vix          = snap.vix,
                    vix_pct_rank = snap.vix_pct_rank,
                )
                logger.info("  ✓ Macro Alloc: %s", macro_alloc_result.status.value)
            except Exception as exc:
                logger.warning("  ⚠ Macro Alloc 計算失敗（報告仍繼續）：%s", exc)

            # ── P3-1：Layer 3 Allocation Override → effective_caps ────────────
            # DEFENSIVE → 戰術上限 × 0.50；其他 / None → 不改動
            effective_caps = apply_macro_alloc_caps(TACTICAL_CAPS, macro_alloc_result)
            if effective_caps is not TACTICAL_CAPS:
                logger.info(
                    "  [P3-1] Layer 3 DEFENSIVE active — tactical caps compressed × %.0f%%",
                    100 * (list(effective_caps.values())[0] / list(TACTICAL_CAPS.values())[0])
                    if TACTICAL_CAPS else 0,
                )

            pos = blended_portfolio_positions(
                signals,
                scouting_mult=SCOUTING_MULT,
                tactical_caps=effective_caps,
            )

            report  = build_report(
                snap, regime, signals, pos,
                health        = health,
                trend         = trend_result,
                macro_alloc   = macro_alloc_result,
                effective_caps = effective_caps,
            )

            out_path = OUTPUT_DIR / f"daily_report_{report_date}.md"
            out_path.write_text(report, encoding="utf-8")
            logger.info("  ✓ 報告已存：%s", out_path)

            if not no_db:
                try:
                    _write_regime_signals(conn, report_date, regime, signals)
                    logger.info("  ✓ DB 寫入成功（regimes + signals）")
                except Exception as exc:
                    logger.warning("  ⚠ DB 寫入失敗（報告仍可用）：%s", exc)

        return True, out_path

    except Exception as exc:
        logger.error("  ✗ Daily Report 失敗：%s", exc)
        return False, None


# ── 最終報告預覽 ──────────────────────────────────────────────────────────────

def _print_report(path: Path) -> None:
    """在終端印出報告全文。"""
    try:
        print("\n" + "=" * 70)
        print(path.read_text(encoding="utf-8"))
        print("=" * 70 + "\n")
    except Exception:
        pass


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="每日量化投資組合報告執行程式",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--date", type=date.fromisoformat, default=date.today(),
        help="報告日期（預設今日）",
    )
    p.add_argument(
        "--lookback", type=int, default=7,
        help="ETL 與 Indicators 的回溯天數（預設 7）",
    )
    p.add_argument("--etl-only",    action="store_true", help="只跑 ETL + Indicators")
    p.add_argument("--report-only", action="store_true", help="跳過 ETL，只產報告")
    p.add_argument("--no-db",       action="store_true", help="不寫 DB")
    p.add_argument("--no-email",    action="store_true", help="不發送 Email")
    p.add_argument("--force-email", action="store_true", help="忽略已寄送記錄，強制重寄")
    p.add_argument("--no-line",     action="store_true", help="不發送 LINE 推播")
    p.add_argument("--force-line",  action="store_true", help="忽略已發送記錄，強制重送 LINE")
    return p.parse_args()


def main() -> None:
    args   = parse_args()
    logger = _setup_logging(args.date)

    banner = f"""
╔══════════════════════════════════════════════════════════════════╗
║          量化投資組合 — 每日執行                                   ║
║          日期：{args.date}   Baseline v1.0                        ║
╚══════════════════════════════════════════════════════════════════╝"""
    print(banner)
    logger.info("每日執行開始  date=%s  lookback=%dd", args.date, args.lookback)

    t0 = time.perf_counter()

    # ── 全局步驟追蹤（供崩潰警告使用）──────────────────────────────────────────
    _current_step = "初始化"

    try:
        etl_ok = ind_ok = True

        # ── Step 0：確保冪等性 INDEX（每次啟動執行一次，幕等安全）───────────────
        _current_step = "Step 0：Index Guard"
        _step_ensure_indexes()   # 失敗只警告，不中斷

        # ── Step 1：ETL ───────────────────────────────────────────────────────
        if not args.report_only:
            _current_step = "Step 1：ETL"
            logger.info("")
            logger.info("━━ Step 1 / 4：ETL ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            etl_ok = _step_etl(lookback_days=args.lookback)
            if not etl_ok:
                logger.warning("  ⚠ ETL 有錯誤，繼續使用既有資料")

        # ── Step 2：Indicators ────────────────────────────────────────────────
        if not args.report_only:
            _current_step = "Step 2：Indicators"
            logger.info("")
            logger.info("━━ Step 2 / 4：衍生指標 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            ind_ok = _step_indicators(lookback_days=args.lookback)
            if not ind_ok:
                logger.warning("  ⚠ 指標計算有錯誤，報告可能使用舊數據")

        # ── Step 3：Daily Report ──────────────────────────────────────────────
        rep_ok, rep_path = False, None
        email_ok = None
        line_ok  = None

        if not args.etl_only:
            _current_step = "Step 3：Daily Report"
            logger.info("")
            logger.info("━━ Step 3 / 5：Daily Report ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            rep_ok, rep_path = _step_report(args.date, no_db=args.no_db)

            if rep_ok and rep_path:
                _print_report(rep_path)
            else:
                logger.error("  Daily Report 失敗，請檢查 DB 連線與資料狀態")
                sys.exit(1)

        # ── Step 4：Email ─────────────────────────────────────────────────────
        if rep_ok and rep_path and not args.no_email and not args.etl_only:
            _current_step = "Step 4：Email"
            logger.info("")
            logger.info("━━ Step 4 / 5：Email ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            from report.send_email import is_already_sent
            if is_already_sent(args.date) and not args.force_email:
                logger.info("  — 今日報告已寄送，略過（如需重寄請加 --force-email）")
                email_ok = True
            else:
                email_ok = _step_email(rep_path, args.date)

        # ── Step 5：LINE ──────────────────────────────────────────────────────
        if rep_ok and rep_path and not args.no_line and not args.etl_only:
            _current_step = "Step 5：LINE"
            logger.info("")
            logger.info("━━ Step 5 / 5：LINE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            from report.send_line import is_already_sent as line_already_sent
            if line_already_sent(args.date) and not args.force_line:
                logger.info("  — 今日 LINE 已發送，略過（如需重送請加 --force-line）")
                line_ok = True
            else:
                line_ok = _step_line(rep_path, args.date)

        # ── 結尾摘要 ──────────────────────────────────────────────────────────
        _current_step = "結尾摘要"
        elapsed = time.perf_counter() - t0
        logger.info("")
        logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        logger.info("完成  耗時 %.1f 秒", elapsed)
        if not args.report_only:
            logger.info("ETL：%s　Indicators：%s",
                        "✓" if etl_ok else "⚠ 部分失敗",
                        "✓" if ind_ok else "⚠ 部分失敗")
        if not args.etl_only:
            logger.info("報告：output/daily_report_%s.md", args.date)
        if email_ok is True:
            logger.info("Email：✓ 已發送")
        elif email_ok is False:
            logger.info("Email：⚠ 發送失敗（報告已存檔，可手動補寄）")
        elif args.no_email:
            logger.info("Email：— 已跳過（--no-email）")
        if line_ok is True:
            logger.info("LINE ：✓ 已發送")
        elif line_ok is False:
            logger.info("LINE ：⚠ 發送失敗（python -m report.send_line --date %s）", args.date)
        elif args.no_line:
            logger.info("LINE ：— 已跳過（--no-line）")
        logger.info("Log ：logs/daily_%s.log", args.date)
        logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    except SystemExit:
        # sys.exit() 是正常流程控制，不視為崩潰，讓它正常穿透
        raise

    except Exception as exc:
        # ── 全局崩潰防護：未預期例外 ─────────────────────────────────────────
        tb_str = traceback.format_exc()
        elapsed = time.perf_counter() - t0

        logger.critical("")
        logger.critical("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        logger.critical("[CRASH] 未捕捉例外！系統異常終止")
        logger.critical("[CRASH] 失敗步驟  : %s", _current_step)
        logger.critical("[CRASH] 錯誤類型  : %s", type(exc).__name__)
        logger.critical("[CRASH] 錯誤訊息  : %s", exc)
        logger.critical("[CRASH] 執行耗時  : %.1f 秒", elapsed)
        logger.critical("[CRASH] Traceback :\n%s", tb_str)
        logger.critical("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

        # 嘗試發送崩潰警告 Email（此函式本身不拋例外）
        try:
            from report.send_email import send_crash_alert
            send_crash_alert(
                failed_step=_current_step,
                exc=exc,
                tb_str=tb_str,
                run_date=args.date,
            )
        except Exception as alert_exc:
            # send_crash_alert 本身不應拋出，但以防萬一記錄到 log
            logger.error("[CRASH_ALERT_INTERNAL] 崩潰警告發送器本身失敗：%s", alert_exc)

        # exit code 2 = 未捕捉崩潰（區分正常 step 失敗的 exit 1）
        sys.exit(2)


if __name__ == "__main__":
    main()
