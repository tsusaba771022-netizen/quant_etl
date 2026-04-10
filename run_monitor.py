"""
Tripwire Monitor — 主進入點
----------------------------
事件觸發式風險警報，每 15 分鐘檢查一次關鍵 z-score 指標。
不影響 baseline 買賣策略、不重跑 daily report pipeline。

執行方式:
  python run_monitor.py                        # 單次執行
  python run_monitor.py --daemon               # 常駐模式（每 15 分鐘）
  python run_monitor.py --daemon --interval-minutes 30
  python run_monitor.py --force-initial-alert  # 第一次執行也評估 Tripwire

Log 位置:
  logs/monitor_YYYY-MM-DD.log

State 位置:
  state/monitor_state.json
"""
from __future__ import annotations

import argparse
import io
import logging
import sys
import time
import traceback
from datetime import date, datetime, timezone
from pathlib import Path

# ── Windows UTF-8 fix ─────────────────────────────────────────────────────────
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

ROOT       = Path(__file__).parent
LOG_DIR    = ROOT / "logs"
STATE_DIR  = ROOT / "state"
LOG_DIR.mkdir(exist_ok=True)
STATE_DIR.mkdir(exist_ok=True)


# ── Logging setup ─────────────────────────────────────────────────────────────

def _setup_logging() -> logging.Logger:
    log_path = LOG_DIR / f"monitor_{date.today()}.log"
    fmt      = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"
    datefmt  = "%Y-%m-%d %H:%M:%S"

    handlers: list = [logging.StreamHandler(sys.stdout)]
    try:
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))
    except Exception:
        pass

    logging.basicConfig(
        level   = logging.INFO,
        format  = fmt,
        datefmt = datefmt,
        handlers= handlers,
        force   = True,
    )
    return logging.getLogger("monitor")


# ── Single cycle ───────────────────────────────────────────────────────────────

def run_one_cycle(force_initial_alert: bool = False) -> None:
    """
    Execute one complete monitor cycle:
      1. Connect to DB
      2. Load z-scores
      3. Evaluate Tripwire
      4. Send alert if triggered
      5. Update state

    All errors are caught and logged — never raises.
    """
    from etl.db import get_connection
    from monitor.state_manager import StateManager, is_escalation
    from monitor.tripwire import run_monitor_cycle
    from report.tripwire_line import send_tripwire_alert

    log = logging.getLogger("monitor.cycle")
    log.info("─── monitor cycle started ─────────────────────────────────────")

    state_mgr = StateManager()

    health = None
    try:
        with get_connection() as conn:
            result = run_monitor_cycle(conn, state_mgr, force_initial_alert)
            # 資料健康度（缺值不中斷 monitor 主流程）
            try:
                from monitor.data_health import check_indicator_health
                health = check_indicator_health(conn)
                log.info("data_health  overall=%s", health.overall)
            except Exception as exc:
                log.warning("data_health check failed (monitor continues): %s", exc)
    except Exception as exc:
        log.error("DB connection / tripwire evaluation failed: %s", exc)
        log.debug(traceback.format_exc())
        return   # Next cycle will retry; daemon stays alive

    v = result.values
    log.info(
        "Risk light: %s  (prev: %s)  source: %s",
        result.risk_light,
        result.prev_light or "UNKNOWN",
        v.source,
    )

    # ── First run: record baseline ────────────────────────────────────────────
    if state_mgr.is_first_run() and not force_initial_alert:
        state_mgr.record_baseline(result.risk_light, v.as_dict())
        log.info("First run — baseline recorded, no alert sent")
        return

    # ── Indicators unavailable: skip ──────────────────────────────────────────
    if result.skipped:
        log.warning("Cycle skipped: %s", result.skip_reason)
        # Still record the check so history is kept
        state_mgr.record_check(result.risk_light, v.as_dict())
        return

    # ── No trigger ────────────────────────────────────────────────────────────
    if not result.triggered:
        log.info("TRIPWIRE_ALERT_SKIPPED_NO_CHANGE  light=%s", result.risk_light)
        state_mgr.record_check(result.risk_light, v.as_dict())
        return

    # Cooldown suppression is already applied in run_monitor_cycle()
    # If triggered=True here, we should send.

    # ── Send alert ────────────────────────────────────────────────────────────
    trigger_reason_str = " | ".join(result.trigger_reasons) or result.alert_type or ""
    log.info(
        "Tripwire triggered  alert_type=%s  reason=%s",
        result.alert_type,
        trigger_reason_str,
    )

    sent = False
    try:
        sent = send_tripwire_alert(
            risk_light      = result.risk_light,
            prev_light      = result.prev_light,
            alert_type      = result.alert_type,
            trigger_reasons = result.trigger_reasons,
            vix_z           = v.vix_z,
            hy_z            = v.hy_z,
            spread_z        = v.spread_z,
            health          = health,
        )
    except Exception as exc:
        log.error("send_tripwire_alert raised unexpectedly: %s", exc)
        log.debug(traceback.format_exc())

    if sent:
        log.info("TRIPWIRE_ALERT_SENT  light=%s  type=%s", result.risk_light, result.alert_type)
        state_mgr.record_alert(
            alert_type     = result.alert_type or "UNKNOWN",
            risk_light     = result.risk_light,
            values         = v.as_dict(),
            trigger_reason = trigger_reason_str,
        )
    else:
        log.error("Alert delivery failed — state not updated to prevent lost alerts")
        # Still record the check so history advances
        state_mgr.record_check(result.risk_light, v.as_dict())


# ── Daemon loop ────────────────────────────────────────────────────────────────

def run_daemon(interval_minutes: int, force_initial_alert: bool) -> None:
    """
    Continuous daemon loop.
    - Runs run_one_cycle() every interval_minutes
    - Single-cycle failure never kills the daemon
    - Heartbeat log every cycle so you can verify the daemon is alive
    """
    log = logging.getLogger("monitor.daemon")
    log.info(
        "Daemon started  interval=%d min  pid=%d",
        interval_minutes, _getpid(),
    )

    # Run immediately on start
    _safe_cycle(log, force_initial_alert)

    interval_sec = interval_minutes * 60
    while True:
        log.info(
            "Heartbeat — next cycle in %d minutes  [%s UTC]",
            interval_minutes,
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        )
        time.sleep(interval_sec)
        _safe_cycle(log, force_initial_alert)


def _safe_cycle(log: logging.Logger, force_initial_alert: bool) -> None:
    """Run one cycle; catch any uncaught exception so the daemon never dies."""
    try:
        run_one_cycle(force_initial_alert)
    except Exception as exc:
        log.error("Unhandled exception in cycle (daemon continues): %s", exc)
        log.debug(traceback.format_exc())


def _getpid() -> int:
    try:
        import os
        return os.getpid()
    except Exception:
        return -1


# ── CLI ────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Tripwire 風險監控 — 事件觸發式 LINE 警報"
    )
    p.add_argument(
        "--daemon",
        action="store_true",
        help="常駐模式：持續每 interval-minutes 分鐘執行一次",
    )
    p.add_argument(
        "--interval-minutes",
        type=int,
        default=15,
        metavar="N",
        help="daemon 模式的執行間隔（分鐘，預設 15）",
    )
    p.add_argument(
        "--force-initial-alert",
        action="store_true",
        help="第一次啟動時也進行 Tripwire 評估（預設只記錄 baseline）",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    log  = _setup_logging()

    log.info(
        "run_monitor  mode=%s  interval=%d  force_initial=%s",
        "daemon" if args.daemon else "once",
        args.interval_minutes,
        args.force_initial_alert,
    )

    try:
        if args.daemon:
            run_daemon(
                interval_minutes    = args.interval_minutes,
                force_initial_alert = args.force_initial_alert,
            )
        else:
            run_one_cycle(force_initial_alert=args.force_initial_alert)

    except KeyboardInterrupt:
        log.info("Monitor stopped by user (KeyboardInterrupt)")
        sys.exit(0)
    except Exception as exc:
        log.critical("Monitor crashed unexpectedly: %s", exc)
        log.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
