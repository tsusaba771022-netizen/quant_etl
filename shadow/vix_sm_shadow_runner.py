"""
VIX SM Shadow Mode Runner
--------------------------
Runs V2-WideHyst (baseline) + V6-NoConfirm + V6b-CooldownPatch in parallel.
One CSV row per config per trading day appended to shadow_log.csv.
Persisted FSM state in shadow_state.json (per-config, isolated).

Isolation contract:
  - No production DB writes
  - No LINE / report / Flex output
  - All artefacts local to shadow/ directory

Usage:
  python -m shadow.vix_sm_shadow_runner              # process up to today
  python -m shadow.vix_sm_shadow_runner --end 2026-05-01
  python -m shadow.vix_sm_shadow_runner --bootstrap  # force re-bootstrap
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

# Project root on path
sys.path.insert(0, str(Path(__file__).parent.parent))

from engine.vix_sm import (
    VixSmConfig, VixSmOutput, VixState, STATE_RANK, evaluate_next_state,
)
from calibration.vix_data import load_vix

logger = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────────────────────

SHADOW_DIR = Path(__file__).parent
STATE_FILE = SHADOW_DIR / "vix_sm_shadow_state.json"
LOG_FILE   = SHADOW_DIR / "vix_sm_shadow_log.csv"

# ── Shadow configs (the three 6/6 eligible candidates) ─────────────────────────

SHADOW_CONFIGS: Dict[str, VixSmConfig] = {
    "V2-WideHyst": VixSmConfig(
        caution_exit         = 16.0,
        defensive_exit       = 22.0,
        panic_exit           = 28.0,
        upgrade_confirm_days = 1,
    ),
    "V6-NoConfirm": VixSmConfig(
        caution_exit         = 16.0,
        defensive_exit       = 22.0,
        panic_exit           = 28.0,
        upgrade_confirm_days = 0,
    ),
    "V6b-CooldownPatch": VixSmConfig(
        caution_exit            = 16.0,
        defensive_exit          = 22.0,
        panic_exit              = 28.0,
        upgrade_confirm_days    = 0,
        downgrade_cooldown_days = 2,
    ),
}

BASELINE = "V2-WideHyst"

# Bootstrap start: full history for initial state computation
_BOOTSTRAP_START = "2018-01-01"
# Days of VIX history kept in state JSON (for shock/accel on restart)
_VIX_BUFFER_DAYS = 5
# Days of history to load for pct_rank window (252 td ≈ 365 cal)
_HISTORY_LOAD_DAYS = 400

# ── Log columns ────────────────────────────────────────────────────────────────

LOG_COLUMNS = [
    "date",
    "config_name",
    "vix_close",
    "vix_pct_rank",
    "shock_1d",
    "accel_3d",
    "prev_state",
    "new_state",
    "transitioned",
    "driver",
    "driver_detail",
    "days_in_state",
    "cooldown_remaining",
    "is_divergence_vs_baseline",
    "divergence_type",
]


# ── Driver classification ──────────────────────────────────────────────────────

def _classify_driver(out: VixSmOutput) -> str:
    """Map FSM output to a stable driver category string."""
    reason = out.reason.lower()
    if "fast-track" in reason:
        return "fast_track"
    if "pending" in reason:
        return "pending"
    if "min_hold not met" in reason:
        return "min_hold_gate"
    if out.transitioned:
        if STATE_RANK[out.state] > STATE_RANK[out.prev_state]:
            return "upgrade"
        return "downgrade"
    return "hold"


# ── VIX metrics ────────────────────────────────────────────────────────────────

def _pct_rank(vix_val: float, history: List[float], window: int = 252) -> float:
    """Percentile rank of vix_val within trailing window (0.0–100.0)."""
    lookback = history[-window:] if len(history) >= window else history
    if not lookback:
        return 50.0
    return round(100.0 * sum(1 for v in lookback if v <= vix_val) / len(lookback), 1)


# ── State JSON I/O ─────────────────────────────────────────────────────────────

def _load_state() -> dict | None:
    if not STATE_FILE.exists():
        return None
    with STATE_FILE.open(encoding="utf-8") as f:
        return json.load(f)


def _save_state(state: dict) -> None:
    with STATE_FILE.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    logger.debug("State saved -> %s", STATE_FILE)


# ── Bootstrap ─────────────────────────────────────────────────────────────────

def _bootstrap(end_date: str, csv_path: str | None = None) -> dict:
    """
    Run all configs over full history to compute initial FSM state.
    Returns state dict keyed by config name.
    """
    logger.info("Bootstrapping shadow state %s -> %s ...", _BOOTSTRAP_START, end_date)
    vix = load_vix(_BOOTSTRAP_START, end_date, csv_path=csv_path)
    state: dict = {}

    for name, cfg in SHADOW_CONFIGS.items():
        cur_state = VixState.NORMAL
        hold      = 1
        consec    = 0
        cooldown  = 0
        buf: List[float] = []

        for _, v in vix.items():
            buf.append(float(v))
            out      = evaluate_next_state(cur_state, buf, hold, cfg, consec, cooldown)
            cur_state = out.state
            hold      = out.days_in_state
            consec    = out.consecutive_above
            cooldown  = out.cooldown_remaining

        state[name] = {
            "state":              cur_state,
            "hold_days":          hold,
            "consecutive_above":  consec,
            "cooldown_remaining": cooldown,
            "last_date":          str(vix.index[-1].date()),
            "vix_buffer":         [round(float(v), 2)
                                   for v in vix.values[-_VIX_BUFFER_DAYS:]],
        }
        logger.info("  %s  state=%-12s hold=%2d  last=%s",
                    name, cur_state, hold, state[name]["last_date"])

    return state


# ── Core per-day processor ─────────────────────────────────────────────────────

def _process_day(
    trade_date:  pd.Timestamp,
    vix_history: List[float],   # all VIX closes oldest..today (today = [-1])
    state:       dict,
) -> Tuple[List[dict], dict]:
    """
    Evaluate all shadow configs for one trading day.

    Parameters
    ----------
    trade_date  : the trading day being evaluated
    vix_history : VIX close series ending with today's value
    state       : mutable per-config FSM state dict (updated in-place)

    Returns
    -------
    (log_rows, updated_state)
    """
    vix_val  = vix_history[-1]
    shock_1d = round(vix_history[-1] - vix_history[-2], 2) if len(vix_history) >= 2 else 0.0
    accel_3d = round(vix_history[-1] - vix_history[-4], 2) if len(vix_history) >= 4 else 0.0
    pct_rank = _pct_rank(vix_val, vix_history)

    # Evaluate all configs first so baseline is known before computing divergence
    outputs: Dict[str, VixSmOutput] = {}
    for name, cfg in SHADOW_CONFIGS.items():
        s   = state[name]
        out = evaluate_next_state(
            s["state"], vix_history, s["hold_days"], cfg,
            s["consecutive_above"], s["cooldown_remaining"],
        )
        outputs[name] = out

    baseline_out    = outputs[BASELINE]
    baseline_driver = _classify_driver(baseline_out)
    date_str        = str(trade_date.date())

    log_rows: List[dict] = []

    for name, out in outputs.items():
        s      = state[name]
        driver = _classify_driver(out)

        # ── Divergence computation ─────────────────────────────────────────
        if name == BASELINE:
            is_div   = 0
            div_type = ""
        else:
            state_div  = out.state      != baseline_out.state
            trans_div  = out.transitioned != baseline_out.transitioned
            ft_div     = (driver == "fast_track") != (baseline_driver == "fast_track")
            parts = []
            if state_div:  parts.append("state")
            if trans_div:  parts.append("transition")
            if ft_div:     parts.append("fast_track")
            is_div   = int(bool(parts))
            div_type = "+".join(parts)

        log_rows.append({
            "date":                      date_str,
            "config_name":               name,
            "vix_close":                 round(vix_val, 2),
            "vix_pct_rank":              pct_rank,
            "shock_1d":                  shock_1d,
            "accel_3d":                  accel_3d,
            "prev_state":                s["state"],
            "new_state":                 out.state,
            "transitioned":              int(out.transitioned),
            "driver":                    driver,
            "driver_detail":             out.reason,
            "days_in_state":             out.days_in_state,
            "cooldown_remaining":        out.cooldown_remaining,
            "is_divergence_vs_baseline": is_div,
            "divergence_type":           div_type,
        })

        # Update persisted state
        state[name] = {
            "state":              out.state,
            "hold_days":          out.days_in_state,
            "consecutive_above":  out.consecutive_above,
            "cooldown_remaining": out.cooldown_remaining,
            "last_date":          date_str,
            "vix_buffer":         (s["vix_buffer"] + [round(vix_val, 2)])[-_VIX_BUFFER_DAYS:],
        }

    return log_rows, state


# ── Log I/O ────────────────────────────────────────────────────────────────────

def _ensure_log_header() -> None:
    if not LOG_FILE.exists():
        with LOG_FILE.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(LOG_COLUMNS)
        logger.info("Created log -> %s", LOG_FILE)


def _append_rows(rows: List[dict]) -> None:
    with LOG_FILE.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=LOG_COLUMNS, extrasaction="ignore")
        for row in rows:
            w.writerow(row)


# ── Main ───────────────────────────────────────────────────────────────────────

def main(argv=None):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        stream=sys.stdout,
    )

    parser = argparse.ArgumentParser(description="VIX SM Shadow Mode Runner")
    parser.add_argument(
        "--end",
        default=None,
        help="Last date to process (YYYY-MM-DD). Default: latest VIX available.",
    )
    parser.add_argument(
        "--csv",
        default=None,
        help="Override VIX CSV path (default: calibration/data/vix_history.csv).",
    )
    parser.add_argument(
        "--bootstrap",
        action="store_true",
        help="Force re-bootstrap even if shadow_state.json exists.",
    )
    args = parser.parse_args(argv)

    # ── Determine run end date ──
    from datetime import date as _date
    end_date = args.end or _date.today().strftime("%Y-%m-%d")

    # ── Bootstrap if needed ──
    if args.bootstrap or _load_state() is None:
        state = _bootstrap(end_date, csv_path=args.csv)
        _save_state(state)
        _ensure_log_header()
        logger.info("Bootstrap complete. Run again (without --bootstrap) to process new days.")
        return

    state = _load_state()

    # ── Determine dates to catch up ──
    last_date_str = state[BASELINE]["last_date"]
    last_date     = pd.Timestamp(last_date_str)

    fetch_start = (last_date - pd.Timedelta(days=_HISTORY_LOAD_DAYS)).strftime("%Y-%m-%d")
    full_vix    = load_vix(fetch_start, end_date, csv_path=args.csv)

    new_dates = full_vix[full_vix.index > last_date]
    if new_dates.empty:
        logger.info("Shadow state already up to date (%s). Nothing to process.", last_date_str)
        return

    logger.info(
        "Processing %d new trading day(s): %s -> %s",
        len(new_dates),
        str(new_dates.index[0].date()),
        str(new_dates.index[-1].date()),
    )

    _ensure_log_header()
    all_rows: List[dict] = []

    for trade_date, _ in new_dates.items():
        # Build VIX history up to and including this trading day
        history = [float(v) for v in full_vix[full_vix.index <= trade_date].values]
        rows, state = _process_day(trade_date, history, state)
        all_rows.extend(rows)

        # Log any transitions immediately
        for r in rows:
            if r["transitioned"]:
                div_note = f"  [DIV:{r['divergence_type']}]" if r["is_divergence_vs_baseline"] else ""
                logger.info(
                    "  [%s] %-18s %s -> %s  (%s)%s",
                    r["date"], r["config_name"],
                    r["prev_state"], r["new_state"],
                    r["driver"], div_note,
                )

    _append_rows(all_rows)
    _save_state(state)

    n_days  = len(new_dates)
    n_trans = sum(1 for r in all_rows if r["transitioned"])
    n_div   = sum(1 for r in all_rows if r["is_divergence_vs_baseline"])
    logger.info(
        "Done. %d day(s) processed  |  %d transition(s)  |  %d divergence row(s)",
        n_days, n_trans, n_div,
    )
    logger.info("Log   -> %s", LOG_FILE)
    logger.info("State -> %s", STATE_FILE)


if __name__ == "__main__":
    main()
