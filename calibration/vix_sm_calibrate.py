"""
VIX State Machine Calibration Script
--------------------------------------
Evaluates VixSmConfig (candidate thresholds) against historical VIX data.
No production DB access. No production wiring.

Usage:
    python -m calibration.vix_sm_calibrate
    python -m calibration.vix_sm_calibrate --start 2018-01-01 --end 2026-04-11
    python -m calibration.vix_sm_calibrate --csv path/to/vix.csv

Outputs:
    calibration/output/vix_sm_trace_{YYYY-MM-DD}.csv     — daily state trace
    calibration/output/vix_sm_metrics_{YYYY-MM-DD}.txt   — full metrics report
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from engine.vix_sm import VixState, VixSmConfig, STATE_RANK, evaluate_next_state
from calibration.vix_data import load_vix

logger = logging.getLogger(__name__)

# ── Module-level constants ────────────────────────────────────────────────────

# Differentiated false alarm lookback windows (trading days after state entry)
# Caution   : brief spikes above 20 that resolve quickly = noisy signal
# Defensive : broader window; genuine stress should persist >= 7 days
# Panic(thr): threshold-based entry; quick VIX reversal = overreaction
# Panic(ft) : fast-track entry via 1d shock; more prone to noise, stricter window
FA_WINDOWS: Dict[str, int] = {
    "caution":          5,
    "defensive":        7,
    "panic_threshold":  5,
    "panic_fast_track": 3,
}

# Flapping detection window (trading days)
FLAP_WINDOW = 5

# VIX history buffer size passed to evaluate_next_state
_HIST_WINDOW = 10

# Named calibration episodes (design doc §7)
EPISODES: List[Dict[str, str]] = [
    {"name": "2018-Q4 Fed panic",   "start": "2018-09-01", "end": "2018-12-31"},
    {"name": "2020-COVID crash",     "start": "2020-02-01", "end": "2020-04-30"},
    {"name": "2022-Bear market",     "start": "2022-01-01", "end": "2022-10-31"},
    {"name": "2023-03 SVB crisis",   "start": "2023-02-15", "end": "2023-04-15"},
    {"name": "2024-08 Japan shock",  "start": "2024-07-15", "end": "2024-09-15"},
]


# ── FSM runner ────────────────────────────────────────────────────────────────

def run_sm_on_series(
    vix_series: pd.Series,
    cfg:         VixSmConfig,
    initial_state: str = VixState.NORMAL,
) -> pd.DataFrame:
    """
    Run the VIX state machine over a historical VIX series.

    Returns
    -------
    pd.DataFrame indexed by date with columns:
      vix, state, prev_state, transitioned, days_in_state, vix_enum,
      reason, fast_track
    """
    records: List[Dict[str, Any]] = []
    state    = initial_state
    hold     = 1
    buf:     List[float] = []
    consec   = 0   # consecutive days above next-level threshold
    cooldown = 0   # days remaining in downgrade cooldown

    for date_val, vix_val in vix_series.items():
        if pd.isna(vix_val):
            logger.warning("Skipping NaN VIX on %s", date_val)
            continue

        buf.append(float(vix_val))
        if len(buf) > _HIST_WINDOW:
            buf = buf[-_HIST_WINDOW:]

        out = evaluate_next_state(state, list(buf), hold, cfg, consec, cooldown)
        records.append({
            "date":              date_val,
            "vix":               float(vix_val),
            "state":             out.state,
            "prev_state":        out.prev_state,
            "transitioned":      out.transitioned,
            "days_in_state":     out.days_in_state,
            "vix_enum":          out.vix_enum,
            "reason":            out.reason,
            "fast_track":        "fast-track" in out.reason,
            "consecutive_above": out.consecutive_above,
            "cooldown_remaining": out.cooldown_remaining,
        })
        state    = out.state
        hold     = out.days_in_state
        consec   = out.consecutive_above
        cooldown = out.cooldown_remaining

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records).set_index("date")
    df.index = pd.to_datetime(df.index)
    return df


# ── Metrics ───────────────────────────────────────────────────────────────────

def compute_state_distribution(trace: pd.DataFrame) -> pd.DataFrame:
    """Total days and % per state over full history."""
    total = len(trace)
    counts = trace["state"].value_counts()
    rows = []
    for s in VixState.ALL:
        n = int(counts.get(s, 0))
        rows.append({
            "state": s,
            "days":  n,
            "pct":   round(100.0 * n / total, 1) if total else 0.0,
        })
    return pd.DataFrame(rows)


def compute_time_in_state_distribution(trace: pd.DataFrame) -> pd.DataFrame:
    """
    For each state, collect all consecutive run lengths and compute:
    count, mean, p25, median, p75, max.
    """
    if trace.empty:
        return pd.DataFrame()

    # Run-length encoding
    runs: List[Dict] = []
    prev_state = trace["state"].iloc[0]
    run_len    = 1

    for s in trace["state"].iloc[1:]:
        if s == prev_state:
            run_len += 1
        else:
            runs.append({"state": prev_state, "dur": run_len})
            prev_state = s
            run_len    = 1
    runs.append({"state": prev_state, "dur": run_len})

    runs_df = pd.DataFrame(runs)
    stats   = []
    for s in VixState.ALL:
        sub = runs_df.loc[runs_df["state"] == s, "dur"]
        if sub.empty:
            stats.append({
                "state": s, "count": 0,
                "mean": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0,
            })
        else:
            stats.append({
                "state":  s,
                "count":  int(len(sub)),
                "mean":   round(float(sub.mean()), 1),
                "p25":    round(float(sub.quantile(0.25)), 1),
                "median": round(float(sub.median()), 1),
                "p75":    round(float(sub.quantile(0.75)), 1),
                "max":    int(sub.max()),
            })
    return pd.DataFrame(stats)


def compute_false_alarms(
    trace: pd.DataFrame, cfg: VixSmConfig
) -> Dict[str, int]:
    """
    State-differentiated false alarm counts.

    Caution   : SM enters Caution → reverts to Normal within 5 days
    Defensive : SM enters Defensive → reverts to Caution/Normal within 7 days
                (only if Panic was NOT reached in that window)
    Panic/thr : VIX drops below panic_exit within 5 days of threshold entry
    Panic/ft  : VIX drops below panic_exit within 3 days of fast-track entry
    """
    fa = {k: 0 for k in FA_WINDOWS}

    if trace.empty:
        return fa

    entries = trace[(trace["transitioned"]) & (trace["state"] != VixState.NORMAL)]

    for entry_date, row in entries.iterrows():
        s          = row["state"]
        ft         = bool(row["fast_track"])
        entry_iloc = trace.index.get_indexer([entry_date])[0]
        if entry_iloc < 0:
            continue

        if s == VixState.CAUTION:
            win      = FA_WINDOWS["caution"]
            end_iloc = min(entry_iloc + win, len(trace) - 1)
            window   = trace.iloc[entry_iloc : end_iloc + 1]
            if (window["state"] == VixState.NORMAL).any():
                fa["caution"] += 1

        elif s == VixState.DEFENSIVE:
            win      = FA_WINDOWS["defensive"]
            end_iloc = min(entry_iloc + win, len(trace) - 1)
            window   = trace.iloc[entry_iloc : end_iloc + 1]
            hit_panic  = (window["state"] == VixState.PANIC).any()
            hit_revert = (window["state"].isin(
                [VixState.CAUTION, VixState.NORMAL]
            )).any()
            if hit_revert and not hit_panic:
                fa["defensive"] += 1

        elif s == VixState.PANIC:
            key      = "panic_fast_track" if ft else "panic_threshold"
            win      = FA_WINDOWS[key]
            end_iloc = min(entry_iloc + win, len(trace) - 1)
            window   = trace.iloc[entry_iloc : end_iloc + 1]
            # False alarm: VIX drops below panic_exit within window
            if (window["vix"] < cfg.panic_exit).any():
                fa[key] += 1

    return fa


def compute_late_detection(
    trace: pd.DataFrame, cfg: VixSmConfig
) -> pd.DataFrame:
    """
    For each threshold (caution/defensive/panic), find upward VIX crossings where
    SM hasn't yet entered the target state, and measure detection lag.

    Detection success definition (revised 2026-04-12):
      "caution"  row : SM first reaches Caution or higher  (rank >= CAUTION)
      "defensive" row: SM first reaches Defensive or higher (rank >= DEFENSIVE)
      "panic"    row : SM first reaches Panic               (rank >= PANIC)

    Rationale for "defensive" row change:
      When fast-track fires from Caution, the SM jumps directly to Panic
      (bypassing Defensive).  Under the old definition (target == Defensive
      exactly) this was counted as a miss / long lag until SM descended back
      through Defensive on the way down.  That misrepresents the FSM — entering
      Panic means the risk level was detected and exceeded Defensive, so the
      event should be marked detected with lag = (date reached Panic) - (cross
      date), not with the return-path lag.

    Returns DataFrame with columns:
      threshold, events, detected, lag_mean, lag_p50, lag_max
    """
    if trace.empty:
        return pd.DataFrame()

    thresholds = [
        ("caution",   cfg.caution_entry,   VixState.CAUTION),
        ("defensive", cfg.defensive_entry,  VixState.DEFENSIVE),
        ("panic",     cfg.panic_entry,      VixState.PANIC),
    ]
    rows = []

    for label, thr, target in thresholds:
        crossings: List[int] = []

        for i in range(1, len(trace)):
            prev_vix = trace["vix"].iloc[i - 1]
            curr_vix = trace["vix"].iloc[i]
            curr_s   = trace["state"].iloc[i]

            # Upward crossing where SM is below target
            if (prev_vix < thr
                    and curr_vix >= thr
                    and STATE_RANK[curr_s] < STATE_RANK[target]):
                crossings.append(i)

        if not crossings:
            rows.append({
                "threshold": label, "events": 0, "detected": 0,
                "lag_mean": "—", "lag_p50": "—", "lag_max": "—",
            })
            continue

        lags: List[int] = []
        for ci in crossings:
            # Look for SM entry into target-or-higher within 30 trading days.
            # "defensive" target: rank >= DEFENSIVE counts (Panic included).
            # This ensures fast-track escalations are not penalised as misses.
            end_i   = min(ci + 30, len(trace))
            window  = trace.iloc[ci:end_i]
            entries = window[
                (window["transitioned"])
                & (window["state"].map(STATE_RANK) >= STATE_RANK[target])
            ]
            if not entries.empty:
                lag = (entries.index[0] - trace.index[ci]).days
                lags.append(lag)

        rows.append({
            "threshold": label,
            "events":    len(crossings),
            "detected":  len(lags),
            "lag_mean":  round(float(np.mean(lags)), 1) if lags else "—",
            "lag_p50":   int(np.median(lags))           if lags else "—",
            "lag_max":   int(max(lags))                 if lags else "—",
        })

    return pd.DataFrame(rows)


def compute_sticky_total(trace: pd.DataFrame, cfg: VixSmConfig) -> int:
    """
    Total trading days where SM is in an elevated state but VIX is already
    below the exit threshold — proxy for 'sticky too long' across all 2079 days.
    """
    if trace.empty:
        return 0
    mask = (
        ((trace["state"] == VixState.CAUTION)   & (trace["vix"] < cfg.caution_exit)) |
        ((trace["state"] == VixState.DEFENSIVE)  & (trace["vix"] < cfg.defensive_exit)) |
        ((trace["state"] == VixState.PANIC)      & (trace["vix"] < cfg.panic_exit))
    )
    return int(mask.sum())


def compute_flapping(trace: pd.DataFrame) -> int:
    """
    Count A→B→A back-and-forth transitions within FLAP_WINDOW trading days.
    """
    if trace.empty:
        return 0

    trans = trace[trace["transitioned"]].copy()
    if len(trans) < 2:
        return 0

    dates   = trans.index.tolist()
    states  = trans["state"].tolist()
    prevs   = trans["prev_state"].tolist()
    flaps   = 0

    for i in range(len(dates) - 1):
        gap = (dates[i + 1] - dates[i]).days
        # transition i: A→B  followed by transition i+1: B→A within window
        if gap <= FLAP_WINDOW and states[i + 1] == prevs[i] and prevs[i + 1] == states[i]:
            flaps += 1

    return flaps


def compute_per_episode(
    trace: pd.DataFrame, cfg: VixSmConfig
) -> pd.DataFrame:
    """Per-episode summary for the 5 named calibration periods."""
    rows = []
    for ep in EPISODES:
        try:
            ep_trace = trace.loc[ep["start"]: ep["end"]].copy()
        except Exception:
            ep_trace = pd.DataFrame()

        if ep_trace.empty:
            rows.append({"episode": ep["name"], "note": "no data in range"})
            continue

        peak_vix  = round(float(ep_trace["vix"].max()), 1)
        peak_date = ep_trace["vix"].idxmax()
        max_state = max(
            ep_trace["state"].unique(),
            key=lambda s: STATE_RANK.get(s, 0),
        )
        state_days = ep_trace["state"].value_counts().to_dict()

        caution_lag   = _episode_detection_lag(ep_trace, cfg.caution_entry,   VixState.CAUTION)
        panic_lag     = _episode_detection_lag(ep_trace, cfg.panic_entry,     VixState.PANIC)
        overshoot     = _episode_hold_overshoot(ep_trace, cfg)

        rows.append({
            "episode":          ep["name"],
            "peak_vix":         peak_vix,
            "peak_date":        str(peak_date.date()) if hasattr(peak_date, "date") else str(peak_date)[:10],
            "max_state":        max_state,
            "caution_days":     int(state_days.get(VixState.CAUTION,   0)),
            "defensive_days":   int(state_days.get(VixState.DEFENSIVE, 0)),
            "panic_days":       int(state_days.get(VixState.PANIC,     0)),
            "caution_lag_d":    caution_lag,
            "panic_lag_d":      panic_lag,
            "overshoot_d":      overshoot,
        })
    return pd.DataFrame(rows)


def _episode_detection_lag(
    ep: pd.DataFrame, threshold: float, target: str
) -> Any:
    """
    Calendar days from first VIX >= threshold to first SM entry into target.
    Returns 0 (immediate), positive int (lag), -1 (VIX never crossed), -2 (SM never entered).
    """
    crossed = ep[ep["vix"] >= threshold]
    if crossed.empty:
        return -1

    first_cross = crossed.index[0]
    sm_entry    = ep[(ep["state"] == target) & (ep.index >= first_cross)]
    if sm_entry.empty:
        return -2

    return int((sm_entry.index[0] - first_cross).days)


def _episode_hold_overshoot(ep: pd.DataFrame, cfg: VixSmConfig) -> int:
    """
    Total days where SM is in an elevated state but VIX already sits below
    the exit threshold — i.e., would have exited without min_hold constraint.
    """
    EXIT = {
        VixState.CAUTION:   cfg.caution_exit,
        VixState.DEFENSIVE: cfg.defensive_exit,
        VixState.PANIC:     cfg.panic_exit,
    }
    overshoot = 0
    for _, row in ep.iterrows():
        s = row["state"]
        if s != VixState.NORMAL and row["vix"] < EXIT[s]:
            overshoot += 1
    return overshoot


# ── Report formatter ──────────────────────────────────────────────────────────

def _fmt(report: Dict) -> str:
    cfg      = report["cfg"]
    lines    = [
        "=" * 72,
        "VIX STATE MACHINE — CALIBRATION REPORT",
        "=" * 72,
        f"Config : VixSmConfig(",
        f"           caution_entry={cfg.caution_entry},  caution_exit={cfg.caution_exit}",
        f"           defensive_entry={cfg.defensive_entry}, defensive_exit={cfg.defensive_exit}",
        f"           panic_entry={cfg.panic_entry},    panic_exit={cfg.panic_exit}",
        f"           caution_min_hold={cfg.caution_min_hold}, "
        f"defensive_min_hold={cfg.defensive_min_hold}, "
        f"panic_min_hold={cfg.panic_min_hold}",
        f"           panic_shock_1d={cfg.panic_shock_1d}, "
        f"panic_accel_3d={cfg.panic_accel_3d})",
        f"Period : {report['start']} ~ {report['end']}  "
        f"({report['n_days']} trading days)",
        "",
        "── State Distribution ──────────────────────────────────────────────",
    ]
    for _, r in report["state_dist"].iterrows():
        lines.append(f"  {r['state']:<12}: {r['days']:>5} days  ({r['pct']:>5.1f}%)")

    lines += [
        "",
        "── Time-in-State Distribution (consecutive run lengths, trading days) ──",
        f"  {'State':<12} {'#Runs':>6} {'Mean':>7} {'P25':>6} {'Median':>8} "
        f"{'P75':>6} {'Max':>5}",
    ]
    for _, r in report["time_dist"].iterrows():
        lines.append(
            f"  {r['state']:<12} {r['count']:>6} {r['mean']:>7.1f} "
            f"{r['p25']:>6.1f} {r['median']:>8.1f} {r['p75']:>6.1f} {r['max']:>5}"
        )

    fa = report["false_alarms"]
    lines += [
        "",
        "── False Alarms (state-differentiated) ─────────────────────────────",
        f"  Caution    reverts→Normal within 5d              : {fa['caution']:>4}",
        f"  Defensive  reverts<Panic within 7d               : {fa['defensive']:>4}",
        f"  Panic/thr  VIX<panic_exit within 5d of entry     : {fa['panic_threshold']:>4}",
        f"  Panic/ft   VIX<panic_exit within 3d of ft-entry  : {fa['panic_fast_track']:>4}",
    ]

    lines += [
        "",
        "── Late Detection (upward VIX crossing → SM entry, calendar days) ──",
        f"  {'Threshold':<12} {'Events':>7} {'Detected':>9} "
        f"{'Lag mean':>9} {'Lag p50':>8} {'Lag max':>8}",
    ]
    for _, r in report["late_det"].iterrows():
        detected = r.get("detected", 0)
        lines.append(
            f"  {r['threshold']:<12} {r['events']:>7} {detected:>9} "
            f"{str(r['lag_mean']):>9} {str(r['lag_p50']):>8} {str(r['lag_max']):>8}"
        )

    lines += [
        "",
        "── Sticky Total (SM elevated, VIX already below exit threshold) ─────",
        f"  Sticky days (global, all 2079d): {report['sticky_total']}",
        "",
        f"── Flapping (A→B→A within {FLAP_WINDOW}d) "
        f"─────────────────────────────────────",
        f"  Flapping transitions: {report['flapping']}",
        "",
        "── Per-Episode Summary ──────────────────────────────────────────────",
        f"  {'Episode':<28} {'PkVIX':>7} {'Date':>11} {'MaxState':>9} "
        f"{'CautD':>6} {'DefD':>5} {'PncD':>5} "
        f"{'C-lag':>6} {'P-lag':>6} {'Over':>5}",
    ]
    for _, r in report["episode_df"].iterrows():
        if "note" in r.index and pd.notna(r.get("note")):
            lines.append(f"  {r['episode']:<28}  {r['note']}")
            continue
        lines.append(
            f"  {str(r['episode']):<28} {r['peak_vix']:>7.1f} "
            f"{str(r.get('peak_date', ''))[:10]:>11} "
            f"{str(r.get('max_state', '')):<9} "
            f"{int(r.get('caution_days', 0)):>6} "
            f"{int(r.get('defensive_days', 0)):>5} "
            f"{int(r.get('panic_days', 0)):>5} "
            f"{str(r.get('caution_lag_d', '—')):>6} "
            f"{str(r.get('panic_lag_d', '—')):>6} "
            f"{int(r.get('overshoot_d', 0)):>5}"
        )
    lines.append("=" * 72)
    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def run_calibration(
    start:    str = "2018-01-01",
    end:      str | None = None,
    csv_path: str | None = None,
    cfg:      VixSmConfig | None = None,
) -> Dict:
    """
    Programmatic entry point (also used by tests with synthetic data).
    Returns a dict with all metrics DataFrames + formatted report text.
    """
    end_date  = end or datetime.today().strftime("%Y-%m-%d")
    cfg       = cfg or VixSmConfig()

    vix       = load_vix(start=start, end=end_date, csv_path=csv_path)
    trace     = run_sm_on_series(vix, cfg)

    state_dist = compute_state_distribution(trace)
    time_dist  = compute_time_in_state_distribution(trace)
    fa           = compute_false_alarms(trace, cfg)
    late_det     = compute_late_detection(trace, cfg)
    flapping     = compute_flapping(trace)
    episode_df   = compute_per_episode(trace, cfg)
    sticky_total = compute_sticky_total(trace, cfg)

    report_dict = {
        "cfg":          cfg,
        "start":        start,
        "end":          end_date,
        "n_days":       len(trace),
        "trace":        trace,
        "state_dist":   state_dist,
        "time_dist":    time_dist,
        "false_alarms": fa,
        "late_det":     late_det,
        "flapping":     flapping,
        "episode_df":   episode_df,
        "sticky_total": sticky_total,
    }
    report_dict["text"] = _fmt(report_dict)
    return report_dict


def main(argv=None):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        stream=sys.stdout,
    )

    parser = argparse.ArgumentParser(description="VIX State Machine Calibration")
    parser.add_argument("--start", default="2018-01-01",
                        help="Start date YYYY-MM-DD (default 2018-01-01)")
    parser.add_argument("--end",   default=None,
                        help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--csv",   default=None,
                        help="Override VIX CSV path")
    args = parser.parse_args(argv)

    logger.info("Starting VIX SM calibration: %s ~ %s", args.start, args.end or "today")

    result = run_calibration(
        start    = args.start,
        end      = args.end,
        csv_path = args.csv,
    )

    # Print to console
    print(result["text"])

    # Write outputs
    output_dir  = Path(__file__).parent / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    run_date    = datetime.today().strftime("%Y-%m-%d")

    trace_path  = output_dir / f"vix_sm_trace_{run_date}.csv"
    report_path = output_dir / f"vix_sm_metrics_{run_date}.txt"

    result["trace"].to_csv(trace_path)
    report_path.write_text(result["text"], encoding="utf-8")

    logger.info("Trace  → %s  (%d rows)", trace_path,  len(result["trace"]))
    logger.info("Report → %s", report_path)


if __name__ == "__main__":
    main()
