"""
VIX SM Multi-Config Comparison
-------------------------------
Runs 5 candidate VixSmConfig variants and produces a side-by-side comparison.
No production DB, no production wiring.

Usage:
    python -m calibration.vix_sm_compare
    python -m calibration.vix_sm_compare --start 2018-01-01 --csv path/to/vix.csv

Output:
    calibration/output/vix_sm_compare_{YYYY-MM-DD}.txt
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

from engine.vix_sm import VixState, VixSmConfig, STATE_RANK
from calibration.vix_data import load_vix
from calibration.vix_sm_calibrate import (
    run_sm_on_series,
    compute_state_distribution,
    compute_time_in_state_distribution,
    compute_false_alarms,
    compute_late_detection,
    compute_flapping,
    compute_per_episode,
    compute_sticky_total,
)

logger = logging.getLogger(__name__)


# ── Candidate configs ─────────────────────────────────────────────────────────
#
# Rationale per variant (against Phase B Baseline observations):
#   Baseline  : Design-doc candidate values. FA_caution=20, Flapping=34 → NOT shadow-eligible.
#   WideHyst  : Widen all exit bands + 1-day confirm gate → target flapping=34 and FA=20.
#   Elevated  : Raise all entry thresholds, longer fast-track gates → fewer FA, more late-detect.
#   Sensitive : Lower all entry thresholds → more early warnings, expect more FA.
#   Balanced  : Surgical: widen only caution band (main noise source) + cooldown + longer Panic hold.
#   V6-V2NoConfirm : V2 base, remove confirm gate (upgrade_confirm_days=0).
#     Original hypothesis: confirm=1 was V2's M3 blocker — disproved.
#     M3 failure was a metric definition issue (Panic not counted as detection).
#     After M3 fix (Def-or-higher), check whether V2 or V6 now clears all 6.
#   V6b-V2CooldownPatch : V6 + downgrade_cooldown_days=2 as anti-flap fallback.
#     Only added if V6 M2 Flapping exceeds limit. Single targeted patch.

CANDIDATE_CONFIGS: Dict[str, VixSmConfig] = {
    "V1-Baseline": VixSmConfig(),
    "V2-WideHyst": VixSmConfig(
        caution_exit             = 16.0,   # band: 20→16 (+2)
        defensive_exit           = 22.0,   # band: 28→22 (+2)
        panic_exit               = 28.0,   # band: 35→28 (+2)
        upgrade_confirm_days     = 1,      # 2 consecutive days above entry before entering
    ),
    "V3-Elevated": VixSmConfig(
        caution_entry            = 22.0,   # raise bar +2
        defensive_entry          = 30.0,   # raise bar +2
        panic_entry              = 38.0,   # raise bar +3
        caution_exit             = 18.0,
        defensive_exit           = 26.0,
        panic_exit               = 33.0,
        panic_min_hold           = 5,      # longer Panic hold
        panic_shock_1d           = 12.0,   # harder fast-track
        panic_accel_3d           = 18.0,
    ),
    "V4-Sensitive": VixSmConfig(
        caution_entry            = 18.0,   # lower bar −2
        caution_exit             = 15.0,
        defensive_entry          = 25.0,   # lower bar −3
        defensive_exit           = 21.0,
        panic_entry              = 32.0,   # lower bar −3
        panic_exit               = 27.0,
        panic_min_hold           = 2,      # shorter hold
        panic_shock_1d           = 8.0,    # easier fast-track
        panic_accel_3d           = 12.0,
    ),
    "V5-Balanced": VixSmConfig(
        # Targeted fix: Caution is the main noise source (FA=20, Flapping=34)
        caution_exit             = 15.0,   # widen Caution band: 20→15 (+5)
        # Keep Defensive and Panic thresholds — they were mostly clean
        panic_min_hold           = 5,      # 2024-08 Japan overshoot=4d → guard margin
        upgrade_confirm_days     = 1,      # require 2 consecutive days for all upgrades
        downgrade_cooldown_days  = 3,      # 3-day cooldown after any downgrade
        panic_shock_1d           = 12.0,   # slightly harder fast-track
    ),
    "V6-V2NoConfirm": VixSmConfig(
        # V2-WideHyst base with upgrade_confirm_days removed.
        # Original intent: fix M3 DefLag by removing confirm gate.
        # Diagnostic finding: M3 failure was a metric issue (old definition
        # only counted Defensive entry, ignoring Panic fast-track as detection).
        # After M3 metric fix, V2 may already pass — V6 retained for comparison.
        caution_exit             = 16.0,
        defensive_exit           = 22.0,
        panic_exit               = 28.0,   # wide band shields M6: Japan VIX drops
                                           # below 28 fast, overshoot estimated ~2d
        upgrade_confirm_days     = 0,      # CHANGE vs V2: confirm gate removed
    ),
    "V6b-CooldownPatch": VixSmConfig(
        # V6 + downgrade_cooldown_days=2 as anti-flap fallback.
        # Added pre-emptively to test whether M2 Flapping can be held near V2
        # levels if V6 alone shows flapping regression.
        caution_exit             = 16.0,
        defensive_exit           = 22.0,
        panic_exit               = 28.0,
        upgrade_confirm_days     = 0,
        downgrade_cooldown_days  = 2,      # ADDITION vs V6: cooldown after downgrade
    ),
}


# ── Shadow mode criteria ──────────────────────────────────────────────────────
#
# A config must pass ALL 6 criteria to be eligible for shadow mode.

def _check_shadow_mode(r: Dict) -> Dict[str, bool]:
    """
    Returns {criterion_label: passed} for each of the 6 shadow mode criteria.
    """
    fa         = r["false_alarms"]
    ep         = r["episode_df"]

    def _ep_row(keyword: str) -> pd.Series:
        rows = ep[ep["episode"].str.contains(keyword, case=False, na=False)]
        return rows.iloc[0] if not rows.empty else pd.Series(dtype=object)

    def _lag_ok(threshold_label: str, max_days: float) -> bool:
        ld   = r["late_det"]
        rows = ld[ld["threshold"] == threshold_label]
        if rows.empty:
            return True
        lag = rows.iloc[0].get("lag_mean", "—")
        if lag == "—" or lag is None:
            return True
        try:
            return float(lag) <= max_days
        except (TypeError, ValueError):
            return True

    svb_row    = _ep_row("SVB")
    covid_row  = _ep_row("COVID")
    japan_row  = _ep_row("Japan")

    results: Dict[str, bool] = {}

    # M1: Caution false alarms ≤ 15
    results["M1 Caution-FA<=15"]  = fa["caution"] <= 15

    # M2: Flapping ≤ 15
    results["M2 Flapping<=15"]    = r["flapping"] <= 15

    # M3: Defensive-or-higher detection lag mean <= 5 calendar days (or no events = OK)
    # "detected" = SM first reaches Defensive OR Panic after VIX crosses defensive_entry.
    # Entering Panic via fast-track counts as successful detection at Defensive level.
    results["M3 Def+Lag<=5d"]     = _lag_ok("defensive", 5.0)

    # M4: 2023-SVB must not escalate beyond Caution
    max_s = svb_row.get("max_state", VixState.NORMAL) if not svb_row.empty else VixState.NORMAL
    results["M4 SVB<=Caution"]    = STATE_RANK.get(str(max_s), 0) <= STATE_RANK[VixState.CAUTION]

    # M5: 2020-COVID must reach Panic within 5 calendar days of VIX first crossing panic_entry
    _p_lag_raw = covid_row.get("panic_lag_d", -2) if not covid_row.empty else -2
    try:
        p_lag = int(_p_lag_raw)
    except (TypeError, ValueError):
        p_lag = -2
    results["M5 COVID-Panic<=5d"] = 0 <= p_lag <= 5

    # M6: 2024-08 Japan overshoot <= 5 days
    _os_raw = japan_row.get("overshoot_d", None) if not japan_row.empty else None
    try:
        overshoot = int(_os_raw) if _os_raw is not None and pd.notna(_os_raw) else None
    except (TypeError, ValueError):
        overshoot = None
    results["M6 Japan-OS<=5d"]    = overshoot is not None and overshoot <= 5

    return results


# ── Comparison table builder ──────────────────────────────────────────────────

_W = 13   # column width per config

def _col(val: Any, width: int = _W) -> str:
    return str(val).rjust(width)

def _pct(n: int, total: int) -> str:
    return f"{100.0 * n / total:.1f}%" if total else "—"

def _lag_str(r_dict: Dict, threshold: str) -> str:
    ld   = r_dict["late_det"]
    rows = ld[ld["threshold"] == threshold]
    if rows.empty or rows.iloc[0].get("detected", 0) == 0:
        return "—/—"
    row = rows.iloc[0]
    return f"{row['lag_mean']}/{row['lag_max']}"

def _ep_field(r_dict: Dict, keyword: str, field: str, default: Any = "—") -> Any:
    ep   = r_dict["episode_df"]
    rows = ep[ep["episode"].str.contains(keyword, case=False, na=False)]
    if rows.empty:
        return default
    val = rows.iloc[0].get(field, default)
    return val if pd.notna(val) else default


def build_comparison_table(
    results: Dict[str, Dict],
    start:   str,
    end:     str,
    n_days:  int,
) -> str:
    names  = list(results.keys())
    w      = _W
    sep    = "─" * (30 + w * len(names))
    thick  = "═" * (30 + w * len(names))

    def header_row(label: str) -> str:
        return f"  {label:<28}" + "".join(_col(n, w) for n in names)

    def data_row(label: str, vals: List[Any]) -> str:
        return f"  {label:<28}" + "".join(_col(v, w) for v in vals)

    lines = [
        thick,
        "VIX SM CONFIG COMPARISON — 5 Candidates",
        f"Period: {start} ~ {end}  ({n_days} trading days)",
        thick,
        header_row("Metric"),
        sep,
        "  CONFIG PARAMETERS",
    ]

    def cfg_row(label: str, fn) -> str:
        return data_row(label, [fn(results[n]["cfg"]) for n in names])

    lines += [
        cfg_row("  caution  entry/exit",
                lambda c: f"{c.caution_entry:.0f}/{c.caution_exit:.0f}"),
        cfg_row("  defensiv entry/exit",
                lambda c: f"{c.defensive_entry:.0f}/{c.defensive_exit:.0f}"),
        cfg_row("  panic    entry/exit",
                lambda c: f"{c.panic_entry:.0f}/{c.panic_exit:.0f}"),
        cfg_row("  upgrade_confirm_d",
                lambda c: c.upgrade_confirm_days),
        cfg_row("  dngrd_cooldown_d",
                lambda c: c.downgrade_cooldown_days),
        cfg_row("  panic_min_hold",
                lambda c: c.panic_min_hold),
        cfg_row("  shock_1d/accel_3d",
                lambda c: f"{c.panic_shock_1d:.0f}/{c.panic_accel_3d:.0f}"),
        sep,
        "  STATE DISTRIBUTION (% of total)",
    ]

    for s in VixState.ALL:
        lines.append(data_row(
            f"  {s}",
            [_pct(
                int(results[n]["state_dist"].loc[
                    results[n]["state_dist"]["state"] == s, "days"
                ].iloc[0] if not results[n]["state_dist"].empty else 0),
                n_days,
            ) for n in names],
        ))

    lines += [sep, "  TIME-IN-STATE (median / max run, trading days)"]
    for s in VixState.ALL:
        def _td(r: Dict, state: str) -> str:
            td = r["time_dist"]
            row = td[td["state"] == state]
            if row.empty:
                return "—/—"
            return f"{row.iloc[0]['median']:.0f}/{row.iloc[0]['max']}"
        lines.append(data_row(f"  {s} med/max",
                               [_td(results[n], s) for n in names]))

    lines += [sep, "  FALSE ALARMS"]
    for key, label in [
        ("caution",          "  Caution (->Nrm <=5d)"),
        ("defensive",        "  Defensiv (<Panic <=7d)"),
        ("panic_threshold",  "  Panic/thr (<=5d)"),
        ("panic_fast_track", "  Panic/ft  (<=3d)"),
    ]:
        lines.append(data_row(label,
                               [results[n]["false_alarms"][key] for n in names]))

    lines += [sep, "  STICKY TOTAL (SM elevated, VIX<exit, all days)"]
    lines.append(data_row("  Sticky days",
                           [results[n]["sticky_total"] for n in names]))

    lines += [sep, "  LATE DETECTION (lag mean/max, calendar days; Def row = Def-or-higher)"]
    for thr, label in [("defensive", "  Def-or-higher"), ("panic", "  Panic")]:
        lines.append(data_row(label,
                               [_lag_str(results[n], thr) for n in names]))

    lines += [sep, f"  FLAPPING (A->B->A within 5d)"]
    lines.append(data_row("  Flapping count",
                           [results[n]["flapping"] for n in names]))

    lines += [sep, "  PER-EPISODE  (max_state / overshoot days)"]
    for keyword, ep_label in [
        ("Q4",    "  2018-Q4 Fed panic"),
        ("COVID", "  2020-COVID crash"),
        ("Bear",  "  2022-Bear market"),
        ("SVB",   "  2023-SVB crisis"),
        ("Japan", "  2024-08 Japan shock"),
    ]:
        def _ep_str(r: Dict, kw: str) -> str:
            ms = _ep_field(r, kw, "max_state", "—")
            os = _ep_field(r, kw, "overshoot_d", 0)
            return f"{ms[:3]}/{os}d"
        lines.append(data_row(ep_label,
                               [_ep_str(results[n], keyword) for n in names]))

    # COVID panic lag separately (shadow mode criterion)
    lines.append(data_row("  2020-COVID panic_lag",
                           [_ep_field(results[n], "COVID", "panic_lag_d", "—") for n in names]))

    lines += [sep, "  SHADOW MODE CRITERIA  (PASS / FAIL)"]
    shadow_results: Dict[str, Dict[str, bool]] = {
        n: _check_shadow_mode(results[n]) for n in names
    }
    criteria = list(next(iter(shadow_results.values())).keys())
    for crit in criteria:
        lines.append(data_row(
            f"  {crit}",
            ["PASS" if shadow_results[n][crit] else "FAIL" for n in names],
        ))
    scores = {n: sum(shadow_results[n].values()) for n in names}
    lines.append(data_row(
        "  TOTAL SCORE",
        [f"{scores[n]}/6" for n in names],
    ))
    lines += [sep]
    lines.append(data_row(
        "  SHADOW ELIGIBLE (6/6)?",
        ["YES" if scores[n] == 6 else "NO" for n in names],
    ))

    # Recommendation
    lines += [thick, "  RECOMMENDATION"]
    eligible = [n for n in names if scores[n] == 6]
    near     = [n for n in names if scores[n] >= 4 and scores[n] < 6]

    if eligible:
        lines.append(f"  Shadow-mode eligible : {', '.join(eligible)}")
    else:
        lines.append("  Shadow-mode eligible : none (no config passed all 6 criteria)")
    if near:
        lines.append(f"  Near-miss (4-5/6)    : {', '.join(near)}")

    # Best by score, then by Caution FA as tiebreak
    ranked = sorted(names, key=lambda n: (scores[n], -results[n]["false_alarms"]["caution"]))
    best   = ranked[-1]
    lines.append(f"  Best overall         : {best}  "
                 f"(score={scores[best]}/6, Caution-FA={results[best]['false_alarms']['caution']}, "
                 f"Flapping={results[best]['flapping']})")

    # Per-criterion failure summary
    lines += ["", "  Why each config fails / near-misses:"]
    for n in names:
        fails = [c for c, ok in shadow_results[n].items() if not ok]
        if fails:
            lines.append(f"    {n:<16}: fails {', '.join(fails)}")
        else:
            lines.append(f"    {n:<16}: ALL PASS")

    lines.append(thick)
    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def main(argv=None):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        stream=sys.stdout,
    )

    parser = argparse.ArgumentParser(description="VIX SM Multi-Config Comparison")
    parser.add_argument("--start", default="2018-01-01")
    parser.add_argument("--end",   default=None)
    parser.add_argument("--csv",   default=None)
    args = parser.parse_args(argv)

    end_date = args.end or datetime.today().strftime("%Y-%m-%d")

    logger.info("Loading VIX data %s ~ %s", args.start, end_date)
    vix = load_vix(start=args.start, end=end_date, csv_path=args.csv)
    n_days = len(vix)
    logger.info("Loaded %d trading days", n_days)

    results: Dict[str, Dict] = {}
    for name, cfg in CANDIDATE_CONFIGS.items():
        logger.info("Running config: %s", name)
        trace        = run_sm_on_series(vix, cfg)
        state_dist   = compute_state_distribution(trace)
        time_dist    = compute_time_in_state_distribution(trace)
        fa           = compute_false_alarms(trace, cfg)
        late_det     = compute_late_detection(trace, cfg)
        flapping     = compute_flapping(trace)
        episode_df   = compute_per_episode(trace, cfg)
        sticky_total = compute_sticky_total(trace, cfg)

        results[name] = {
            "cfg":          cfg,
            "trace":        trace,
            "state_dist":   state_dist,
            "time_dist":    time_dist,
            "false_alarms": fa,
            "late_det":     late_det,
            "flapping":     flapping,
            "episode_df":   episode_df,
            "sticky_total": sticky_total,
        }
        logger.info(
            "  %s -> Caution-FA=%d  Flapping=%d  Sticky=%d",
            name, fa["caution"], flapping, sticky_total,
        )

    table = build_comparison_table(results, args.start, end_date, n_days)
    print(table)

    output_dir = Path(__file__).parent / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    run_date   = datetime.today().strftime("%Y-%m-%d")
    out_path   = output_dir / f"vix_sm_compare_{run_date}.txt"
    out_path.write_text(table, encoding="utf-8")
    logger.info("Comparison -> %s", out_path)


if __name__ == "__main__":
    main()
