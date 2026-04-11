"""
VIX State Machine — Pure Function Core
---------------------------------------
4-state FSM: Normal ↔ Caution ↔ Defensive ↔ Panic

Design rules (ref: Round 3 design document, 2026-04-11):
  - Pure function: no I/O, no DB, no datetime, no side effects
  - All thresholds in VixSmConfig (no magic numbers)
  - Forbidden skips enforced by adjacency: each step moves max 1 level,
    EXCEPT panic fast-track (Caution/Defensive → Panic on shock/acceleration)
  - Hysteresis: exit thresholds are lower than entry thresholds
  - Hold / cooldown: downgrade gated by min_hold_days
  - upgrade_confirm_days: require N+1 consecutive days above entry before upgrading
  - downgrade_cooldown_days: N trading days after downgrade before re-upgrade allowed

Candidate threshold note (design doc §11.4):
  All VixSmConfig default values are unconfirmed pending calibration.
  Do NOT treat as production-grade until shadow mode criteria are met (§11.6).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ── State constants ─────────────────────────────────────────────────────────────

class VixState:
    NORMAL    = "Normal"
    CAUTION   = "Caution"
    DEFENSIVE = "Defensive"
    PANIC     = "Panic"

    ALL: Tuple[str, ...] = ("Normal", "Caution", "Defensive", "Panic")


# Public rank mapping (also used by calibration modules)
STATE_RANK: Dict[str, int] = {
    VixState.NORMAL:    0,
    VixState.CAUTION:   1,
    VixState.DEFENSIVE: 2,
    VixState.PANIC:     3,
}


# ── Config ──────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class VixSmConfig:
    """
    All VIX state machine thresholds.
    All values are CANDIDATE values pending calibration (design doc §11.4).
    Do not promote to production without completing shadow mode criteria (§11.6).

    upgrade_confirm_days:
      0 (default) = enter next state immediately when VIX >= entry threshold.
      N > 0       = VIX must remain >= entry threshold for N+1 consecutive days.
      Fast-track to Panic always bypasses this gate.

    downgrade_cooldown_days:
      0 (default) = no cooldown; can re-upgrade immediately after downgrading.
      N > 0       = after downgrading, N trading days must pass before re-upgrading.
      Fast-track to Panic always bypasses this gate.
    """

    # Entry thresholds (升級方向)
    caution_entry:           float = 20.0
    defensive_entry:         float = 28.0
    panic_entry:             float = 35.0

    # Exit thresholds (降級方向, hysteresis band)
    caution_exit:            float = 18.0   # band = 2.0
    defensive_exit:          float = 24.0   # band = 4.0
    panic_exit:              float = 30.0   # band = 5.0

    # Upgrade confirmation (days beyond first threshold crossing; 0 = immediate)
    upgrade_confirm_days:    int   = 0

    # Downgrade cooldown (trading days after downgrade before re-upgrade; 0 = none)
    downgrade_cooldown_days: int   = 0

    # Min hold / cooldown (days, 僅限降級方向)
    caution_min_hold:        int   = 1
    defensive_min_hold:      int   = 2
    panic_min_hold:          int   = 3      # candidate — see design doc §11.1

    # Panic fast-track (Caution 或 Defensive → Panic 旁道; Normal 禁用)
    panic_shock_1d:          float = 10.0   # 1-day VIX jump >= X  → fast-track
    panic_accel_3d:          float = 15.0   # 3-day VIX rise >= X  → fast-track


# ── Output ──────────────────────────────────────────────────────────────────────

@dataclass
class VixSmOutput:
    state:              str    # "Normal" / "Caution" / "Defensive" / "Panic"
    prev_state:         str    # state before this evaluation
    transitioned:       bool   # True if state changed
    days_in_state:      int    # 1 if transitioned, else prev days_in_state + 1
    vix_enum:           str    # "[Low]" / "[Normal]" / "[Warning]" / "[Confirmed]"
    reason:             str    # one-line rationale
    consecutive_above:  int    # caller must pass back on next call
    cooldown_remaining: int    # caller must pass back on next call


# ── Internal helpers ─────────────────────────────────────────────────────────────

def _vix_enum(vix: float) -> str:
    """Map raw VIX level → display enum (level-based, state-independent)."""
    if vix < 15.0:
        return "[Low]"
    if vix < 20.0:
        return "[Normal]"
    if vix < 35.0:
        return "[Warning]"
    return "[Confirmed]"


def _shock_1d(history: List[float]) -> float:
    """1-day VIX jump: history[-1] - history[-2]. Returns 0.0 if insufficient data."""
    return history[-1] - history[-2] if len(history) >= 2 else 0.0


def _accel_3d(history: List[float]) -> float:
    """3-day VIX acceleration: history[-1] - history[-4]. Returns 0.0 if insufficient data."""
    return history[-1] - history[-4] if len(history) >= 4 else 0.0


def _check_panic_fast_track(
    history: List[float], cfg: VixSmConfig
) -> Tuple[bool, str]:
    """
    Check panic fast-track conditions.
    Caller is responsible for only invoking this from Caution or Defensive.
    Returns (triggered, reason_string).
    """
    shock = _shock_1d(history)
    accel = _accel_3d(history)

    if shock >= cfg.panic_shock_1d:
        return True, (
            f"panic fast-track: 1d shock={shock:.1f}>={cfg.panic_shock_1d:.0f}"
        )
    if accel >= cfg.panic_accel_3d:
        return True, (
            f"panic fast-track: 3d accel={accel:.1f}>={cfg.panic_accel_3d:.0f}"
        )
    return False, ""


def _next_entry_threshold(state: str, cfg: VixSmConfig) -> Optional[float]:
    """Entry threshold for the next-higher state. None if already at Panic."""
    if state == VixState.NORMAL:     return cfg.caution_entry
    if state == VixState.CAUTION:    return cfg.defensive_entry
    if state == VixState.DEFENSIVE:  return cfg.panic_entry
    return None


# ── Main function ────────────────────────────────────────────────────────────────

def evaluate_next_state(
    current_state:      str,
    vix_history:        List[float],
    hold_days:          int,
    cfg:                VixSmConfig,
    consecutive_above:  int = 0,
    cooldown_remaining: int = 0,
) -> VixSmOutput:
    """
    Pure FSM transition function. No I/O, no DB, no side effects.

    Parameters
    ----------
    current_state      : one of VixState.ALL
    vix_history        : VIX values [oldest ... today]; today = vix_history[-1].
                         Minimum 1 value. 2+ required for 1d shock. 4+ for 3d accel.
    hold_days          : days already spent in current_state (>= 1)
    cfg                : VixSmConfig with all thresholds
    consecutive_above  : days VIX has been >= next-level entry threshold (from last call)
    cooldown_remaining : days until downgrade cooldown expires (from last call)

    Returns
    -------
    VixSmOutput.  The caller must save .consecutive_above and .cooldown_remaining
    and pass them back on the next call to maintain FSM continuity.

    Transition rules (enforced by this function):
      Upgrade path  : Normal → Caution → Defensive → Panic (one step; confirm gate)
      Downgrade path: Panic → Defensive → Caution → Normal (one step; min_hold gate)
      Panic fast-track: Caution/Defensive → Panic (shock/accel; bypasses confirm+cooldown)
      Forbidden     : Normal → Defensive/Panic, Caution → Panic (non-fast-track),
                      Panic → Caution/Normal, Defensive → Normal
    """
    if current_state not in VixState.ALL:
        raise ValueError(
            f"Unknown state: {current_state!r}. Must be one of {VixState.ALL}"
        )
    if not vix_history:
        raise ValueError("vix_history must contain at least 1 value")

    vix       = vix_history[-1]
    entry_thr = _next_entry_threshold(current_state, cfg)

    # Update consecutive days above next-level entry threshold
    new_consec = (
        consecutive_above + 1
        if (entry_thr is not None and vix >= entry_thr)
        else 0
    )
    # Tick down cooldown (one day passes)
    new_cooldown = max(0, cooldown_remaining - 1)

    # Upgrade is gated: needs N+1 consecutive confirmed days AND cooldown expired
    can_upgrade = (new_consec >= 1 + cfg.upgrade_confirm_days) and (new_cooldown == 0)

    def _out(new_st: str, reason: str) -> VixSmOutput:
        trans    = new_st != current_state
        is_dn    = trans and STATE_RANK[new_st] < STATE_RANK[current_state]

        if trans:
            # After transition, re-compute consecutive for the new state
            nt         = _next_entry_threshold(new_st, cfg)
            out_consec = 1 if (nt is not None and vix >= nt) else 0
            # Downgrade sets cooldown; upgrade/fast-track clears it
            out_cooldown = cfg.downgrade_cooldown_days if is_dn else 0
        else:
            out_consec   = new_consec
            out_cooldown = new_cooldown

        return VixSmOutput(
            state              = new_st,
            prev_state         = current_state,
            transitioned       = trans,
            days_in_state      = 1 if trans else hold_days + 1,
            vix_enum           = _vix_enum(vix),
            reason             = reason,
            consecutive_above  = out_consec,
            cooldown_remaining = max(0, out_cooldown),
        )

    # ── Panic fast-track (Caution or Defensive → Panic; bypasses confirm+cooldown) ──
    # Normal → Panic via fast-track is FORBIDDEN (design doc §forbidden transitions)
    if current_state in (VixState.CAUTION, VixState.DEFENSIVE):
        ft, ft_reason = _check_panic_fast_track(vix_history, cfg)
        if ft:
            logger.info(
                "VIX SM fast-track: %s → Panic | %s", current_state, ft_reason
            )
            return _out(VixState.PANIC, ft_reason)

    # ── State-specific transitions ─────────────────────────────────────────────

    if current_state == VixState.NORMAL:
        if can_upgrade:
            note = f" (confirmed {new_consec}d)" if cfg.upgrade_confirm_days > 0 else ""
            return _out(VixState.CAUTION,
                        f"VIX={vix:.1f}>={cfg.caution_entry:.0f}{note} → Caution")
        if new_consec > 0:
            cd_note = f" cooldown={new_cooldown}d" if new_cooldown > 0 else ""
            return _out(VixState.NORMAL,
                        f"VIX={vix:.1f}>={cfg.caution_entry:.0f} "
                        f"pending {new_consec}/{1 + cfg.upgrade_confirm_days}d{cd_note}")

    elif current_state == VixState.CAUTION:
        if can_upgrade:
            note = f" (confirmed {new_consec}d)" if cfg.upgrade_confirm_days > 0 else ""
            return _out(VixState.DEFENSIVE,
                        f"VIX={vix:.1f}>={cfg.defensive_entry:.0f}{note} → Defensive")
        if new_consec > 0:
            return _out(VixState.CAUTION,
                        f"VIX={vix:.1f}>={cfg.defensive_entry:.0f} "
                        f"pending {new_consec}/{1 + cfg.upgrade_confirm_days}d")
        if vix < cfg.caution_exit:
            if hold_days >= cfg.caution_min_hold:
                return _out(VixState.NORMAL,
                            f"VIX={vix:.1f}<{cfg.caution_exit:.0f}, "
                            f"hold={hold_days}>={cfg.caution_min_hold} → Normal")
            return _out(VixState.CAUTION,
                        f"VIX={vix:.1f}<{cfg.caution_exit:.0f} "
                        f"but hold={hold_days}<{cfg.caution_min_hold} (min_hold not met)")

    elif current_state == VixState.DEFENSIVE:
        if can_upgrade:
            note = f" (confirmed {new_consec}d)" if cfg.upgrade_confirm_days > 0 else ""
            return _out(VixState.PANIC,
                        f"VIX={vix:.1f}>={cfg.panic_entry:.0f}{note} → Panic")
        if new_consec > 0:
            return _out(VixState.DEFENSIVE,
                        f"VIX={vix:.1f}>={cfg.panic_entry:.0f} "
                        f"pending {new_consec}/{1 + cfg.upgrade_confirm_days}d")
        if vix < cfg.defensive_exit:
            if hold_days >= cfg.defensive_min_hold:
                return _out(VixState.CAUTION,
                            f"VIX={vix:.1f}<{cfg.defensive_exit:.0f}, "
                            f"hold={hold_days}>={cfg.defensive_min_hold} → Caution")
            return _out(VixState.DEFENSIVE,
                        f"VIX={vix:.1f}<{cfg.defensive_exit:.0f} "
                        f"but hold={hold_days}<{cfg.defensive_min_hold} (min_hold not met)")

    elif current_state == VixState.PANIC:
        # No upgrade possible from Panic
        if vix < cfg.panic_exit:
            if hold_days >= cfg.panic_min_hold:
                return _out(VixState.DEFENSIVE,
                            f"VIX={vix:.1f}<{cfg.panic_exit:.0f}, "
                            f"hold={hold_days}>={cfg.panic_min_hold} → Defensive")
            return _out(VixState.PANIC,
                        f"VIX={vix:.1f}<{cfg.panic_exit:.0f} "
                        f"but hold={hold_days}<{cfg.panic_min_hold} (min_hold not met)")

    # ── Hold (no threshold crossed) ───────────────────────────────────────────
    return _out(
        current_state,
        f"VIX={vix:.1f} no threshold crossed, "
        f"holding {current_state} (day {hold_days + 1})",
    )
