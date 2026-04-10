"""
Monitor State Manager
---------------------
Persist tripwire monitor state in state/monitor_state.json.

State schema:
  initialized_at     : ISO8601 — first successful run timestamp
  last_alert_time    : ISO8601 — last time an alert was sent
  last_alert_type    : str     — "LIGHT_CHANGE" | "DELTA_SPIKE"
  last_risk_light    : str     — "GREEN" | "YELLOW" | "RED"
  last_trigger_reason: str     — human-readable reason for last alert
  last_seen_values   : dict    — {VIX_Z_252, HY_OAS_Z_252, YIELD_SPREAD_10Y2Y_Z_252, timestamp}
  history            : list    — rolling last 8 readings (covers ~2 hours at 15-min intervals)
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

STATE_FILE = Path(__file__).parent.parent / "state" / "monitor_state.json"
COOLDOWN_HOURS = 4
HISTORY_MAX = 8  # 2 hours at 15-min intervals


# ── datetime helpers ──────────────────────────────────────────────────────────

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now_utc().isoformat()


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


# ── Risk level helper ─────────────────────────────────────────────────────────

_LIGHT_LEVEL: Dict[str, int] = {"GREEN": 0, "YELLOW": 1, "RED": 2}


def is_escalation(prev_light: Optional[str], new_light: str) -> bool:
    """Return True when risk level is going up (requires cooldown bypass)."""
    prev = _LIGHT_LEVEL.get(prev_light or "GREEN", 0)
    curr = _LIGHT_LEVEL.get(new_light, 0)
    return curr > prev


# ── StateManager ──────────────────────────────────────────────────────────────

class StateManager:
    """Thread-safe (single-process) read/write wrapper for monitor_state.json."""

    def __init__(self, state_path: Path = STATE_FILE):
        self._path = state_path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._state: Dict[str, Any] = self._load()

    # ── I/O ───────────────────────────────────────────────────────────────────

    def _load(self) -> Dict[str, Any]:
        if self._path.exists():
            try:
                with open(self._path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                logger.debug("monitor_state loaded from %s", self._path)
                return data
            except Exception as exc:
                logger.warning(
                    "monitor_state corrupt/unreadable — starting fresh. error=%s", exc
                )
        return {}

    def _save(self) -> None:
        try:
            with open(self._path, "w", encoding="utf-8") as f:
                json.dump(self._state, f, ensure_ascii=False, indent=2)
            logger.debug("monitor_state saved to %s", self._path)
        except Exception as exc:
            logger.error("Failed to persist monitor_state: %s", exc)

    # ── Read ──────────────────────────────────────────────────────────────────

    def is_first_run(self) -> bool:
        return "initialized_at" not in self._state

    def get_last_risk_light(self) -> Optional[str]:
        return self._state.get("last_risk_light")

    def get_last_seen_values(self) -> Dict:
        return self._state.get("last_seen_values", {})

    def get_history(self) -> List[Dict]:
        return self._state.get("history", [])

    def get_initialized_at(self) -> Optional[datetime]:
        return _parse_dt(self._state.get("initialized_at"))

    # ── Cooldown ──────────────────────────────────────────────────────────────

    def is_cooldown_active(self, alert_type: str) -> bool:
        """
        Return True if the same alert_type is still within the 4-hour cooldown.
        Different alert types have independent cooldowns.
        """
        last_time_str = self._state.get("last_alert_time")
        last_type = self._state.get("last_alert_type")
        if not last_time_str or last_type != alert_type:
            return False
        last_time = _parse_dt(last_time_str)
        if last_time is None:
            return False
        return (_now_utc() - last_time) < timedelta(hours=COOLDOWN_HOURS)

    # ── Write ─────────────────────────────────────────────────────────────────

    def record_baseline(self, risk_light: str, values: Dict) -> None:
        """Called on first run: save baseline without sending alert."""
        now = _now_iso()
        self._state["initialized_at"] = now
        self._state["last_risk_light"] = risk_light
        self._state["last_seen_values"] = {**values, "timestamp": now}
        self._append_history(risk_light, values, now)
        self._save()
        logger.info("Baseline recorded  light=%s", risk_light)

    def record_alert(
        self,
        alert_type: str,
        risk_light: str,
        values: Dict,
        trigger_reason: str,
    ) -> None:
        """Called after successfully sending an alert."""
        now = _now_iso()
        self._state["last_alert_time"] = now
        self._state["last_alert_type"] = alert_type
        self._state["last_risk_light"] = risk_light
        self._state["last_trigger_reason"] = trigger_reason
        self._state["last_seen_values"] = {**values, "timestamp": now}
        self._append_history(risk_light, values, now)
        self._save()

    def record_check(self, risk_light: str, values: Dict) -> None:
        """Called after each check cycle regardless of whether alert was sent."""
        now = _now_iso()
        self._state["last_risk_light"] = risk_light
        self._state["last_seen_values"] = {**values, "timestamp": now}
        self._append_history(risk_light, values, now)
        self._save()

    def _append_history(self, risk_light: str, values: Dict, timestamp: str) -> None:
        history: List[Dict] = self._state.get("history", [])
        history.append({
            "timestamp": timestamp,
            "risk_light": risk_light,
            "VIX_Z_252": values.get("VIX_Z_252"),
            "HY_OAS_Z_252": values.get("HY_OAS_Z_252"),
            "YIELD_SPREAD_10Y2Y_Z_252": values.get("YIELD_SPREAD_10Y2Y_Z_252"),
        })
        self._state["history"] = history[-HISTORY_MAX:]
