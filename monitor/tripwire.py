"""
Tripwire Core Logic
--------------------
Responsibilities:
  1. Read z-score indicators from DB (derived_indicators table)
  2. Fallback: compute z-scores from raw market/macro data if DB has none
  3. Determine risk light (GREEN / YELLOW / RED)
  4. Evaluate Tripwire Rule A (light change) and Rule B (1-hour delta spike)
  5. Respect cooldown / escalation logic
  6. Return TripwireResult for the caller to act on

Design:
  - No side effects (no alert sending, no state writes) — caller handles those
  - All missing data is handled gracefully; never crash the daemon
  - Uses only standard psycopg2 + pandas; no new dependencies

Indicators monitored:
  VIX_Z_252                 — from raw_market_data (^VIX)
  HY_OAS_Z_252              — from macro_data (HY_OAS / FRED BAMLH0A0HYM2)
  YIELD_SPREAD_10Y2Y_Z_252  — derived from US_10Y_YIELD - US_2Y_YIELD
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Tripwire threshold constants ──────────────────────────────────────────────

# Risk light thresholds
RED_THRESHOLD_VIX    = 2.0
RED_THRESHOLD_HY     = 2.0
YELLOW_THRESHOLD_VIX = 1.0
YELLOW_THRESHOLD_HY  = 1.0
YELLOW_THRESHOLD_SPD = -1.0   # spread z-score ≤ this → YELLOW (inverted risk)

# Delta (Rule B) thresholds — 1-hour change
DELTA_VIX_THRESHOLD  = 0.8
DELTA_HY_THRESHOLD   = 0.6

# Stale data tolerance: accept z-score values up to 7 days old
MAX_DATA_STALENESS_DAYS = 7

# Rolling window for fallback z-score computation
ZSCORE_WINDOW = 252

# ── Data structures ────────────────────────────────────────────────────────────

@dataclass
class ZScoreValues:
    vix_z:    Optional[float] = None
    hy_z:     Optional[float] = None
    spread_z: Optional[float] = None
    source:   str = "unknown"   # "db" | "computed" | "partial" | "unavailable"

    def as_dict(self) -> Dict:
        return {
            "VIX_Z_252":                self.vix_z,
            "HY_OAS_Z_252":             self.hy_z,
            "YIELD_SPREAD_10Y2Y_Z_252": self.spread_z,
        }

    def has_core_indicators(self) -> bool:
        """True if at least VIX and HY are available (minimum for alerting)."""
        return self.vix_z is not None and self.hy_z is not None

    def is_fully_unavailable(self) -> bool:
        return self.vix_z is None and self.hy_z is None and self.spread_z is None


@dataclass
class TripwireResult:
    risk_light:     str                 # "GREEN" | "YELLOW" | "RED"
    prev_light:     Optional[str]       # last known state
    values:         ZScoreValues
    triggered:      bool = False
    alert_type:     Optional[str] = None  # "LIGHT_CHANGE" | "DELTA_SPIKE"
    trigger_reasons: List[str] = field(default_factory=list)
    delta_vix:      Optional[float] = None
    delta_hy:       Optional[float] = None
    skipped:        bool = False        # True if core indicators unavailable
    skip_reason:    str = ""


# ── Risk light logic ──────────────────────────────────────────────────────────

def compute_risk_light(v: ZScoreValues) -> str:
    """
    Determine risk light from z-score values.
    Encapsulated here so thresholds can be changed in one place.

    RED    : VIX_Z >= 2.0  OR  HY_OAS_Z >= 2.0
    YELLOW : (not RED) AND (VIX_Z >= 1.0 OR HY_OAS_Z >= 1.0 OR SPREAD_Z <= -1.0)
    GREEN  : otherwise
    """
    vix = v.vix_z if v.vix_z is not None else 0.0
    hy  = v.hy_z  if v.hy_z  is not None else 0.0
    spd = v.spread_z if v.spread_z is not None else 0.0

    if vix >= RED_THRESHOLD_VIX or hy >= RED_THRESHOLD_HY:
        return "RED"
    if (
        vix >= YELLOW_THRESHOLD_VIX
        or hy  >= YELLOW_THRESHOLD_HY
        or spd <= YELLOW_THRESHOLD_SPD
    ):
        return "YELLOW"
    return "GREEN"


# ── DB reading ────────────────────────────────────────────────────────────────

_TARGET_INDICATORS = (
    "VIX_Z_252",
    "HY_OAS_Z_252",
    "YIELD_SPREAD_10Y2Y_Z_252",
)

_ZSCORE_QUERY = """
    SELECT DISTINCT ON (indicator) indicator, value, time
    FROM derived_indicators
    WHERE indicator = ANY(%s)
      AND time >= NOW() - INTERVAL '7 days'
    ORDER BY indicator, time DESC
"""

def _read_zscores_from_db(conn) -> Optional[ZScoreValues]:
    """
    Query derived_indicators for the most recent z-score values.
    Returns None if DB query fails entirely.
    Returns ZScoreValues with partial Nones if some indicators are missing.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(_ZSCORE_QUERY, (list(_TARGET_INDICATORS),))
            rows = cur.fetchall()
    except Exception as exc:
        logger.warning("DB query for z-scores failed: %s", exc)
        return None

    result: Dict[str, float] = {}
    for indicator, value, ts in rows:
        if value is not None:
            result[indicator] = float(value)
            logger.debug("DB z-score  %s = %.4f  (as of %s)", indicator, value, ts)

    if not result:
        logger.info("No z-scores found in derived_indicators — will attempt fallback")
        return None

    v = ZScoreValues(
        vix_z    = result.get("VIX_Z_252"),
        hy_z     = result.get("HY_OAS_Z_252"),
        spread_z = result.get("YIELD_SPREAD_10Y2Y_Z_252"),
        source   = "db",
    )
    logger.info(
        "Z-scores from DB  VIX=%.3f  HY=%.3f  SPREAD=%.3f",
        v.vix_z    if v.vix_z    is not None else float("nan"),
        v.hy_z     if v.hy_z     is not None else float("nan"),
        v.spread_z if v.spread_z is not None else float("nan"),
    )
    return v


# ── Fallback: compute z-scores from raw DB data ───────────────────────────────

def _rolling_zscore(series, window: int = ZSCORE_WINDOW):
    """
    Vectorised rolling z-score with denominator floor.

    Uses the same MIN_ZSCORE_DENOMINATOR as cleaner.compute_rolling_zscore()
    so that near-zero rolling std cannot produce explosive z-score values.
    Returns NaN where data is insufficient (< window rows).
    """
    from etl.cleaner import MIN_ZSCORE_DENOMINATOR
    mu         = series.rolling(window, min_periods=window).mean()
    sigma      = series.rolling(window, min_periods=window).std()
    safe_sigma = sigma.clip(lower=MIN_ZSCORE_DENOMINATOR)
    z          = (series - mu) / safe_sigma
    return z


def _fetch_macro_series(conn, indicator: str, days: int = 500):
    """Fetch a macro_data indicator as pandas Series, indexed by time."""
    import pandas as pd
    query = """
        SELECT time, value
        FROM macro_data
        WHERE indicator = %s
          AND frequency = '1d'
          AND value IS NOT NULL
          AND time >= NOW() - INTERVAL '%s days'
        ORDER BY time ASC
    """ % ("%s", days)
    try:
        with conn.cursor() as cur:
            cur.execute(query, (indicator,))
            rows = cur.fetchall()
        if not rows:
            return pd.Series(dtype=float)
        idx = pd.DatetimeIndex([r[0] for r in rows])
        vals = [float(r[1]) for r in rows]
        return pd.Series(vals, index=idx, name=indicator)
    except Exception as exc:
        logger.warning("Failed to fetch macro series %s: %s", indicator, exc)
        return pd.Series(dtype=float)


def _fetch_market_close(conn, symbol: str, days: int = 500):
    """Fetch a market asset's close prices as pandas Series."""
    import pandas as pd
    query = """
        SELECT rmd.time, rmd.close
        FROM raw_market_data rmd
        JOIN assets a ON a.asset_id = rmd.asset_id
        WHERE a.symbol = %s
          AND rmd.frequency = '1d'
          AND rmd.close IS NOT NULL
          AND rmd.time >= NOW() - INTERVAL %s
        ORDER BY rmd.time ASC
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (symbol, f"{days} days"))
            rows = cur.fetchall()
        if not rows:
            return pd.Series(dtype=float)
        idx = pd.DatetimeIndex([r[0] for r in rows])
        vals = [float(r[1]) for r in rows]
        return pd.Series(vals, index=idx, name=symbol)
    except Exception as exc:
        logger.warning("Failed to fetch market close %s: %s", symbol, exc)
        return pd.Series(dtype=float)


def _latest_zscore(series, window: int = ZSCORE_WINDOW) -> Optional[float]:
    """Return the most recent z-score value from a series, or None."""
    import pandas as pd
    if series.empty:
        return None
    # Forward fill (analysis-layer only, does not affect DB)
    filled = series.ffill()
    z = _rolling_zscore(filled, window)
    z_clean = z.dropna()
    if z_clean.empty:
        return None
    return float(z_clean.iloc[-1])


def _compute_zscores_from_raw(conn) -> ZScoreValues:
    """
    Fallback: compute z-scores directly from macro_data + raw_market_data.
    Uses the last 500 days (> 252 required for valid z-score).
    Returns ZScoreValues; individual fields may be None if data unavailable.
    """
    import pandas as pd

    logger.info("Computing z-scores from raw DB data (fallback)")

    # VIX_Z_252 — from raw_market_data (^VIX close)
    vix_z: Optional[float] = None
    try:
        vix_series = _fetch_market_close(conn, "^VIX", days=500)
        if not vix_series.empty:
            vix_z = _latest_zscore(vix_series)
            if vix_z is not None:
                logger.info("Fallback VIX_Z_252 = %.4f", vix_z)
        else:
            logger.warning("No VIX close data for fallback z-score")
    except Exception as exc:
        logger.warning("VIX z-score fallback failed: %s", exc)

    # HY_OAS_Z_252 — from macro_data (HY_OAS)
    hy_z: Optional[float] = None
    try:
        hy_series = _fetch_macro_series(conn, "HY_OAS", days=500)
        if not hy_series.empty:
            hy_z = _latest_zscore(hy_series)
            if hy_z is not None:
                logger.info("Fallback HY_OAS_Z_252 = %.4f", hy_z)
        else:
            logger.warning("No HY_OAS data for fallback z-score")
    except Exception as exc:
        logger.warning("HY_OAS z-score fallback failed: %s", exc)

    # YIELD_SPREAD_10Y2Y_Z_252 — from macro_data (10Y - 2Y)
    spread_z: Optional[float] = None
    try:
        y10 = _fetch_macro_series(conn, "US_10Y_YIELD", days=500)
        y2  = _fetch_macro_series(conn, "US_2Y_YIELD",  days=500)
        if not y10.empty and not y2.empty:
            spread = (y10 - y2).dropna()
            if not spread.empty:
                spread_z = _latest_zscore(spread)
                if spread_z is not None:
                    logger.info("Fallback YIELD_SPREAD_10Y2Y_Z_252 = %.4f", spread_z)
        else:
            logger.warning("Missing yield data for spread z-score fallback")
    except Exception as exc:
        logger.warning("Spread z-score fallback failed: %s", exc)

    source = "computed" if (vix_z is not None or hy_z is not None) else "unavailable"
    return ZScoreValues(
        vix_z    = vix_z,
        hy_z     = hy_z,
        spread_z = spread_z,
        source   = source,
    )


# ── Z-score retrieval (with fallback) ────────────────────────────────────────

def get_current_zscores(conn) -> ZScoreValues:
    """
    Get current z-scores: DB first, then compute from raw if needed.
    Never raises; returns ZScoreValues with source="unavailable" on full failure.
    """
    # Priority A: read from derived_indicators
    v = _read_zscores_from_db(conn)
    if v is not None and v.has_core_indicators():
        return v

    # Priority B: compute from raw data
    logger.info("Falling back to raw-data z-score computation")
    try:
        v_raw = _compute_zscores_from_raw(conn)
        if v_raw.has_core_indicators():
            return v_raw
        # Merge partial results from DB and computed
        if v is not None:
            merged = ZScoreValues(
                vix_z    = v.vix_z    if v.vix_z    is not None else v_raw.vix_z,
                hy_z     = v.hy_z     if v.hy_z     is not None else v_raw.hy_z,
                spread_z = v.spread_z if v.spread_z is not None else v_raw.spread_z,
                source   = "partial",
            )
            return merged
        return v_raw
    except Exception as exc:
        logger.error("Fallback z-score computation failed entirely: %s", exc)
        return ZScoreValues(source="unavailable")


# ── Delta calculation ─────────────────────────────────────────────────────────

def get_one_hour_ago_values(history: List[Dict]) -> Optional[Dict]:
    """
    From the stored history, find the reading closest to 1 hour ago.
    History entries are dicts with 'timestamp' and z-score fields.
    Returns None if no suitable entry found.
    """
    if not history:
        return None

    now = datetime.now(timezone.utc)
    target = now - timedelta(hours=1)

    best = None
    best_diff = timedelta(days=999)

    for entry in history:
        ts_str = entry.get("timestamp")
        if not ts_str:
            continue
        try:
            ts = datetime.fromisoformat(ts_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            diff = abs(ts - target)
            # Accept entries within ±10 minutes of the 1-hour mark
            if diff < best_diff and diff < timedelta(minutes=10):
                best = entry
                best_diff = diff
        except Exception:
            continue

    return best


def compute_deltas(
    current: ZScoreValues,
    past_entry: Optional[Dict],
) -> Tuple[Optional[float], Optional[float]]:
    """
    Compute 1-hour deltas for VIX_Z and HY_OAS_Z.
    Returns (delta_vix, delta_hy) — either may be None if data missing.
    """
    if past_entry is None:
        return None, None

    delta_vix: Optional[float] = None
    delta_hy:  Optional[float] = None

    try:
        past_vix = past_entry.get("VIX_Z_252")
        if current.vix_z is not None and past_vix is not None:
            delta_vix = current.vix_z - float(past_vix)
    except Exception:
        pass

    try:
        past_hy = past_entry.get("HY_OAS_Z_252")
        if current.hy_z is not None and past_hy is not None:
            delta_hy = current.hy_z - float(past_hy)
    except Exception:
        pass

    return delta_vix, delta_hy


# ── Tripwire evaluation ───────────────────────────────────────────────────────

def evaluate_tripwires(
    values: ZScoreValues,
    risk_light: str,
    prev_light: Optional[str],
    history: List[Dict],
) -> Tuple[bool, Optional[str], List[str], Optional[float], Optional[float]]:
    """
    Evaluate Rule A (light change) and Rule B (delta spike).

    Returns:
        triggered        : bool
        alert_type       : "LIGHT_CHANGE" | "DELTA_SPIKE" | None
        trigger_reasons  : list of human-readable strings
        delta_vix        : float | None
        delta_hy         : float | None
    """
    reasons: List[str] = []
    alert_type: Optional[str] = None
    triggered = False

    # ── Rule A: Risk light changed ────────────────────────────────────────────
    if prev_light is not None and risk_light != prev_light:
        triggered = True
        alert_type = "LIGHT_CHANGE"
        reasons.append(f"燈號改變：{prev_light} → {risk_light}")
        logger.info("Rule A triggered: %s → %s", prev_light, risk_light)

    # ── Rule B: 1-hour delta spike ────────────────────────────────────────────
    past_entry = get_one_hour_ago_values(history)
    delta_vix, delta_hy = compute_deltas(values, past_entry)

    delta_triggered = False
    if delta_vix is not None and abs(delta_vix) >= DELTA_VIX_THRESHOLD:
        delta_triggered = True
        direction = "+" if delta_vix > 0 else ""
        reasons.append(
            f"VIX_Z 1小時變化 {direction}{delta_vix:.2f}（|Δ| ≥ {DELTA_VIX_THRESHOLD}）"
        )
        logger.info("Rule B (VIX delta) triggered: Δ=%.3f", delta_vix)

    if delta_hy is not None and abs(delta_hy) >= DELTA_HY_THRESHOLD:
        delta_triggered = True
        direction = "+" if delta_hy > 0 else ""
        reasons.append(
            f"HY_OAS_Z 1小時變化 {direction}{delta_hy:.2f}（|Δ| ≥ {DELTA_HY_THRESHOLD}）"
        )
        logger.info("Rule B (HY delta) triggered: Δ=%.3f", delta_hy)

    if delta_triggered and alert_type is None:
        # Only upgrade to DELTA_SPIKE if Rule A didn't already set LIGHT_CHANGE
        triggered = True
        alert_type = "DELTA_SPIKE"

    return triggered, alert_type, reasons, delta_vix, delta_hy


# ── Main monitor cycle ────────────────────────────────────────────────────────

def run_monitor_cycle(
    conn,
    state_mgr,
    force_initial_alert: bool = False,
) -> TripwireResult:
    """
    Execute one monitor cycle.

    Steps:
      1. Load z-scores (DB → fallback)
      2. Compute risk light
      3. Evaluate tripwires
      4. Apply cooldown / escalation logic
      5. Return TripwireResult (caller sends alert and updates state)

    This function has NO side effects (no DB writes, no alert sends, no state writes).
    """
    from monitor.state_manager import is_escalation

    # ── Step 1: Load z-scores ─────────────────────────────────────────────────
    values = get_current_zscores(conn)
    logger.info(
        "Indicators loaded  VIX_Z=%s  HY_OAS_Z=%s  SPREAD_Z=%s  source=%s",
        _fmt(values.vix_z),
        _fmt(values.hy_z),
        _fmt(values.spread_z),
        values.source,
    )

    if values.is_fully_unavailable():
        logger.warning(
            "All core indicators unavailable — skipping alert evaluation this cycle"
        )
        return TripwireResult(
            risk_light="GREEN",
            prev_light=state_mgr.get_last_risk_light(),
            values=values,
            skipped=True,
            skip_reason="All indicators unavailable",
        )

    # ── Step 2: Compute risk light ────────────────────────────────────────────
    risk_light = compute_risk_light(values)
    prev_light = state_mgr.get_last_risk_light()
    history    = state_mgr.get_history()

    logger.info(
        "Risk light  current=%s  previous=%s",
        risk_light, prev_light or "UNKNOWN",
    )

    # ── Step 3: First run — baseline only ─────────────────────────────────────
    if state_mgr.is_first_run():
        if force_initial_alert:
            logger.info("--force-initial-alert set: will evaluate tripwires on first run")
        else:
            logger.info("First run — recording baseline, no alert sent")
            return TripwireResult(
                risk_light = risk_light,
                prev_light = None,
                values     = values,
                triggered  = False,
            )

    # ── Step 4: Evaluate tripwires ────────────────────────────────────────────
    triggered, alert_type, reasons, delta_vix, delta_hy = evaluate_tripwires(
        values, risk_light, prev_light, history
    )

    logger.info(
        "Tripwire eval  triggered=%s  alert_type=%s  delta_vix=%s  delta_hy=%s",
        triggered,
        alert_type or "none",
        _fmt(delta_vix),
        _fmt(delta_hy),
    )

    if not triggered:
        logger.info("TRIPWIRE_ALERT_SKIPPED_NO_CHANGE")
        return TripwireResult(
            risk_light   = risk_light,
            prev_light   = prev_light,
            values       = values,
            triggered    = False,
            delta_vix    = delta_vix,
            delta_hy     = delta_hy,
        )

    # ── Step 5: Cooldown check ────────────────────────────────────────────────
    escalating = is_escalation(prev_light, risk_light)

    if not escalating and state_mgr.is_cooldown_active(alert_type):
        logger.info(
            "TRIPWIRE_ALERT_SKIPPED_COOLDOWN  alert_type=%s", alert_type
        )
        return TripwireResult(
            risk_light      = risk_light,
            prev_light      = prev_light,
            values          = values,
            triggered       = False,   # suppressed by cooldown
            alert_type      = alert_type,
            trigger_reasons = reasons,
            delta_vix       = delta_vix,
            delta_hy        = delta_hy,
        )

    if escalating:
        logger.info(
            "Cooldown bypassed: risk escalation %s → %s", prev_light, risk_light
        )

    # ── Step 6: Alert should fire ─────────────────────────────────────────────
    return TripwireResult(
        risk_light      = risk_light,
        prev_light      = prev_light,
        values          = values,
        triggered       = True,
        alert_type      = alert_type,
        trigger_reasons = reasons,
        delta_vix       = delta_vix,
        delta_hy        = delta_hy,
    )


# ── Formatting helper ─────────────────────────────────────────────────────────

def _fmt(v: Optional[float]) -> str:
    return f"{v:+.3f}" if v is not None else "N/A"
