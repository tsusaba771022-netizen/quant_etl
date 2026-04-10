"""
Data Health Check
-----------------
Queries derived_indicators for the freshness and availability of the
three Tripwire z-score indicators:
  VIX_Z_252 / HY_OAS_Z_252 / YIELD_SPREAD_10Y2Y_Z_252

Used by:
  - report/daily_report.py  → full Markdown table at report footer
  - report/tripwire_line.py → compact one-line footer in LINE alert

Design:
  - check_indicator_health(conn) → DataHealthResult
  - format_health_md(result)     → Markdown section string
  - format_health_compact(result)→ one-line string for LINE
  - Never raises; all DB errors caught and reflected in status=NA
  - forward_filled is inferred from staleness > FFILL_THRESHOLD_DAYS
    (threshold set to 1 so weekends also show as ffill — informative only)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

logger = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────

FFILL_THRESHOLD_DAYS  = 1   # flag forward-fill if data older than 1 calendar day
WARN_THRESHOLD_DAYS   = 4   # WARN status if staleness > 4 days
STALE_THRESHOLD_DAYS  = 7   # STALE status if staleness > 7 days

_INDICATORS = [
    "VIX_Z_252",
    "HY_OAS_Z_252",
    "YIELD_SPREAD_10Y2Y_Z_252",
]

_LABEL = {
    "VIX_Z_252":                "VIX Z-Score",
    "HY_OAS_Z_252":             "HY OAS Z-Score",
    "YIELD_SPREAD_10Y2Y_Z_252": "Yield Spread Z-Score",
}

_SHORT_LABEL = {
    "VIX_Z_252":                "VIX",
    "HY_OAS_Z_252":             "HY_OAS",
    "YIELD_SPREAD_10Y2Y_Z_252": "SPREAD",
}

_STATUS_EMOJI = {
    "OK":      "✅",
    "WARN":    "⚠️",
    "STALE":   "🔴",
    "NA":      "⬛",
    "UNKNOWN": "❓",
}

_STATUS_ORDER = {"OK": 0, "WARN": 1, "STALE": 2, "NA": 3, "UNKNOWN": 4}


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class DataHealthItem:
    name:              str
    label:             str
    short_label:       str
    last_updated:      Optional[object] = None   # date | None
    staleness_days:    Optional[float]  = None
    last_value:        Optional[float]  = None
    is_na:             bool             = False
    is_forward_filled: bool             = False
    status:            str              = "UNKNOWN"

    @property
    def status_emoji(self) -> str:
        return _STATUS_EMOJI.get(self.status, "❓")


@dataclass
class DataHealthResult:
    items:   List[DataHealthItem] = field(default_factory=list)
    as_of:   Optional[datetime]   = None
    overall: str                  = "UNKNOWN"

    @property
    def overall_emoji(self) -> str:
        return _STATUS_EMOJI.get(self.overall, "❓")


# ── DB query ──────────────────────────────────────────────────────────────────

_QUERY = """
    SELECT DISTINCT ON (indicator)
        indicator,
        value,
        time::date                                       AS last_updated,
        EXTRACT(EPOCH FROM (NOW() - time)) / 86400.0    AS staleness_days
    FROM derived_indicators
    WHERE indicator = ANY(%s)
      AND time >= NOW() - INTERVAL '30 days'
    ORDER BY indicator, time DESC
"""


def check_indicator_health(conn) -> DataHealthResult:
    """
    Query derived_indicators for z-score freshness.
    Never raises — returns DataHealthResult(overall=NA) on total DB failure.
    """
    as_of  = datetime.now(timezone.utc)
    result = DataHealthResult(as_of=as_of)

    try:
        with conn.cursor() as cur:
            cur.execute(_QUERY, (list(_INDICATORS),))
            rows = cur.fetchall()
    except Exception as exc:
        logger.warning("data_health: DB query failed: %s", exc)
        for name in _INDICATORS:
            result.items.append(DataHealthItem(
                name=name,
                label=_LABEL.get(name, name),
                short_label=_SHORT_LABEL.get(name, name),
                is_na=True,
                status="NA",
            ))
        result.overall = "NA"
        return result

    # Map rows by indicator name
    found: dict = {}
    for indicator, value, last_updated, staleness_days in rows:
        found[indicator] = (value, last_updated, float(staleness_days or 0))

    for name in _INDICATORS:
        label       = _LABEL.get(name, name)
        short_label = _SHORT_LABEL.get(name, name)

        if name not in found:
            item = DataHealthItem(
                name=name, label=label, short_label=short_label,
                is_na=True, status="NA",
            )
        else:
            value, last_updated, staleness = found[name]
            is_na    = value is None
            is_ffill = staleness > FFILL_THRESHOLD_DAYS

            if is_na:
                status = "NA"
            elif staleness > STALE_THRESHOLD_DAYS:
                status = "STALE"
            elif staleness > WARN_THRESHOLD_DAYS:
                status = "WARN"
            else:
                status = "OK"

            item = DataHealthItem(
                name              = name,
                label             = label,
                short_label       = short_label,
                last_updated      = last_updated,
                staleness_days    = round(staleness, 1),
                last_value        = float(value) if value is not None else None,
                is_na             = is_na,
                is_forward_filled = is_ffill,
                status            = status,
            )

        result.items.append(item)
        logger.debug(
            "data_health  %-35s  status=%-5s  staleness=%s days",
            name,
            item.status,
            f"{item.staleness_days:.1f}" if item.staleness_days is not None else "N/A",
        )

    # Overall = worst individual status
    if result.items:
        result.overall = max(
            result.items,
            key=lambda i: _STATUS_ORDER.get(i.status, 9),
        ).status
    else:
        result.overall = "NA"

    logger.info("data_health  overall=%s  items=%d", result.overall, len(result.items))
    return result


# ── Formatters ────────────────────────────────────────────────────────────────

def format_health_md(result: DataHealthResult) -> str:
    """
    Full Markdown section for the daily report footer.
    """
    ts = result.as_of.strftime("%Y-%m-%d %H:%M UTC") if result.as_of else "N/A"

    lines: List[str] = [
        "## 資料健康度（Data Health Check）",
        "",
        f"> 檢查時間：{ts}　｜　整體狀態：{result.overall_emoji} **{result.overall}**",
        "",
        "| 指標 | 狀態 | 最後更新日 | 距今（天）| 最新值 | Forward Fill |",
        "|------|------|-----------|-----------|--------|-------------|",
    ]

    for item in result.items:
        upd   = str(item.last_updated)  if item.last_updated  is not None else "N/A"
        days  = f"{item.staleness_days:.1f}" if item.staleness_days is not None else "N/A"
        val   = f"{item.last_value:+.3f}"    if item.last_value    is not None else "N/A"
        ffill = "⚠️ 是" if item.is_forward_filled else "否"
        lines.append(
            f"| {item.label} | {item.status_emoji} {item.status} "
            f"| {upd} | {days} | {val} | {ffill} |"
        )

    lines.append("")

    # Stale / NA warnings
    problem_items = [i for i in result.items if i.status in ("STALE", "NA")]
    if problem_items:
        lines += [
            "> ⚠️ **資料警告**：以下指標資料過舊或缺失，Tripwire 判斷可信度降低：",
        ]
        for i in problem_items:
            detail = f"（距今 {i.staleness_days:.1f} 天）" if i.staleness_days is not None else ""
            lines.append(f"> - `{i.name}`：{i.status}{detail}")
        lines.append("")

    # Forward-fill reminder (exclude NA items — they have no data to fill)
    ffill_items = [
        i for i in result.items
        if i.is_forward_filled and i.status not in ("NA",)
    ]
    if ffill_items:
        lines += [
            "> ℹ️ **Forward Fill 說明**：以下指標使用前期資料補填（非當日計算值），"
            "假日 / 非交易日屬正常現象：",
        ]
        for i in ffill_items:
            lines.append(
                f"> - `{i.name}`：最後計算日 {i.last_updated}，距今 {i.staleness_days:.1f} 天"
            )
        lines.append("")

    return "\n".join(lines)


def format_health_compact(result: DataHealthResult) -> str:
    """
    Single-line compact summary for LINE alert footer.
    Example: "資料健康：✅VIX | ⚠️HY_OAS(3.2d) | 🔴SPREAD(N/A)"
    """
    parts: List[str] = []
    for item in result.items:
        lbl = item.short_label
        if item.status == "OK":
            parts.append(f"✅{lbl}")
        elif item.status == "WARN":
            days = f"{item.staleness_days:.1f}" if item.staleness_days is not None else "?"
            parts.append(f"⚠️{lbl}({days}d)")
        elif item.status == "STALE":
            days = f"{item.staleness_days:.1f}" if item.staleness_days is not None else "?"
            parts.append(f"🔴{lbl}({days}d)")
        else:
            parts.append(f"⬛{lbl}(N/A)")

    return "資料健康：" + " | ".join(parts)
