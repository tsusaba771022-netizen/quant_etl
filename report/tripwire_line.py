"""
Tripwire LINE 警報模組
-----------------------
專門用於 Tripwire 事件觸發的短版 LINE 警報推播。
比 daily report 更簡潔，聚焦於「現在發生了什麼、建議怎麼做」。

公開介面:
  send_tripwire_alert(result, prev_light) -> bool

Log markers (搜尋用):
  TRIPWIRE_ALERT_SENT
  TRIPWIRE_LINE_FLEX_FAILED
  TRIPWIRE_LINE_TEXT_FALLBACK_SENT
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import List, Optional

from typing import TYPE_CHECKING

import requests
from dotenv import load_dotenv

if TYPE_CHECKING:
    from monitor.data_health import DataHealthResult

load_dotenv()

logger = logging.getLogger(__name__)

LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"

# ── Design tokens (match existing line_flex.py style) ────────────────────────
_BODY_BG = "#0B0F1A"
_FTR_BG  = "#080B12"
_DIM     = "#182535"
_WHITE   = "#FFFFFF"
_LABEL   = "#4D7A8C"
_VALUE   = "#C8D8E8"
_MUTED   = "#607A8A"

_LIGHT_COLOR = {
    "GREEN":  "#00A86B",
    "YELLOW": "#D4820A",
    "RED":    "#C62828",
}
_LIGHT_ICON = {
    "GREEN":  "🟢",
    "YELLOW": "🟡",
    "RED":    "🔴",
}
_LIGHT_LABEL = {
    "GREEN":  "GREEN  低風險",
    "YELLOW": "YELLOW  警戒",
    "RED":    "RED  高風險",
}

# ── Action recommendation ─────────────────────────────────────────────────────

def _action_text(risk_light: str, prev_light: Optional[str], alert_type: Optional[str]) -> str:
    if alert_type == "DELTA_SPIKE":
        return "劇烈市場變動，等待日報前暫緩大額操作"
    # Light change based
    if risk_light == "RED":
        return "暫停戰術加碼，等待日報確認後調整部位"
    if risk_light == "YELLOW":
        prev = prev_light or "GREEN"
        if prev == "GREEN":
            return "提高警戒，縮短操作週期，關注後續確認訊號"
        return "風險持續，維持保守配置，追蹤下一輪日報"
    if risk_light == "GREEN":
        return "風險改善，可持續觀察是否完全解除警戒"
    return "維持現有配置，等待日報確認"


# ── Flex message builder ──────────────────────────────────────────────────────

def _t(text: str, **kw) -> dict:
    d: dict = {"type": "text", "text": str(text)}
    d.update(kw)
    return d


def _sep() -> dict:
    return {
        "type": "box",
        "layout": "vertical",
        "contents": [],
        "height": "1px",
        "backgroundColor": _DIM,
        "margin": "md",
    }


def _kv(label: str, value: str, val_color: str = _VALUE) -> dict:
    return {
        "type": "box",
        "layout": "horizontal",
        "contents": [
            _t(label, color=_LABEL, size="xs", flex=4),
            _t(value, color=val_color, size="sm", flex=5, align="end", weight="bold"),
        ],
        "margin": "sm",
    }


def _z_color(z: Optional[float]) -> str:
    if z is None:
        return _MUTED
    if abs(z) >= 2.0:
        return "#EF5350"   # red alert
    if abs(z) >= 1.0:
        return "#FFA726"   # amber warning
    return "#3DD68C"       # green normal


def _fmt_z(z: Optional[float]) -> str:
    if z is None:
        return "N/A"
    return f"{z:+.2f}"


def build_tripwire_flex(
    risk_light: str,
    prev_light: Optional[str],
    alert_type: Optional[str],
    trigger_reasons: List[str],
    vix_z: Optional[float],
    hy_z: Optional[float],
    spread_z: Optional[float],
    ts: Optional[datetime] = None,
    health: "Optional[DataHealthResult]" = None,
) -> dict:
    """Build a Flex Message dict for a tripwire alert."""
    accent = _LIGHT_COLOR.get(risk_light, "#888888")
    ts_str = (ts or datetime.now(timezone.utc)).strftime("%Y-%m-%d %H:%M UTC")

    # Change description
    if alert_type == "LIGHT_CHANGE" and prev_light:
        change_text = f"{prev_light} → {risk_light}"
    elif alert_type == "DELTA_SPIKE":
        change_text = "劇烈變動警報"
    else:
        change_text = risk_light

    # Trigger reasons (max 3 lines)
    reason_components = []
    for r in trigger_reasons[:3]:
        reason_components.append(
            _t(f"• {r}", color="#E0E0E0", size="sm", wrap=True, margin="sm")
        )
    if not reason_components:
        reason_components.append(_t("• 風險狀態變化", color="#E0E0E0", size="sm"))

    action = _action_text(risk_light, prev_light, alert_type)

    body_contents = [
        # Header: Tripwire title
        {
            "type": "box",
            "layout": "horizontal",
            "contents": [
                _t("⚡  TRIPWIRE ALERT", color=accent, size="sm", weight="bold", flex=8),
                _t(ts_str, color=_MUTED, size="xxs", flex=5, align="end"),
            ],
            "margin": "none",
        },
        _sep(),
        # Light badge
        {
            "type": "box",
            "layout": "horizontal",
            "contents": [
                _t("燈號", color=_LABEL, size="xs", flex=3),
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        _t(
                            f"{_LIGHT_ICON.get(risk_light, '⚪')}  {_LIGHT_LABEL.get(risk_light, risk_light)}",
                            color=accent,
                            size="sm",
                            weight="bold",
                        )
                    ],
                    "flex": 6,
                    "justifyContent": "flex-end",
                },
            ],
            "margin": "md",
        },
        _kv("狀態變化", change_text, val_color=accent),
        _sep(),
        # Trigger reasons
        _t("觸發原因", color=_LABEL, size="xs", weight="bold", margin="md"),
        *reason_components,
        _sep(),
        # Core z-score values
        _t("核心指標（Z-Score 252日）", color=_LABEL, size="xs", weight="bold", margin="md"),
        _kv("VIX_Z",    _fmt_z(vix_z),    val_color=_z_color(vix_z)),
        _kv("HY_OAS_Z", _fmt_z(hy_z),     val_color=_z_color(hy_z)),
        _kv("SPREAD_Z", _fmt_z(spread_z), val_color=_z_color(spread_z)),
        _sep(),
        # Action recommendation
        {
            "type": "box",
            "layout": "vertical",
            "contents": [
                _t("⚡ " + action, color="#FFA726", size="sm", wrap=True, weight="bold"),
            ],
            "paddingAll": "sm",
            "backgroundColor": "#111927",
            "cornerRadius": "4px",
            "margin": "md",
        },
    ]

    # ── Data health compact footer（optional）────────────────────────────────
    if health is not None:
        try:
            from monitor.data_health import format_health_compact
            health_str = format_health_compact(health)
            body_contents += [
                _sep(),
                _t(health_str, color=_MUTED, size="xxs", wrap=True, margin="sm"),
            ]
        except Exception:
            pass   # health footer is non-critical; never crash the alert

    return {
        "type": "flex",
        "altText": f"Tripwire 警報｜{risk_light}  {change_text}",
        "contents": {
            "type": "bubble",
            "size": "kilo",
            "styles": {
                "body":   {"backgroundColor": _BODY_BG},
                "footer": {"backgroundColor": _FTR_BG},
            },
            "body": {
                "type":     "box",
                "layout":   "vertical",
                "contents": body_contents,
                "paddingAll": "lg",
                "spacing":  "none",
            },
        },
    }


def build_tripwire_text(
    risk_light: str,
    prev_light: Optional[str],
    alert_type: Optional[str],
    trigger_reasons: List[str],
    vix_z: Optional[float],
    hy_z: Optional[float],
    spread_z: Optional[float],
    health: "Optional[DataHealthResult]" = None,
) -> str:
    """Build a plain-text fallback for tripwire alerts."""
    SEP = "─" * 24

    if alert_type == "LIGHT_CHANGE" and prev_light:
        change_text = f"{prev_light} → {risk_light}"
    elif alert_type == "DELTA_SPIKE":
        change_text = "劇烈變動警報"
    else:
        change_text = risk_light

    icon = _LIGHT_ICON.get(risk_light, "⚪")
    action = _action_text(risk_light, prev_light, alert_type)

    lines = [
        f"⚡ Axiom Quant Tripwire 警報",
        SEP,
        f"{icon} 燈號：{risk_light}　（{change_text}）",
        SEP,
        "觸發原因",
    ]
    for r in trigger_reasons:
        lines.append(f"  {r}")
    lines += [
        SEP,
        "核心指標（Z-Score 252日）",
        f"  VIX_Z    {_fmt_z(vix_z)}",
        f"  HY_OAS_Z {_fmt_z(hy_z)}",
        f"  SPREAD_Z {_fmt_z(spread_z)}",
        SEP,
        f"⚡ {action}",
    ]

    # Data health compact footer（optional）
    if health is not None:
        try:
            from monitor.data_health import format_health_compact
            lines += [SEP, format_health_compact(health)]
        except Exception:
            pass

    return "\n".join(lines)


# ── HTTP push ─────────────────────────────────────────────────────────────────

def _push_message(token: str, user_id: str, message_obj: dict) -> bool:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json; charset=UTF-8",
    }
    payload = {"to": user_id, "messages": [message_obj]}
    try:
        resp = requests.post(LINE_PUSH_URL, headers=headers, json=payload, timeout=15)
    except requests.Timeout:
        logger.error("LINE push timeout (15s)")
        return False
    except requests.RequestException as exc:
        logger.error("LINE push network error: %s", exc)
        return False

    if resp.status_code == 200:
        return True
    logger.error(
        "LINE push HTTP error  status=%s  body=%s",
        resp.status_code,
        resp.text[:300].replace("\n", " "),
    )
    return False


# ── Public entry point ────────────────────────────────────────────────────────

def send_tripwire_alert(
    risk_light: str,
    prev_light: Optional[str],
    alert_type: Optional[str],
    trigger_reasons: List[str],
    vix_z: Optional[float],
    hy_z: Optional[float],
    spread_z: Optional[float],
    health: "Optional[DataHealthResult]" = None,
) -> bool:
    """
    Send a tripwire alert via LINE.
    Tries Flex Message first; falls back to plain text.
    Never raises — all errors are logged and False is returned.

    Returns True on success (either Flex or text fallback), False on total failure.
    """
    enabled = os.getenv("LINE_ENABLED", "true").strip().lower()
    if enabled not in ("true", "1", "yes"):
        logger.info("LINE_ENABLED=false — tripwire alert skipped")
        return True

    token   = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
    user_id = os.getenv("LINE_USER_ID", "").strip()
    if not token or not user_id:
        logger.warning(
            "Tripwire alert failed: missing LINE credentials "
            "(LINE_CHANNEL_ACCESS_TOKEN / LINE_USER_ID)"
        )
        return False

    now = datetime.now(timezone.utc)

    # ── Flex attempt ──────────────────────────────────────────────────────────
    try:
        flex_msg = build_tripwire_flex(
            risk_light, prev_light, alert_type,
            trigger_reasons, vix_z, hy_z, spread_z, ts=now,
            health=health,
        )
        if _push_message(token, user_id, flex_msg):
            logger.info(
                "TRIPWIRE_ALERT_SENT  type=flex  light=%s  alert_type=%s",
                risk_light, alert_type,
            )
            return True
        logger.warning("TRIPWIRE_LINE_FLEX_FAILED  falling back to text")
    except Exception as exc:
        logger.warning("TRIPWIRE_LINE_FLEX_FAILED  error=%s  falling back to text", exc)

    # ── Text fallback ─────────────────────────────────────────────────────────
    try:
        text = build_tripwire_text(
            risk_light, prev_light, alert_type,
            trigger_reasons, vix_z, hy_z, spread_z,
            health=health,
        )
        text_msg = {"type": "text", "text": text}
        if _push_message(token, user_id, text_msg):
            logger.info(
                "TRIPWIRE_LINE_TEXT_FALLBACK_SENT  light=%s  alert_type=%s",
                risk_light, alert_type,
            )
            return True
    except Exception as exc:
        logger.error("Tripwire text fallback failed: %s", exc)

    logger.error("Tripwire alert failed: both Flex and text delivery failed")
    return False
