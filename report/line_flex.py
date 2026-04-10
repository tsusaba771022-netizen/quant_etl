"""
LINE Flex Message 組裝模組 — Axiom Quant 戰情卡
--------------------------------------------------
生成高端量化戰情卡：深色科技底板、分層資訊架構、快速掃讀設計。

公開介面：
  build_line_flex_payload(md_text, report_date) -> dict
  返回可直接放入 messages list 的 Flex Message dict。
"""
from __future__ import annotations

import re
from datetime import date

# ── 設計常數 ──────────────────────────────────────────────────────────────────

_SCENARIO_COLOR: dict[str, str] = {
    "A":       "#00A86B",   # 冷靜科技綠
    "B":       "#D4820A",   # 琥珀
    "C":       "#C62828",   # 警報紅
    "Neutral": "#1565C0",   # 深藍
}
_BODY_BG  = "#0B0F1A"      # 深海底
_FTR_BG   = "#080B12"      # 更深頁腳
_BADGE_BG = "#111927"      # 結論卡底

_WHITE    = "#FFFFFF"
_LABEL    = "#4D7A8C"      # 章節標籤（低調藍灰）
_VALUE    = "#C8D8E8"      # 數值（冷白）
_MUTED    = "#607A8A"      # 次要說明文字
_DIM      = "#182535"      # 分隔線

_GREEN    = "#3DD68C"      # Z-Score 正常區
_AMBER    = "#FFA726"      # Z-Score 警戒區
_RED      = "#EF5350"      # Z-Score 風險區

_SC_LABEL: dict[str, str] = {
    "A":       "▲  SCENARIO  A",
    "B":       "■  SCENARIO  B",
    "C":       "▼  SCENARIO  C",
    "Neutral": "●  NEUTRAL",
}


# ── 基礎元件 ──────────────────────────────────────────────────────────────────

def _t(text: str, **kw) -> dict:
    """text component 簡寫"""
    d: dict = {"type": "text", "text": str(text)}
    d.update(kw)
    return d


def _sep() -> dict:
    """1 px 深色分隔線，帶上下 margin。"""
    return {
        "type": "box",
        "layout": "vertical",
        "contents": [],
        "height": "1px",
        "backgroundColor": _DIM,
        "margin": "lg",
    }


def _section_label(text: str) -> dict:
    """章節標籤：全大寫、細小、低對比。"""
    return {
        "type": "box",
        "layout": "vertical",
        "contents": [_t(text, color=_LABEL, size="xxs", weight="bold")],
        "margin": "md",
        "paddingBottom": "xs",
    }


def _kv(label: str, value: str,
        val_color: str = _VALUE,
        lf: int = 4, vf: int = 5) -> dict:
    """
    水平鍵值列：label 左對齊、value 右對齊。
    lf / vf 為 flex 比例。
    """
    return {
        "type": "box",
        "layout": "horizontal",
        "contents": [
            _t(label, color=_LABEL, size="sm", flex=lf),
            _t(value, color=val_color, size="sm", flex=vf,
               align="end", weight="bold"),
        ],
        "paddingTop": "xs",
        "paddingBottom": "xs",
    }


def _kv_wrap(label: str, value: str, val_color: str = _VALUE) -> dict:
    """
    操作建議用：label 粗體左欄，value 允許換行。
    label 固定 2 格，value 7 格。
    """
    return {
        "type": "box",
        "layout": "horizontal",
        "contents": [
            _t(label, color=_LABEL, size="xs", weight="bold", flex=2),
            _t(value, color=val_color, size="xs", flex=7, wrap=True),
        ],
        "paddingTop": "xs",
        "paddingBottom": "xs",
    }


def _z_color(z_text: str) -> str:
    """依 Z-Score 值返回顏色：abs<1 → 綠，1-2 → 琥珀，≥2 → 紅。"""
    m = re.search(r'([+-]?\d+\.\d+)', z_text)
    if not m:
        return _VALUE
    v = abs(float(m.group(1)))
    if v >= 2.0:
        return _RED
    if v >= 1.0:
        return _AMBER
    return _GREEN


# ── 報告解析 ──────────────────────────────────────────────────────────────────

def _pick(pattern: str, text: str, default: str = "N/A") -> str:
    m = re.search(pattern, text)
    return m.group(1).strip() if m else default


def _parse(md_text: str) -> dict:
    """從日報 markdown 萃取 Flex 所需的所有欄位。找不到時填 N/A。"""
    p: dict = {}

    # 基本狀態
    p["scenario"]   = _pick(r'\*\*Scenario\*\*：(\w+)', md_text)
    p["confidence"] = _pick(r'\*\*Confidence\*\*：.+?(High|Medium|Low)', md_text)
    p["conclusion"] = _pick(r'\*\*今日結論：(.+?)\*\*', md_text)

    # 目標配置
    p["voo"]  = _pick(r'\|\s*VOO\s*\|\s*\*\*([\d.]+%)\*\*',      md_text)
    p["qqqm"] = _pick(r'\|\s*QQQM\s*\|\s*\*\*([\d.]+%)\*\*',     md_text)
    p["smh"]  = _pick(r'\|\s*SMH\s*\|\s*\*\*([\d.]+%)\*\*',      md_text)
    p["tsmc"] = _pick(r'\|\s*2330\.TW\s*\|\s*\*\*([\d.]+%)\*\*', md_text)
    p["cash"] = _pick(r'\|\s*現金\s*\|\s*\*\*([\d.]+%)\*\*',      md_text)

    # 風險指標
    p["vix"]     = _pick(r'\|\s*VIX 波動指數\s*\|\s*([\d.]+)',           md_text)
    p["vix_pct"] = _pick(r'\|\s*VIX 百分位 \(252日\)\s*\|\s*([\d.]+%)',  md_text)
    p["hy_oas"]  = _pick(r'\|\s*HY OAS 信用利差\s*\|\s*([\d.]+%)',       md_text)
    p["spread"]  = _pick(r'\|\s*10Y-2Y 利差\s*\|\s*([+\-\d.]+%)',        md_text)
    pmi_raw      = _pick(r'\|\s*Macro Growth \(CFNAI\)\s*\|\s*([^|\n]+)',  md_text)
    _pmi_num     = re.search(r'\d+\.?\d*', pmi_raw)
    p["pmi"]     = _pmi_num.group(0) if _pmi_num else "N/A"

    # 操作建議
    p["op_summary"] = _pick(r'四、今日操作建議[\s\S]{0,50}>\s*([^\n>][^\n]+)', md_text)
    p["voo_op"]     = _pick(r'\*\*VOO（核心）\*\*：([^\n]+)',      md_text)
    p["qqqm_sig"]   = _pick(r'\*\*QQQM（戰術）\*\*：([^\n]+)',     md_text)
    p["smh_sig"]    = _pick(r'\*\*SMH（戰術）\*\*：([^\n]+)',       md_text)
    p["tsmc_sig"]   = _pick(r'\*\*2330\.TW（戰術）\*\*：([^\n]+)', md_text)

    # 昨日對比
    sc_raw        = _pick(r'Scenario：(\w+ → \*\*\w+\*\*[^|\n]+)', md_text)
    p["sc_change"]    = re.sub(r'\*\*(\w+)\*\*', r'\1', sc_raw)
    rdrv_raw          = _pick(r'主要驅動因子\s*\|\s*([^|\n]+)', md_text)
    p["risk_drivers"] = re.sub(r'\*\*|\s{2,}', ' ', rdrv_raw).strip()

    # Z-Score（section 一-B 子文字）
    _zsec_m   = re.search(r'## 一-B、標準化風險座標[\s\S]+?(?=\n---)', md_text)
    _zsec     = _zsec_m.group(0) if _zsec_m else ""
    _zclean   = lambda s: re.sub(r'\s{2,}', ' ', s).strip()
    p["vix_z"]    = _zclean(_pick(r'\|\s*VIX（波動率）\s*\|\s*([^|]+)\|',      _zsec))
    p["hy_z"]     = _zclean(_pick(r'\|\s*HY OAS（信用利差）\s*\|\s*([^|]+)\|', _zsec))
    p["spread_z"] = _zclean(_pick(r'\|\s*10Y-2Y 利差\s*\|\s*([^|]+)\|',        _zsec))
    p["z_signal"] = _pick(
        r'\*\*Z-Score 風險燈號\*\*\s*\n\s*\n\s*-\s*(.+?)(?=\n)', _zsec
    )

    # 資料提醒
    p["missing"] = _pick(r'缺失指標[^：]*：([^\n>]+)', md_text)
    ffill_m      = re.search(
        r'\*\*HY OAS\*\*：沿用\s*`([^`]+)`\s*資料（距今 (\d+) 天', md_text
    )
    p["ffill"] = (
        f"HY OAS 沿用 {ffill_m.group(1)} (+{ffill_m.group(2)}d)"
        if ffill_m else None
    )

    return p


# ── Flex 組裝 ─────────────────────────────────────────────────────────────────

def build_line_flex_payload(md_text: str, report_date: date) -> dict:
    """
    解析日報 markdown，返回 LINE Flex Message payload dict。
    格式：{"type": "flex", "altText": "...", "contents": <bubble>}
    """
    p   = _parse(md_text)
    sc  = p["scenario"]
    hc  = _SCENARIO_COLOR.get(sc, _SCENARIO_COLOR["Neutral"])

    # ── Header ────────────────────────────────────────────────────────────
    header = {
        "type": "box",
        "layout": "horizontal",
        "contents": [
            _t("AXIOM QUANT", color=_WHITE, weight="bold", size="sm", flex=5),
            _t(str(report_date), color="#FFFFFF80", size="xs",
               flex=4, align="end"),
        ],
        "paddingAll": "md",
    }

    # ── Section 1: Scenario badge + Conclusion ────────────────────────────
    badge_box = {
        "type": "box",
        "layout": "vertical",
        "contents": [
            _t(_SC_LABEL.get(sc, sc), color=hc, weight="bold",
               size="lg", align="center"),
            _t(f"Confidence：{p['confidence']}", color=_MUTED, size="xs",
               align="center", margin="xs"),
        ],
        "paddingTop": "lg",
        "paddingBottom": "sm",
    }

    conclusion_card = {
        "type": "box",
        "layout": "vertical",
        "backgroundColor": _BADGE_BG,
        "cornerRadius": "md",
        "paddingAll": "md",
        "margin": "sm",
        "contents": [
            _t(p["conclusion"], color=_WHITE, size="sm", wrap=True, weight="bold"),
        ],
    }

    # ── Section 2: Allocation ─────────────────────────────────────────────
    alloc = [
        _sep(),
        _section_label("ALLOCATION"),
        {
            "type": "box",
            "layout": "vertical",
            "contents": [
                _kv("VOO",      p["voo"],  _WHITE),
                _kv("QQQM",     p["qqqm"]),
                _kv("SMH",      p["smh"]),
                _kv("2330.TW",  p["tsmc"]),
                _kv("CASH",     p["cash"], _AMBER),
            ],
            "margin": "sm",
        },
    ]

    # ── Section 3: Z-Score Radar ──────────────────────────────────────────
    zscore = [
        _sep(),
        _section_label("Z-SCORE RADAR  /  252D"),
        {
            "type": "box",
            "layout": "vertical",
            "contents": [
                _kv("VIX",    p["vix_z"],    _z_color(p["vix_z"])),
                _kv("HY OAS", p["hy_z"],     _z_color(p["hy_z"])),
                _kv("Spread", p["spread_z"], _z_color(p["spread_z"])),
            ],
            "margin": "sm",
        },
    ]
    if p["z_signal"] and p["z_signal"] != "N/A":
        zscore.append(
            _t(p["z_signal"], color=_MUTED, size="xxs", margin="xs", wrap=True)
        )

    # ── Section 4: Risk Snapshot ──────────────────────────────────────────
    vix_display = p["vix"]
    if p["vix_pct"] != "N/A":
        vix_display = f"{p['vix']}  ({p['vix_pct']})"

    risk = [
        _sep(),
        _section_label("RISK SNAPSHOT"),
        {
            "type": "box",
            "layout": "vertical",
            "contents": [
                _kv("VIX",    vix_display, _VALUE, lf=3, vf=6),
                _kv("HY OAS", p["hy_oas"], _VALUE, lf=3, vf=6),
                _kv("Spread", p["spread"], _VALUE, lf=3, vf=6),
                _kv("PMI",    p["pmi"],    _VALUE, lf=3, vf=6),
            ],
            "margin": "sm",
        },
    ]

    # ── Section 5: Operation ──────────────────────────────────────────────
    ops = [
        _sep(),
        _section_label("OPERATION"),
    ]
    if p["op_summary"] != "N/A":
        ops.append(
            _t(p["op_summary"], color=_VALUE, size="xs", wrap=True, margin="sm")
        )
    ops.append({
        "type": "box",
        "layout": "vertical",
        "contents": [
            _kv_wrap("VOO",     p["voo_op"]),
            _kv_wrap("QQQM",    p["qqqm_sig"]),
            _kv_wrap("SMH",     p["smh_sig"]),
            _kv_wrap("2330.TW", p["tsmc_sig"]),
        ],
        "margin": "sm",
    })

    # ── Section 6: Yesterday ──────────────────────────────────────────────
    yest = [
        _sep(),
        _section_label("VS YESTERDAY"),
    ]
    if p["sc_change"] != "N/A":
        yest.append(_kv("Scenario", p["sc_change"]))
    if p["risk_drivers"] != "N/A":
        yest.append(
            _t(p["risk_drivers"], color=_MUTED, size="xxs", wrap=True, margin="xs")
        )

    # ── Body ──────────────────────────────────────────────────────────────
    body = {
        "type": "box",
        "layout": "vertical",
        "paddingAll": "md",
        "contents": (
            [badge_box, conclusion_card]
            + alloc
            + zscore
            + risk
            + ops
            + yest
        ),
    }

    # ── Footer（僅有資料問題時顯示）───────────────────────────────────────
    notes: list[str] = []
    if p["missing"] and p["missing"] not in ("N/A",):
        notes.append(f"缺值：{p['missing'].strip()}")
    if p["ffill"]:
        notes.append(p["ffill"])
    if p["confidence"] != "High":
        notes.append(f"Confidence {p['confidence']}：部分指標缺失，供參考")

    footer = None
    if notes:
        footer = {
            "type": "box",
            "layout": "vertical",
            "paddingAll": "sm",
            "contents": [
                _t("ℹ  " + "  /  ".join(notes),
                   color=_LABEL, size="xxs", wrap=True),
            ],
        }

    # ── Bubble ────────────────────────────────────────────────────────────
    styles: dict = {
        "header": {"backgroundColor": hc},
        "body":   {"backgroundColor": _BODY_BG},
    }
    bubble: dict = {
        "type":   "bubble",
        "size":   "mega",
        "styles": styles,
        "header": header,
        "body":   body,
    }
    if footer:
        bubble["footer"] = footer
        styles["footer"] = {"backgroundColor": _FTR_BG}

    return {
        "type":     "flex",
        "altText":  f"Axiom Quant 戰情日報 {report_date}  Scenario {sc}",
        "contents": bubble,
    }
