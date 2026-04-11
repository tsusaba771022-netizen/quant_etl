"""
Daily Portfolio Report
-----------------------
輸入：Snapshot + RegimeResult + Signals + Positions
輸出：繁體中文 Markdown 日報

固定 Baseline v1.0：
  Core     : VOO 70%
  Tactical : QQQM(上限12%) / SMH(上限10%) / 2330.TW(上限8%)，總上限30%
  Scouting Multiplier : 0.50
"""
from __future__ import annotations

import re
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Dict, NamedTuple, Optional

if TYPE_CHECKING:
    from monitor.data_health import DataHealthResult
    from engine.trend import TrendResult
    from engine.macro_alloc import MacroAllocResult

from engine.regime import RegimeResult
from engine.regime_matrix import RegimeMatrix, RegimeMatrixResult
from engine.signals import AssetSignal
from engine.snapshot import Snapshot
from backtest.strategy import Positions
from indicators.zscore import ZSCORE_DISPLAY_ORDER, ZSCORE_TARGETS

OUTPUT_DIR = Path(__file__).parent.parent / "output"


# ── Risk Summary 結構（單一來源，避免 icon / level / message 各自判斷）──────────

class RiskSummary(NamedTuple):
    """
    Z-Score 風險燈號的完整結構。
    所有欄位由 _zscore_risk_signal_v2() 同時產生，確保一致性。
    """
    level:   str   # "green" | "yellow" | "red"
    icon:    str   # "🟢" | "🟡" | "🔴"
    title:   str   # 短標題（e.g. "風險警告"）
    message: str   # 完整描述文字（含指標名稱與數值）

# ── Baseline v1.0 參數 ──────────────────────────────────────────────────────────
SCOUTING_MULT: float = 0.50
CORE_WEIGHT:   float = 0.70

TACTICAL_CAPS: Dict[str, float] = {
    "QQQM":    0.12,
    "SMH":     0.10,
    "2330.TW": 0.08,
}
TACTICAL_CAP_DISPLAY: Dict[str, str] = {
    "QQQM":    "12%",
    "SMH":     "10%",
    "2330.TW": " 8%",
}
ASSET_DISPLAY: Dict[str, str] = {
    "VOO":     "VOO  (Vanguard S&P 500 ETF)",
    "QQQM":    "QQQM (Invesco Nasdaq 100 ETF)",
    "SMH":     "SMH  (VanEck Semiconductor ETF)",
    "2330.TW": "2330.TW (台積電)",
}

# ── Scenario 標題 ───────────────────────────────────────────────────────────────
SCENARIO_HEADER: Dict[str, str] = {
    "A":       "✅ Scenario A — 恐慌錯殺，長線加碼視窗",
    "B":       "⚡ Scenario B — 修正但未崩，保守布局",
    "C":       "🚨 Scenario C — 結構性惡化，降低曝險",
    "Neutral": "🟦 Neutral — 市場平靜，持倉觀察",
}
SCENARIO_ACTION: Dict[str, str] = {
    "A":       "市場出現恐慌性拋售但信用結構健康，可分批試探建倉戰術部位。",
    "B":       "市場壓力升溫，尚未系統性崩潰，保守分批布局，嚴守個別標的上限。",
    "C":       "信用或實體經濟明確惡化，戰術部位訊號全數 NO_TRADE，現金優先。",
    "Neutral": "宏觀無明確壓力，戰術部位視個別訊號調整，不需大幅異動。",
}

# ── Signal 顯示 ─────────────────────────────────────────────────────────────────
SIGNAL_ICON: Dict[str, str] = {
    "BUY":      "🟢 BUY",
    "WAIT":     "🟡 WAIT",
    "NO_TRADE": "🔴 NO_TRADE",
}
STRENGTH_ICON: Dict[str, str] = {
    "Conviction": "⭐⭐⭐ Conviction",
    "Main":       "⭐⭐ Main",
    "Scouting":   "⭐ Scouting",
}
CONFIDENCE_ICON: Dict[str, str] = {
    "High":   "🟢 High",
    "Medium": "🟡 Medium",
    "Low":    "🔴 Low",
}


# ── Helper ─────────────────────────────────────────────────────────────────────

def _fmt(val, fmt_str: str, suffix: str = "", na: str = "N/A") -> str:
    if val is None:
        return na
    return f"{val:{fmt_str}}{suffix}"


def _cfnai_status(val) -> str:
    # CFNAI: 0 = historical avg; >+0.70 = above-trend; <-0.70 = recession risk onset
    if val is None:
        return "N/A ⚠️"
    if val >= 0.70:   return f"{val:+.2f}  📈 高於趨勢成長 (>+0.7)"
    if val >= 0.10:   return f"{val:+.2f}  📊 溫和擴張"
    if val >= -0.10:  return f"{val:+.2f}  📊 接近均值"
    if val >= -0.70:  return f"{val:+.2f}  📉 放緩"
    return                f"{val:+.2f}  ⚠️ 衰退風險 (<-0.7)"


def _oas_status(oas) -> str:
    if oas is None:  return "N/A"
    if oas < 3.5:    return f"{oas:.2f}%  🟢 健康"
    if oas < 5.0:    return f"{oas:.2f}%  🟡 偏高"
    if oas < 7.0:    return f"{oas:.2f}%  🟠 警戒"
    return               f"{oas:.2f}%  🔴 危險 (>7%)"


def _spread_status(sp) -> str:
    if sp is None:   return "N/A"
    if sp > 1.5:     return f"{sp:+.2f}%  🟢 正斜率健康"
    if sp > 0:       return f"{sp:+.2f}%  🟡 略為正斜率"
    if sp > -0.5:    return f"{sp:+.2f}%  🟠 輕微倒掛"
    return               f"{sp:+.2f}%  🔴 深度倒掛"


def _vix_status(vix) -> str:
    if vix is None:  return "N/A"
    if vix < 15:     return f"{vix:.1f}  🟢 低波動"
    if vix < 20:     return f"{vix:.1f}  🟡 正常"
    if vix < 28:     return f"{vix:.1f}  🟠 偏高"
    return               f"{vix:.1f}  🔴 恐慌 (>28)"


# ── 核心建構函式 ───────────────────────────────────────────────────────────────

def build_report(
    snap:           Snapshot,
    regime:         RegimeResult,
    signals:        Dict[str, AssetSignal],
    pos:            Positions,
    output_dir:     Path = OUTPUT_DIR,
    health:         "Optional[DataHealthResult]" = None,
    trend:          "Optional[TrendResult]" = None,
    macro_alloc:    "Optional[MacroAllocResult]" = None,
    effective_caps: "Optional[Dict[str, float]]" = None,
) -> str:
    lines: list[str] = []
    matrix = RegimeMatrix().compute(snap)   # Growth × Inflation（不拋例外）
    _header(lines, snap, regime, pos)
    _macro_section(lines, snap)
    _zscore_section(lines, snap)           # Phase 1：標準化風險座標
    _allocation_summary_section(lines, pos)
    _regime_section(lines, regime, snap)
    _regime_matrix_section(lines, matrix)  # Growth × Inflation 平行觀察
    _portfolio_section(lines, signals, pos, effective_caps=effective_caps)
    _action_section(lines, regime, signals)
    _yesterday_comparison_section(lines, snap, regime, pos, output_dir)
    _discipline_section(lines, snap, regime)       # 執行紀律防呆提醒
    _layer_section(lines, regime, trend, macro_alloc)  # 四層風險架構（Phase 1 參考）
    _data_health_section(lines, health)            # 資料健康度儀表板
    _footer(lines, snap)
    return "\n".join(lines)


# ── 各區塊 ────────────────────────────────────────────────────────────────────

def _one_line_summary(regime: RegimeResult, pos: Positions) -> str:
    """生成一句話結論，例如：今日結論：維持 VOO 70%，戰術倉全數觀望，現金 30%。"""
    tactical_parts = []
    for asset in ["QQQM", "SMH", "2330.TW"]:
        w = pos.weights.get(asset, 0.0)
        if w > 0.001:
            tactical_parts.append(f"{asset} {w*100:.1f}%")

    if tactical_parts:
        tactical_str = "、".join(tactical_parts)
    else:
        tactical_str = "戰術倉全數觀望"

    cash_str = f"現金 {pos.cash_weight*100:.0f}%"
    return f"今日結論：維持 VOO 70%，{tactical_str}，{cash_str}。"


def _header(lines: list, snap: Snapshot, regime: RegimeResult, pos: Positions) -> None:
    summary = _one_line_summary(regime, pos)
    lines += [
        "# 每日投資組合報告",
        "",
        f"> **分析日期**：{snap.as_of}　｜　"
        f"**Scenario**：{regime.scenario}　｜　"
        f"**Confidence**：{CONFIDENCE_ICON.get(regime.confidence_score, regime.confidence_score)}",
        "",
        f"**{summary}**",
        "",
        "---",
        "",
    ]


def _macro_section(lines: list, snap: Snapshot) -> None:
    # ISM PMI 行：月資料需標示實際資料日期
    pmi_date_note = (
        f"（資料日期：{snap.ism_pmi_date}）" if snap.ism_pmi_date is not None else ""
    )
    # HY OAS 行：日資料也標示日期（若有）
    oas_date_note = (
        f"（{snap.hy_oas_date}）" if snap.hy_oas_date is not None else ""
    )
    lines += [
        "## 一、宏觀市場指標",
        "",
        "| 指標 | 數值 | 資料日期 |",
        "|------|------|---------|",
        f"| Macro Growth (CFNAI) | {_cfnai_status(snap.ism_pmi)} | {pmi_date_note} |",
        f"| HY OAS 信用利差 | {_oas_status(snap.hy_oas)} | {oas_date_note} |",
        f"| 10Y-2Y 利差 | {_spread_status(snap.spread_10y2y)} | |",
        f"| VIX 波動指數 | {_vix_status(snap.vix)} | |",
        f"| VIX 百分位 (252日) | {_fmt(snap.vix_pct_rank, '.1%', na='N/A')} | |",
        "",
    ]
    if snap.missing_indicators:
        lines += [
            f"> ⚠️ **缺失指標**（不影響流程，但降低 Confidence）：{', '.join(snap.missing_indicators)}",
            "",
        ]


def _zscore_risk_signal(z_scores: Dict[str, Optional[float]]) -> tuple[str, str]:
    """
    根據 VIX / HY_OAS / YIELD_SPREAD z-score 計算風險燈號。

    規則（平行觀察層，不影響 Scenario A/B/C 判定）：
      🔴 高風險 : VIX_Z_252 >= 2.0  OR  HY_OAS_Z_252 >= 2.0
      🟡 警戒  : VIX_Z_252 >= 1.0  OR  HY_OAS_Z_252 >= 1.0
                 OR  YIELD_SPREAD_10Y2Y_Z_252 <= -1.0
      🟢 正常  : 其他（含全部為 None 的情況）

    Returns: (icon, one_line_description)
    """
    vix_z = z_scores.get("VIX_Z_252")
    hy_z  = z_scores.get("HY_OAS_Z_252")
    sp_z  = z_scores.get("YIELD_SPREAD_10Y2Y_Z_252")

    # 🔴 高風險
    if (vix_z is not None and vix_z >= 2.0) or (hy_z is not None and hy_z >= 2.0):
        reasons = []
        if vix_z is not None and vix_z >= 2.0:
            reasons.append(f"VIX z={vix_z:+.2f}")
        if hy_z is not None and hy_z >= 2.0:
            reasons.append(f"HY OAS z={hy_z:+.2f}")
        return "🔴", f"高風險（{', '.join(reasons)}）：市場壓力顯著高於歷史均值，不建議擴大戰術曝險"

    # 🟡 警戒
    alert_reasons = []
    if vix_z is not None and vix_z >= 1.0:
        alert_reasons.append(f"VIX z={vix_z:+.2f}")
    if hy_z is not None and hy_z >= 1.0:
        alert_reasons.append(f"HY OAS z={hy_z:+.2f}")
    if sp_z is not None and sp_z <= -1.0:
        alert_reasons.append(f"Spread z={sp_z:+.2f}")
    if alert_reasons:
        return "🟡", f"警戒（{', '.join(alert_reasons)}）：部分指標偏離歷史均值，戰術倉建議保守執行"

    # 🟢 正常
    return "🟢", "正常：標準化指標均在歷史均值附近（±1σ），無明顯異常訊號"


def _z_interpret(z: Optional[float], risk_dir: str = "up") -> str:
    """
    將 z-score 轉為中文解讀文字。

    risk_dir:
      "up"      — z 越高，風險越高（VIX、HY OAS、VVIX）
      "down"    — z 越低，風險越高（10Y-2Y 利差）
      "neutral" — 中性，僅描述偏高/偏低
    """
    if z is None:
        return "N/A  ⚪"

    # ── 文字描述 ──────────────────────────────────────────────────────────────
    abs_z = abs(z)
    if abs_z < 1.0:
        level, color = "正常範圍（±1σ）", "🟢"
    elif abs_z < 2.0:
        level, color = ("偏高" if z > 0 else "偏低"), "🟡"
    elif abs_z < 3.0:
        level, color = ("顯著偏高（>+2σ）" if z > 0 else "顯著偏低（<-2σ）"), "🟠"
    else:
        level, color = ("極端高（>+3σ）" if z > 0 else "極端低（<-3σ）"), "🔴"

    # ── 風險方向修正 ──────────────────────────────────────────────────────────
    risk_hint = ""
    if risk_dir == "up" and z >= 2.0:
        risk_hint = " ⚠️ 風險訊號"
    elif risk_dir == "down" and z <= -2.0:
        risk_hint = " ⚠️ 風險訊號（利差收窄/倒掛）"

    return f"{z:+.2f}  {color} {level}{risk_hint}"


def _zscore_section(lines: list, snap: Snapshot) -> None:
    """
    標準化風險座標區塊（Phase 1 新增）。

    - 僅顯示已計算的 z-score（N/A 表示尚未執行 zscore indicator）
    - 不影響 Scenario / Signal baseline
    - 若全部為 None，顯示提示訊息
    """
    if not snap.z_scores:
        return

    any_loaded = any(v is not None for v in snap.z_scores.values())

    lines += [
        "## 一-B、標準化風險座標（Rolling Z-Score 252日）",
        "",
        "> 以過去 252 個交易日為基準，將各指標標準化至同一座標系。"
        "  ±1σ = 正常範圍；±2σ = 顯著偏離；±3σ = 歷史極端值。",
        "> **注意**：z-score 僅供觀察，不直接改變 Scenario 判定或 Signal。",
        "",
    ]

    if not any_loaded:
        lines += [
            "> ⚠️ **Z-Score 尚無資料**（請先執行 `python -m indicators.run_indicators --only zscore`）",
            "",
            "---",
            "",
        ]
        return

    lines += [
        "| 指標 | Z-Score 解讀 |",
        "|------|-------------|",
    ]

    # 按顯示順序輸出
    n_na = 0
    for z_name in ZSCORE_DISPLAY_ORDER:
        z_val = snap.z_scores.get(z_name)
        if z_val is None:
            n_na += 1
            continue   # 未計算的 optional 指標不顯示

        cfg      = ZSCORE_TARGETS.get(z_name, {})
        label    = cfg.get("label_zh", z_name)
        risk_dir = cfg.get("risk_dir", "neutral")
        interp   = _z_interpret(z_val, risk_dir)
        lines.append(f"| {label} | {interp} |")

    if n_na > 0:
        lines += [
            "",
            f"> ℹ️ {n_na} 個指標無資料（可能為 optional 且尚未接入資料層）",
        ]

    # ── Z-Score 風險燈號（平行觀察層）────────────────────────────────────────
    summary = _zscore_risk_signal_v2(snap.z_scores)
    lines += [
        "",
        "**Z-Score 風險燈號**",
        "",
        f"- {summary.icon} {summary.message}",
        "> ⚠️ 此燈號為標準化指標的平行觀察層，不取代目前 Scenario A/B/C 判定。",
        "",
    ]

    lines += ["---", ""]


def _zscore_risk_signal_v2(z_scores: Dict[str, Optional[float]]) -> RiskSummary:
    """
    悲觀覆寫版（Pessimistic Override）：
    只要任一指標超過閾值，就強制切成對應燈號。

    返回 RiskSummary，level / icon / title / message 由此單一函式產生，
    避免 UI 顏色與文案各自判斷造成不同步。

    觸發方向規則（P2）：
      VIX    : 對稱（|z| >= 閾值）— 過低 = 過度樂觀，過高 = 恐慌，兩方向均有意義
      HY OAS : 單向（z >= 閾值）  — 利差擴大才代表信用壓力；利差收窄屬信用健康
      Spread : 對稱（|z| >= 閾值）— 極端正斜率與倒掛均為異常

    分級規則：
      red    : 任一指標超過 2.0 閾值
      yellow : （無紅燈）任一指標超過 1.0 閾值
      green  : 所有指標未超過閾值（或全部為 None）
    """
    watched = [
        ("VIX",    z_scores.get("VIX_Z_252")),
        ("HY OAS", z_scores.get("HY_OAS_Z_252")),
        ("Spread", z_scores.get("YIELD_SPREAD_10Y2Y_Z_252")),
    ]

    def _over(label: str, value: float, threshold: float) -> bool:
        # HY OAS：單向觸發，僅利差擴大（z 正向）才算信用壓力
        if label == "HY OAS":
            return value >= threshold
        # VIX / Spread：對稱觸發
        return abs(value) >= threshold

    red_reasons = [
        f"{label} z={value:+.2f}"
        for label, value in watched
        if value is not None and _over(label, value, 2.0)
    ]
    if red_reasons:
        indicators = "、".join(red_reasons)
        return RiskSummary(
            level   = "red",
            icon    = "🔴",
            title   = "風險警告",
            message = f"風險警告｜異常指標：{indicators}。系統切換為防禦模式。",
        )

    yellow_reasons = [
        f"{label} z={value:+.2f}"
        for label, value in watched
        if value is not None and _over(label, value, 1.0)
    ]
    if yellow_reasons:
        indicators = "、".join(yellow_reasons)
        return RiskSummary(
            level   = "yellow",
            icon    = "🟡",
            title   = "風險升溫",
            message = f"風險升溫｜需留意指標偏離：{indicators}。",
        )

    return RiskSummary(
        level   = "green",
        icon    = "🟢",
        title   = "正常",
        message = "正常｜主要風險指標仍在可接受範圍內。",
    )


def _regime_section(lines: list, regime: RegimeResult, snap: Snapshot) -> None:
    lines += [
        "## 二、Regime 判定",
        "",
        f"### {SCENARIO_HEADER.get(regime.scenario, regime.scenario)}",
        "",
        f"| 項目 | 數值 |",
        f"|------|------|",
        f"| Regime 標籤 | {regime.regime} |",
        f"| Market Phase | {regime.market_phase} |",
        f"| Regime Score | {regime.regime_score:.1f} / 100 |",
        f"| Confidence | {CONFIDENCE_ICON.get(regime.confidence_score, regime.confidence_score)} |",
        "",
        "**維度分數（0~100，越高越健康）**",
        "",
        "| 維度 | 得分 | 權重 |",
        "|------|------|------|",
        f"| Macro（CFNAI）| {regime.macro_score:.1f} | 30% |",
        f"| Credit（HY OAS）| {regime.credit_score:.1f} | 30% |",
        f"| Liquidity（殖利率曲線）| {regime.liquidity_score:.1f} | 15% |",
        f"| Sentiment（VIX）| {regime.sentiment_score:.1f} | 25% |",
        "",
        f"> {regime.rationale}",
        "",
    ]

    if regime.scenario == "C":
        oas_cur  = f"{snap.hy_oas:.2f}%" if snap.hy_oas is not None else "N/A"
        vix_cur  = f"{snap.vix:.1f}" if snap.vix is not None else "N/A"
        lines += [
            "> **解除 Scenario C 條件**（滿足其一即可升級）",
            ">",
            f"> - 🟠 HY OAS 信用利差：目前 {oas_cur}，**需降至 ≤ 7.0%**",
            f"> - 🟠 VIX 波動指數：目前 {vix_cur}，**需降至 ≤ 20**",
            "",
        ]

    lines += [
        "---",
        "",
    ]


def _regime_matrix_section(lines: list, matrix: RegimeMatrixResult) -> None:
    """
    Growth × Inflation 宏觀座標矩陣（平行觀察層）。

    - 僅供觀察，不影響 Scenario A/B/C 判定或任何 Signal
    - 輸入來自 Snapshot.z_scores（已由 _load_zscores 載入）
    - 任一軸缺資料 → 顯示 N/A，不中斷報告生成
    """
    lines += [
        "## 二-B、Growth × Inflation 宏觀座標矩陣",
        "",
        "> **平行觀察層**：此矩陣為補充視角，不改變 Scenario 判定或投資組合配置。",
        "> Growth 軸 = CFNAI z-score（60M）× 0.5 + 10Y-2Y 利差 z-score × 0.5",
        "> Inflation 軸 = 10Y 殖利率 z-score × 0.6 + 2Y 殖利率 z-score × 0.4",
        "",
        "| 軸 | 合成分數 | 使用指標 |",
        "|---|---------|---------|",
    ]

    growth_used    = "、".join(matrix.growth_used)    or "—"
    inflation_used = "、".join(matrix.inflation_used) or "—"

    lines += [
        f"| 📈 Growth    | {matrix.growth_label}    | {growth_used} |",
        f"| 🌡️ Inflation | {matrix.inflation_label} | {inflation_used} |",
        "",
    ]

    if matrix.is_valid:
        lines += [
            f"### 當前象限：{matrix.quadrant_icon} {matrix.quadrant}",
            "",
            f"> {matrix.quadrant_desc}",
            "",
            "| 象限 | 條件 | 說明 |",
            "|------|------|------|",
            "| 🟢 Goldilocks  | Growth ↑, Inflation ↓ | 成長擴張、通膨平抑，最理想環境 |",
            "| 🟡 Overheating | Growth ↑, Inflation ↑ | 景氣過熱，注意緊縮政策風險 |",
            "| 🔴 Recession   | Growth ↓, Inflation ↓ | 景氣下行、通縮壓力 |",
            "| 🚨 Stagflation | Growth ↓, Inflation ↑ | 停滯性通膨，最惡劣組合 |",
        ]
    else:
        missing_axes = []
        if matrix.growth_score is None:
            missing_axes.append("Growth（需 ISM_PMI_MFG_Z_60M 或 YIELD_SPREAD_10Y2Y_Z_252）")
        if matrix.inflation_score is None:
            missing_axes.append("Inflation（需 US_10Y_YIELD_Z_252 或 US_2Y_YIELD_Z_252）")
        lines += [
            "> ⚠️ **象限無法判定**：以下軸缺乏資料",
            "> - " + "\n> - ".join(missing_axes),
            "> 請先執行 `python -m indicators.run_indicators --only zscore`",
        ]

    lines += ["", "---", ""]


def _portfolio_section(
    lines: list,
    signals: Dict[str, AssetSignal],
    pos: Positions,
    effective_caps: "Optional[Dict[str, float]]" = None,
) -> None:
    lines += [
        "## 三、投資組合配置（今日目標）",
        "",
    ]

    # ── Core ──────────────────────────────────────────────────────────────────
    lines += [
        "### 核心持倉（固定，不受 Regime 影響）",
        "",
        "| 標的 | 目標比例 | 說明 |",
        "|------|---------|------|",
        f"| {ASSET_DISPLAY['VOO']} | **70.0%** | 長期核心，每月定期定額 |",
        "",
    ]

    # ── Tactical ──────────────────────────────────────────────────────────────
    lines += [
        "### 戰術持倉（訊號驅動，總上限 30%）",
        "",
        f"| 標的 | 訊號 | 強度 | 目標比例 | 個別上限 | 1W 漲跌 | 1M 漲跌 |",
        f"|------|------|------|---------|---------|---------|---------|",
    ]

    for asset in ["QQQM", "SMH", "2330.TW"]:
        sig = signals.get(asset)
        # 計算有效上限顯示字串（DEFENSIVE 時壓縮並加警示）
        if (effective_caps is not None
                and effective_caps.get(asset) is not None
                and TACTICAL_CAPS.get(asset) is not None
                and effective_caps[asset] < TACTICAL_CAPS[asset]):
            cap_str = f"{effective_caps[asset]*100:.1f}% ⚠️"
        else:
            cap_str = TACTICAL_CAP_DISPLAY[asset]

        if sig is None:
            lines.append(f"| {ASSET_DISPLAY[asset]} | — | — | — | {cap_str} | — | — |")
            continue

        target_w  = pos.weights.get(asset, 0.0)
        sig_icon  = SIGNAL_ICON.get(sig.signal_type, sig.signal_type)
        str_icon  = STRENGTH_ICON.get(sig.signal_strength, sig.signal_strength)
        chg1w     = _fmt(sig.metadata.get("chg_1w_pct"), "+.2f", "%")
        chg1m     = _fmt(sig.metadata.get("chg_1m_pct"), "+.2f", "%")
        fk_warn   = " ⚠️FK" if sig.falling_knife else ""

        lines.append(
            f"| {ASSET_DISPLAY[asset]} "
            f"| {sig_icon}{fk_warn} "
            f"| {str_icon} "
            f"| **{target_w*100:.1f}%** "
            f"| {cap_str} "
            f"| {chg1w} "
            f"| {chg1m} |"
        )

    # ── Cash ──────────────────────────────────────────────────────────────────
    tactical_sum = sum(pos.weights.get(a, 0.0) for a in ["QQQM", "SMH", "2330.TW"])
    cash_w = pos.cash_weight

    # Baseline 參數備注（DEFENSIVE 時額外說明）
    defensive_note = ""
    if (effective_caps is not None
            and any(effective_caps.get(a, TACTICAL_CAPS.get(a, 0)) < TACTICAL_CAPS.get(a, 0)
                    for a in ["QQQM", "SMH", "2330.TW"])):
        defensive_note = "　｜　⚠️ **Layer 3 DEFENSIVE active — 戰術上限壓縮 × 50%**"

    lines += [
        "",
        "### 現金部位",
        "",
        f"| 項目 | 比例 |",
        f"|------|------|",
        f"| VOO（核心）| 70.0% |",
        f"| 戰術合計 | {tactical_sum*100:.1f}% |",
        f"| **現金** | **{cash_w*100:.1f}%** |",
        f"| **總計** | **100.0%** |",
        "",
        f"> **Baseline 參數**：scouting\\_mult = {SCOUTING_MULT}，戰術上限 QQQM 12% / SMH 10% / 2330.TW 8%{defensive_note}",
        "",
        "---",
        "",
    ]


def _allocation_summary_section(lines: list, pos: Positions) -> None:
    """今日目標配置彙總（放在宏觀指標之後）。"""
    cash_w = pos.cash_weight
    lines += [
        "## 今日目標配置",
        "",
        "| 標的 | 配置比例 |",
        "|------|---------|",
        f"| VOO | **70.0%** |",
        f"| QQQM | **{pos.weights.get('QQQM', 0.0)*100:.1f}%** |",
        f"| SMH | **{pos.weights.get('SMH', 0.0)*100:.1f}%** |",
        f"| 2330.TW | **{pos.weights.get('2330.TW', 0.0)*100:.1f}%** |",
        f"| 現金 | **{cash_w*100:.1f}%** |",
        f"| **合計** | **100.0%** |",
        "",
        "---",
        "",
    ]


def _action_section(
    lines: list,
    regime: RegimeResult,
    signals: Dict[str, AssetSignal],
) -> None:
    lines += [
        "## 四、今日操作建議",
        "",
        f"> {SCENARIO_ACTION.get(regime.scenario, '')}",
        "",
    ]

    action_lines = []

    # VOO：永遠持有，只在特定條件提示
    action_lines.append("- **VOO（核心）**：維持 70% 目標，若因市場波動偏離超過 ±3% 則再平衡。")

    for asset in ["QQQM", "SMH", "2330.TW"]:
        sig = signals.get(asset)
        if sig is None:
            continue

        if sig.signal_type == "NO_TRADE":
            action_lines.append(
                f"- **{asset}（戰術）**：🔴 NO_TRADE — 不建倉，持有現金替代。"
            )
        elif sig.signal_type == "WAIT":
            action_lines.append(
                f"- **{asset}（戰術）**：🟡 WAIT — 暫停加碼，等待動能確認後再布局。"
            )
        elif sig.signal_type == "BUY":
            fk_note = "（注意：Falling Knife 偵測，分批極小量進場）" if sig.falling_knife else ""
            cap     = TACTICAL_CAPS[asset]
            # target_w not available here; read from metadata or just show strength
            action_lines.append(
                f"- **{asset}（戰術）**：🟢 BUY / {sig.signal_strength} — "
                f"依目標配置表建倉（個別上限 {cap*100:.0f}%）{fk_note}"
            )

    lines += action_lines
    lines += ["", "---", ""]


def _yesterday_comparison_section(
    lines:      list,
    snap:       Snapshot,
    regime:     RegimeResult,
    pos:        Positions,
    output_dir: Path,
) -> None:
    """與昨日相比：Scenario 變動、配置變動、主要驅動因子。"""
    yesterday = snap.as_of - timedelta(days=1)
    prev_path = output_dir / f"daily_report_{yesterday}.md"

    if not prev_path.exists():
        return  # 無昨日報告，略過此區塊

    try:
        prev_text = prev_path.read_text(encoding="utf-8")
    except Exception:
        return

    # ── 解析昨日 Scenario ────────────────────────────────────────────────────
    m = re.search(r"\*\*Scenario\*\*：(\w+)", prev_text)
    prev_scenario = m.group(1) if m else "?"

    # ── 解析昨日配置 ─────────────────────────────────────────────────────────
    def _parse_weight(text: str, label: str) -> Optional[float]:
        m = re.search(rf"\| {re.escape(label)} \| \*\*([\d.]+)%\*\* \|", text)
        return float(m.group(1)) / 100 if m else None

    prev_qqqm    = _parse_weight(prev_text, "QQQM")
    prev_smh     = _parse_weight(prev_text, "SMH")
    prev_2330    = _parse_weight(prev_text, "2330.TW")
    prev_cash_m  = re.search(r"\| 現金 \| \*\*([\d.]+)%\*\* \|", prev_text)
    prev_cash    = float(prev_cash_m.group(1)) / 100 if prev_cash_m else None

    # ── 解析昨日 HY OAS / VIX ────────────────────────────────────────────────
    m_oas = re.search(r"HY OAS 信用利差 \| ([\d.]+)%", prev_text)
    m_vix = re.search(r"VIX 波動指數 \| ([\d.]+)", prev_text)
    prev_oas = float(m_oas.group(1)) if m_oas else None
    prev_vix = float(m_vix.group(1)) if m_vix else None

    # ── Scenario 比較 ────────────────────────────────────────────────────────
    if prev_scenario == regime.scenario:
        sc_line = f"Scenario：{prev_scenario} → **{regime.scenario}**（持平）"
    else:
        # 判斷升/降
        order = {"A": 0, "B": 1, "Neutral": 2, "C": 3}
        prev_ord = order.get(prev_scenario, 99)
        curr_ord = order.get(regime.scenario, 99)
        direction = "惡化 ⬇️" if curr_ord > prev_ord else "改善 ⬆️"
        sc_line = f"Scenario：{prev_scenario} → **{regime.scenario}**（{direction}）"

    # ── 配置比較 ─────────────────────────────────────────────────────────────
    def _delta_str(label: str, prev: Optional[float], curr: float) -> str:
        if prev is None:
            return f"{label}：— → **{curr*100:.1f}%**"
        delta = curr - prev
        if abs(delta) < 0.005:
            return f"{label}：{curr*100:.1f}%（持平）"
        sign = "+" if delta > 0 else ""
        return f"{label}：{prev*100:.1f}% → **{curr*100:.1f}%**（{sign}{delta*100:.1f}%）"

    alloc_lines = [
        _delta_str("QQQM",    prev_qqqm, pos.weights.get("QQQM",    0.0)),
        _delta_str("SMH",     prev_smh,  pos.weights.get("SMH",     0.0)),
        _delta_str("2330.TW", prev_2330, pos.weights.get("2330.TW", 0.0)),
        _delta_str("現金",    prev_cash, pos.cash_weight),
    ]

    # ── 主要驅動因子 ─────────────────────────────────────────────────────────
    drivers = []
    if snap.hy_oas is not None and prev_oas is not None:
        d = snap.hy_oas - prev_oas
        if abs(d) >= 0.05:
            drivers.append(f"HY OAS {prev_oas:.2f}% → {snap.hy_oas:.2f}%（{'+' if d>0 else ''}{d:.2f}%）")
    if snap.vix is not None and prev_vix is not None:
        d = snap.vix - prev_vix
        if abs(d) >= 0.5:
            drivers.append(f"VIX {prev_vix:.1f} → {snap.vix:.1f}（{'+' if d>0 else ''}{d:.1f}）")
    driver_str = "　｜　".join(drivers) if drivers else "主要指標無顯著變動"

    lines += [
        "## 五、與昨日相比",
        "",
        f"| 項目 | 變動 |",
        f"|------|------|",
        f"| {sc_line} | |",
    ]
    for al in alloc_lines:
        lines.append(f"| {al} | |")
    lines += [
        f"| 主要驅動因子 | {driver_str} |",
        "",
        "---",
        "",
    ]


# ── 執行紀律防呆提醒（純報表層，不影響策略邏輯）─────────────────────────────

# Scenario → 加碼模式文字
_SCALING_MODE: Dict[str, tuple] = {
    "A": (
        "🟢 機會加碼模式（本月可提高至最高 12 萬）",
        "目前 Scenario A：恐慌錯殺，信用結構健康，"
        "可考慮將本月額外資金提高至上限，分批加入戰術倉。",
    ),
    "B": (
        "🟡 標準模式（建議每月 6–8 萬）",
        "目前 Scenario B：市場修正但未崩，維持標準每月投入，"
        "戰術倉依個別訊號分批加碼即可，不需大幅調整。",
    ),
    "Neutral": (
        "🟡 標準模式（建議每月 6–8 萬）",
        "目前 Scenario Neutral：宏觀無明確壓力，按計劃執行標準定投，"
        "戰術倉維持現有部位，靜待明確訊號。",
    ),
    "C": (
        "🔴 保守模式（維持每月 6–8 萬，戰術加碼暫停）",
        "目前 Scenario C：結構性惡化，戰術倉額外加碼暫停，"
        "但核心 VOO 每月固定投入不中斷。"
        "等待 HY OAS ≤ 7.0% 或 VIX ≤ 20 再恢復戰術加碼。",
    ),
}


def _discipline_section(
    lines:  list,
    snap:   Snapshot,
    regime: RegimeResult,
) -> None:
    """
    執行紀律防呆提醒（第六節）。
    純報表層，三個子區塊：
      1. 長期定投紀律（固定文字）
      2. 本月加碼模式（依 Scenario 動態調整文字）
      3. 資料品質與使用限制（依 Snapshot 缺值狀態動態生成）
    不修改任何策略邏輯、權重、Signal。
    """
    lines += [
        "## 六、執行紀律提醒",
        "",
        "> 本區塊為固定防呆提示，協助正確使用本報告，**不構成任何具體操作指令**。",
        "",
    ]
    _discipline_core_reminder(lines)
    _discipline_scaling_reminder(lines, regime)
    _discipline_data_quality(lines, snap)


def _discipline_core_reminder(lines: list) -> None:
    """長期定投紀律——固定文字，每日顯示。"""
    lines += [
        "### 長期定投紀律",
        "",
        "| 原則 | 說明 |",
        "|------|------|",
        "| 核心 VOO 70%：持續定投 | 不因任何 Scenario 或短期訊號中斷每月固定投入 |",
        "| 戰術系統是加碼工具，非進出場訊號 | Scenario 判定決定「額外資金」配置方向，不是停止投資的依據 |",
        "| Scenario C ≠ 停止投資 | C 代表**戰術倉保守觀望**，核心 VOO 定投照常執行 |",
        "| 勿以 daily report 合理化不投資 | 市場永遠有理由等待；定期投入的複利效果不可用短期訊號抵銷 |",
        "",
    ]


def _discipline_scaling_reminder(lines: list, regime: RegimeResult) -> None:
    """本月加碼模式——依當前 Scenario 動態標示建議模式。"""
    scenario  = regime.scenario
    mode_text, mode_note = _SCALING_MODE.get(
        scenario,
        _SCALING_MODE["Neutral"],   # fallback
    )

    lines += [
        "### 本月加碼模式",
        "",
        f"> **目前模式**：{mode_text}",
        f">",
        f"> {mode_note}",
        "",
        "| 情境 | 每月投入參考 | 操作方向 |",
        "|------|------------|---------|",
        "| 標準月（B / Neutral） | 6–8 萬 | 核心 VOO 定期定額，戰術倉依訊號分批 |",
        "| 機會月（Scenario A）  | 最高 12 萬 | 提高額外資金，加入戰術倉分批布局 |",
        "| 保守月（Scenario C）  | 維持 6–8 萬 | 戰術倉暫停額外加碼，核心 VOO 不停 |",
        "",
        "> **額外資金原則**：超出標準月的加碼金額，一律進入戰術倉（QQQM / SMH / 2330.TW），"
        "不改變 VOO 70% 核心配置比例。",
        "",
    ]


def _discipline_data_quality(lines: list, snap: Snapshot) -> None:
    """
    資料品質與使用限制——動態生成。
    依 Snapshot 中的缺值狀態、資料日期、信心水準顯示提醒。
    """
    lines += ["### 資料品質與使用限制", ""]

    # ── 最近一期沿用的指標 ────────────────────────────────────────────────────
    reuse_lines: list[str] = []

    # CFNAI（月資料，必然沿用上期）
    if snap.ism_pmi is not None and snap.ism_pmi_date is not None:
        reuse_lines.append(
            f"- **CFNAI**：使用 `{snap.ism_pmi_date}` 期資料（月資料，屬正常現象，每月更新一次）"
        )
    elif snap.ism_pmi is None:
        reuse_lines.append(
            "- **CFNAI**：目前無可用資料（過去 45 天內未取得，Confidence 已下調）"
        )

    # HY OAS（日資料，假日/非交易日可能沿用）
    if snap.hy_oas is not None and snap.hy_oas_date is not None:
        days_gap = (snap.as_of - snap.hy_oas_date).days
        if days_gap >= 1:
            reuse_lines.append(
                f"- **HY OAS**：沿用 `{snap.hy_oas_date}` 資料"
                f"（距今 {days_gap} 天，假日或非交易日前值填補，正常現象）"
            )
    elif snap.hy_oas is None:
        reuse_lines.append("- **HY OAS**：目前無可用資料（Confidence 已下調）")

    if reuse_lines:
        lines += ["**最近一期沿用的指標：**", ""] + reuse_lines + [""]
    else:
        lines += ["> ✅ 所有日常指標均取得當日資料，無需沿用前期。", ""]

    # ── 信心下降的指標 ────────────────────────────────────────────────────────
    missing = snap.missing_indicators
    if missing:
        lines += ["**信心下降的指標（缺值/無資料）：**", ""]
        for m in missing:
            lines.append(f"- `{m}`：本期無可用資料")
        lines += [
            "",
            "> 上述缺失指標已反映於 Regime Confidence 評級，"
            "戰術倉訊號可信度相應下降。",
            "",
        ]

    # ── Z-Score 覆蓋狀況 ─────────────────────────────────────────────────────
    if snap.z_scores:
        z_missing = [k for k, v in snap.z_scores.items() if v is None]
        z_available = len(snap.z_scores) - len(z_missing)
        if z_missing:
            lines += [
                f"> ℹ️ 標準化風險座標（Z-Score）：{z_available}/{len(snap.z_scores)} 個指標有效，"
                f"缺失：{', '.join(z_missing[:3])}{'…' if len(z_missing) > 3 else ''}。"
                f"可執行 `python -m indicators.run_indicators --only zscore` 補算。",
                "",
            ]

    # ── 整體信心水準 ──────────────────────────────────────────────────────────
    _CONF_ICON = {"High": "🟢", "Medium": "🟡", "Low": "🔴"}
    _CONF_NOTE = {
        "High":   "所有關鍵指標齊全，戰術訊號可信度正常，可依報告建議操作。",
        "Medium": "部分指標缺失（通常為月資料），戰術訊號仍可參考，但建議謹慎分批。",
        "Low":    "多個關鍵指標缺失，戰術訊號可信度明顯下降，本月建議維持標準定投，暫停戰術加碼。",
    }
    conf  = snap.confidence_score
    icon  = _CONF_ICON.get(conf, "⚪")
    note  = _CONF_NOTE.get(conf, "")

    lines += [
        f"> **整體資料品質**：{icon} **{conf}**　——　{note}",
        ">",
        "> ⚠️ **資料品質不影響核心定投**。每月固定 VOO 投入與長期計畫，無論資料品質如何均應持續執行。",
        "",
        "---",
        "",
    ]


def _layer_section(
    lines:       list,
    regime:      RegimeResult,
    trend:       "Optional[TrendResult]",
    macro_alloc: "Optional[MacroAllocResult]",
) -> None:
    """
    四層風險架構狀態（Layer Status）。
    Phase 1：僅展示，尚未連接 allocation override 邏輯。
    此區塊放在 discipline_section 之後，不被 send_line.py / line_flex.py 的 regex 讀取。
    """
    lines += [
        "## 七、四層風險架構（Layer Status）",
        "",
        "> ⚠️ **Phase 1 — 僅供參考**：Trend Layer 與 Macro Alloc 計算已上線，"
        "但尚未連接 allocation override 邏輯。以下狀態不影響本報告的 Scenario / Signal。",
        "",
    ]

    # ── Layer 2：Trend Risk Cap ────────────────────────────────────────────────
    lines += ["### Layer 2：Trend Risk Cap（VOO 200 日均線）", ""]
    if trend is None:
        lines += ["> ℹ️ 未計算（TrendLayer 未呼叫或發生錯誤）。", ""]
    else:
        _TREND_ICON = {
            "TREND_WARMUP":   "⚪ WARMUP",
            "TREND_OK":       "🟢 OK",
            "TREND_CAUTION":  "🟡 CAUTION",
            "TREND_RISK_CAP": "🔴 RISK_CAP",
        }
        icon = _TREND_ICON.get(trend.status.value, trend.status.value)
        close_str = f"{trend.close:.2f}" if trend.close is not None else "N/A"
        sma_str   = f"{trend.sma_200:.2f}" if trend.sma_200 is not None else "N/A"
        slope_str = f"{trend.sma_200_slope:+.4f}" if trend.sma_200_slope is not None else "N/A"
        lines += [
            f"| 項目 | 值 |",
            f"|------|----|",
            f"| 狀態 | **{icon}** |",
            f"| VOO 收盤 | {close_str} |",
            f"| SMA_200 | {sma_str} |",
            f"| SMA_200 Slope（20 日） | {slope_str} |",
            f"| 歷史資料筆數 | {trend.history_len} |",
            f"| 說明 | {trend.rationale} |",
            "",
        ]
        if trend.status.value == "TREND_WARMUP":
            lines += [
                "> ℹ️ Warm-up 狀態：歷史資料不足 220 筆，Trend Risk Cap 暫時旁路（bypass）。",
                "",
            ]
        elif trend.status.value == "TREND_RISK_CAP":
            lines += [
                "> ⚠️ RISK_CAP 狀態：VOO 收盤低於 200DMA 且均線已平坦/下彎。"
                "Phase 2 連接後，將限制最大 equity exposure，不得高風險滿配。",
                "",
            ]

    # ── Layer 3：Macro Allocation ─────────────────────────────────────────────
    lines += ["### Layer 3：Macro Allocation（CFNAI × Yield Spread × VIX）", ""]
    if macro_alloc is None:
        lines += ["> ℹ️ 未計算（classify_macro_alloc 未呼叫或發生錯誤）。", ""]
    else:
        _ALLOC_ICON = {
            "AGGRESSIVE": "🟢 AGGRESSIVE",
            "NEUTRAL":    "🟡 NEUTRAL",
            "DEFENSIVE":  "🔴 DEFENSIVE",
        }
        icon = _ALLOC_ICON.get(macro_alloc.status.value, macro_alloc.status.value)
        cfnai_str   = f"{macro_alloc.cfnai:+.2f}"   if macro_alloc.cfnai        is not None else "N/A"
        spread_str  = f"{macro_alloc.spread:+.2f}%"  if macro_alloc.spread       is not None else "N/A"
        vix_str     = f"{macro_alloc.vix:.1f}"       if macro_alloc.vix          is not None else "N/A"
        pct_str     = f"{macro_alloc.vix_pct_rank:.0%}" if macro_alloc.vix_pct_rank is not None else "N/A"
        lines += [
            f"| 項目 | 值 |",
            f"|------|----|",
            f"| 狀態 | **{icon}** |",
            f"| CFNAI | {cfnai_str} |",
            f"| Yield Spread | {spread_str} |",
            f"| VIX | {vix_str} |",
            f"| VIX 百分位（252日） | {pct_str} |",
            f"| 說明 | {macro_alloc.rationale} |",
            "",
        ]
        # Credit Veto 壓制提示（Scenario C）
        if regime.scenario == "C":
            lines += [
                "> 🔒 **Credit Veto active**（Scenario C）：Layer 3 傾向被 Layer 1 壓制，"
                "Macro Alloc 狀態僅供記錄，不影響當前 allocation 決策。",
                "",
            ]
        else:
            lines += [
                "> ℹ️ Layer 3 傾向在 Phase 2 連接後，將在 Credit Veto / Trend Cap 允許的"
                "上限內決定 allocation 傾向。",
                "",
            ]

    lines += ["---", ""]


def _data_health_section(lines: list, health: "Optional[DataHealthResult]") -> None:
    """資料健康度儀表板（Tripwire z-score 三指標的新鮮度與可用性）。"""
    if health is None:
        lines += [
            "## 資料健康度（Data Health Check）",
            "",
            "> ℹ️ 本次未執行資料健康度檢查（health=None）。",
            "",
            "---",
            "",
        ]
        return

    from monitor.data_health import format_health_md
    section = format_health_md(health)
    lines += [section, "---", ""]


def _footer(lines: list, snap: Snapshot) -> None:
    lines += [
        f"*報告生成：{snap.as_of}　｜　策略版本：Baseline v1.0　｜　"
        "系統：quant\\_etl daily\\_report*",
    ]
