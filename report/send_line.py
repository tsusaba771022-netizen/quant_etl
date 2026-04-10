"""
LINE 推播模組
--------------
使用 LINE Messaging API (Push Message) 將每日報告短版摘要推送至 LINE。

.env 必填：
  LINE_CHANNEL_ACCESS_TOKEN  長效型 Channel Access Token
  LINE_USER_ID               推播目標 LINE User ID（以 U 開頭，共 33 字元）

.env 選填：
  LINE_ENABLED               true（預設）/ false → false 時靜默跳過

取得 LINE_USER_ID 的最簡方式：
  1. 將 LINE Bot 加為好友，對 Bot 傳送任一訊息
  2. 在 LINE Developers Console → Webhook 設定一個臨時接收端
     （推薦 https://webhook.site），可即時看到 JSON payload
  3. payload 中 source.userId 就是你的 USER_ID

直接發送測試：
  python -m report.send_line
  python -m report.send_line --date 2026-04-08
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from datetime import date
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

from report.line_flex import build_line_flex_payload

load_dotenv()

logger = logging.getLogger(__name__)

LINE_PUSH_URL   = "https://api.line.me/v2/bot/message/push"
SENT_MARKER_DIR = Path(__file__).parent.parent / "logs"

# ── 防重複發送 ────────────────────────────────────────────────────────────────

def _marker_path(report_date: date) -> Path:
    return SENT_MARKER_DIR / f"line_sent_{report_date}.flag"


def is_already_sent(report_date: date) -> bool:
    """回傳 True = 當日 LINE 推播已有 marker（代表今日已送出過）。"""
    return _marker_path(report_date).exists()


def _write_sent_marker(report_date: date) -> None:
    try:
        SENT_MARKER_DIR.mkdir(exist_ok=True)
        _marker_path(report_date).touch()
    except Exception:
        pass


# ── 萃取 / 解讀輔助函式 ──────────────────────────────────────────────────────

def _pick(pattern: str, text: str, default: str = "N/A") -> str:
    """從 text 以 pattern 擷取第一個 group，失敗回傳 default。"""
    m = re.search(pattern, text)
    return m.group(1).strip() if m else default


def _interp_vix(val: str, pct: str) -> str:
    try:
        v = float(val)
    except (ValueError, TypeError):
        return "無資料"
    try:
        p = float(pct.replace("%", ""))
        pct_tag = f"（{p:.0f}th pct）"
    except Exception:
        pct_tag = ""
    if v < 15:
        return f"{v}{pct_tag} 極低，市場情緒過度樂觀，留意反轉風險"
    elif v < 20:
        return f"{v}{pct_tag} 正常，市場平穩，無明顯壓力"
    elif v < 25:
        return f"{v}{pct_tag} 偏高，風險意識升溫，宜保守布局"
    elif v < 30:
        return f"{v}{pct_tag} 高，市場緊張，短期波動加劇"
    else:
        return f"{v}{pct_tag} 極高，恐慌模式，大幅降低風險敞口"


def _interp_hy(val: str) -> str:
    try:
        v = float(val.replace("%", ""))
    except (ValueError, TypeError):
        return "無資料"
    if v < 3.5:
        return f"{val} 極低，信用市場可能過熱，留意回調"
    elif v < 5.0:
        return f"{val} 健康，信用市場正常，無警示"
    elif v < 7.0:
        return f"{val} 偏高，信用風險升溫，需持續追蹤"
    else:
        return f"{val} 高危，信用市場承壓，C 觸發條件已達"


def _interp_spread(val: str) -> str:
    try:
        v = float(val.replace("%", "").replace("+", ""))
    except (ValueError, TypeError):
        return "無資料"
    if v > 1.0:
        return f"{val} 正斜率健康，無衰退訊號"
    elif v >= 0:
        return f"{val} 略為正斜率，曲線趨於扁平，持續觀察"
    elif v >= -0.5:
        return f"{val} 輕微倒掛，歷史衰退前兆，需警戒"
    else:
        return f"{val} 明顯倒掛，衰退風險升高"


def _interp_cfnai(cfnai: str) -> str:
    """解讀 CFNAI 數值（0 = 歷史均值；+0.70 = 高於趨勢；-0.70 = 衰退風險起點）。"""
    if cfnai in ("N/A", "", "NA"):
        return "N/A — 本月資料尚未取得，總體經濟動能未知"
    try:
        v = float(cfnai.replace("+", ""))
    except (ValueError, TypeError):
        return f"{cfnai} — 資料異常"
    if v >= 0.70:
        return f"{v:+.2f}  高於趨勢成長（≥+0.70），景氣動能充足"
    elif v >= 0.10:
        return f"{v:+.2f}  溫和擴張（+0.10 ~ +0.70），偏正向"
    elif v >= -0.10:
        return f"{v:+.2f}  接近歷史均值（±0.10），中性"
    elif v >= -0.70:
        return f"{v:+.2f}  放緩（-0.10 ~ -0.70），需觀察是否惡化"
    else:
        return f"{v:+.2f}  衰退風險（<-0.70），Scenario C 判定條件之一"


def _gen_observations(scenario: str, vix: str, hy: str, spread: str, pmi: str) -> list[str]:
    obs: list[str] = []
    try:
        v = float(vix)
        if v >= 20:
            obs.append(f"VIX 若突破 25 → Sentiment 轉差，C 觸發機率升高")
    except Exception:
        pass
    try:
        h = float(hy.replace("%", ""))
        if h < 5.0:
            obs.append(f"HY OAS 若升破 7.0% → C 觸發條件達標，應即時降倉")
        else:
            obs.append(f"HY OAS 已偏高，持續監控是否突破 7.0%")
    except Exception:
        pass
    try:
        sp = float(spread.replace("%", "").replace("+", ""))
        if sp < 0:
            obs.append(f"殖利率曲線倒掛中，若持續超過 6 個月為衰退前兆")
        elif sp < 0.5:
            obs.append(f"殖利率曲線趨於扁平，若轉負需升級警戒")
    except Exception:
        pass
    if pmi in ("N/A", "", "NA"):
        obs.append("Macro Growth (CFNAI) 資料缺失，月底更新後可能改變 Macro 維度判定")
    return obs[:3] or ["目前無明顯風險轉折點，維持現有配置"]


# ── 主訊息組裝 ────────────────────────────────────────────────────────────────

def build_line_message(md_text: str, report_date: date) -> str:
    """
    解析日報 markdown，組出中短版 LINE 推播文字。
    所有欄位找不到時填 N/A，不拋例外。
    """
    S = "─" * 22    # 分隔線

    # ── 基本資訊 ──────────────────────────────────────────────────────────
    scenario   = _pick(r'\*\*Scenario\*\*：(\w+)', md_text)
    confidence = _pick(r'\*\*Confidence\*\*：.+?(High|Medium|Low)', md_text)
    conclusion = _pick(r'\*\*今日結論：(.+?)\*\*', md_text)

    # ── 目標配置 ──────────────────────────────────────────────────────────
    voo  = _pick(r'\|\s*VOO\s*\|\s*\*\*([\d.]+%)\*\*',      md_text)
    qqqm = _pick(r'\|\s*QQQM\s*\|\s*\*\*([\d.]+%)\*\*',     md_text)
    smh  = _pick(r'\|\s*SMH\s*\|\s*\*\*([\d.]+%)\*\*',      md_text)
    tsmc = _pick(r'\|\s*2330\.TW\s*\|\s*\*\*([\d.]+%)\*\*', md_text)
    cash = _pick(r'\|\s*現金\s*\|\s*\*\*([\d.]+%)\*\*',      md_text)

    # ── 風險指標 ──────────────────────────────────────────────────────────
    vix     = _pick(r'\|\s*VIX 波動指數\s*\|\s*([\d.]+)',          md_text)
    vix_pct = _pick(r'\|\s*VIX 百分位 \(252日\)\s*\|\s*([\d.]+%)', md_text)
    hy_oas  = _pick(r'\|\s*HY OAS 信用利差\s*\|\s*([\d.]+%)',      md_text)
    spread  = _pick(r'\|\s*10Y-2Y 利差\s*\|\s*([+\-\d.]+%)',       md_text)
    pmi_raw  = _pick(r'\|\s*Macro Growth \(CFNAI\)\s*\|\s*([^|\n]+)', md_text)
    _pmi_num = re.search(r'\d+\.?\d*', pmi_raw)
    pmi      = _pmi_num.group(0) if _pmi_num else "N/A"

    # ── Regime 判定理由 ───────────────────────────────────────────────────
    regime_why_raw = _pick(r'> Scenario \w+：(.+?)(?=\n)', md_text)
    # | 分隔符轉換成換行，方便 LINE 閱讀
    regime_lines = [p.strip() for p in regime_why_raw.split("|") if p.strip()]

    # ── 今日操作建議 ──────────────────────────────────────────────────────
    # 概況一句話（## 四 之後的第一個 blockquote）
    op_summary = _pick(
        r'四、今日操作建議[\s\S]{0,50}>\s*([^\n>][^\n]+)', md_text
    )
    # VOO 操作
    voo_op   = _pick(r'\*\*VOO（核心）\*\*：([^\n]+)',      md_text)
    qqqm_sig = _pick(r'\*\*QQQM（戰術）\*\*：([^\n]+)',     md_text)
    smh_sig  = _pick(r'\*\*SMH（戰術）\*\*：([^\n]+)',      md_text)
    tsmc_sig = _pick(r'\*\*2330\.TW（戰術）\*\*：([^\n]+)', md_text)
    # 月度加碼模式
    monthly_raw = _pick(r'\*\*目前模式\*\*：([^\n]+)', md_text)
    # 去掉前置 emoji（非 ASCII 的 \S+ 字元）
    monthly_mode = re.sub(r'^\S+\s*', '', monthly_raw) if monthly_raw != "N/A" else "N/A"

    # ── 昨日對比 ──────────────────────────────────────────────────────────
    # 擷取「B → **B**（持平）」→ 清理成「B → B（持平）」
    sc_change_raw = _pick(r'Scenario：(\w+ → \*\*\w+\*\*[^|\n]+)', md_text)
    sc_change = re.sub(r'\*\*(\w+)\*\*', r'\1', sc_change_raw)   # 去 bold
    risk_drivers = _pick(r'主要驅動因子\s*\|\s*([^|\n]+)', md_text)
    # 清理 markdown 殘留
    risk_drivers = re.sub(r'\*\*|\s{2,}', ' ', risk_drivers).strip()

    # ── 資料提醒 ──────────────────────────────────────────────────────────
    missing_raw = _pick(r'缺失指標[^：]*：([^\n>]+)', md_text)
    # HY OAS forward fill 日期
    ffill_m = re.search(
        r'\*\*HY OAS\*\*：沿用\s*`([^`]+)`\s*資料（距今 (\d+) 天', md_text
    )
    ffill_note = (
        f"HY OAS 沿用 {ffill_m.group(1)}（距今 {ffill_m.group(2)} 天，假日正常）"
        if ffill_m else None
    )

    # ── Z-Score 雷達（從 section 一-B 子文字解析，避免與 section 一 標籤撞名）──
    # section 一 有 "10Y-2Y 利差"（原始值），section 一-B 也有同名欄位（z-score）
    # 先取出 section 一-B 子文字，再在其中做 regex
    _zsec_m = re.search(r'## 一-B、標準化風險座標[\s\S]+?(?=\n---)', md_text)
    _zsec   = _zsec_m.group(0) if _zsec_m else ""

    def _zpick(pat: str) -> str:
        raw = _pick(pat, _zsec)
        return re.sub(r'\s{2,}', '  ', raw).strip()

    vix_z    = _zpick(r'\|\s*VIX（波動率）\s*\|\s*([^|]+)\|')
    hy_z     = _zpick(r'\|\s*HY OAS（信用利差）\s*\|\s*([^|]+)\|')
    spread_z = _zpick(r'\|\s*10Y-2Y 利差\s*\|\s*([^|]+)\|')
    # Z-Score 風險燈號（daily_report 的 _zscore_risk_signal 輸出）
    signal_raw = _pick(
        r'\*\*Z-Score 風險燈號\*\*\s*\n\s*\n\s*-\s*(.+?)(?=\n)',
        _zsec,
    )

    # ── 數值解讀 ──────────────────────────────────────────────────────────
    vix_note    = _interp_vix(vix, vix_pct)
    hy_note     = _interp_hy(hy_oas)
    spread_note = _interp_spread(spread)
    pmi_note    = _interp_cfnai(pmi)

    # ── 觀察重點（程式化生成）────────────────────────────────────────────
    obs = _gen_observations(scenario, vix, hy_oas, spread, pmi)

    # ── Scenario 標籤 ─────────────────────────────────────────────────────
    _LABEL = {"A": "✅ A", "B": "⚡ B", "C": "🚨 C", "Neutral": "🟦 Neutral"}
    sc_str = _LABEL.get(scenario, scenario)

    # ── 組裝 ──────────────────────────────────────────────────────────────
    parts: list[str] = []

    def add(*lines: str) -> None:
        parts.extend(lines)

    # 1. 標題
    add(f"【量化日報】{report_date}", S)

    # 2. 基本狀態
    add(f"📊 {sc_str}　Confidence：{confidence}")

    # 3. 一句話結論
    add("", f"💡 {conclusion}", S)

    # 4. 今日目標配置
    add(
        "📦 今日目標配置",
        f"  VOO    {voo}（核心定投）",
        f"  QQQM   {qqqm}",
        f"  SMH    {smh}",
        f"  2330   {tsmc}",
        f"  現金   {cash}",
        S,
    )

    # 5. 風險摘要（含解讀）
    add(
        "⚠️ 風險摘要",
        f"  VIX    {vix_note}",
        f"  HY OAS {hy_note}",
        f"  Spread {spread_note}",
        f"  PMI    {pmi_note}",
        S,
    )

    # 5-B. Z-Score 雷達
    add("📐 Z-Score 雷達（252日基準）")
    # 若 z-score 絕對值 > 2.0 加注 ⚠️
    def _z_warn(z_text: str) -> str:
        m = re.search(r'([+-]\d+\.\d+)', z_text)
        if m:
            try:
                v = float(m.group(1))
                if abs(v) >= 2.0:
                    return z_text + "  ⚠️"
            except ValueError:
                pass
        return z_text

    add(
        f"  VIX    {_z_warn(vix_z)}",
        f"  HY OAS {_z_warn(hy_z)}",
        f"  Spread {_z_warn(spread_z)}",
    )
    if signal_raw and signal_raw != "N/A":
        add(f"  燈號  {signal_raw}")
    add(S)

    # 6. 為何判成此 Scenario
    add(f"🔍 為何 Scenario {scenario}")
    for rl in regime_lines:
        add(f"  {rl}")
    add(S)

    # 7. 今日操作建議
    add("🎯 今日操作建議")
    if op_summary != "N/A":
        add(f"  {op_summary}")
    add(
        f"  VOO  ：{voo_op}",
        f"  QQQM ：{qqqm_sig}",
        f"  SMH  ：{smh_sig}",
        f"  2330 ：{tsmc_sig}",
        f"  📅 本月模式：{monthly_mode}",
        S,
    )

    # 8. 與昨日相比
    add("📈 與昨日相比")
    if sc_change != "N/A":
        add(f"  Scenario {sc_change}")
    if risk_drivers != "N/A":
        add(f"  {risk_drivers}")
    add(S)

    # 9. 觀察重點
    add("🔭 觀察重點")
    for i, o in enumerate(obs, 1):
        add(f"  {i}. {o}")
    add(S)

    # 10. 資料提醒（只在有問題時顯示）
    data_notes: list[str] = []
    if missing_raw and missing_raw not in ("N/A",):
        data_notes.append(f"缺值：{missing_raw.strip()}")
    if ffill_note:
        data_notes.append(ffill_note)
    if confidence != "High":
        data_notes.append(f"Confidence {confidence}：部分指標缺失，訊號供參考")
    if data_notes:
        add("ℹ️ 資料提醒")
        for n in data_notes:
            add(f"  • {n}")

    return "\n".join(parts)


# ── 測試模式工具 ──────────────────────────────────────────────────────────────

def _inject_test_banner(flex_payload: dict) -> dict:
    """
    在 Flex Message 頂部注入可見的 TEST 警示橫幅。
    操作在副本上進行，不修改傳入 dict。
    注入位置：bubble body contents 最前面，用顯眼紅底白字標示。
    """
    import copy
    payload = copy.deepcopy(flex_payload)

    test_bar = {
        "type": "box",
        "layout": "horizontal",
        "backgroundColor": "#B71C1C",
        "paddingAll": "sm",
        "contents": [{
            "type": "text",
            "text": "⚠️  TEST MESSAGE — 請勿當作正式訊號  ⚠️",
            "color": "#FFFFFF",
            "size": "xs",
            "weight": "bold",
            "align": "center",
            "wrap": True,
        }],
    }

    try:
        bubble = payload["contents"]
        body   = bubble.get("body", {})
        contents = body.get("contents", [])
        body["contents"] = [test_bar] + contents
        bubble["body"] = body
        # altText 也加 [TEST] 前綴，LINE 通知列會顯示
        payload["altText"] = "[TEST] " + payload.get("altText", "")
    except Exception as exc:
        logger.warning("_inject_test_banner 注入失敗（繼續發送原始 payload）：%s", exc)

    return payload


# 合成測試情境 markdown（不依賴 DB，供 --scenario 使用）
_SCENARIO_TEMPLATES: dict[str, str] = {

    "normal": """\
# 每日投資組合報告

> **分析日期**：{date}　｜　**Scenario**：B　｜　**Confidence**：🟡 Medium

**今日結論：維持 VOO 70%，戰術倉全數觀望，現金 30%。**

---

## 一、宏觀市場指標

| 指標 | 數值 | 資料日期 |
|------|------|---------|
| Macro Growth (CFNAI) | +0.23  📊 溫和擴張 | （資料日期：2026-03-03） |
| HY OAS 信用利差 | 3.90%  🟢 健康 |  |
| 10Y-2Y 利差 | +0.25%  🟡 略為正斜率 | |
| VIX 波動指數 | 18.2  🟡 正常 | |
| VIX 百分位 (252日) | 38.0% | |

## 一-B、標準化風險座標（Rolling Z-Score 252日）

> 以過去 252 個交易日為基準。

| 指標 | Z-Score 解讀 |
|------|-------------|
| VIX（波動率） | +0.80  🟡 偏高 |
| HY OAS（信用利差） | +0.90  🟡 偏高 |
| 10Y-2Y 利差 | -0.50  🟢 正常範圍（±1σ） |

**Z-Score 風險燈號**

- 🟡 風險升溫｜需留意指標偏離：VIX z=+0.80、HY OAS z=+0.90。
> ⚠️ 此燈號為標準化指標的平行觀察層，不取代目前 Scenario A/B/C 判定。

---

## 二、Regime 判定

### ⚡ Scenario B — 修正但未崩，保守布局

| 項目 | 數值 |
|------|------|
| Regime 標籤 | Slowdown |
| Market Phase | Late Cycle |
| Regime Score | 42.5 / 100 |
| Confidence | 🟡 Medium |

> Scenario B：市場壓力升溫，尚未系統性崩潰，保守分批布局。

## 三、目標配置

| 資產 | 目標權重 |
|------|---------|
| VOO | **70%** |
| QQQM | **0%** |
| SMH | **0%** |
| 2330.TW | **0%** |
| 現金 | **30%** |

## 四、今日操作建議

> 保守觀望，等待更明確的市場訊號。

**VOO（核心）**：維持 70%，不調整。
**QQQM（戰術）**：🟡 WAIT — 暫停加碼，等待動能確認後再布局。
**SMH（戰術）**：🟡 WAIT — 暫停加碼，等待動能確認後再布局。
**2330.TW（戰術）**：🟡 WAIT — 暫停加碼，等待動能確認後再布局。

**目前模式**：🟡 標準模式（建議每月 6–8 萬）

## 九、昨日對比

| 項目 | 昨日 → 今日 |
|------|------------|
| Scenario：B → **B**（持平） | |
| 主要驅動因子 | HY OAS 小幅升溫 |
""",

    "hy-red": """\
# 每日投資組合報告

> **分析日期**：{date}　｜　**Scenario**：C　｜　**Confidence**：🟡 Medium

**今日結論：維持 VOO 70%，戰術倉全數觀望，現金 30%。**

---

## 一、宏觀市場指標

| 指標 | 數值 | 資料日期 |
|------|------|---------|
| Macro Growth (CFNAI) | -0.15  📉 放緩 | （資料日期：2026-03-03） |
| HY OAS 信用利差 | 6.82%  🟠 警戒 |  |
| 10Y-2Y 利差 | -0.35%  🟠 輕微倒掛 | |
| VIX 波動指數 | 24.5  🟠 偏高 | |
| VIX 百分位 (252日) | 71.0% | |

## 一-B、標準化風險座標（Rolling Z-Score 252日）

> 以過去 252 個交易日為基準。

| 指標 | Z-Score 解讀 |
|------|-------------|
| VIX（波動率） | +1.60  🟡 偏高 |
| HY OAS（信用利差） | +2.40  🟠 顯著偏高（>+2σ） ⚠️ 風險訊號 |
| 10Y-2Y 利差 | -0.80  🟢 正常範圍（±1σ） |

**Z-Score 風險燈號**

- 🔴 風險警告｜異常指標：HY OAS z=+2.40。系統切換為防禦模式。
> ⚠️ 此燈號為標準化指標的平行觀察層，不取代目前 Scenario A/B/C 判定。

---

## 二、Regime 判定

### 🚨 Scenario C — 結構性惡化，降低曝險

| 項目 | 數值 |
|------|------|
| Regime 標籤 | Contraction |
| Market Phase | Risk-Off |
| Regime Score | 22.0 / 100 |
| Confidence | 🟡 Medium |

> Scenario C：信用或實體經濟明確惡化，戰術部位訊號全數 NO_TRADE，現金優先。

## 三、目標配置

| 資產 | 目標權重 |
|------|---------|
| VOO | **70%** |
| QQQM | **0%** |
| SMH | **0%** |
| 2330.TW | **0%** |
| 現金 | **30%** |

## 四、今日操作建議

> 信用市場壓力顯著，維持防禦配置，不擴大戰術曝險。

**VOO（核心）**：維持 70%，不調整。
**QQQM（戰術）**：🔴 NO_TRADE — 信用市場壓力過大，暫停一切加碼。
**SMH（戰術）**：🔴 NO_TRADE — 信用市場壓力過大，暫停一切加碼。
**2330.TW（戰術）**：🔴 NO_TRADE — 信用市場壓力過大，暫停一切加碼。

**目前模式**：🔴 暫停加碼模式（等待信用市場回穩）

## 九、昨日對比

| 項目 | 昨日 → 今日 |
|------|------------|
| Scenario：B → **C**（惡化） | |
| 主要驅動因子 | HY OAS 突破 +2σ 觸發紅燈 |
""",

    "pmi-missing": """\
# 每日投資組合報告

> **分析日期**：{date}　｜　**Scenario**：B　｜　**Confidence**：🟡 Medium

**今日結論：維持 VOO 70%，戰術倉全數觀望，現金 30%。**

---

## 一、宏觀市場指標

| 指標 | 數值 | 資料日期 |
|------|------|---------|
| Macro Growth (CFNAI) | +0.23  📊 溫和擴張 | （資料日期：2026-03-03） |
| HY OAS 信用利差 | 3.75%  🟢 健康 |  |
| 10Y-2Y 利差 | +0.20%  🟡 略為正斜率 | |
| VIX 波動指數 | 17.8  🟡 正常 | |
| VIX 百分位 (252日) | 35.0% | |

> ⚠️ **缺失指標**（不影響流程，但降低 Confidence）：ISM_PMI_MFG（今日無新值，沿用 2026-03-03 最近期資料）

## 一-B、標準化風險座標（Rolling Z-Score 252日）

> 以過去 252 個交易日為基準。

| 指標 | Z-Score 解讀 |
|------|-------------|
| VIX（波動率） | +0.60  🟢 正常範圍（±1σ） |
| HY OAS（信用利差） | +0.70  🟢 正常範圍（±1σ） |
| 10Y-2Y 利差 | -0.40  🟢 正常範圍（±1σ） |

**Z-Score 風險燈號**

- 🟢 正常｜主要風險指標仍在可接受範圍內。
> ⚠️ 此燈號為標準化指標的平行觀察層，不取代目前 Scenario A/B/C 判定。

---

## 二、Regime 判定

### ⚡ Scenario B — 修正但未崩，保守布局

| 項目 | 數值 |
|------|------|
| Regime 標籤 | Slowdown |
| Market Phase | Mid Cycle |
| Regime Score | 48.0 / 100 |
| Confidence | 🟡 Medium |

> Scenario B：市場壓力升溫，尚未系統性崩潰。ISM PMI 使用上月值，待本月更新。

## 三、目標配置

| 資產 | 目標權重 |
|------|---------|
| VOO | **70%** |
| QQQM | **0%** |
| SMH | **0%** |
| 2330.TW | **0%** |
| 現金 | **30%** |

## 四、今日操作建議

> PMI 沿用上期值，Confidence 降為 Medium。配置維持保守觀望。

**VOO（核心）**：維持 70%，不調整。
**QQQM（戰術）**：🟡 WAIT — 暫停加碼，等待動能確認後再布局。
**SMH（戰術）**：🟡 WAIT — 暫停加碼，等待動能確認後再布局。
**2330.TW（戰術）**：🟡 WAIT — 暫停加碼，等待動能確認後再布局。

**目前模式**：🟡 標準模式（建議每月 6–8 萬）

## 九、昨日對比

| 項目 | 昨日 → 今日 |
|------|------------|
| Scenario：B → **B**（持平，PMI 沿用上期值） | |
| 主要驅動因子 | PMI 當日無新值，系統自動採用最近有效值 |
""",
}


def _make_scenario_md(scenario_id: str, report_date: date) -> str:
    """
    生成指定情境的合成 markdown。
    不依賴 DB，專供 --scenario 測試使用。
    """
    template = _SCENARIO_TEMPLATES.get(scenario_id)
    if template is None:
        raise ValueError(
            f"未知情境 {scenario_id!r}，可選：{list(_SCENARIO_TEMPLATES)}"
        )
    return template.format(date=report_date)


# ── HTTP 發送底層 ─────────────────────────────────────────────────────────────

def _push_line_message(
    token: str,
    user_id: str,
    message_obj: dict,
    report_date: date,
) -> bool:
    """
    向 LINE Push API 發送單一 message object。
    回傳 True = HTTP 200，False = 任何失敗。
    不寫 marker、不記業務 log，由呼叫端負責。
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json; charset=UTF-8",
    }
    payload = {"to": user_id, "messages": [message_obj]}

    try:
        resp = requests.post(LINE_PUSH_URL, headers=headers, json=payload, timeout=15)
    except requests.Timeout:
        logger.error("LINE_HTTP_FAILED  date=%s  error=Timeout(15s)", report_date)
        return False
    except requests.RequestException as exc:
        logger.error("LINE_HTTP_FAILED  date=%s  error=%s", report_date, exc)
        return False

    if resp.status_code == 200:
        return True

    body_snippet = resp.text[:300].replace("\n", " ")
    logger.error(
        "LINE_HTTP_FAILED  date=%s  status=%s  body=%s",
        report_date, resp.status_code, body_snippet,
    )
    return False


# ── 發送 ──────────────────────────────────────────────────────────────────────

def send_line_report(
    report_path: Path,
    report_date: Optional[date] = None,
    *,
    dry_run: bool = False,
    test_user_id: Optional[str] = None,
    _md_override: Optional[str] = None,
) -> bool:
    """
    讀取 report_path 的 .md 日報，以 Flex Message 推播至 LINE。
    Flex 失敗時自動 fallback 為純文字。

    Parameters
    ----------
    report_path   : 日報 .md 檔案路徑
    report_date   : 報告日期（None → 今日）
    dry_run       : True → 僅生成 payload 並輸出到 log，不實際送出，不寫 marker
    test_user_id  : 非 None → 推播至此測試 User ID，而非 .env 的 LINE_USER_ID
                    payload 自動加注 TEST 橫幅，不寫正式 marker
    _md_override  : 非 None → 直接使用此 markdown 文字，跳過 report_path 讀檔
                    （僅供測試情境使用，不在正式流程中呼叫）

    回傳值
    ------
    True  = 成功（送出 / dry-run 完成 / LINE_ENABLED=false 靜默跳過）
    False = 失敗（憑證缺漏 / 兩種格式皆失敗）

    此函式不拋例外，所有錯誤均記錄 log。
    現有呼叫 send_line_report(path, date) 行為完全不變（新參數均有預設值）。
    """
    is_test_mode = dry_run or (test_user_id is not None)

    # dry-run 模式不檢查 LINE_ENABLED（目的就是本地驗證 payload）
    if not dry_run:
        enabled = os.getenv("LINE_ENABLED", "true").strip().lower()
        if enabled not in ("true", "1", "yes"):
            logger.info("LINE_ENABLED=false，跳過 LINE 推播")
            return True

    token = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
    if not dry_run and not token:
        logger.warning(
            "LINE_FAILED  reason=missing_token  "
            "hint=請在 .env 設定 LINE_CHANNEL_ACCESS_TOKEN"
        )
        return False

    # 決定推播目標
    if test_user_id:
        target_user_id = test_user_id
        logger.info("LINE_TEST_MODE  target=%s（非正式推播）", target_user_id)
    elif not dry_run:
        target_user_id = os.getenv("LINE_USER_ID", "").strip()
        if not target_user_id:
            logger.warning(
                "LINE_FAILED  reason=missing_user_id  "
                "hint=請在 .env 設定 LINE_USER_ID"
            )
            return False
    else:
        target_user_id = "DRY_RUN_NO_SEND"

    report_date = report_date or date.today()

    # 讀取 markdown
    if _md_override is not None:
        md_text = _md_override
        logger.info("LINE_MD_OVERRIDE  date=%s  len=%d（合成測試情境）",
                    report_date, len(md_text))
    else:
        if not report_path.exists():
            logger.error("LINE_FAILED  date=%s  reason=report_not_found  path=%s",
                         report_date, report_path)
            return False
        md_text = report_path.read_text(encoding="utf-8")

    # ── 建立 Flex Message ────────────────────────────────────────────────
    flex_msg: Optional[dict] = None
    try:
        flex_msg = build_line_flex_payload(md_text, report_date)
        if is_test_mode:
            flex_msg = _inject_test_banner(flex_msg)
    except Exception as exc:
        logger.warning(
            "LINE_FLEX_BUILD_FAILED  date=%s  error=%s  falling_back=text",
            report_date, exc,
        )

    # ── Dry-run：只輸出 payload，不送出 ─────────────────────────────────
    if dry_run:
        import json
        if flex_msg:
            logger.info(
                "LINE_DRY_RUN  date=%s  payload_preview=\n%s",
                report_date,
                json.dumps(flex_msg, ensure_ascii=False, indent=2)[:2000],
            )
        else:
            text_preview = build_line_message(md_text, report_date)
            logger.info(
                "LINE_DRY_RUN  date=%s  text_preview=\n%s",
                report_date, text_preview[:1000],
            )
        logger.info("LINE_DRY_RUN_COMPLETE  date=%s  no_message_sent", report_date)
        return True

    # ── 嘗試 Flex Message ────────────────────────────────────────────────
    if flex_msg is not None:
        if _push_line_message(token, target_user_id, flex_msg, report_date):
            if is_test_mode:
                logger.info(
                    "LINE_FLEX_TEST_SENT  date=%s  to=%s（TEST，不寫正式 marker）",
                    report_date, target_user_id,
                )
            else:
                logger.info("LINE_FLEX_SENT  date=%s  to=%s", report_date, target_user_id)
                _write_sent_marker(report_date)
            return True
        logger.warning(
            "LINE_FLEX_FAILED  date=%s  reason=api_error  falling_back=text",
            report_date,
        )

    # ── Fallback：純文字 ─────────────────────────────────────────────────
    text_body = build_line_message(md_text, report_date)
    if is_test_mode:
        text_body = "⚠️ TEST MESSAGE — 請勿當作正式訊號 ⚠️\n\n" + text_body
    text_msg = {"type": "text", "text": text_body}

    if _push_line_message(token, target_user_id, text_msg, report_date):
        if is_test_mode:
            logger.info(
                "LINE_TEXT_TEST_SENT  date=%s  to=%s（TEST fallback，不寫正式 marker）",
                report_date, target_user_id,
            )
        else:
            logger.info("LINE_TEXT_FALLBACK_SENT  date=%s  to=%s",
                        report_date, target_user_id)
            _write_sent_marker(report_date)
        return True

    logger.error("LINE_FAILED  date=%s  reason=both_flex_and_text_failed", report_date)
    return False


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    import io
    if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    p = argparse.ArgumentParser(
        description="發送每日報告 LINE 推播",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用範例：

  # 正式推播（今日報告）
  python -m report.send_line

  # Dry-run：生成 payload 但不送出
  python -m report.send_line --dry-run

  # 使用合成情境 dry-run（不需要 DB 或報告檔案）
  python -m report.send_line --dry-run --scenario hy-red

  # 測試推播到指定 User ID（帶 TEST 橫幅）
  python -m report.send_line --test-user-id Uxxxxxxxxx --scenario hy-red

  # 推播指定日期的已存報告至測試對象
  python -m report.send_line --date 2026-04-10 --test-user-id Uxxxxxxxxx

可用情境（--scenario）：
  normal       正常情境（Scenario B，所有指標正常）
  hy-red       單項紅燈情境（HY OAS z=+2.40 觸發紅燈）
  pmi-missing  PMI 當日缺值但前期有效值（沿用上月資料）
""",
    )
    p.add_argument("--date",         type=date.fromisoformat, default=date.today(),
                   help="報告日期（預設今日）")
    p.add_argument("--output-dir",   type=Path,
                   default=Path(__file__).parent.parent / "output",
                   help="報告目錄（預設 output/）")
    p.add_argument("--dry-run",      action="store_true",
                   help="只生成 payload 並輸出到 log，不實際送出")
    p.add_argument("--test-user-id", type=str, default=None,
                   metavar="LINE_USER_ID",
                   help="推播至此測試 User ID（帶 TEST 橫幅，不寫正式 marker）")
    p.add_argument("--scenario",     type=str, default=None,
                   choices=list(_SCENARIO_TEMPLATES),
                   help="使用合成測試情境（不需要報告檔案）")
    args = p.parse_args()

    # ── 決定 markdown 來源 ────────────────────────────────────────────────
    if args.scenario:
        md_text   = _make_scenario_md(args.scenario, args.date)
        # 合成情境用虛擬路徑（不會被讀取，因為有 _md_override）
        path      = args.output_dir / f"__test_scenario_{args.scenario}__.md"
        logger.info(
            "LINE_SCENARIO  scenario=%s  date=%s  mode=%s",
            args.scenario, args.date,
            "dry-run" if args.dry_run else f"test-push→{args.test_user_id or 'PROD'}",
        )
    else:
        md_text = None
        path    = args.output_dir / f"daily_report_{args.date}.md"

    ok = send_line_report(
        path,
        args.date,
        dry_run      = args.dry_run,
        test_user_id = args.test_user_id,
        _md_override = md_text,
    )
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
