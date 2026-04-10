"""
Macro Allocation Matrix (Layer 3)
----------------------------------
顯式條件矩陣（deterministic rule block）。

禁止：簡單平均 / 票數相加 / 「由模型綜合判斷」。
必須：每個輸出狀態都由明確條件決定，不留灰色地帶。

輸入：cfnai, spread, vix（scalar，允許 None = 資料缺失）
輸出：MacroAllocStatus (AGGRESSIVE / NEUTRAL / DEFENSIVE)

門檻沿用既有常數，不自創新 threshold：
  VIX_ELEVATED    = 20.0    (from engine/regime.py)
  SPREAD_INVERSION = 0.0    (from engine/regime.py)
  CFNAI_MILD_EXPANSION = 0.10  (與 daily_report._cfnai_status 語意一致)
  CFNAI_RECESSION_RISK = -0.70 (與 daily_report._cfnai_status 語意一致)

Layer 3 權責限制（嚴格）：
  - 不可覆寫 Credit Veto（Layer 1）
  - 不可解除 Trend Risk Cap（Layer 2）
  - VIX elevated 只影響 Layer 3，不得升格為 veto / RED
  - 只能在既有風險上限內決定傾向
  - Phase 1 VIX DEFENSIVE 不做噪音過濾（Phase 2 再評估連續 N 日 / percentile filter）
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

# 沿用 engine/regime.py 既有常數，不重複定義
from engine.regime import SPREAD_INVERSION, VIX_ELEVATED

logger = logging.getLogger(__name__)

# ── CFNAI 語意門檻（與 report/daily_report._cfnai_status() 保持一致）──────────
CFNAI_MILD_EXPANSION: float = 0.10   # >= → 溫和擴張（AGGRESSIVE 必要條件）
CFNAI_RECESSION_RISK: float = -0.70  # <  → 衰退風險起點（DEFENSIVE 觸發）


# ── Status Enum ────────────────────────────────────────────────────────────────

class MacroAllocStatus(str, Enum):
    AGGRESSIVE = "AGGRESSIVE"   # 全部正向條件成立
    NEUTRAL    = "NEUTRAL"      # 無主導訊號
    DEFENSIVE  = "DEFENSIVE"    # 任一負向條件觸發


# ── Output ─────────────────────────────────────────────────────────────────────

@dataclass
class MacroAllocResult:
    status:    MacroAllocStatus
    rationale: str
    cfnai:     Optional[float]
    spread:    Optional[float]
    vix:       Optional[float]


# ── 純計算函式（deterministic，可單元測試）─────────────────────────────────────

def classify_macro_alloc(
    cfnai:  Optional[float],
    spread: Optional[float],
    vix:    Optional[float],
) -> MacroAllocResult:
    """
    顯式條件矩陣，嚴格 deterministic（非投票 / 非平均）。

    評估順序：
      1. DEFENSIVE 優先：任一條件成立即觸發，不再評估 AGGRESSIVE
      2. AGGRESSIVE：全部條件必須成立（任一缺值 → 降 NEUTRAL）
      3. NEUTRAL：其餘所有情況

    Parameters
    ----------
    cfnai  : CFNAI 指標值（對應 snap.ism_pmi，內部欄位名保留）
    spread : 10Y-2Y 利差 %（snap.spread_10y2y）
    vix    : VIX 收盤值（snap.vix）

    Returns
    -------
    MacroAllocResult
    """
    # ── Step 1: DEFENSIVE 條件（任一成立即觸發，優先級最高）──────────────────
    defensive_reasons: list[str] = []

    if cfnai is not None and cfnai < CFNAI_RECESSION_RISK:
        defensive_reasons.append(
            f"CFNAI={cfnai:+.2f} < {CFNAI_RECESSION_RISK:+.2f} (recession risk onset)"
        )

    if spread is not None and spread < SPREAD_INVERSION:
        defensive_reasons.append(
            f"Yield Spread={spread:+.2f}% < {SPREAD_INVERSION}% (yield curve inverted)"
        )

    # Phase 1：VIX elevated 觸發 DEFENSIVE（無噪音過濾，Phase 2 再評估）
    if vix is not None and vix >= VIX_ELEVATED:
        defensive_reasons.append(
            f"VIX={vix:.1f} >= {VIX_ELEVATED} (elevated — Phase 1, no noise filter)"
        )

    if defensive_reasons:
        rationale = "DEFENSIVE triggered by: " + " | ".join(defensive_reasons)
        logger.info("[MACRO_ALLOC] %s", rationale)
        return MacroAllocResult(
            status    = MacroAllocStatus.DEFENSIVE,
            rationale = rationale,
            cfnai     = cfnai,
            spread    = spread,
            vix       = vix,
        )

    # ── Step 2: AGGRESSIVE 條件（全部成立才觸發，缺值降 NEUTRAL）────────────
    cfnai_ok  = cfnai  is not None and cfnai  >= CFNAI_MILD_EXPANSION
    spread_ok = spread is None     or  spread >  SPREAD_INVERSION
    vix_ok    = vix    is None     or  vix    <  VIX_ELEVATED

    if cfnai_ok and spread_ok and vix_ok:
        spread_str = f"{spread:+.2f}%" if spread is not None else "N/A"
        vix_str    = f"{vix:.1f}"      if vix    is not None else "N/A"
        rationale  = (
            f"AGGRESSIVE: CFNAI={cfnai:+.2f}>={CFNAI_MILD_EXPANSION}"
            f" | Spread={spread_str}>0"
            f" | VIX={vix_str}<{VIX_ELEVATED}"
        )
        logger.info("[MACRO_ALLOC] %s", rationale)
        return MacroAllocResult(
            status    = MacroAllocStatus.AGGRESSIVE,
            rationale = rationale,
            cfnai     = cfnai,
            spread    = spread,
            vix       = vix,
        )

    # ── Step 3: NEUTRAL（其餘所有情況）──────────────────────────────────────
    neutral_notes: list[str] = []

    if cfnai is None:
        neutral_notes.append("CFNAI=N/A (data missing, cannot confirm expansion)")
    elif cfnai < CFNAI_MILD_EXPANSION:
        neutral_notes.append(
            f"CFNAI={cfnai:+.2f} < {CFNAI_MILD_EXPANSION:+.2f} (below mild expansion threshold)"
        )

    if not neutral_notes:
        neutral_notes.append("no dominant signal")

    rationale = "NEUTRAL: " + "; ".join(neutral_notes)
    logger.info("[MACRO_ALLOC] %s", rationale)
    return MacroAllocResult(
        status    = MacroAllocStatus.NEUTRAL,
        rationale = rationale,
        cfnai     = cfnai,
        spread    = spread,
        vix       = vix,
    )
