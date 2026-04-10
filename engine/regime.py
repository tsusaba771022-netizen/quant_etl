"""
Regime Engine
-------------
輸入：Snapshot
輸出：RegimeResult

ISM_PMI_MFG 為可缺失欄位：
  - 有值  → 納入 macro_score 計算
  - 缺失  → macro_score 以其他指標估算，confidence 下降一級
  - 缺失不 fail，報表中標示 ISM_PMI_MFG = N/A

confidence_score 文字對應：
  High   → 所有關鍵指標有值
  Medium → ISM_PMI_MFG 缺失，其餘 OK（目前系統預期狀態）
  Low    → 多個關鍵指標缺失
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .snapshot import Snapshot

logger = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────

VIX_PANIC        = 28.0
VIX_ELEVATED     = 20.0
HY_OAS_SAFE      = 5.0
HY_OAS_DANGER    = 7.0   # 原 5.5% → 6.0% → 7.0%（歷史上 7%+ 僅 2008/COVID 初）
PMI_RECESSION    = 45.0
PMI_SLOWDOWN     = 48.0
SPREAD_INVERSION = 0.0    # 10Y-2Y < 0 → inverted yield curve


# ── Output ────────────────────────────────────────────────────────────────────

@dataclass
class SubScore:
    score:  float   # 0~100（100 = 最健康/無壓力）
    note:   str
    weight: float

    @property
    def weighted(self) -> float:
        return self.score * self.weight


@dataclass
class RegimeResult:
    scenario:         str    # A / B / C / Neutral
    regime:           str    # 描述性標籤
    market_phase:     str    # Panic / Risk-off / Stabilization / Normal
    regime_score:     float  # 0~100
    confidence_score: str    # High / Medium / Low
    macro_score:      float
    liquidity_score:  float
    credit_score:     float
    sentiment_score:  float
    rationale:        str
    missing:          List[str] = field(default_factory=list)
    notes:            Dict     = field(default_factory=dict)


# ── Engine ─────────────────────────────────────────────────────────────────────

class RegimeEngine:

    def __init__(
        self,
        hy_oas_c_threshold: float = HY_OAS_DANGER,
        pmi_na_triggers_b: bool = True,
    ) -> None:
        """
        hy_oas_c_threshold : Scenario C 雙條件觸發的 HY_OAS 門檻（預設 7.0）。
        pmi_na_triggers_b  : True（預設） = PMI 缺失時觸發 Scenario B（Baseline 行為）。
                             False = PMI 缺失不單獨觸發 B；
                               若無其他 B 條件成立，則落至 Neutral。
        """
        self.hy_oas_c_threshold = hy_oas_c_threshold
        self.pmi_na_triggers_b  = pmi_na_triggers_b

    def run(self, snap: Snapshot) -> RegimeResult:
        sub = self._compute_sub_scores(snap)
        regime_score = self._aggregate(sub)
        scenario, market_phase, regime_label, rationale = self._classify(snap, regime_score)

        notes = {
            "as_of":            str(snap.as_of),
            "confidence":       snap.confidence_score,
            "missing":          snap.missing_indicators,
            "ISM_PMI_MFG":      snap.ism_pmi if snap.ism_pmi is not None else "N/A",
            "HY_OAS":           snap.hy_oas,
            "VIX":              snap.vix,
            "VIX_PCT_RANK_252": snap.vix_pct_rank,
            "SPREAD_10Y2Y":     snap.spread_10y2y,
        }

        result = RegimeResult(
            scenario         = scenario,
            regime           = regime_label,
            market_phase     = market_phase,
            regime_score     = round(regime_score, 2),
            confidence_score = snap.confidence_score,
            macro_score      = round(sub["macro"].score, 2),
            liquidity_score  = round(sub["liquidity"].score, 2),
            credit_score     = round(sub["credit"].score, 2),
            sentiment_score  = round(sub["sentiment"].score, 2),
            rationale        = rationale,
            missing          = snap.missing_indicators,
            notes            = notes,
        )

        logger.info(
            "Regime=%s (%s)  score=%.1f  confidence=%s  missing=%s",
            scenario, regime_label, regime_score,
            snap.confidence_score, snap.missing_indicators,
        )
        return result

    # ── Sub-scores ────────────────────────────────────────────────────────────

    def _compute_sub_scores(self, snap: Snapshot) -> Dict[str, SubScore]:
        return {
            "macro":     self._macro_score(snap),
            "credit":    self._credit_score(snap),
            "liquidity": self._liquidity_score(snap),
            "sentiment": self._sentiment_score(snap),
        }

    def _macro_score(self, snap: Snapshot) -> SubScore:
        """
        ISM_PMI_MFG 可缺失：
          有值  → 直接評分
          缺失  → 以 yield spread 方向作為代理，標注 N/A
        """
        if snap.ism_pmi is not None:
            pmi = snap.ism_pmi
            if pmi >= 55:       score = 90
            elif pmi >= 50:     score = 70
            elif pmi >= PMI_SLOWDOWN:  score = 50
            elif pmi >= PMI_RECESSION: score = 25
            else:               score = 5
            note = f"ISM_PMI={pmi:.1f} → score={score}"
        else:
            # ISM_PMI_MFG = N/A：用 yield spread 方向作為代理估計
            if snap.spread_10y2y is not None:
                if snap.spread_10y2y > 1.0:    score = 65  # 正斜率，偏健康
                elif snap.spread_10y2y > 0:     score = 50
                elif snap.spread_10y2y > -0.5:  score = 35
                else:                           score = 20  # 深度倒掛
                note = (
                    f"ISM_PMI_MFG = N/A，"
                    f"以 YIELD_SPREAD_10Y2Y={snap.spread_10y2y:.2f}% 代理估算 → score={score}"
                )
            else:
                score = 50   # 完全無資料，給中性
                note  = "ISM_PMI_MFG = N/A，YIELD_SPREAD 亦缺失 → 中性 50"

        return SubScore(score, note, weight=0.30)

    def _credit_score(self, snap: Snapshot) -> SubScore:
        if snap.hy_oas is None:
            return SubScore(50, "HY_OAS = N/A → 中性 50", weight=0.30)

        hy = snap.hy_oas
        if hy < 3.5:      score = 85
        elif hy < HY_OAS_SAFE:   score = 65
        elif hy < HY_OAS_DANGER: score = 35
        else:             score = 10
        note = f"HY_OAS={hy:.2f}% → score={score}"
        return SubScore(score, note, weight=0.30)

    def _liquidity_score(self, snap: Snapshot) -> SubScore:
        if snap.spread_10y2y is None:
            return SubScore(50, "YIELD_SPREAD = N/A → 中性 50", weight=0.15)

        sp = snap.spread_10y2y
        if sp > 1.5:    score = 80
        elif sp > 0.5:  score = 65
        elif sp > 0:    score = 50
        elif sp > -0.5: score = 35
        else:           score = 15   # 深度倒掛
        note = f"YIELD_SPREAD_10Y2Y={sp:.2f}% → score={score}"
        return SubScore(score, note, weight=0.15)

    def _sentiment_score(self, snap: Snapshot) -> SubScore:
        if snap.vix_pct_rank is not None:
            # pct_rank 越高 → 越恐慌 → score 越低
            score = round((1.0 - snap.vix_pct_rank) * 100)
            note  = f"VIX_PCT_RANK_252={snap.vix_pct_rank:.2f} → score={score}"
        elif snap.vix is not None:
            # fallback: raw VIX level
            vix = snap.vix
            score = max(10, min(90, int(90 - (vix - 12) * 2.2)))
            note  = f"VIX={vix:.1f} (no pct_rank) → score={score}"
        else:
            return SubScore(50, "VIX = N/A → 中性 50", weight=0.25)

        return SubScore(float(score), note, weight=0.25)

    # ── Aggregation ───────────────────────────────────────────────────────────

    @staticmethod
    def _aggregate(sub: Dict[str, SubScore]) -> float:
        total_w = sum(s.weight for s in sub.values())
        return sum(s.weighted for s in sub.values()) / total_w if total_w else 50.0

    # ── Classification ────────────────────────────────────────────────────────

    def _classify(
        self, snap: Snapshot, regime_score: float
    ):
        vix    = snap.vix or 0
        hy     = snap.hy_oas
        pmi    = snap.ism_pmi        # may be None
        spread = snap.spread_10y2y

        # ── Scenario C：結構性惡化（優先判定）────────────────────────────────
        #
        # 觸發規則（任一成立即觸發）：
        #   1. PMI < 45（實體經濟衰退，單條件即可）
        #   2. HY_OAS > 7.0%  且  VIX > VIX_ELEVATED（20）
        #      — 需雙條件確認，避免升息環境下 Effective Yield 虛高誤判
        #
        c_triggers = []

        if pmi is not None and pmi < PMI_RECESSION:
            c_triggers.append(f"PMI={pmi:.1f}<{PMI_RECESSION}")

        if hy is not None and hy > self.hy_oas_c_threshold:
            if vix > VIX_ELEVATED:
                # 雙條件成立：信用壓力 + 市場恐慌同步確認
                c_triggers.append(
                    f"HY_OAS={hy:.2f}%>{self.hy_oas_c_threshold}% + VIX={vix:.1f}>{VIX_ELEVATED}"
                )
            # else：HY_OAS 偏高但無市場恐慌確認 → 不升至 C，留給 Scenario B 處理

        if c_triggers:
            return (
                "C", "Panic", "Risk-off / Crisis",
                f"Scenario C：{' | '.join(c_triggers)}。"
                "信用或實體經濟明確惡化，建議降低風險曝險。"
            )

        # ── Scenario A：極度恐慌但信用健康 ────────────────────────────────────
        vix_stressed  = vix > VIX_PANIC
        credit_ok     = hy is not None and hy < HY_OAS_SAFE
        growth_ok     = (pmi is None) or (pmi >= PMI_SLOWDOWN)

        if vix_stressed and credit_ok and growth_ok:
            pmi_note = f"PMI={pmi:.1f}" if pmi else "PMI=N/A（無法確認，但未見惡化訊號）"
            return (
                "A", "Panic", "Panic / Potential Reversal",
                f"Scenario A：VIX={vix:.1f}>{VIX_PANIC}（極度恐慌）"
                f" | HY_OAS={hy:.2f}%<{HY_OAS_SAFE}%（信用健康）"
                f" | {pmi_note}。"
                "可能為恐慌驅動的估值錯殺，建議分批試探建倉。",
            )

        # ── Scenario B：修正但未崩 ─────────────────────────────────────────────
        b_triggers = []
        if vix > VIX_ELEVATED:
            b_triggers.append(f"VIX={vix:.1f} elevated")
        if spread is not None and spread < SPREAD_INVERSION:
            b_triggers.append(f"Yield curve inverted ({spread:.2f}%)")
        if hy is not None and hy >= HY_OAS_SAFE:
            b_triggers.append(f"HY_OAS={hy:.2f}% 偏高")
        if pmi is None and self.pmi_na_triggers_b:
            b_triggers.append("ISM_PMI_MFG=N/A（無法確認成長動能）")

        if b_triggers:
            return (
                "B", "Risk-off", "Risk-off / Soft Landing",
                f"Scenario B：{' | '.join(b_triggers)}。"
                "尚未出現系統性崩潰訊號，建議保守分批部署。",
            )

        # ── Neutral：無明確壓力訊號 ──────────────────────────────────────────
        return (
            "Neutral", "Normal", "Expansion / Soft Landing",
            f"市場無明確壓力訊號（regime_score={regime_score:.1f}）。"
            + ("" if pmi else " ISM_PMI_MFG=N/A，成長動能無法確認。"),
        )
