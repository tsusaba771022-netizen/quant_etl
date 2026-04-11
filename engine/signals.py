"""
Signal Engine
-------------
輸入：Snapshot + RegimeResult
輸出：{asset_name: AssetSignal}

signal_type   : BUY / WAIT / NO_TRADE（對應新 schema signals.signal_type）
signal_strength: Conviction / Main / Scouting（對應 signals.signal_strength）

判定邏輯：
  1. Regime ceiling：Scenario C → 最高 WAIT；Neutral/B → BUY；A → BUY/Conviction
  2. Falling knife check：1W change < -5% → 降級
  3. 2330.TW 額外考量：若 spread 深度倒掛，提高謹慎度
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List

from .regime import RegimeResult
from .snapshot import AssetData, Snapshot

logger = logging.getLogger(__name__)

FALLING_KNIFE_THRESHOLD = -5.0   # 1W change% < -5% 視為仍在急跌


# ── Output ────────────────────────────────────────────────────────────────────

@dataclass
class AssetSignal:
    asset:          str
    signal_type:    str    # BUY / WAIT / NO_TRADE
    signal_strength:str    # Conviction / Main / Scouting
    scenario:       str
    rationale:      str
    falling_knife:  bool = False
    metadata:       Dict = field(default_factory=dict)


# ── Engine ─────────────────────────────────────────────────────────────────────

class SignalEngine:

    def __init__(self, neutral_action: str = "WAIT") -> None:
        """
        neutral_action : 控制 Scenario Neutral 時的訊號行為。
            'WAIT'        （預設）→ WAIT/Scouting，position = 0
            'BUY_SCOUTING'        → BUY/Scouting，position = scouting_mult × cap
            'BUY_MAIN'            → BUY/Main，position = 0.70 × cap
        """
        assert neutral_action in ("WAIT", "BUY_SCOUTING", "BUY_MAIN"), \
            f"neutral_action must be WAIT / BUY_SCOUTING / BUY_MAIN, got {neutral_action!r}"
        self.neutral_action = neutral_action

    def run(self, snap: Snapshot, regime: RegimeResult) -> Dict[str, AssetSignal]:
        return {
            asset: self._signal_for(snap, regime, ad)
            for asset, ad in snap.assets.items()
        }

    def _signal_for(
        self, snap: Snapshot, regime: RegimeResult, ad: AssetData
    ) -> AssetSignal:

        # ── Falling knife check ───────────────────────────────────────────────
        fk = (
            ad.chg_1w_pct is not None
            and ad.chg_1w_pct < FALLING_KNIFE_THRESHOLD
        )

        # ── Base signal from regime ───────────────────────────────────────────
        if regime.scenario == "C":
            signal_type   = "NO_TRADE"
            signal_strength = "Scouting"

        elif regime.scenario == "A":
            if fk:
                signal_type    = "WAIT"
                signal_strength = "Scouting"
            else:
                signal_type    = "BUY"
                signal_strength = "Conviction"

        elif regime.scenario == "B":
            if fk:
                signal_type    = "WAIT"
                signal_strength = "Scouting"
            else:
                signal_type    = "BUY"
                signal_strength = "Scouting"

        else:   # Neutral
            if self.neutral_action == "BUY_SCOUTING":
                signal_type    = "BUY"
                signal_strength = "Scouting"
            elif self.neutral_action == "BUY_MAIN":
                signal_type    = "BUY"
                signal_strength = "Main"
            else:   # "WAIT"（預設）
                signal_type    = "WAIT"
                signal_strength = "Scouting"

        # ── Asset-specific adjustments ────────────────────────────────────────
        signal_type, signal_strength = self._asset_override(
            ad, snap, regime, signal_type, signal_strength
        )

        # ── Rationale ─────────────────────────────────────────────────────────
        parts = [
            f"Regime={regime.scenario}  confidence={regime.confidence_score}",
            f"signal={signal_type}/{signal_strength}",
        ]
        if ad.close:
            parts.append(f"price={ad.close:.2f}")
        if ad.chg_1w_pct is not None:
            parts.append(f"Δ1W={ad.chg_1w_pct:+.2f}%")
        if ad.chg_1m_pct is not None:
            parts.append(f"Δ1M={ad.chg_1m_pct:+.2f}%")
        if fk:
            parts.append("⚠️ falling knife detected")
        if "ISM_PMI_MFG" in regime.missing:
            parts.append("CFNAI=N/A")

        metadata = {
            "close":       ad.close,
            "sma_5":       ad.sma_5,
            "chg_1w_pct":  ad.chg_1w_pct,
            "chg_1m_pct":  ad.chg_1m_pct,
            "falling_knife": fk,
            "regime_score":  regime.regime_score,
            "missing":       regime.missing,
        }

        logger.info(
            "%s → %s/%s  fk=%s  regime=%s(%s)",
            ad.symbol, signal_type, signal_strength,
            fk, regime.scenario, regime.confidence_score,
        )

        return AssetSignal(
            asset           = ad.symbol,
            signal_type     = signal_type,
            signal_strength = signal_strength,
            scenario        = regime.scenario,
            rationale       = " | ".join(parts),
            falling_knife   = fk,
            metadata        = metadata,
        )

    @staticmethod
    def _asset_override(
        ad: AssetData,
        snap: Snapshot,
        regime: RegimeResult,
        signal_type: str,
        signal_strength: str,
    ):
        """資產特定條件調整訊號。"""
        # 2330.TW：yield curve 深度倒掛 → 降一級謹慎
        if ad.symbol == "2330.TW":
            if snap.spread_10y2y is not None and snap.spread_10y2y < -1.0:
                if signal_type == "BUY" and signal_strength == "Conviction":
                    signal_strength = "Main"
                elif signal_type == "BUY" and signal_strength == "Main":
                    signal_strength = "Scouting"

        # SMH：1M 跌幅 > 20% 且 regime 非 A → WAIT
        if ad.symbol == "QQQM" or ad.symbol == "SMH":
            if (
                signal_type == "BUY"
                and regime.scenario != "A"
                and ad.chg_1m_pct is not None
                and ad.chg_1m_pct < -20.0
            ):
                signal_type    = "WAIT"
                signal_strength = "Scouting"

        return signal_type, signal_strength
