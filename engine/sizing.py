"""
Position Sizer
--------------
輸入：AssetSignal + RegimeResult + FallingKnifeResult
輸出：PositionResult（per asset）

計算邏輯（乘法型，依序疊加）：
  base_position
  × regime_multiplier   (A=1.5, B=0.75, C=0.25)
  × fk_multiplier       (falling_knife → 0.40)
  × valuation_mult      (PE percentile 調整)
  × signal_gate         (AVOID → 0, REDUCE → 0.3)
  = raw_position

  clamp to [0, max_single_asset]
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict

from .config import CORE_ASSETS, SIZING
from .defense import FallingKnifeResult
from .regime import RegimeResult
from .signals import AssetSignal
from .snapshot import MarketSnapshot

logger = logging.getLogger(__name__)


# ── Output ────────────────────────────────────────────────────────────────────

@dataclass
class PositionResult:
    asset:              str
    position_pct:       float       # 建議加碼量佔投組 %
    regime_mult:        float
    fk_mult:            float
    valuation_mult:     float
    signal_gate:        float
    rationale:          str


# ── Sizer ─────────────────────────────────────────────────────────────────────

class PositionSizer:

    def run(
        self,
        snap:    MarketSnapshot,
        regime:  RegimeResult,
        signals: Dict[str, AssetSignal],
        fk_map:  Dict[str, FallingKnifeResult],
    ) -> Dict[str, PositionResult]:
        return {
            asset: self._size(snap, regime, signals[asset], fk_map.get(asset))
            for asset in CORE_ASSETS
            if asset in signals
        }

    def _size(
        self,
        snap:    MarketSnapshot,
        regime:  RegimeResult,
        sig:     AssetSignal,
        fk:      FallingKnifeResult | None,
    ) -> PositionResult:

        # ① Signal gate（AVOID=0, REDUCE=0.3, NEUTRAL=0.6, BUY=0.85, STRONG_BUY=1.0）
        gate = {
            "STRONG_BUY": 1.00,
            "BUY":        0.85,
            "NEUTRAL":    0.60,
            "REDUCE":     0.30,
            "AVOID":      0.00,
        }.get(sig.signal, 0.0)

        # ② Regime multiplier
        rm = {
            "A": SIZING.regime_A_mult,
            "B": SIZING.regime_B_mult,
            "C": SIZING.regime_C_mult,
            "U": SIZING.regime_B_mult,   # Undetermined → conservative
        }.get(regime.regime, SIZING.regime_B_mult)

        # ③ Falling knife multiplier
        fk_mult = SIZING.falling_knife_mult if (fk and fk.falling_knife) else 1.0

        # ④ Valuation multiplier
        vm = self._valuation_mult(snap, sig.asset)

        # ⑤ Compute
        raw = SIZING.base_position * rm * fk_mult * vm * gate

        # ⑥ Clamp
        position = round(max(SIZING.min_position, min(SIZING.max_single_asset, raw)), 2)

        note = (
            f"base={SIZING.base_position}% "
            f"× regime_{regime.regime}({rm:.2f}) "
            f"× fk({fk_mult:.2f}) "
            f"× val({vm:.2f}) "
            f"× gate({gate:.2f}) "
            f"= {raw:.2f}% → clamped={position}%"
        )

        logger.info("%s: %s", sig.asset, note)
        return PositionResult(
            asset=sig.asset, position_pct=position,
            regime_mult=rm, fk_mult=fk_mult,
            valuation_mult=vm, signal_gate=gate,
            rationale=note,
        )

    def _valuation_mult(self, snap: MarketSnapshot, asset: str) -> float:
        pe_pct = snap.val(f"{asset}_FWD_PE_PCT")
        if pe_pct is None:
            return 1.0   # N/A → neutral，不獎不懲
        if pe_pct < 10:
            return SIZING.pe_pct_below_10_bonus
        if pe_pct < 20:
            return SIZING.pe_pct_below_20_bonus
        if pe_pct < 50:
            return 1.0
        if pe_pct < 70:
            return SIZING.pe_pct_above_50_penalty
        return SIZING.pe_pct_above_70_penalty
