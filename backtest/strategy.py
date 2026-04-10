"""
Signal → Position Strategy
---------------------------
三種組合模式：
  1. SingleAsset         : 單一資產，全倉 / 空倉
  2. EqualWeight         : 等權重多資產，依訊號強度決定持倉比例
  3. BlendedPortfolio    : Core + Tactical 分層架構
       Core    : VOO 固定 70%（長期持有，不做頻繁進出場）
       Tactical: QQQM/SMH/2330.TW 各有上限，由 regime 訊號驅動

Tactical 上限（佔總組合）：
  QQQM    → 12%
  SMH     → 10%
  2330.TW → 8%
  合計上限 = 30%

訊號強度對應持倉乘數（position multiplier）：
  BUY  + Conviction  → 1.00（滿足上限）
  BUY  + Main        → 0.70
  BUY  + Scouting    → scouting_mult（預設 0.40，可外部注入）
  WAIT               → 0.00（持現金）
  NO_TRADE           → 0.00
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from engine.signals import AssetSignal
from etl.config import (
    CORE_ASSETS, CORE_WEIGHT,
    TACTICAL_ASSETS, TACTICAL_CAPS,
)

# ── 固定乘數 ──────────────────────────────────────────────────────────────────

_FIXED_MULTIPLIER: Dict[tuple, float] = {
    ("BUY",      "Conviction"): 1.00,
    ("BUY",      "Main"):       0.70,
    ("WAIT",     "Conviction"): 0.00,
    ("WAIT",     "Main"):       0.00,
    ("WAIT",     "Scouting"):   0.00,
    ("NO_TRADE", "Conviction"): 0.00,
    ("NO_TRADE", "Main"):       0.00,
    ("NO_TRADE", "Scouting"):   0.00,
}

DEFAULT_SCOUTING_MULT: float = 0.40

# ── P3-1 Layer 3 Allocation Override ─────────────────────────────────────────

MACRO_DEFENSIVE_CAP_MULT: float = 0.50


def apply_macro_alloc_caps(
    base_caps: Dict[str, float],
    macro_alloc=None,   # Optional[MacroAllocResult] — lazy import avoids circular dep
) -> Dict[str, float]:
    """
    Layer 3 Allocation Override（P3-1）：
      DEFENSIVE  → 所有戰術上限 × MACRO_DEFENSIVE_CAP_MULT（0.50）
      其他 / None → 原封不動回傳 base_caps（不改動）

    純函式：不修改 base_caps，回傳新 dict。
    MacroAllocStatus 為 str Enum，直接與字串比較即可。
    """
    if macro_alloc is not None and macro_alloc.status == "DEFENSIVE":
        return {a: c * MACRO_DEFENSIVE_CAP_MULT for a, c in base_caps.items()}
    return base_caps


def signal_multiplier(
    sig: AssetSignal,
    scouting_mult: float = DEFAULT_SCOUTING_MULT,
) -> float:
    if sig.signal_type == "BUY" and sig.signal_strength == "Scouting":
        return scouting_mult
    return _FIXED_MULTIPLIER.get((sig.signal_type, sig.signal_strength), 0.0)


# ── Positions dataclass ───────────────────────────────────────────────────────

@dataclass
class Positions:
    """當日持倉（weight 加總 ≤ 1.0，餘為現金）。"""
    weights: Dict[str, float]    # asset → weight

    @property
    def cash_weight(self) -> float:
        return max(0.0, 1.0 - sum(self.weights.values()))

    @property
    def core_weight(self) -> float:
        return sum(w for a, w in self.weights.items() if a in CORE_ASSETS)

    @property
    def tactical_weight(self) -> float:
        return sum(w for a, w in self.weights.items() if a in TACTICAL_ASSETS)


# ── Strategy 1：Single Asset ──────────────────────────────────────────────────

def single_asset_positions(
    asset: str,
    signals: Dict[str, AssetSignal],
    scouting_mult: float = DEFAULT_SCOUTING_MULT,
) -> Positions:
    sig = signals.get(asset)
    if sig is None:
        return Positions(weights={})
    w = signal_multiplier(sig, scouting_mult)
    return Positions(weights={asset: w} if w > 0 else {})


# ── Strategy 2：Equal Weight ──────────────────────────────────────────────────

def equal_weight_positions(
    assets: list,
    signals: Dict[str, AssetSignal],
    scouting_mult: float = DEFAULT_SCOUTING_MULT,
) -> Positions:
    """
    等權重：
      - 基礎權重 = 1 / len(assets)
      - 實際權重 = base_weight * signal_multiplier
    """
    base = 1.0 / len(assets) if assets else 0.0
    weights: Dict[str, float] = {}
    for asset in assets:
        sig = signals.get(asset)
        if sig is None:
            continue
        w = base * signal_multiplier(sig, scouting_mult)
        if w > 0:
            weights[asset] = w
    return Positions(weights=weights)


# ── Strategy 3：Blended Portfolio（Core + Tactical）──────────────────────────

def blended_portfolio_positions(
    signals: Dict[str, AssetSignal],
    scouting_mult: float = DEFAULT_SCOUTING_MULT,
    core_weight: float = CORE_WEIGHT,
    tactical_caps: Optional[Dict[str, float]] = None,
) -> Positions:
    """
    分層組合：
      Core（VOO）    : 固定 core_weight（預設 70%），不受 regime 影響
      Tactical       : QQQM/SMH/2330.TW 各依上限 × signal_multiplier

    Tactical 總權重 <= 1.0 - core_weight（預設 30%）

    範例（sm=0.40, BUY/Scouting）：
      VOO  = 70.0%
      QQQM = 12% × 0.40 = 4.8%
      SMH  = 10% × 0.40 = 4.0%
      2330 =  8% × 0.40 = 3.2%
      現金 = 18.0%

    範例（Scenario A, BUY/Conviction）：
      VOO  = 70.0%
      QQQM = 12.0%
      SMH  = 10.0%
      2330 =  8.0%
      現金 =  0.0%
    """
    if tactical_caps is None:
        tactical_caps = TACTICAL_CAPS

    weights: Dict[str, float] = {}

    # ── Core：固定持有 ────────────────────────────────────────────────────────
    for asset in CORE_ASSETS:
        weights[asset] = core_weight

    # ── Tactical：訊號驅動，受個別上限約束 ───────────────────────────────────
    tactical_budget = 1.0 - core_weight   # 預設 0.30
    tactical_used   = 0.0

    for asset in TACTICAL_ASSETS:
        if tactical_used >= tactical_budget:
            break
        cap = tactical_caps.get(asset, 0.0)
        sig = signals.get(asset)
        if sig is None:
            continue
        raw_w = cap * signal_multiplier(sig, scouting_mult)
        # 不超出各資產上限，也不超出剩餘戰術預算
        w = min(raw_w, cap, tactical_budget - tactical_used)
        if w > 0:
            weights[asset] = round(w, 6)
            tactical_used += w

    return Positions(weights=weights)
