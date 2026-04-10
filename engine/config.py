"""
Engine Configuration
--------------------
所有閾值與權重集中於此，避免魔法數字散落各模組。
修改參數只需改此一處。
"""
from dataclasses import dataclass, field
from typing import Dict, List, Tuple


# ── Scenario thresholds ───────────────────────────────────────────────────────

@dataclass(frozen=True)
class ScenarioThresholds:
    # ── Scenario A: Institutional Gold Pit
    vix_stress:             float = 28.0    # VIX >
    vix_term_inversion:     float = 1.0     # VIX3M / VIX <
    hy_oas_safe:            float = 5.0     # HY OAS < (%)
    pe_pct_cheap:           float = 20.0    # Forward PE percentile <

    # ── Scenario B: Liquidity / Rate distortion
    move_high:              float = 120.0   # MOVE index >
    dxy_high:               float = 105.0   # DXY >

    # ── Scenario C: Structural deterioration
    pmi_recession:          float = 45.0    # PMI <
    hy_oas_danger:          float = 5.5     # HY OAS > (%)
    copper_oil_weak:        float = 0.020   # Copper/Oil ratio 1M change <  (relative)

    # ── Borderline signals
    pmi_slowdown:           float = 48.0    # PMI <  (slowdown but not recession)
    vix_elevated:           float = 20.0    # VIX elevated but not stressed
    hy_oas_caution:         float = 4.0     # HY OAS starting to widen

    # ── Taiwan-specific
    dxy_twd_stress:         float = 105.0   # DXY threshold for TWD pressure
    usdtwd_stress:          float = 32.5    # USD/TWD threshold
    caixin_pmi_weak:        float = 50.0    # Caixin PMI < 50 → higher margin of safety


SCENARIO = ScenarioThresholds()


# ── Falling Knife thresholds ──────────────────────────────────────────────────

@dataclass(frozen=True)
class FallingKnifeThresholds:
    # Asset price momentum: 若短期動能仍為負且跌勢未止
    price_chg_1w_severe:    float = -0.05   # 1W < -5%  → 仍在急跌中
    price_chg_1w_caution:   float = -0.03   # 1W < -3%  → 減速但未止跌
    # VIX momentum: VIX 仍在快速上升
    vix_pct_rank_extreme:   float = 0.90    # VIX pct_rank > 90%
    vix_rising_threshold:   float = 0.10    # VIX 5日漲幅 > 10% → 壓力仍在加速
    # Credit: HY OAS 仍在快速擴張
    hy_oas_widening:        float = 0.30    # HY OAS 5日變化 > 0.30% → 加速擴張
    # Breadth: 下跌廣度仍強
    breadth_200ma_danger:   float = 30.0    # % above 200MA < 30%


FK = FallingKnifeThresholds()


# ── Position sizing parameters ────────────────────────────────────────────────

@dataclass(frozen=True)
class SizingConfig:
    # 基礎部位（佔可投資組合 %）
    base_position:          float = 5.0     # 每個資產的基準加碼量

    # Regime multipliers
    regime_A_mult:          float = 1.5     # Scenario A: 積極
    regime_B_mult:          float = 0.75    # Scenario B: 保守
    regime_C_mult:          float = 0.25    # Scenario C: 防守

    # Falling knife multiplier（疊加在 regime 之上）
    falling_knife_mult:     float = 0.40    # 偵測到 falling knife 時大幅縮減

    # Valuation adjustments（forward PE percentile）
    pe_pct_below_10_bonus:  float = 1.30    # < 10%：極便宜，加成
    pe_pct_below_20_bonus:  float = 1.15    # < 20%：便宜，小加成
    pe_pct_above_50_penalty:float = 0.80    # > 50%：偏貴，縮減
    pe_pct_above_70_penalty:float = 0.50    # > 70%：昂貴，大幅縮減

    # Position limits
    max_single_asset:       float = 12.0    # 單一資產最大加碼量 %
    min_position:           float = 0.0     # 若 < 此值則不建倉


SIZING = SizingConfig()


# ── Regime scoring weights ────────────────────────────────────────────────────
# 各維度對「機會得分」的貢獻權重（加總 = 1.0）
REGIME_WEIGHTS: Dict[str, float] = {
    "vix":        0.20,   # 情緒 / 恐慌
    "credit":     0.25,   # 信用健康度（HY OAS）
    "liquidity":  0.15,   # 流動性（MOVE, DXY, NFCI）
    "growth":     0.25,   # 實體經濟（PMI, 就業）
    "valuation":  0.15,   # 估值（PE percentile）
}

# 信號對應規則 (regime_score 閾值)
# regime_score 0~100: 越高 = 越有機會
SIGNAL_THRESHOLDS: List[Tuple[float, str]] = [
    (80.0, "STRONG_BUY"),
    (60.0, "BUY"),
    (40.0, "NEUTRAL"),
    (25.0, "REDUCE"),
    (0.0,  "AVOID"),
]


# ── Core assets ───────────────────────────────────────────────────────────────
# symbol → (exchange, display_name, currency)
CORE_ASSETS: Dict[str, Tuple[str, str, str]] = {
    "VOO":     ("NYSE",   "Vanguard S&P 500 ETF",         "USD"),
    "QQQM":    ("NASDAQ", "Invesco Nasdaq 100 ETF",        "USD"),
    "SMH":     ("NYSE",   "VanEck Semiconductor ETF",      "USD"),
    "2330.TW": ("TWSE",   "Taiwan Semiconductor Mfg Co",   "TWD"),
}

# DB 中實際有資料的近似 proxy（若目標資產尚未加入 ETL）
ASSET_PROXY: Dict[str, str] = {
    "VOO":  "SPY",     # S&P 500 ETF proxy
    "QQQM": "QQQ",     # Nasdaq 100 ETF proxy
    "SMH":  "SOXX",    # Semiconductor ETF proxy
    "2330.TW": "2330.TW",  # 直接對應
}

# ── Freshness windows (days) ──────────────────────────────────────────────────
FRESHNESS: Dict[str, int] = {
    "macro":      7,
    "flow":       2,
    "sentiment":  2,
    "valuation":  90,
    "price":      2,
}
