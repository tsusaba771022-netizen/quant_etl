"""
Falling Knife Detector
-----------------------
即使 regime = A（極佳加碼點），也需確認：
  1. 下跌動能是否已減速？（價格動能）
  2. VIX 是否仍在加速攀升？（恐慌動能）
  3. HY OAS 是否仍在快速擴張？（信用動能）
  4. 市場廣度是否持續惡化？（廣度）

若任一條件觸發 → falling_knife = True → 倉位減半，等待穩定訊號。

輸出：FallingKnifeResult（per asset 或 global）
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .config import FK, CORE_ASSETS
from .snapshot import MarketSnapshot

logger = logging.getLogger(__name__)


# ── Output ────────────────────────────────────────────────────────────────────

@dataclass
class FKCheck:
    name:       str
    triggered:  bool
    value:      Optional[float]
    threshold:  float
    note:       str


@dataclass
class FallingKnifeResult:
    asset:          str             # "VOO", "QQQM", "SMH", "2330.TW", or "GLOBAL"
    falling_knife:  bool
    checks:         List[FKCheck] = field(default_factory=list)
    reason:         str = ""

    @property
    def triggered_checks(self) -> List[FKCheck]:
        return [c for c in self.checks if c.triggered]


# ── Detector ──────────────────────────────────────────────────────────────────

class FallingKnifeDetector:
    """
    依照以下層次判定：
    1. Global checks（VIX momentum、HY OAS widening、breadth）
    2. Asset-specific checks（price momentum）
    """

    def run(self, snap: MarketSnapshot) -> Dict[str, FallingKnifeResult]:
        """
        回傳 dict: {asset_name -> FallingKnifeResult}
        包含 "GLOBAL" 與各核心資產。
        """
        global_result = self._global_checks(snap)
        results: Dict[str, FallingKnifeResult] = {"GLOBAL": global_result}

        for asset in CORE_ASSETS:
            results[asset] = self._asset_checks(snap, asset, global_result)

        return results

    # ── Global checks ─────────────────────────────────────────────────────────

    def _global_checks(self, snap: MarketSnapshot) -> FallingKnifeResult:
        checks: List[FKCheck] = []

        # ① VIX pct_rank 極端（恐慌仍在加速）
        vix_pct = snap.val("VIX_PCT_RANK_252")
        if vix_pct is not None:
            triggered = vix_pct > FK.vix_pct_rank_extreme
            checks.append(FKCheck(
                name="VIX_PCT_RANK_EXTREME",
                triggered=triggered,
                value=vix_pct,
                threshold=FK.vix_pct_rank_extreme,
                note=(
                    f"VIX pct_rank={vix_pct:.2f} > {FK.vix_pct_rank_extreme} "
                    f"→ 恐慌位於歷史前 {(1-FK.vix_pct_rank_extreme)*100:.0f}% 極端水位"
                    if triggered else
                    f"VIX pct_rank={vix_pct:.2f} 正常"
                ),
            ))
        else:
            # VIX pct_rank N/A → 使用 raw VIX 粗判
            vix_raw = snap.val("VIX")
            if vix_raw is not None:
                triggered = vix_raw > 35
                checks.append(FKCheck(
                    name="VIX_RAW_EXTREME",
                    triggered=triggered,
                    value=vix_raw,
                    threshold=35.0,
                    note=f"VIX={vix_raw:.1f} {'> 35 (极端)' if triggered else '正常'}（pct_rank N/A）",
                ))

        # ② HY OAS 快速擴張（信用壓力加速）
        # 利用 HY_OAS 本身無法得到 5日變化量，故只用 level 判定
        hy_oas = snap.val("HY_OAS")
        if hy_oas is not None:
            triggered = hy_oas > 5.0    # 進入 C 臨界
            checks.append(FKCheck(
                name="HY_OAS_LEVEL",
                triggered=triggered,
                value=hy_oas,
                threshold=5.0,
                note=(
                    f"HY_OAS={hy_oas:.2f}% > 5.0%，信用市場承壓"
                    if triggered else
                    f"HY_OAS={hy_oas:.2f}% 信用正常"
                ),
            ))

        # ③ 廣度指標（% above 200MA）→ 目前 N/A
        breadth = snap.val("SP500_PCT_ABOVE_200MA")
        if breadth is not None:
            triggered = breadth < FK.breadth_200ma_danger
            checks.append(FKCheck(
                name="BREADTH_200MA",
                triggered=triggered,
                value=breadth,
                threshold=FK.breadth_200ma_danger,
                note=(
                    f"% above 200MA={breadth:.1f}% < {FK.breadth_200ma_danger}%，廣度持續惡化"
                    if triggered else
                    f"% above 200MA={breadth:.1f}% 廣度尚可"
                ),
            ))
        else:
            checks.append(FKCheck(
                name="BREADTH_200MA",
                triggered=False,
                value=None,
                threshold=FK.breadth_200ma_danger,
                note="% above 200MA N/A（不納入觸發條件）",
            ))

        triggered_list = [c for c in checks if c.triggered]
        is_fk = len(triggered_list) >= 1   # 任一 global 觸發即為 falling knife

        reason = ""
        if is_fk:
            reason = "Global falling knife detected: " + " | ".join(
                c.name for c in triggered_list
            )
        else:
            reason = "No global falling knife signals"

        logger.info("FallingKnife GLOBAL: triggered=%s (%d/%d checks)",
                    is_fk, len(triggered_list), len(checks))
        return FallingKnifeResult("GLOBAL", is_fk, checks, reason)

    # ── Asset-specific checks ─────────────────────────────────────────────────

    def _asset_checks(
        self,
        snap: MarketSnapshot,
        asset: str,
        global_result: FallingKnifeResult,
    ) -> FallingKnifeResult:
        checks: List[FKCheck] = []

        # ① 1週價格動能（最直接的 falling knife 訊號）
        chg_1w = snap.val(f"{asset}_CHG_1W")
        if chg_1w is not None:
            severe   = chg_1w < FK.price_chg_1w_severe * 100    # already stored as %
            caution  = chg_1w < FK.price_chg_1w_caution * 100
            triggered = severe
            checks.append(FKCheck(
                name="PRICE_CHG_1W",
                triggered=triggered,
                value=chg_1w,
                threshold=FK.price_chg_1w_severe * 100,
                note=(
                    f"{asset} 1W change={chg_1w:.2f}% — 仍在急跌 (< {FK.price_chg_1w_severe*100:.0f}%)"
                    if severe else
                    f"{asset} 1W change={chg_1w:.2f}% — "
                    + ("減速但未止跌" if caution else "動能正常")
                ),
            ))
        else:
            checks.append(FKCheck(
                name="PRICE_CHG_1W",
                triggered=False,
                value=None,
                threshold=FK.price_chg_1w_severe * 100,
                note=f"{asset} 1W change N/A",
            ))

        # ② 1月動能補充（中期趨勢確認）
        chg_1m = snap.val(f"{asset}_CHG_1M")
        if chg_1m is not None:
            triggered_1m = chg_1m < -15.0   # 月跌幅 > 15%，趨勢性下跌
            checks.append(FKCheck(
                name="PRICE_CHG_1M",
                triggered=triggered_1m,
                value=chg_1m,
                threshold=-15.0,
                note=(
                    f"{asset} 1M change={chg_1m:.2f}% — 趨勢性下跌，需等待底部確認"
                    if triggered_1m else
                    f"{asset} 1M change={chg_1m:.2f}% — 月動能尚可接受"
                ),
            ))

        # ③ 價格低於 MA5（短期趨勢仍向下）
        price = snap.val(f"{asset}_PRICE")
        ma5   = snap.val(f"{asset}_MA5")
        if price is not None and ma5 is not None:
            below_ma5 = price < ma5
            checks.append(FKCheck(
                name="BELOW_MA5",
                triggered=below_ma5,
                value=round(price - ma5, 4),
                threshold=0.0,
                note=(
                    f"{asset} price={price:.2f} < MA5={ma5:.2f}（仍在均線下方）"
                    if below_ma5 else
                    f"{asset} price={price:.2f} ≥ MA5={ma5:.2f}（站上短期均線）"
                ),
            ))

        # 合併 global + asset-specific
        asset_triggered = [c for c in checks if c.triggered]
        # Falling knife: global 觸發 OR 資產本身嚴重動能觸發
        is_fk = global_result.falling_knife or len(asset_triggered) >= 1

        reason_parts = []
        if global_result.falling_knife:
            reason_parts.append(f"Global: {global_result.reason}")
        if asset_triggered:
            reason_parts.append("Asset: " + " | ".join(c.name for c in asset_triggered))
        reason = "; ".join(reason_parts) if reason_parts else "No falling knife"

        return FallingKnifeResult(asset, is_fk, checks, reason)
