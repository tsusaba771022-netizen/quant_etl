"""
Growth × Inflation Regime Matrix
----------------------------------
將已計算的 z-score 壓縮為兩條主軸，提供平行觀察座標。

Growth  軸：ISM_PMI_MFG_Z_60M (w=0.50)  + YIELD_SPREAD_10Y2Y_Z_252 (w=0.50)
Inflation 軸：US_10Y_YIELD_Z_252 (w=0.60) + US_2Y_YIELD_Z_252 (w=0.40)

象限判斷（sign-based）：
  growth > 0, inflation < 0 → Goldilocks   （成長擴張、通膨平抑）
  growth > 0, inflation > 0 → Overheating  （過熱）
  growth < 0, inflation < 0 → Recession    （衰退、通縮壓力）
  growth < 0, inflation > 0 → Stagflation  （停滯性通膨）

設計原則：
- 任一軸所有輸入皆 None → 該軸設為 None，不中斷 pipeline
- 可用輸入的權重按比例重新歸一（available-weight composite）
- 結果僅為平行觀察，不影響 Scenario A/B/C 判定
- 可直接從 Snapshot.z_scores 讀取，不需額外 DB 查詢
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from engine.snapshot import Snapshot

logger = logging.getLogger(__name__)

# ── 軸設定 ──────────────────────────────────────────────────────────────────────
# 每個 tuple: (z_score_key, weight)
GROWTH_INPUTS: List[Tuple[str, float]] = [
    ("ISM_PMI_MFG_Z_60M",         0.50),
    ("YIELD_SPREAD_10Y2Y_Z_252",  0.50),
]

INFLATION_INPUTS: List[Tuple[str, float]] = [
    ("US_10Y_YIELD_Z_252",  0.60),
    ("US_2Y_YIELD_Z_252",   0.40),
]

# 象限定義：(growth_positive, inflation_positive) → label
QUADRANT_MAP: Dict[Tuple[bool, bool], Tuple[str, str]] = {
    (True,  False): ("Goldilocks",  "🟢"),
    (True,  True):  ("Overheating", "🟡"),
    (False, False): ("Recession",   "🔴"),
    (False, True):  ("Stagflation", "🚨"),
}

QUADRANT_DESC: Dict[str, str] = {
    "Goldilocks":  "成長擴張 + 通膨平抑，宏觀環境最理想",
    "Overheating": "成長擴張 + 通膨偏高，注意升息壓力",
    "Recession":   "成長收縮 + 通縮壓力，景氣下行",
    "Stagflation": "成長收縮 + 通膨偏高，最惡劣組合",
}


# ── 結果 dataclass ──────────────────────────────────────────────────────────────

@dataclass
class RegimeMatrixResult:
    """Growth × Inflation 軸的合成結果。"""

    # 主軸合成分數（加權 z-score）
    growth_score:    Optional[float]    = None
    inflation_score: Optional[float]    = None

    # 象限標籤與 icon（軸任一為 None → "N/A"）
    quadrant:        str                = "N/A"
    quadrant_icon:   str                = "⚪"

    # 各軸實際使用的指標（for 報告顯示）
    growth_used:     List[str]          = field(default_factory=list)
    inflation_used:  List[str]          = field(default_factory=list)

    # 是否有效（兩軸都有至少一個輸入）
    is_valid:        bool               = False

    @property
    def quadrant_desc(self) -> str:
        return QUADRANT_DESC.get(self.quadrant, "")

    @property
    def growth_label(self) -> str:
        if self.growth_score is None:
            return "N/A"
        sign = "+" if self.growth_score >= 0 else ""
        direction = "擴張" if self.growth_score >= 0 else "收縮"
        return f"{sign}{self.growth_score:.2f}  （{direction}）"

    @property
    def inflation_label(self) -> str:
        if self.inflation_score is None:
            return "N/A"
        sign = "+" if self.inflation_score >= 0 else ""
        direction = "偏高" if self.inflation_score >= 0 else "偏低"
        return f"{sign}{self.inflation_score:.2f}  （{direction}）"


# ── 核心計算類別 ────────────────────────────────────────────────────────────────

class RegimeMatrix:
    """
    從 Snapshot.z_scores 計算 Growth × Inflation 座標矩陣。

    使用方式：
        result = RegimeMatrix().compute(snap)
    """

    def compute(self, snap: Snapshot) -> RegimeMatrixResult:
        """從 Snapshot 計算矩陣結果，不拋出例外。"""
        result = RegimeMatrixResult()

        try:
            growth    = self._composite(snap, GROWTH_INPUTS,    "Growth")
            inflation = self._composite(snap, INFLATION_INPUTS, "Inflation")

            result.growth_score    = growth[0]
            result.growth_used     = growth[1]
            result.inflation_score = inflation[0]
            result.inflation_used  = inflation[1]

            # 象限只有兩軸都有效才能判定
            if growth[0] is not None and inflation[0] is not None:
                key = (growth[0] >= 0, inflation[0] >= 0)
                label, icon         = QUADRANT_MAP[key]
                result.quadrant     = label
                result.quadrant_icon= icon
                result.is_valid     = True
                logger.info(
                    "[REGIME_MATRIX] Growth=%.3f (%s)  Inflation=%.3f (%s)  → %s %s",
                    growth[0], ",".join(growth[1]) or "none",
                    inflation[0], ",".join(inflation[1]) or "none",
                    icon, label,
                )
            else:
                missing = []
                if growth[0] is None:
                    missing.append("Growth")
                if inflation[0] is None:
                    missing.append("Inflation")
                logger.info(
                    "[REGIME_MATRIX] 以下軸無資料，象限無法判定：%s", missing
                )

        except Exception as exc:
            logger.warning("[REGIME_MATRIX] 計算失敗，回傳空結果：%s", exc)

        return result

    # ── 內部：加權合成 ──────────────────────────────────────────────────────────

    @staticmethod
    def _composite(
        snap: Snapshot,
        inputs: List[Tuple[str, float]],
        axis_name: str,
    ) -> Tuple[Optional[float], List[str]]:
        """
        可用輸入的加權平均 z-score（權重按比例重歸一）。

        Returns
        -------
        (composite_score, list_of_used_keys)
        composite_score = None 若所有輸入均無資料
        """
        available: List[Tuple[str, float, float]] = []   # (key, weight, value)

        for key, weight in inputs:
            val = snap.z_scores.get(key)
            if val is not None:
                available.append((key, weight, val))
            else:
                logger.debug("[REGIME_MATRIX] %s 軸：%s = None，跳過", axis_name, key)

        if not available:
            return None, []

        # 重新歸一化權重
        total_weight = sum(w for _, w, _ in available)
        composite = sum((w / total_weight) * v for _, w, v in available)
        used_keys = [k for k, _, _ in available]

        return composite, used_keys
