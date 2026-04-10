"""
VIX Rolling Statistics
-----------------------
Input  : vix_series — pd.Series (DatetimeIndex UTC, values = VIX close)
Output : 三個 indicator，每日各一筆

  VIX_ROLLING_MEAN_20   → 20 日滾動均值
  VIX_ROLLING_STD_20    → 20 日滾動標準差
  VIX_PCT_RANK_252      → 當日 VIX 在過去 252 日的百分位數（0.0 ~ 1.0）
"""
import logging
from typing import List

import numpy as np
import pandas as pd

from .base import BaseIndicator, IndicatorRow

logger = logging.getLogger(__name__)

MEAN_STD_WINDOW = 20
PCT_RANK_WINDOW = 252


class VIXRollingStats(BaseIndicator):
    """VIX 滾動統計：mean、std、percentile rank。"""

    lookback_days = PCT_RANK_WINDOW

    def __init__(self, asset_id: int, frequency: str = "1d") -> None:
        self.asset_id  = asset_id
        self.frequency = frequency

    def compute(self, vix_series: pd.Series) -> List[IndicatorRow]:
        if vix_series is None or vix_series.empty:
            logger.warning("VIXRollingStats: empty vix_series")
            return []

        vix = vix_series.dropna().sort_index()
        rows: List[IndicatorRow] = []
        rows.extend(self._compute_mean(vix))
        rows.extend(self._compute_std(vix))
        rows.extend(self._compute_pct_rank(vix))

        logger.info("VIXRollingStats: %d rows", len(rows))
        return rows

    def _compute_mean(self, vix: pd.Series) -> List[IndicatorRow]:
        mean_s = vix.rolling(window=MEAN_STD_WINDOW, min_periods=MEAN_STD_WINDOW).mean()
        return self._series_to_rows(
            mean_s, "VIX_ROLLING_MEAN_20", self.asset_id, self.frequency
        )

    def _compute_std(self, vix: pd.Series) -> List[IndicatorRow]:
        std_s = vix.rolling(window=MEAN_STD_WINDOW, min_periods=MEAN_STD_WINDOW).std(ddof=1)
        return self._series_to_rows(
            std_s, "VIX_ROLLING_STD_20", self.asset_id, self.frequency
        )

    def _compute_pct_rank(self, vix: pd.Series) -> List[IndicatorRow]:
        """當日 VIX 在過去 252 個交易日中的百分位數（0.0 ~ 1.0）。"""
        def _rank(window: np.ndarray) -> float:
            return float(np.sum(window <= window[-1]) / len(window))

        pct_s = vix.rolling(window=PCT_RANK_WINDOW, min_periods=50).apply(_rank, raw=True)
        return self._series_to_rows(
            pct_s, "VIX_PCT_RANK_252", self.asset_id, self.frequency
        )
