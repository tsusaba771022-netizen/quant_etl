"""
Moving Average (MA5)
--------------------
Input  : close_df — wide DataFrame (DatetimeIndex UTC, columns = asset_id)
Output : indicator = "SMA_5"，每個 asset 每個有效日期一筆
"""
import logging
from typing import List

import pandas as pd

from .base import BaseIndicator, IndicatorRow

logger = logging.getLogger(__name__)


class MovingAverage(BaseIndicator):
    """
    Simple Moving Average。
    indicator 名稱格式：SMA_{period}（e.g. SMA_5）
    """

    def __init__(self, period: int = 5, frequency: str = "1d") -> None:
        self.period        = period
        self.frequency     = frequency
        self.indicator     = f"SMA_{period}"
        self.lookback_days = period

    def compute(self, close_df: pd.DataFrame) -> List[IndicatorRow]:
        if close_df.empty:
            logger.warning("%s: empty input", self.indicator)
            return []

        ma = close_df.rolling(window=self.period, min_periods=self.period).mean()
        rows = self._wide_to_rows(ma, self.indicator, self.frequency)

        logger.info("%s: %d rows across %d assets",
                    self.indicator, len(rows), len(close_df.columns))
        return rows
