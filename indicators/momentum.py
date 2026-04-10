"""
Price Change % (Δ1W / Δ1M)
----------------------------
Input  : close_df — wide DataFrame (DatetimeIndex UTC, columns = asset_id)
Output :
  indicator = "PRICE_CHG_PCT_1W"  (5 個交易日漲跌幅)
  indicator = "PRICE_CHG_PCT_1M"  (21 個交易日漲跌幅)
  value 單位：% (e.g. 3.2 表示 +3.2%)
"""
import logging
from typing import List

import pandas as pd

from .base import BaseIndicator, IndicatorRow

logger = logging.getLogger(__name__)

PERIODS = [
    (5,  "PRICE_CHG_PCT_1W"),
    (21, "PRICE_CHG_PCT_1M"),
]


class PriceChangePct(BaseIndicator):
    """Δ1W / Δ1M 滾動價格漲跌幅。"""

    lookback_days = max(p for p, _ in PERIODS)   # 21

    def __init__(self, frequency: str = "1d") -> None:
        self.frequency = frequency

    def compute(self, close_df: pd.DataFrame) -> List[IndicatorRow]:
        if close_df.empty:
            logger.warning("PriceChangePct: empty input")
            return []

        rows: List[IndicatorRow] = []
        for period, indicator in PERIODS:
            pct_df = close_df.pct_change(periods=period) * 100   # → %
            rows.extend(self._wide_to_rows(pct_df, indicator, self.frequency, unit="%"))

        logger.info("PriceChangePct: %d rows across %d assets",
                    len(rows), len(close_df.columns))
        return rows
