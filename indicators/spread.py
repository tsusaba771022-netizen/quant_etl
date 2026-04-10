"""
10Y-2Y Yield Spread
--------------------
Input  : macro_df — wide DataFrame (DatetimeIndex UTC, columns = indicator)
         需包含 'US_10Y_YIELD' 與 'US_2Y_YIELD' 兩欄。

Output : indicator = "YIELD_SPREAD_10Y2Y"
         asset_id  → 合成資產 'US_YIELD_SPREAD_10Y2Y'（SYNTHETIC exchange）

前置條件
--------
macro_data 必須已寫入 US_10Y_YIELD (DGS10) 與 US_2Y_YIELD (DGS2)。
若尚未加入 ETL，請在 etl/config.py FRED_SERIES 補上後重跑 ETL。
目前若資料不存在，本模組會 skip 並輸出 warning，不中斷系統。
"""
import logging
from typing import List

import pandas as pd

from .base import BaseIndicator, IndicatorRow

logger = logging.getLogger(__name__)

CODE_10Y         = "US_10Y_YIELD"
CODE_2Y          = "US_2Y_YIELD"
SYNTHETIC_SYMBOL = "US_YIELD_SPREAD_10Y2Y"


class YieldSpread(BaseIndicator):
    """10Y - 2Y 美國公債殖利率利差。"""

    lookback_days = 0

    def __init__(self, asset_id: int, frequency: str = "daily") -> None:
        self.asset_id  = asset_id
        self.frequency = frequency

    def compute(self, macro_df: pd.DataFrame) -> List[IndicatorRow]:
        if macro_df.empty:
            logger.warning("YieldSpread: empty macro_df")
            return []

        for col in (CODE_10Y, CODE_2Y):
            if col not in macro_df.columns:
                logger.warning(
                    "YieldSpread: '%s' not found in macro_data. "
                    "Add DGS10/DGS2 to etl/config.py FRED_SERIES and re-run ETL.",
                    col,
                )
                return []

        spread = (macro_df[CODE_10Y] - macro_df[CODE_2Y]).dropna()
        rows   = self._series_to_rows(
            spread, "YIELD_SPREAD_10Y2Y", self.asset_id, self.frequency, unit="%"
        )
        logger.info("YieldSpread (10Y-2Y): %d rows", len(rows))
        return rows
