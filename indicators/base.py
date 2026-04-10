"""
Base Indicator Contract
-----------------------
所有指標模組繼承 BaseIndicator，統一：
  - compute() 介面
  - IndicatorRow 輸出格式（對應新 schema，無 params 欄位）
  - to_tuple() → 可直接傳入 upsert_indicators()

新 schema derived_indicators 欄位：
  indicator, asset_id, time, frequency, value, unit,
  calculation_method, source
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd


# ── Output row ─────────────────────────────────────────────────────────────────

@dataclass
class IndicatorRow:
    """
    對應 derived_indicators 表的單筆資料。
    indicator 欄位直接編碼參數（e.g. SMA_5、VIX_ROLLING_MEAN_20）。
    """
    indicator:  str
    asset_id:   Optional[int]   # macro 指標可為 None（需用合成資產）
    time:       pd.Timestamp
    frequency:  str
    value:      float
    unit:       str = ""

    def to_tuple(self) -> tuple:
        """轉成 psycopg2 execute_values 用的 tuple。"""
        return (
            self.indicator,
            self.asset_id,
            self.time,
            self.frequency,
            self.value,
            self.unit,
            "Derived Calculation",   # calculation_method
            "Derived Calculation",   # source
        )


# ── Base class ─────────────────────────────────────────────────────────────────

class BaseIndicator(ABC):
    """
    所有指標的基礎類別。

    子類別必須定義：
      - lookback_days : int，compute() 所需的歷史 buffer 天數

    子類別必須實作：
      - compute(**kwargs) → List[IndicatorRow]
    """
    lookback_days: int = 0

    @abstractmethod
    def compute(self, **kwargs) -> List[IndicatorRow]:
        pass

    # ── Helpers ────────────────────────────────────────────────────────────────

    @staticmethod
    def _safe_float(val) -> Optional[float]:
        if val is None:
            return None
        try:
            f = float(val)
            return None if pd.isna(f) else f
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _series_to_rows(
        series: pd.Series,
        indicator: str,
        asset_id: Optional[int],
        frequency: str = "1d",
        unit: str = "",
    ) -> List[IndicatorRow]:
        """pd.Series（DatetimeIndex）→ IndicatorRow list，跳過 NaN。"""
        rows = []
        for ts, val in series.items():
            safe = BaseIndicator._safe_float(val)
            if safe is None:
                continue
            rows.append(IndicatorRow(
                indicator=indicator,
                asset_id=asset_id,
                time=ts,
                frequency=frequency,
                value=safe,
                unit=unit,
            ))
        return rows

    @staticmethod
    def _wide_to_rows(
        df: pd.DataFrame,
        indicator: str,
        frequency: str = "1d",
        unit: str = "",
    ) -> List[IndicatorRow]:
        """wide DataFrame（DatetimeIndex, columns=asset_id）→ IndicatorRow list。"""
        rows = []
        for ts, row in df.iterrows():
            for asset_id in df.columns:
                val = BaseIndicator._safe_float(row[asset_id])
                if val is None:
                    continue
                rows.append(IndicatorRow(
                    indicator=indicator,
                    asset_id=int(asset_id),
                    time=ts,
                    frequency=frequency,
                    value=val,
                    unit=unit,
                ))
        return rows
