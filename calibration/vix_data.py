"""
VIX Historical Data Loader
---------------------------
Load order:
  1. Local CSV  — calibration/data/vix_history.csv  (or caller-specified path)
  2. yfinance fallback — download & cache to same CSV path
  3. RuntimeError if both fail (no silent crash)

Expected CSV format:
  date,close
  2018-01-02,17.22
  ...

Returned value: pd.Series[float] indexed by DatetimeIndex (trading days),
                name="close", sorted ascending, NaNs dropped.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

_DEFAULT_CSV_PATH = Path(__file__).parent / "data" / "vix_history.csv"
_YFINANCE_TICKER  = "^VIX"


def load_vix(
    start:    str,
    end:      str,
    csv_path: Optional[str] = None,
) -> pd.Series:
    """
    Load VIX close prices for [start, end].

    Parameters
    ----------
    start    : "YYYY-MM-DD" (inclusive)
    end      : "YYYY-MM-DD" (inclusive)
    csv_path : override local CSV path; defaults to calibration/data/vix_history.csv

    Returns
    -------
    pd.Series, DatetimeIndex, name="close", non-empty or raises RuntimeError.
    """
    resolved = Path(csv_path) if csv_path else _DEFAULT_CSV_PATH

    # ── Attempt 1: local CSV ──────────────────────────────────────────────────
    series = _load_from_csv(resolved, start, end)
    if series is not None and not series.empty:
        logger.info("VIX: loaded %d rows from CSV %s", len(series), resolved)
        return series

    # ── Attempt 2: yfinance → cache ───────────────────────────────────────────
    logger.info("VIX: CSV not found or empty — trying yfinance %s", _YFINANCE_TICKER)
    series = _load_from_yfinance(start, end, cache_path=resolved)
    if series is not None and not series.empty:
        logger.info("VIX: loaded %d rows from yfinance, cached → %s", len(series), resolved)
        return series

    # ── Failure ───────────────────────────────────────────────────────────────
    raise RuntimeError(
        f"Cannot load VIX data for {start} ~ {end}. "
        f"Checked CSV: {resolved}. yfinance also failed. "
        "Ensure internet access or place vix_history.csv in calibration/data/."
    )


# ── Loaders ──────────────────────────────────────────────────────────────────

def _load_from_csv(path: Path, start: str, end: str) -> Optional[pd.Series]:
    if not path.exists():
        logger.debug("CSV not found: %s", path)
        return None
    try:
        df = pd.read_csv(path, parse_dates=["date"], index_col="date")
        if df.empty:
            logger.warning("CSV %s is empty", path)
            return None

        # Support single-column CSV without header 'close'
        if "close" not in df.columns:
            if df.shape[1] == 1:
                df.columns = ["close"]
                logger.debug("CSV %s: inferred single column as 'close'", path)
            else:
                logger.warning("CSV %s: no 'close' column, columns=%s", path, df.columns.tolist())
                return None

        series = (
            df["close"]
            .astype(float)
            .sort_index()
            .loc[start:end]
            .dropna()
        )
        series.name = "close"
        if series.empty:
            logger.warning("CSV has no rows in range %s ~ %s", start, end)
            return None
        return series

    except Exception as exc:
        logger.warning("CSV load error (%s): %s", path, exc)
        return None


def _load_from_yfinance(start: str, end: str, cache_path: Path) -> Optional[pd.Series]:
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance not installed — cannot fetch VIX")
        return None

    try:
        # yfinance 1.x returns MultiIndex columns; end date is exclusive so add 1 day
        import pandas as pd_inner
        end_exclusive = (
            pd_inner.Timestamp(end) + pd_inner.Timedelta(days=1)
        ).strftime("%Y-%m-%d")

        df = yf.download(
            _YFINANCE_TICKER,
            start=start,
            end=end_exclusive,
            progress=False,
            auto_adjust=True,
        )

        if df.empty:
            logger.warning("yfinance returned empty DataFrame for %s", _YFINANCE_TICKER)
            return None

        # Flatten MultiIndex if present (yfinance >= 0.2)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        close = df["Close"].astype(float).dropna()
        close.index = pd.to_datetime(close.index)
        close.index.name = "date"
        close.name = "close"
        close = close.sort_index()

        # Cache to CSV for future runs (improves reproducibility)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_df = pd.DataFrame({"date": close.index, "close": close.values})
        cache_df.to_csv(cache_path, index=False)
        logger.info("Cached %d rows → %s", len(close), cache_path)

        # Filter to requested range (cached file covers broader range)
        close = close.loc[start:end]
        return close if not close.empty else None

    except Exception as exc:
        logger.warning("yfinance download failed: %s", exc)
        return None
