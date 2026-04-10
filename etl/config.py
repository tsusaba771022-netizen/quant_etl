"""
ETL Configuration
-----------------
所有設定從環境變數讀取，支援 .env 檔案（python-dotenv）。
"""
import os
from dataclasses import dataclass
from typing import Dict, Any

from dotenv import load_dotenv

load_dotenv()


# ── Database ─────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class DBConfig:
    host:     str = os.getenv("PG_HOST",     "localhost")
    port:     int = int(os.getenv("PG_PORT", "5432"))
    dbname:   str = os.getenv("PG_DBNAME",   "quant")
    user:     str = os.getenv("PG_USER",     "postgres")
    password: str = os.getenv("PG_PASSWORD", "")

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} "
            f"dbname={self.dbname} user={self.user} "
            f"password={self.password}"
        )


DB = DBConfig()

# ── Market symbols (yfinance) ─────────────────────────────────────────────────

@dataclass(frozen=True)
class MarketSymbol:
    symbol:     str
    name:       str
    asset_type: str   # matches assets.asset_type ENUM
    exchange:   str
    currency:   str

# yfinance ticker -> metadata for assets table
MARKET_SYMBOLS: Dict[str, MarketSymbol] = {
    # ── Primary Assets ────────────────────────────────────────────────────────
    "VOO":     MarketSymbol("VOO",     "Vanguard S&P 500 ETF",             "etf",   "NYSE",   "USD"),
    "VT":      MarketSymbol("VT",      "Vanguard Total World Stock ETF",   "etf",   "NYSE",   "USD"),
    "QQQM":    MarketSymbol("QQQM",    "Invesco NASDAQ 100 ETF",           "etf",   "NASDAQ", "USD"),
    "2330.TW": MarketSymbol("2330.TW", "Taiwan Semiconductor Mfg Co Ltd", "stock", "TWSE",   "TWD"),
    "SMH":     MarketSymbol("SMH",     "VanEck Semiconductor ETF",         "etf",   "NASDAQ", "USD"),
    # ── Proxy / Reference Assets ──────────────────────────────────────────────
    "SPY":     MarketSymbol("SPY",     "SPDR S&P 500 ETF Trust",           "etf",   "NYSE",   "USD"),
    "QQQ":     MarketSymbol("QQQ",     "Invesco QQQ Trust",                "etf",   "NASDAQ", "USD"),
    "SOXX":    MarketSymbol("SOXX",    "iShares Semiconductor ETF",        "etf",   "NASDAQ", "USD"),
    # ── Market Indicators ─────────────────────────────────────────────────────
    "^VIX":    MarketSymbol("^VIX",    "CBOE Volatility Index",            "stock", "CBOE",   "USD"),
}

# ── Asset Universe Classifications ────────────────────────────────────────────

# 核心資產：長期持有，不做頻繁進出場
CORE_ASSETS: list = ["VOO"]
CORE_WEIGHT: float = 0.70          # 核心部位目標權重

# 戰術資產：由 regime / signals 決定進出場與比例
TACTICAL_ASSETS: list = ["QQQM", "SMH", "2330.TW"]
TACTICAL_WEIGHT_MAX: float = 0.30  # 戰術部位總上限

# 戰術資產各自上限（佔總組合）
TACTICAL_CAPS: dict = {
    "QQQM":    0.12,
    "SMH":     0.10,
    "2330.TW": 0.08,
}

# 正式決策資產 = Core + Tactical
PRIMARY_ASSETS: list = CORE_ASSETS + TACTICAL_ASSETS  # ["VOO","QQQM","SMH","2330.TW"]

# 參考 / Proxy 資產（供 snapshot 代理計算 + benchmark 比較）
PROXY_ASSETS: list = ["SPY", "QQQ", "SOXX"]

# Proxy 對應關係（用於報表標示）
PRIMARY_TO_PROXY: dict = {
    "VOO":     "SPY",
    "QQQM":    "QQQ",
    "SMH":     "SOXX",
    "2330.TW": None,   # 無直接 proxy
}

# 市場情緒指標
MARKET_INDICATORS: list = ["^VIX"]

# 需要計算衍生指標的 DB symbol（使用 proxy symbol，非前端名稱）
# VOO→SPY, QQQM→QQQ, SMH→SOXX, 2330.TW→2330.TW
INDICATOR_EQUITY_SYMBOLS: list = ["SPY", "QQQ", "SOXX", "2330.TW"]

# ── FRED macro series ─────────────────────────────────────────────────────────

# indicator_code -> FRED metadata for macro_data table
FRED_SERIES: Dict[str, Dict[str, Any]] = {
    "ISM_PMI_MFG": {
        # Plan B: PMI/NAPM retired on current FRED path;
        # use CFNAI (Chicago Fed National Activity Index) as monthly macro growth proxy.
        # CFNAI semantics: 0 = historical avg; >+0.70 = above-trend; <-0.70 = recession risk onset.
        "fred_id":   "CFNAI",
        "unit":      "index",
        "frequency": "monthly",   # macro_frequency ENUM
        "country":   "US",
        "source":    "fred",
    },
    "HY_OAS": {
        "fred_id":   "BAMLH0A0HYM2",    # OAS（信用利差），非 EY（有效殖利率）
        "unit":      "percent",
        "frequency": "daily",
        "country":   "US",
        "source":    "fred",
    },
    "US_10Y_YIELD": {
        "fred_id":   "DGS10",
        "unit":      "percent",
        "frequency": "daily",
        "country":   "US",
        "source":    "fred",
    },
    "US_2Y_YIELD": {
        "fred_id":   "DGS2",
        "unit":      "percent",
        "frequency": "daily",
        "country":   "US",
        "source":    "fred",
    },
}

FRED_API_KEY:    str = os.getenv("FRED_API_KEY", "")
ETL_LOOKBACK_DAYS: int = int(os.getenv("ETL_LOOKBACK_DAYS", "7"))
