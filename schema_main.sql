-- ============================================================
-- Quant ETL — 主要資料表 DDL（Canonical Schema）
-- ============================================================
-- 全部使用 CREATE TABLE IF NOT EXISTS + CREATE UNIQUE INDEX IF NOT EXISTS
-- 可安全重複執行（冪等），不影響已存在的表或資料。
--
-- 冪等性設計摘要：
-- ┌─────────────────────┬────────────────────────────────┬───────────────────┐
-- │ 表                  │ 唯一索引（conflict target）     │ upsert 策略       │
-- ├─────────────────────┼────────────────────────────────┼───────────────────┤
-- │ assets              │ (symbol)                        │ DO NOTHING        │
-- │ raw_market_data     │ (asset_id, time, frequency)     │ DO NOTHING        │
-- │ macro_data          │ (indicator, time, frequency)    │ DO NOTHING        │
-- │ derived_indicators  │ (indicator, asset_id, time,     │ DO UPDATE value   │
-- │                     │  frequency)                     │                   │
-- │ regimes             │ (time)                          │ DO UPDATE all     │
-- │ signals             │ (asset_id, time)                │ DO UPDATE all     │
-- │ backtest_results    │ BIGSERIAL PK（append-only）     │ 刻意無 upsert，   │
-- │                     │                                 │ 保留歷史記錄      │
-- │ backtest_equity_    │ (backtest_id, time)             │ DO NOTHING        │
-- │ curve               │                                 │                   │
-- └─────────────────────┴────────────────────────────────┴───────────────────┘
--
-- 執行方式：
--   psql -U postgres -d quant -f schema_main.sql
-- ============================================================


-- ── assets ────────────────────────────────────────────────────────────────────
-- 所有資產（市場資產 + 合成資產）的主表
-- 冪等鍵：symbol（全域唯一）

CREATE TABLE IF NOT EXISTS assets (
    asset_id    BIGSERIAL       PRIMARY KEY,
    symbol      VARCHAR(32)     NOT NULL,
    name        VARCHAR(256),
    asset_type  VARCHAR(32),
    exchange    VARCHAR(32),
    currency    VARCHAR(8)      DEFAULT 'USD'
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_assets_symbol
    ON assets (symbol);


-- ── raw_market_data ───────────────────────────────────────────────────────────
-- 原始市場 OHLCV 資料（yfinance 來源）
-- TimescaleDB hypertable（若已安裝 timescaledb extension）
-- 冪等鍵：(asset_id, time, frequency)
-- upsert 策略：DO NOTHING（原始資料以第一次寫入為準）

CREATE TABLE IF NOT EXISTS raw_market_data (
    asset_id    BIGINT          NOT NULL REFERENCES assets(asset_id),
    time        TIMESTAMPTZ     NOT NULL,
    frequency   VARCHAR(16)     NOT NULL DEFAULT '1d',
    open        NUMERIC(18, 6),
    high        NUMERIC(18, 6),
    low         NUMERIC(18, 6),
    close       NUMERIC(18, 6),
    adj_close   NUMERIC(18, 6),
    volume      NUMERIC(24, 2),
    source      VARCHAR(64),
    source_code VARCHAR(64)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_market_data_key
    ON raw_market_data (asset_id, time, frequency);

CREATE INDEX IF NOT EXISTS ix_raw_market_data_time
    ON raw_market_data (time DESC);

-- 若已安裝 TimescaleDB，解除下列注釋：
-- SELECT create_hypertable('raw_market_data', 'time', if_not_exists => TRUE);


-- ── macro_data ────────────────────────────────────────────────────────────────
-- 總經指標資料（FRED 來源）
-- 冪等鍵：(indicator, time, frequency)
-- upsert 策略：DO NOTHING（原始資料以第一次寫入為準）

CREATE TABLE IF NOT EXISTS macro_data (
    indicator   VARCHAR(64)     NOT NULL,
    time        TIMESTAMPTZ     NOT NULL,
    frequency   VARCHAR(16)     NOT NULL DEFAULT 'daily',
    value       NUMERIC(18, 6),
    unit        VARCHAR(32),
    source      VARCHAR(64)     DEFAULT 'fred',
    source_code VARCHAR(64)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_macro_data_key
    ON macro_data (indicator, time, frequency);

CREATE INDEX IF NOT EXISTS ix_macro_data_indicator_time
    ON macro_data (indicator, time DESC);

-- 若已安裝 TimescaleDB，解除下列注釋：
-- SELECT create_hypertable('macro_data', 'time', if_not_exists => TRUE);


-- ── derived_indicators ────────────────────────────────────────────────────────
-- 衍生指標（SMA、Momentum、Z-Score、Spread…）
-- 冪等鍵：(indicator, asset_id, time, frequency)
-- upsert 策略：DO UPDATE value（重算後覆蓋，確保最新計算值入庫）

CREATE TABLE IF NOT EXISTS derived_indicators (
    indicator          VARCHAR(128)    NOT NULL,
    asset_id           BIGINT          NOT NULL REFERENCES assets(asset_id),
    time               TIMESTAMPTZ     NOT NULL,
    frequency          VARCHAR(16)     NOT NULL DEFAULT '1d',
    value              NUMERIC(18, 6),
    unit               VARCHAR(32),
    calculation_method VARCHAR(128),
    source             VARCHAR(128)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_derived_indicators_key
    ON derived_indicators (indicator, asset_id, time, frequency);

CREATE INDEX IF NOT EXISTS ix_derived_indicators_indicator_time
    ON derived_indicators (indicator, time DESC);

-- 若已安裝 TimescaleDB，解除下列注釋：
-- SELECT create_hypertable('derived_indicators', 'time', if_not_exists => TRUE);


-- ── regimes ───────────────────────────────────────────────────────────────────
-- 每日 Regime 判定結果（由 run_daily.py / run_engine.py 寫入）
-- 冪等鍵：(time)  ← 每天只保留最新一筆，重跑會覆蓋
-- upsert 策略：DO UPDATE（重跑覆蓋同日判定）

CREATE TABLE IF NOT EXISTS regimes (
    time             DATE            NOT NULL,
    scenario         VARCHAR(16),
    regime           VARCHAR(128),
    market_phase     VARCHAR(64),
    regime_score     NUMERIC(6, 2),
    confidence_score VARCHAR(16),
    macro_score      NUMERIC(6, 2),
    liquidity_score  NUMERIC(6, 2),
    credit_score     NUMERIC(6, 2),
    sentiment_score  NUMERIC(6, 2),
    notes            JSONB           DEFAULT '{}'
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_regimes_time
    ON regimes (time);

CREATE INDEX IF NOT EXISTS ix_regimes_time_desc
    ON regimes (time DESC);


-- ── signals ───────────────────────────────────────────────────────────────────
-- 每日每資產訊號（由 run_daily.py / run_engine.py 寫入）
-- 冪等鍵：(asset_id, time)  ← 每資產每天只保留最新訊號，重跑會覆蓋
-- upsert 策略：DO UPDATE（重跑覆蓋同日訊號）

CREATE TABLE IF NOT EXISTS signals (
    asset_id        BIGINT          NOT NULL REFERENCES assets(asset_id),
    time            DATE            NOT NULL,
    signal_type     VARCHAR(32),
    signal_strength VARCHAR(32),
    scenario        VARCHAR(16),
    rationale       TEXT,
    metadata        JSONB           DEFAULT '{}'
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_signals_asset_time
    ON signals (asset_id, time);

CREATE INDEX IF NOT EXISTS ix_signals_time_desc
    ON signals (time DESC);


-- ── backtest_results ──────────────────────────────────────────────────────────
-- 回測執行記錄（append-only，不做 upsert）
-- 每次回測會新增一筆，保留完整歷史。
-- 不需要 unique constraint（by design）。

CREATE TABLE IF NOT EXISTS backtest_results (
    backtest_id     BIGSERIAL       PRIMARY KEY,
    run_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    strategy_name   VARCHAR(128)    NOT NULL,
    assets          TEXT[]          NOT NULL,
    start_date      DATE            NOT NULL,
    end_date        DATE            NOT NULL,
    cagr            NUMERIC(10, 6),
    max_drawdown    NUMERIC(10, 6),
    sharpe          NUMERIC(10, 4),
    total_return    NUMERIC(10, 6),
    annual_returns  JSONB,
    case_studies    JSONB,
    metadata        JSONB           NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS ix_backtest_results_strategy_date
    ON backtest_results (strategy_name, start_date, end_date);


-- ── backtest_equity_curve ─────────────────────────────────────────────────────
-- 回測每日淨值曲線（linked to backtest_results）
-- 冪等鍵：(backtest_id, time)（PRIMARY KEY）

CREATE TABLE IF NOT EXISTS backtest_equity_curve (
    backtest_id     BIGINT          NOT NULL
                        REFERENCES backtest_results(backtest_id) ON DELETE CASCADE,
    time            DATE            NOT NULL,
    equity          NUMERIC(18, 6)  NOT NULL,
    daily_return    NUMERIC(12, 8),
    drawdown        NUMERIC(12, 8),
    regime          VARCHAR(64),
    scenario        VARCHAR(16),
    confidence      VARCHAR(16),
    positions       JSONB,
    PRIMARY KEY (backtest_id, time)
);

CREATE INDEX IF NOT EXISTS ix_backtest_equity_bid
    ON backtest_equity_curve (backtest_id, time DESC);


-- ============================================================
-- Replay Safety Verification Queries
-- 執行以下查詢驗證重跑後無重複資料
-- ============================================================

-- 1. raw_market_data 無重複
-- SELECT asset_id, time, frequency, COUNT(*)
-- FROM raw_market_data
-- GROUP BY asset_id, time, frequency
-- HAVING COUNT(*) > 1;
-- 預期：0 rows

-- 2. macro_data 無重複
-- SELECT indicator, time, frequency, COUNT(*)
-- FROM macro_data
-- GROUP BY indicator, time, frequency
-- HAVING COUNT(*) > 1;
-- 預期：0 rows

-- 3. derived_indicators 無重複
-- SELECT indicator, asset_id, time, frequency, COUNT(*)
-- FROM derived_indicators
-- GROUP BY indicator, asset_id, time, frequency
-- HAVING COUNT(*) > 1;
-- 預期：0 rows

-- 4. regimes 無重複（每天一筆）
-- SELECT time, COUNT(*)
-- FROM regimes
-- GROUP BY time
-- HAVING COUNT(*) > 1;
-- 預期：0 rows

-- 5. signals 無重複（每資產每天一筆）
-- SELECT asset_id, time, COUNT(*)
-- FROM signals
-- GROUP BY asset_id, time
-- HAVING COUNT(*) > 1;
-- 預期：0 rows
