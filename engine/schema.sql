-- ============================================================
-- Macro Engine Output Tables
-- ============================================================

-- ── engine_regime_log ─────────────────────────────────────────
-- 每次執行引擎的全局 regime 判定結果
CREATE TABLE IF NOT EXISTS engine_regime_log (
    run_id          BIGSERIAL       PRIMARY KEY,
    run_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    as_of_date      DATE            NOT NULL,

    regime          CHAR(1)         NOT NULL CHECK (regime IN ('A', 'B', 'C', 'U')),
    regime_score    NUMERIC(6,2)    NOT NULL,   -- 0(危險) ~ 100(極佳機會)
    stress_score    NUMERIC(6,2)    NOT NULL,   -- 0(平靜)  ~ 100(極端壓力)
    confidence_pct  NUMERIC(6,2)    NOT NULL,   -- 資料完整度 0~100

    -- Sub-scores (各維度原始得分，便於回測分析)
    vix_score       NUMERIC(6,2),
    credit_score    NUMERIC(6,2),
    liquidity_score NUMERIC(6,2),
    growth_score    NUMERIC(6,2),
    valuation_score NUMERIC(6,2),

    -- 原始快照（JSON，便於 debug）
    snapshot        JSONB           NOT NULL DEFAULT '{}',

    UNIQUE (as_of_date)             -- 每天只保留最新一次
);

-- ── engine_signals ────────────────────────────────────────────
-- 每個核心資產的訊號與建議倉位
CREATE TABLE IF NOT EXISTS engine_signals (
    run_id          BIGINT          NOT NULL REFERENCES engine_regime_log(run_id),
    as_of_date      DATE            NOT NULL,
    asset_id        INTEGER         NOT NULL REFERENCES assets(asset_id),

    -- Signal
    signal          VARCHAR(16)     NOT NULL
                    CHECK (signal IN ('STRONG_BUY','BUY','NEUTRAL','REDUCE','AVOID')),
    position_pct    NUMERIC(6,2)    NOT NULL,   -- 建議部位佔投組 %

    -- Defense flags
    falling_knife   BOOLEAN         NOT NULL DEFAULT FALSE,
    fk_reason       TEXT,

    -- Asset-level sub-scores
    valuation_score NUMERIC(6,2),
    momentum_score  NUMERIC(6,2),
    breadth_score   NUMERIC(6,2),

    -- Detail JSON
    details         JSONB           NOT NULL DEFAULT '{}',

    PRIMARY KEY (as_of_date, asset_id)
);

-- ── engine_data_log ───────────────────────────────────────────
-- 資料溯源紀錄（對應 output 溯源驗證表）
CREATE TABLE IF NOT EXISTS engine_data_log (
    run_id          BIGINT          NOT NULL REFERENCES engine_regime_log(run_id),
    as_of_date      DATE            NOT NULL,
    dimension       VARCHAR(64)     NOT NULL,   -- e.g. "信用", "情緒"
    indicator_name  VARCHAR(128)    NOT NULL,
    raw_value       NUMERIC(24,8),
    unit            VARCHAR(32),
    data_timestamp  TIMESTAMPTZ,
    source          VARCHAR(128),
    code            VARCHAR(64),
    is_stale        BOOLEAN         NOT NULL DEFAULT FALSE,
    is_derived      BOOLEAN         NOT NULL DEFAULT FALSE,
    na_reason       TEXT,

    PRIMARY KEY (as_of_date, indicator_name)
);

-- Index for report queries
CREATE INDEX IF NOT EXISTS ix_engine_signals_date
    ON engine_signals (as_of_date DESC);

CREATE INDEX IF NOT EXISTS ix_engine_data_log_date
    ON engine_data_log (as_of_date DESC);
