"""
Engine DB Writer — 冪等性寫入層
---------------------------------
統一 regimes / signals 的 upsert 邏輯，供以下模組共用：
  - engine/run_engine.py
  - report/run_daily_report.py

設計原則：
- regimes 冪等鍵：(time)                → DO UPDATE 覆蓋同日判定
- signals 冪等鍵：(asset_id, time)      → DO UPDATE 覆蓋同日訊號
- 同一天重跑 N 次，DB 結果與跑一次相同
- 若 UNIQUE INDEX 不存在 → ON CONFLICT 會拋 exception（讓呼叫端發現問題）
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Dict, Optional

import psycopg2.extras
from psycopg2.extensions import connection as PgConnection

from engine.regime import RegimeResult
from engine.signals import AssetSignal
from engine.snapshot import CORE_ASSET_PROXIES

logger = logging.getLogger(__name__)


def write_regime(
    conn: PgConnection,
    as_of: date,
    regime: RegimeResult,
) -> None:
    """
    冪等寫入 regimes 表。

    冪等鍵：time
    策略：DO UPDATE — 重跑覆蓋同日判定（確保 DB 反映最新計算結果）

    Requires: UNIQUE INDEX ON regimes (time)
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO regimes
                (time, scenario, regime, market_phase, regime_score,
                 confidence_score, macro_score, liquidity_score,
                 credit_score, sentiment_score, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (time) DO UPDATE SET
                scenario         = EXCLUDED.scenario,
                regime           = EXCLUDED.regime,
                market_phase     = EXCLUDED.market_phase,
                regime_score     = EXCLUDED.regime_score,
                confidence_score = EXCLUDED.confidence_score,
                macro_score      = EXCLUDED.macro_score,
                liquidity_score  = EXCLUDED.liquidity_score,
                credit_score     = EXCLUDED.credit_score,
                sentiment_score  = EXCLUDED.sentiment_score,
                notes            = EXCLUDED.notes
            """,
            (
                as_of,
                regime.scenario,
                regime.regime,
                regime.market_phase,
                regime.regime_score,
                regime.confidence_score,
                regime.macro_score,
                regime.liquidity_score,
                regime.credit_score,
                regime.sentiment_score,
                psycopg2.extras.Json(regime.notes),
            ),
        )
    logger.info("[DB] regimes  date=%s  scenario=%s  (upserted)", as_of, regime.scenario)


def write_signals(
    conn: PgConnection,
    as_of: date,
    signals: Dict[str, AssetSignal],
    regime: RegimeResult,
    extra_meta: Optional[Dict] = None,
) -> None:
    """
    冪等寫入 signals 表（所有資產，逐筆 upsert）。

    冪等鍵：(asset_id, time)
    策略：DO UPDATE — 重跑覆蓋同日同資產的訊號

    Parameters
    ----------
    extra_meta : 額外的 metadata（e.g. scouting_mult），合併進每筆訊號的 metadata JSONB
                 預設 None（不附加）

    Requires: UNIQUE INDEX ON signals (asset_id, time)
    """
    written = 0
    for asset, sig in signals.items():
        # 解析 proxy symbol → asset_id
        proxy = CORE_ASSET_PROXIES.get(asset, asset)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT asset_id FROM assets WHERE symbol = %s", (proxy,)
            )
            row = cur.fetchone()

        if row is None:
            logger.warning(
                "[DB] signals: asset_id not found  asset=%s  proxy=%s  (skipped)",
                asset, proxy,
            )
            continue

        asset_id = row[0]
        meta = {
            **sig.metadata,
            "regime":     regime.regime,
            "confidence": regime.confidence_score,
            **(extra_meta or {}),
        }

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO signals
                    (asset_id, time, signal_type, signal_strength,
                     scenario, rationale, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (asset_id, time) DO UPDATE SET
                    signal_type     = EXCLUDED.signal_type,
                    signal_strength = EXCLUDED.signal_strength,
                    scenario        = EXCLUDED.scenario,
                    rationale       = EXCLUDED.rationale,
                    metadata        = EXCLUDED.metadata
                """,
                (
                    asset_id,
                    as_of,
                    sig.signal_type,
                    sig.signal_strength,
                    sig.scenario,
                    sig.rationale,
                    psycopg2.extras.Json(meta),
                ),
            )
        written += 1

    logger.info("[DB] signals  date=%s  written=%d/%d  (upserted)", as_of, written, len(signals))
