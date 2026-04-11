"""
Pre-Backtest Validation Checks
-------------------------------
每個 check 函式直接查詢 PostgreSQL，回傳 CheckResult。
結果彙總後交由 report.py 輸出，不含任何格式邏輯。

設計原則
--------
- 每個 check 獨立，失敗不影響其他 check
- DB 表不存在 / 無資料 → 回傳 FAIL，不拋例外
- 全部 SQL 為唯讀（SELECT only）
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from psycopg2.extensions import connection as PgConnection

logger = logging.getLogger(__name__)


# ── CheckResult ───────────────────────────────────────────────────────────────

STATUS_ORDER = {"PASS": 0, "WARN": 1, "FAIL": 2, "INFO": 3}

@dataclass
class CheckResult:
    name:      str
    status:    str          # PASS | WARN | FAIL | INFO
    message:   str
    value:     Any = None   # 實際量測值
    threshold: Any = None   # 判定閾值
    details:   Dict = field(default_factory=dict)

    @property
    def is_critical(self) -> bool:
        return self.status == "FAIL"

    @property
    def emoji(self) -> str:
        return {"PASS": "✅", "WARN": "⚠️", "FAIL": "🔴", "INFO": "ℹ️"}.get(self.status, "")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_query(conn: PgConnection, sql: str, params=None) -> Optional[List[Tuple]]:
    """執行 SELECT，任何例外回傳 None（含 table not found）。"""
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()
    except Exception as exc:
        logger.debug("Query failed: %s — %s", sql[:80], exc)
        conn.rollback()
        return None


def _scalar(conn, sql, params=None) -> Optional[Any]:
    rows = _safe_query(conn, sql, params)
    return rows[0][0] if rows and rows[0][0] is not None else None


# ── Thresholds ────────────────────────────────────────────────────────────────

MIN_ROWS_ASSET       = 100       # 每個資產最少交易日
MAX_NULL_RATE_CLOSE  = 0.02      # close 欄最大缺值率（FAIL）
MAX_NULL_RATE_WARN   = 0.05      # 衍生指標缺值率（WARN）
MAX_STALENESS_DAYS   = 7         # 最新資料距今最多幾天（price/macro）
MAX_MACRO_GAP_DAYS   = 45        # macro forward fill 最大允許間距
MIN_ALIGNMENT_RATE   = 0.95      # 可對齊的交易日比例下限（FAIL < 0.80）
WARN_ALIGNMENT_RATE  = 0.95      # WARN 閾值
MIN_REGIME_ROWS      = 30        # engine_regime_log 最少筆數

# 核心資產 proxy（與 ETL config 對應）
PROXY_SYMBOLS = {
    "VOO":     ("VOO",     "NYSE"),    # VOO 直接在 DB；SPY exchange='NYSE Arca'（yfinance 真實值），避免漂移
    "QQQM":    ("QQQ",     "NASDAQ"),
    "SMH":     ("SOXX",    "NASDAQ"),
    "2330.TW": ("2330.TW", "TWSE"),
    "VIX":     ("^VIX",    "CBOE"),
}

CORE_MACRO = [
    "ISM_PMI_MFG",
    "HY_OAS",
]

CORE_DERIVED = [
    ("SMA",           '{"period": 5}'),
    ("PRICE_CHG_PCT", '{"label": "1W", "period": 5}'),
    ("PRICE_CHG_PCT", '{"label": "1M", "period": 21}'),
    ("VIX_ROLLING",   '{"stat": "mean", "window": 20}'),
    ("VIX_ROLLING",   '{"stat": "pct_rank", "window": 252}'),
]


# ══════════════════════════════════════════════════════════════════════════════
# 1. raw_market_data checks
# ══════════════════════════════════════════════════════════════════════════════

def check_raw_market_data(
    conn: PgConnection,
    start: Optional[date] = None,
    end:   Optional[date] = None,
) -> List[CheckResult]:
    results: List[CheckResult] = []
    time_filter = _build_time_filter("r.time", start, end)

    # ── 1-A: 整體筆數 ─────────────────────────────────────────────────────────
    total = _scalar(conn, f"""
        SELECT COUNT(*) FROM raw_market_data r
        WHERE r.frequency = '1d' {time_filter}
    """)
    if total is None or total == 0:
        results.append(CheckResult(
            "raw_market_data.total_rows", "FAIL",
            "raw_market_data 完全無資料，請先執行 ETL",
            value=0,
        ))
        return results   # 後續 check 無意義

    results.append(CheckResult(
        "raw_market_data.total_rows", "INFO",
        f"raw_market_data 共 {total:,} 筆（頻率=1d）",
        value=total,
    ))

    # ── 1-B: 每個核心資產 ─────────────────────────────────────────────────────
    for target, (proxy, exchange) in PROXY_SYMBOLS.items():
        rows = _safe_query(conn, f"""
            SELECT
                COUNT(*)                                               AS row_count,
                MIN(r.time)::date                                      AS start_date,
                MAX(r.time)::date                                      AS end_date,
                ROUND(SUM(CASE WHEN r.close IS NULL THEN 1 ELSE 0 END)
                      ::numeric / NULLIF(COUNT(*), 0), 4)              AS null_rate_close,
                EXTRACT(EPOCH FROM (NOW() - MAX(r.time))) / 86400      AS staleness_days
            FROM raw_market_data r
            JOIN assets a ON a.asset_id = r.asset_id
            WHERE a.symbol = %s AND a.exchange = %s
              AND r.frequency = '1d' {time_filter}
        """, (proxy, exchange))

        if not rows or rows[0][0] == 0:
            results.append(CheckResult(
                f"raw_market_data.{target}",
                "FAIL",
                f"{target}（proxy={proxy}）無資料，請先執行 ETL",
                value=0,
                details={"proxy": proxy, "exchange": exchange},
            ))
            continue

        row_count, start_d, end_d, null_close, stale = rows[0]
        row_count = int(row_count)
        null_close = float(null_close or 0)
        stale = float(stale or 0)

        # Row count
        status = "PASS" if row_count >= MIN_ROWS_ASSET else "WARN"
        results.append(CheckResult(
            f"raw_market_data.{target}.row_count", status,
            f"{target}: {row_count:,} 筆  {start_d} ~ {end_d}",
            value=row_count, threshold=MIN_ROWS_ASSET,
            details={"start": str(start_d), "end": str(end_d)},
        ))

        # Close null rate
        if null_close > MAX_NULL_RATE_CLOSE:
            status = "FAIL"
        elif null_close > 0:
            status = "WARN"
        else:
            status = "PASS"
        results.append(CheckResult(
            f"raw_market_data.{target}.null_rate_close", status,
            f"{target} close 缺值率 {null_close*100:.2f}%",
            value=round(null_close, 4), threshold=MAX_NULL_RATE_CLOSE,
        ))

        # Staleness
        stale_status = "FAIL" if stale > MAX_STALENESS_DAYS else "PASS"
        results.append(CheckResult(
            f"raw_market_data.{target}.staleness", stale_status,
            f"{target} 最新資料距今 {stale:.1f} 天（最後日期={end_d}）",
            value=round(stale, 1), threshold=MAX_STALENESS_DAYS,
        ))

    # ── 1-C: 交易日缺口偵測（>5 天連續缺口） ─────────────────────────────────
    gap_rows = _safe_query(conn, f"""
        SELECT
            a.symbol,
            MAX(gap_days) AS max_gap
        FROM (
            SELECT
                asset_id,
                EXTRACT(EPOCH FROM (time - LAG(time) OVER (
                    PARTITION BY asset_id ORDER BY time
                ))) / 86400 AS gap_days
            FROM raw_market_data
            WHERE frequency = '1d' {time_filter}
        ) g
        JOIN assets a ON a.asset_id = g.asset_id
        WHERE g.gap_days > 5
        GROUP BY a.symbol
    """)
    if gap_rows:
        for sym, max_gap in gap_rows:
            if sym in [v[0] for v in PROXY_SYMBOLS.values()]:
                status = "WARN" if max_gap < 14 else "FAIL"
                results.append(CheckResult(
                    f"raw_market_data.{sym}.max_gap", status,
                    f"{sym} 最大連續缺口 {max_gap:.0f} 天（可能含長假）",
                    value=round(max_gap, 0), threshold=14,
                ))

    return results


# ══════════════════════════════════════════════════════════════════════════════
# 2. macro_data checks
# ══════════════════════════════════════════════════════════════════════════════

def check_macro_data(
    conn: PgConnection,
    start: Optional[date] = None,
    end:   Optional[date] = None,
) -> List[CheckResult]:
    results: List[CheckResult] = []
    time_filter = _build_time_filter("time", start, end)

    total = _scalar(conn, f"SELECT COUNT(*) FROM macro_data WHERE 1=1 {time_filter}")
    if total is None or total == 0:
        results.append(CheckResult(
            "macro_data.total_rows", "FAIL",
            "macro_data 完全無資料，請先執行 ETL",
            value=0,
        ))
        return results

    results.append(CheckResult(
        "macro_data.total_rows", "INFO",
        f"macro_data 共 {total:,} 筆",
        value=total,
    ))

    # ── Per indicator ─────────────────────────────────────────────────────────
    rows = _safe_query(conn, f"""
        SELECT
            indicator,
            COUNT(*)                                               AS row_count,
            MIN(time)::date                                        AS start_date,
            MAX(time)::date                                        AS end_date,
            ROUND(SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END)
                  ::numeric / NULLIF(COUNT(*), 0), 4)              AS null_rate,
            EXTRACT(EPOCH FROM (NOW() - MAX(time))) / 86400        AS staleness_days
        FROM macro_data
        WHERE 1=1 {time_filter}
        GROUP BY indicator
        ORDER BY indicator
    """)

    found_codes = set()
    if rows:
        for code, row_count, start_d, end_d, null_rate, stale in rows:
            found_codes.add(code)
            row_count = int(row_count)
            null_rate = float(null_rate or 0)
            stale     = float(stale or 0)
            is_core   = code in CORE_MACRO

            status = "PASS"
            if row_count < 12:   # 月資料至少 1 年
                status = "FAIL" if is_core else "WARN"

            results.append(CheckResult(
                f"macro_data.{code}.row_count", status,
                f"{code}: {row_count:,} 筆  {start_d} ~ {end_d}",
                value=row_count,
                details={"start": str(start_d), "end": str(end_d),
                         "is_core": is_core},
            ))

            # Null rate
            null_status = "FAIL" if null_rate > 0.10 else ("WARN" if null_rate > 0 else "PASS")
            results.append(CheckResult(
                f"macro_data.{code}.null_rate", null_status,
                f"{code} 缺值率 {null_rate*100:.2f}%",
                value=round(null_rate, 4),
            ))

            # Staleness（月資料允許 45 天）
            stale_limit = 45 if row_count < 500 else MAX_STALENESS_DAYS
            stale_status = "FAIL" if stale > stale_limit else "PASS"
            if is_core:
                results.append(CheckResult(
                    f"macro_data.{code}.staleness", stale_status,
                    f"{code} 最新資料距今 {stale:.1f} 天（上限={stale_limit}）",
                    value=round(stale, 1), threshold=stale_limit,
                ))

    # 核心指標缺失判定
    for code in CORE_MACRO:
        if code not in found_codes:
            results.append(CheckResult(
                f"macro_data.{code}.missing", "FAIL",
                f"核心指標 {code} 不存在，請在 etl/config.py 加入並執行 ETL",
                value=0,
            ))

    return results


# ══════════════════════════════════════════════════════════════════════════════
# 3. derived_indicators checks
# ══════════════════════════════════════════════════════════════════════════════

def check_derived_indicators(
    conn: PgConnection,
    start: Optional[date] = None,
    end:   Optional[date] = None,
) -> List[CheckResult]:
    results: List[CheckResult] = []
    time_filter = _build_time_filter("d.time", start, end)

    total = _scalar(conn, f"""
        SELECT COUNT(*) FROM derived_indicators d
        WHERE d.frequency = '1d' {time_filter}
    """)
    if total is None or total == 0:
        results.append(CheckResult(
            "derived_indicators.total_rows", "FAIL",
            "derived_indicators 完全無資料，請先執行 indicators 模組",
            value=0,
        ))
        return results

    results.append(CheckResult(
        "derived_indicators.total_rows", "INFO",
        f"derived_indicators 共 {total:,} 筆",
        value=total,
    ))

    # ── Per indicator_name × asset ─────────────────────────────────────────────
    rows = _safe_query(conn, f"""
        SELECT
            d.indicator_name,
            d.params::text                                         AS params,
            a.symbol,
            COUNT(*)                                               AS row_count,
            MIN(d.time)::date                                      AS start_date,
            MAX(d.time)::date                                      AS end_date,
            ROUND(SUM(CASE WHEN d.value IS NULL THEN 1 ELSE 0 END)
                  ::numeric / NULLIF(COUNT(*), 0), 4)              AS null_rate,
            EXTRACT(EPOCH FROM (NOW() - MAX(d.time))) / 86400      AS staleness_days
        FROM derived_indicators d
        JOIN assets a ON a.asset_id = d.asset_id
        WHERE d.frequency = '1d' {time_filter}
        GROUP BY d.indicator_name, d.params::text, a.symbol
        ORDER BY d.indicator_name, a.symbol
    """)

    if rows:
        found_indicators = set()
        for ind_name, params, symbol, row_count, start_d, end_d, null_rate, stale in rows:
            row_count = int(row_count)
            null_rate = float(null_rate or 0)
            stale     = float(stale or 0)
            key       = f"{ind_name}:{symbol}"
            found_indicators.add(f"{ind_name}|{params}")

            null_status = (
                "FAIL" if null_rate > MAX_NULL_RATE_CLOSE else
                "WARN" if null_rate > MAX_NULL_RATE_WARN else "PASS"
            )
            results.append(CheckResult(
                f"derived_indicators.{ind_name}.{symbol}",
                null_status,
                f"{ind_name} ({params[:40]}) | {symbol}: {row_count:,} 筆  缺值={null_rate*100:.2f}%  {start_d}~{end_d}",
                value={"rows": row_count, "null_rate": round(null_rate, 4)},
                details={"start": str(start_d), "end": str(end_d), "staleness_days": round(stale, 1)},
            ))

        # ── Coverage check：核心指標是否都有計算 ─────────────────────────────
        for ind_name, params in CORE_DERIVED:
            # 只要有任何一個資產的該指標，就算覆蓋到
            found = any(k.startswith(f"{ind_name}|") for k in found_indicators)
            if not found:
                results.append(CheckResult(
                    f"derived_indicators.{ind_name}.coverage",
                    "FAIL",
                    f"核心指標 {ind_name} (params={params}) 未找到，請執行 indicators 模組",
                    value=0,
                ))
            else:
                results.append(CheckResult(
                    f"derived_indicators.{ind_name}.coverage",
                    "PASS",
                    f"核心指標 {ind_name} 已覆蓋",
                ))

    return results


# ══════════════════════════════════════════════════════════════════════════════
# 4. engine_regime_log / engine_signals checks
# ══════════════════════════════════════════════════════════════════════════════

def check_engine_outputs(
    conn: PgConnection,
    start: Optional[date] = None,
    end:   Optional[date] = None,
) -> List[CheckResult]:
    results: List[CheckResult] = []

    # ── regime_log ────────────────────────────────────────────────────────────
    time_filter = _build_time_filter("as_of_date", start, end)
    rows = _safe_query(conn, f"""
        SELECT
            COUNT(*)                          AS row_count,
            MIN(as_of_date)                   AS start_date,
            MAX(as_of_date)                   AS end_date,
            ROUND(AVG(regime_score)::numeric, 1)  AS avg_regime_score,
            ROUND(AVG(confidence_pct)::numeric, 1) AS avg_confidence,
            COUNT(DISTINCT regime)             AS n_distinct_regimes,
            STRING_AGG(DISTINCT regime, ', ' ORDER BY regime)  AS regimes_seen
        FROM engine_regime_log
        WHERE 1=1 {time_filter}
    """)

    if not rows or rows[0][0] == 0:
        results.append(CheckResult(
            "engine_regime_log.total_rows", "WARN",
            "engine_regime_log 無資料，請先執行 engine 模組（非 backtest 必要條件）",
            value=0,
        ))
    else:
        cnt, start_d, end_d, avg_score, avg_conf, n_reg, reg_seen = rows[0]
        cnt = int(cnt)

        status = "PASS" if cnt >= MIN_REGIME_ROWS else "WARN"
        results.append(CheckResult(
            "engine_regime_log.total_rows", status,
            f"engine_regime_log: {cnt:,} 筆  {start_d} ~ {end_d}  "
            f"avg_score={avg_score}  avg_conf={avg_conf}%  regimes={reg_seen}",
            value=cnt, threshold=MIN_REGIME_ROWS,
            details={"start": str(start_d), "end": str(end_d)},
        ))

    # ── engine_signals ────────────────────────────────────────────────────────
    sig_rows = _safe_query(conn, f"""
        SELECT
            signal,
            COUNT(*) AS cnt
        FROM engine_signals
        WHERE 1=1 {time_filter}
        GROUP BY signal
        ORDER BY cnt DESC
    """)

    if not sig_rows:
        results.append(CheckResult(
            "engine_signals.distribution", "WARN",
            "engine_signals 無資料",
            value=0,
        ))
    else:
        sig_dist = {r[0]: int(r[1]) for r in sig_rows}
        total_sig = sum(sig_dist.values())
        results.append(CheckResult(
            "engine_signals.distribution", "INFO",
            f"engine_signals 共 {total_sig} 筆  分佈：{sig_dist}",
            value=sig_dist,
        ))

    return results


# ══════════════════════════════════════════════════════════════════════════════
# 5. Time alignment check（日資料 ↔ 月資料 forward fill 驗證）
# ══════════════════════════════════════════════════════════════════════════════

def check_time_alignment(
    conn: PgConnection,
    start: Optional[date] = None,
    end:   Optional[date] = None,
    macro_code: str = "ISM_PMI_MFG",
) -> List[CheckResult]:
    """
    對每個交易日，確認能找到指定 macro_code 在 MAX_MACRO_GAP_DAYS 以內的最新資料。
    可對齊率 >= WARN_ALIGNMENT_RATE → PASS
    可對齊率 >= 0.80              → WARN
    可對齊率<  0.80              → FAIL
    """
    results: List[CheckResult] = []
    time_filter = _build_time_filter("r.time", start, end)

    rows = _safe_query(conn, f"""
        WITH trading_days AS (
            SELECT DISTINCT date_trunc('day', r.time)::date AS td
            FROM raw_market_data r
            JOIN assets a ON a.asset_id = r.asset_id
            WHERE r.frequency = '1d'
              AND a.symbol = 'SPY'
            {time_filter}
        ),
        macro_latest AS (
            SELECT
                td.td AS trading_day,
                MAX(md.time::date) AS latest_macro_date
            FROM trading_days td
            LEFT JOIN macro_data md
              ON md.time::date <= td.td
             AND md.indicator = %s
            GROUP BY td.td
        )
        SELECT
            COUNT(*)                                                        AS total_days,
            SUM(CASE WHEN latest_macro_date IS NULL THEN 1 ELSE 0 END)     AS days_no_macro,
            SUM(CASE WHEN (trading_day - latest_macro_date) > %s THEN 1 ELSE 0 END)
                                                                            AS days_stale_macro,
            ROUND(
                SUM(CASE WHEN latest_macro_date IS NOT NULL
                          AND (trading_day - latest_macro_date) <= %s
                          THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0),
                4
            )                                                               AS alignment_rate
        FROM macro_latest
    """, (macro_code, MAX_MACRO_GAP_DAYS, MAX_MACRO_GAP_DAYS))

    if not rows or rows[0][0] is None or rows[0][0] == 0:
        results.append(CheckResult(
            f"time_alignment.{macro_code}", "WARN",
            "無法計算時間對齊率（trading days 或 macro_data 無資料）",
        ))
        return results

    total, no_macro, stale_macro, rate = rows[0]
    total      = int(total)
    no_macro   = int(no_macro or 0)
    stale_macro= int(stale_macro or 0)
    rate       = float(rate or 0)

    if rate >= WARN_ALIGNMENT_RATE:
        status = "PASS"
    elif rate >= 0.80:
        status = "WARN"
    else:
        status = "FAIL"

    results.append(CheckResult(
        f"time_alignment.{macro_code}", status,
        (
            f"日資料 ↔ {macro_code} 時間對齊率 {rate*100:.1f}%  "
            f"（共 {total} 個交易日，無對應 macro={no_macro}，"
            f"macro 過期={stale_macro}，上限={MAX_MACRO_GAP_DAYS}天）"
        ),
        value=round(rate, 4), threshold=WARN_ALIGNMENT_RATE,
        details={
            "total_trading_days": total,
            "days_no_macro":      no_macro,
            "days_stale_macro":   stale_macro,
            "max_gap_allowed":    MAX_MACRO_GAP_DAYS,
        },
    ))

    # HY OAS（日頻，應有更高對齊率）
    rows_hy = _safe_query(conn, f"""
        WITH trading_days AS (
            SELECT DISTINCT date_trunc('day', r.time)::date AS td
            FROM raw_market_data r
            JOIN assets a ON a.asset_id = r.asset_id
            WHERE r.frequency = '1d' AND a.symbol = 'SPY'
            {time_filter}
        )
        SELECT
            COUNT(*) AS total,
            ROUND(
                SUM(CASE WHEN md.time IS NOT NULL THEN 1 ELSE 0 END)::numeric
                / NULLIF(COUNT(*), 0), 4
            ) AS align_rate
        FROM trading_days td
        LEFT JOIN LATERAL (
            SELECT time FROM macro_data
            WHERE indicator = 'HY_OAS'
              AND time::date <= td.td
            ORDER BY time DESC LIMIT 1
        ) md ON true
    """)

    if rows_hy and rows_hy[0][0]:
        total_hy, rate_hy = int(rows_hy[0][0]), float(rows_hy[0][1] or 0)
        hy_status = "PASS" if rate_hy >= WARN_ALIGNMENT_RATE else "WARN"
        results.append(CheckResult(
            "time_alignment.HY_OAS", hy_status,
            f"日資料 ↔ HY_OAS 對齊率 {rate_hy*100:.1f}%（{total_hy} 交易日）",
            value=round(rate_hy, 4),
        ))

    return results


# ── Utility ───────────────────────────────────────────────────────────────────

def _build_time_filter(col: str, start: Optional[date], end: Optional[date]) -> str:
    parts = []
    if start:
        parts.append(f"AND {col} >= '{start}'")
    if end:
        parts.append(f"AND {col} <= '{end}'")
    return " ".join(parts)
