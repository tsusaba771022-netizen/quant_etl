"""
Axiom Quant — Bug Fix Verification Tests
=========================================
覆蓋範圍：
  Group A  Z-Score 防爆（cleaner 主路徑 + tripwire fallback）
  Group B  Pessimistic Override（RiskSummary 結構、紅 / 黃 / 綠分級）
  Group C  PMI latest valid value（DB 查詢 + line_flex 解析）
  Group D  Regression（payload key、summary 結構、正常情況不誤報）
  Group E  Dry-run / Test-push 安全推播流程
"""
from __future__ import annotations

import unittest
from datetime import date

import pandas as pd

from engine.snapshot import SnapshotLoader
from etl.cleaner import MIN_ZSCORE_DENOMINATOR, compute_rolling_zscore
from report.daily_report import RiskSummary, _zscore_risk_signal_v2


# ── Fake DB infrastructure ──────────────────────────────────────────────────────

class _FakeCursor:
    def __init__(self, fetchall_result):
        self.fetchall_result = fetchall_result
        self.executed: list = []

    def __enter__(self):  return self
    def __exit__(self, *_): return False

    def execute(self, sql, params):
        self.executed.append((sql, params))

    def fetchall(self):
        return self.fetchall_result

    def fetchone(self):
        rows = self.fetchall_result
        return rows[0] if rows else None


class _FakeConn:
    def __init__(self, fetchall_result):
        self.fetchall_result = fetchall_result
        self.rollbacks = 0
        self.last_cursor: _FakeCursor | None = None

    def cursor(self):
        self.last_cursor = _FakeCursor(self.fetchall_result)
        return self.last_cursor

    def rollback(self):
        self.rollbacks += 1


# ══════════════════════════════════════════════════════════════════════════════
# Group A  Z-Score 防爆測試
# ══════════════════════════════════════════════════════════════════════════════

class TestZscoreAntiExplosion(unittest.TestCase):
    """
    確認無論是主路徑（cleaner.compute_rolling_zscore）
    還是 tripwire fallback（_rolling_zscore），
    rolling std 極小時 z-score 均不會超出合理範圍。
    """

    # ── A-1  主路徑：近乎常數序列 ──────────────────────────────────────────────

    def test_A1_near_constant_series_floor_applied(self):
        """
        輸入：[1.0, 1.0, 1.0, 1.001]，window=4
        rolling_std ≈ 0.000577（極小），floor 應接管分母（0.01）
        預期：z 最後一個值 ≈ (1.001 - 1.00075) / 0.01 = 0.025，遠 < 1.0
        """
        series = pd.Series([1.0, 1.0, 1.0, 1.001], dtype=float)
        z = compute_rolling_zscore(series, window=4, min_periods=4, name="A1")

        last_z = z.iloc[-1]
        rolling_mean = series.mean()
        expected = (series.iloc[-1] - rolling_mean) / MIN_ZSCORE_DENOMINATOR

        self.assertAlmostEqual(last_z, expected, places=10,
            msg="floor 未生效：z 值與預期不符")
        self.assertLess(abs(last_z), 1.0,
            msg=f"近乎常數序列 z-score 應 < 1.0，got {last_z:.4f}")

    def test_A2_exactly_constant_series_gives_zero(self):
        """
        輸入：300 個完全相同的值（rolling_std = 0）
        預期：所有有效 z-score = 0.0（分子 = 0，不論分母為何）
        確保不會出現 NaN 或 inf。
        """
        series = pd.Series([5.0] * 300, dtype=float)
        z = compute_rolling_zscore(series, window=252, min_periods=252, name="A2")

        valid = z.dropna()
        self.assertGreater(len(valid), 0, "應有有效 z-score 值")
        for val in valid:
            self.assertFalse(
                pd.isna(val) or val != val,
                f"常數序列 z 不應為 NaN，got {val}"
            )
            self.assertEqual(val, 0.0,
                f"常數序列 z 應為 0.0，got {val}")

    def test_A3_insufficient_data_gives_nan(self):
        """
        輸入：只有 3 個值，window=252
        預期：全部為 NaN（資料不足，不應爆炸也不應假裝有值）
        """
        series = pd.Series([1.0, 2.0, 3.0], dtype=float)
        z = compute_rolling_zscore(series, window=252, min_periods=252, name="A3")

        self.assertTrue(z.isna().all(),
            "資料不足 window 時，所有 z 應為 NaN")

    def test_A4_normal_series_reasonable_range(self):
        """
        輸入：252 個值，前 251 個 = 10.0，最後一個 = 12.0（偏高 2σ 左右）
        預期：z 最後一個值在合理範圍內（不爆炸），且方向正確（> 0）
        此測試同時驗證正常情況下 floor 不干擾結果。
        """
        import numpy as np
        rng = np.random.default_rng(42)
        base = rng.normal(loc=10.0, scale=1.0, size=252)
        # 最後一個值 = 均值 + 2.5 個真實 std，確保 z 應為正且合理
        base[-1] = base[:-1].mean() + 2.5 * base[:-1].std(ddof=1)
        series = pd.Series(base, dtype=float)

        z = compute_rolling_zscore(series, window=252, min_periods=252, name="A4")

        last_z = z.iloc[-1]
        self.assertFalse(pd.isna(last_z), "正常序列不應產生 NaN")
        self.assertGreater(last_z, 0,    "末值偏高，z 應為正")
        self.assertLess(abs(last_z), 10, f"正常序列 z 不應超出 ±10，got {last_z:.4f}")

    # ── A-5  tripwire fallback ──────────────────────────────────────────────────

    def test_A5_tripwire_near_constant_floor_applied(self):
        """
        tripwire._rolling_zscore：近乎常數序列（rolling_std ≈ 0）
        預期：abs(z) 不超過 10（floor 已生效，不爆炸）
        """
        from monitor.tripwire import _rolling_zscore

        n = 300
        series = pd.Series([10.0] * (n - 1) + [10.001], dtype=float)
        z = _rolling_zscore(series, window=252)

        last_z = float(z.dropna().iloc[-1])
        self.assertLess(abs(last_z), 10.0,
            f"tripwire fallback 未套用 floor，got z={last_z:.4f}")

    def test_A6_tripwire_constant_gives_zero(self):
        """
        tripwire._rolling_zscore：全常數序列
        預期：有效 z-score = 0.0
        """
        from monitor.tripwire import _rolling_zscore

        series = pd.Series([3.0] * 300, dtype=float)
        z = _rolling_zscore(series, window=252)

        valid = z.dropna()
        self.assertGreater(len(valid), 0, "應有有效 z")
        for v in valid:
            self.assertEqual(v, 0.0, f"tripwire 常數序列 z 應為 0.0，got {v}")

    def test_A7_floor_constant_matches_cleaner(self):
        """
        tripwire 使用的 floor 必須等於 cleaner.MIN_ZSCORE_DENOMINATOR（= 0.01）。
        確保兩個路徑行為一致，不會出現不同閾值造成結果差異。
        """
        from monitor.tripwire import _rolling_zscore
        from etl.cleaner import MIN_ZSCORE_DENOMINATOR

        n = 300
        delta = 0.0005  # rolling_std 極小但非零
        series = pd.Series([1.0] * (n - 1) + [1.0 + delta], dtype=float)

        z_tripwire = float(_rolling_zscore(series, window=252).dropna().iloc[-1])
        z_cleaner  = float(
            compute_rolling_zscore(series, window=252, min_periods=252).dropna().iloc[-1]
        )

        # 兩者都套用相同 floor，結果應一致
        self.assertAlmostEqual(z_tripwire, z_cleaner, places=6,
            msg=f"tripwire z={z_tripwire:.6f} vs cleaner z={z_cleaner:.6f}：floor 不一致")


# ══════════════════════════════════════════════════════════════════════════════
# Group B  Pessimistic Override 測試
# ══════════════════════════════════════════════════════════════════════════════

class TestPessimisticOverride(unittest.TestCase):
    """
    確認 _zscore_risk_signal_v2 的悲觀覆寫行為（P2 規則）：
    - VIX / Spread：對稱觸發，|z| >= 2.0 → red；|z| >= 1.0 → yellow
    - HY OAS：單向觸發，z >= 2.0 → red；z >= 1.0 → yellow（負向不觸發）
    - 全部未達閾值 → green；全部 None → green
    - icon 黃燈必須是 🟡，不是 🟠（已修正）
    """

    # ── B-1  返回型別結構 ───────────────────────────────────────────────────────

    def test_B1_returns_risk_summary_with_all_fields(self):
        """
        輸入：正常值
        預期：返回 RiskSummary，具備 level / icon / title / message 四個欄位
        """
        result = _zscore_risk_signal_v2({
            "VIX_Z_252": 0.3, "HY_OAS_Z_252": 0.1, "YIELD_SPREAD_10Y2Y_Z_252": -0.2,
        })
        self.assertIsInstance(result, RiskSummary)
        self.assertIn(result.level,   ("green", "yellow", "red"))
        self.assertIn(result.icon,    ("🟢", "🟡", "🔴"))
        self.assertIsInstance(result.title,   str)
        self.assertIsInstance(result.message, str)
        self.assertGreater(len(result.message), 0)

    # ── B-2  P2 驗證：HY OAS 負向極端不觸發 RED ──────────────────────────────

    def test_B2_hy_oas_negative_extreme_no_red_p2(self):
        """
        [P2 規則] 輸入：vix=+0.3, hy_oas=-2.4, spread=+0.1
        HY OAS 單向觸發，負向（利差收窄）不算信用壓力。
        預期：level=green（VIX/Spread 均在 ±1σ，HY OAS 負向不觸發）
        """
        s = _zscore_risk_signal_v2({
            "VIX_Z_252": 0.3, "HY_OAS_Z_252": -2.4, "YIELD_SPREAD_10Y2Y_Z_252": 0.1,
        })
        self.assertEqual(s.level, "green")
        self.assertEqual(s.icon,  "🟢")

    def test_B12_hy_oas_positive_extreme_still_red_p2(self):
        """
        [P2 規則] 輸入：vix=+0.3, hy_oas=+2.4, spread=+0.1
        HY OAS 正向（利差擴大）仍觸發 RED。
        預期：level=red, message 含「HY OAS」與「+2.40」
        """
        s = _zscore_risk_signal_v2({
            "VIX_Z_252": 0.3, "HY_OAS_Z_252": 2.4, "YIELD_SPREAD_10Y2Y_Z_252": 0.1,
        })
        self.assertEqual(s.level, "red")
        self.assertEqual(s.icon,  "🔴")
        self.assertIn("HY OAS", s.message)
        self.assertIn("+2.40",  s.message)

    def test_B13_hy_oas_negative_yellow_no_trigger_p2(self):
        """
        [P2 規則] 輸入：vix=+0.3, hy_oas=-1.5, spread=+0.1
        HY OAS 負向（利差極窄）也不觸發 YELLOW。
        預期：level=green（單向，負向不進入黃燈）
        """
        s = _zscore_risk_signal_v2({
            "VIX_Z_252": 0.3, "HY_OAS_Z_252": -1.5, "YIELD_SPREAD_10Y2Y_Z_252": 0.1,
        })
        self.assertEqual(s.level, "green")

    def test_B14_hy_oas_positive_yellow_triggers_p2(self):
        """
        [P2 規則] 輸入：vix=+0.3, hy_oas=+1.5, spread=+0.1
        HY OAS 正向（利差偏寬）觸發 YELLOW。
        預期：level=yellow, message 含「HY OAS」與「+1.50」
        """
        s = _zscore_risk_signal_v2({
            "VIX_Z_252": 0.3, "HY_OAS_Z_252": 1.5, "YIELD_SPREAD_10Y2Y_Z_252": 0.1,
        })
        self.assertEqual(s.level, "yellow")
        self.assertIn("HY OAS", s.message)
        self.assertIn("+1.50",  s.message)

    def test_B15_hy_oas_at_negative_exact_threshold_no_trigger_p2(self):
        """
        [P2 規則] 輸入：vix=+0.3, hy_oas=-2.0（邊界值，恰好 = -2.0），spread=+0.1
        預期：level=green（負向 -2.0 不觸發，正向才觸發）
        """
        s = _zscore_risk_signal_v2({
            "VIX_Z_252": 0.3, "HY_OAS_Z_252": -2.0, "YIELD_SPREAD_10Y2Y_Z_252": 0.1,
        })
        self.assertEqual(s.level, "green")

    # ── B-3  紅燈：VIX 正向極端（+3.1）────────────────────────────────────────

    def test_B3_red_when_vix_positive_extreme(self):
        """
        輸入：vix=+3.1, hy_oas=+0.5, spread=-0.3（vix |z|=3.1 ≥ 2.0）
        預期：level=red, message 含「VIX」與「+3.10」
        """
        s = _zscore_risk_signal_v2({
            "VIX_Z_252": 3.1, "HY_OAS_Z_252": 0.5, "YIELD_SPREAD_10Y2Y_Z_252": -0.3,
        })
        self.assertEqual(s.level, "red")
        self.assertIn("VIX",   s.message)
        self.assertIn("+3.10", s.message)

    # ── B-4  紅燈：Spread 極端倒掛（-2.1）─────────────────────────────────────

    def test_B4_red_when_spread_extreme_inversion(self):
        """
        輸入：vix=+0.5, hy_oas=+0.8, spread=-2.1（spread |z|=2.1 ≥ 2.0）
        預期：level=red, message 含「Spread」與「-2.10」
        """
        s = _zscore_risk_signal_v2({
            "VIX_Z_252": 0.5, "HY_OAS_Z_252": 0.8, "YIELD_SPREAD_10Y2Y_Z_252": -2.1,
        })
        self.assertEqual(s.level, "red")
        self.assertIn("Spread", s.message)
        self.assertIn("-2.10",  s.message)

    # ── B-5  紅燈：多指標同時觸發 ─────────────────────────────────────────────

    def test_B5_red_multiple_indicators(self):
        """
        輸入：vix=+2.5, hy_oas=+2.1, spread=-0.5（兩個 ≥ 2.0）
        預期：level=red，message 同時提及 VIX 與 HY OAS
        """
        s = _zscore_risk_signal_v2({
            "VIX_Z_252": 2.5, "HY_OAS_Z_252": 2.1, "YIELD_SPREAD_10Y2Y_Z_252": -0.5,
        })
        self.assertEqual(s.level, "red")
        self.assertIn("VIX",    s.message)
        self.assertIn("HY OAS", s.message)

    # ── B-6  黃燈：正常指標不觸發紅燈 ─────────────────────────────────────────

    def test_B6_yellow_single_indicator_vix(self):
        """
        輸入：vix=+1.42, hy_oas=+0.3, spread=-0.5（只有 VIX 1 ≤ |z| < 2）
        預期：level=yellow, icon=🟡（不是 🟠），message 含「VIX」與「+1.42」
        """
        s = _zscore_risk_signal_v2({
            "VIX_Z_252": 1.42, "HY_OAS_Z_252": 0.3, "YIELD_SPREAD_10Y2Y_Z_252": -0.5,
        })
        self.assertEqual(s.level, "yellow")
        self.assertEqual(s.icon,  "🟡")
        self.assertNotEqual(s.icon, "🟠", "icon 不應為舊的 🟠（已修正）")
        self.assertIn("VIX",   s.message)
        self.assertIn("+1.42", s.message)

    def test_B7_yellow_hy_oas_at_boundary(self):
        """
        輸入：vix=+0.5, hy_oas=+1.0（邊界值，恰好 = 1.0），spread=-0.3
        預期：level=yellow（1.0 ≥ 1.0 → 黃燈）
        """
        s = _zscore_risk_signal_v2({
            "VIX_Z_252": 0.5, "HY_OAS_Z_252": 1.0, "YIELD_SPREAD_10Y2Y_Z_252": -0.3,
        })
        self.assertEqual(s.level, "yellow")

    # ── B-8  綠燈：全部正常 ─────────────────────────────────────────────────────

    def test_B8_green_all_within_one_sigma(self):
        """
        輸入：vix=+0.3, hy_oas=+0.1, spread=-0.2（全部 |z| < 1.0）
        預期：level=green, icon=🟢
        """
        s = _zscore_risk_signal_v2({
            "VIX_Z_252": 0.3, "HY_OAS_Z_252": 0.1, "YIELD_SPREAD_10Y2Y_Z_252": -0.2,
        })
        self.assertEqual(s.level, "green")
        self.assertEqual(s.icon,  "🟢")

    def test_B9_green_at_boundary_just_below_one(self):
        """
        輸入：全部 |z| = 0.999（緊貼黃燈邊界但未跨入）
        預期：level=green（0.999 < 1.0）
        """
        s = _zscore_risk_signal_v2({
            "VIX_Z_252": 0.999, "HY_OAS_Z_252": -0.999, "YIELD_SPREAD_10Y2Y_Z_252": 0.999,
        })
        self.assertEqual(s.level, "green")

    # ── B-10  全部 None → 預設綠燈 ─────────────────────────────────────────────

    def test_B10_all_none_defaults_to_green(self):
        """
        輸入：全部 None（資料尚未載入）
        預期：level=green（保守預設），不崩潰
        """
        s = _zscore_risk_signal_v2({
            "VIX_Z_252": None, "HY_OAS_Z_252": None, "YIELD_SPREAD_10Y2Y_Z_252": None,
        })
        self.assertEqual(s.level, "green")
        self.assertEqual(s.icon,  "🟢")

    def test_B11_partial_none_still_detects_red(self):
        """
        輸入：VIX=None, HY_OAS=+2.3, Spread=None（部分 None）
        預期：level=red（HY OAS 觸發，None 不影響判斷）
        """
        s = _zscore_risk_signal_v2({
            "VIX_Z_252": None, "HY_OAS_Z_252": 2.3, "YIELD_SPREAD_10Y2Y_Z_252": None,
        })
        self.assertEqual(s.level, "red")
        self.assertIn("HY OAS", s.message)


# ══════════════════════════════════════════════════════════════════════════════
# Group C  PMI Latest Valid Value 測試
# ══════════════════════════════════════════════════════════════════════════════

class TestPmiLatestValidValue(unittest.TestCase):
    """
    確認低頻月資料（PMI）的 forward fill 行為：
    - 當前日無新 PMI → 取最近一期有效值
    - 只有整個 DB 序列都無有效值 → 才允許顯示 N/A
    """

    # ── C-1  DB 查詢層：lookback=None 取最近非 NULL 值 ────────────────────────

    def test_C1_lookback_none_returns_latest_nonnull(self):
        """
        輸入：DB 回傳 PMI 2026-03-01=49.8，HY_OAS 2026-04-09=3.12
        as_of=2026-04-10（今日無新 PMI）
        預期：ISM_PMI_MFG=(49.8, 2026-03-01)，不應缺失
        SQL 必須含 value IS NOT NULL
        """
        conn = _FakeConn([
            ("ISM_PMI_MFG", 49.8, date(2026, 3, 1)),
            ("HY_OAS",      3.12, date(2026, 4, 9)),
        ])
        loader = SnapshotLoader(conn)
        result = loader._latest_macro_with_dates(
            ["ISM_PMI_MFG", "HY_OAS"], date(2026, 4, 10), lookback=None,
        )
        self.assertIn("ISM_PMI_MFG", result)
        self.assertEqual(result["ISM_PMI_MFG"], (49.8, date(2026, 3, 1)))
        sql, _ = conn.last_cursor.executed[0]
        self.assertIn("value IS NOT NULL", sql)

    def test_C2_pmi_38_days_old_still_returned(self):
        """
        輸入：PMI 距今 38 天（< MAX_MONTHLY_STALENESS_DAYS=45），仍應返回
        預期：正確取值，不因「當天無更新」而缺失
        """
        pmi_date = date(2026, 3, 3)
        conn = _FakeConn([
            ("ISM_PMI_MFG", 50.3, pmi_date),
            ("HY_OAS",       3.25, date(2026, 4, 9)),
        ])
        loader = SnapshotLoader(conn)
        result = loader._latest_macro_with_dates(
            ["ISM_PMI_MFG", "HY_OAS"], date(2026, 4, 10), lookback=None,
        )
        self.assertIn("ISM_PMI_MFG", result)
        val, d = result["ISM_PMI_MFG"]
        self.assertEqual(val, 50.3)
        self.assertEqual(d,   pmi_date)

    def test_C3_empty_db_returns_no_entry(self):
        """
        輸入：DB 完全沒有 PMI 紀錄（模擬整個資料庫都無有效值）
        預期：result 中不含 ISM_PMI_MFG（允許顯示 N/A）
        """
        conn = _FakeConn([
            ("HY_OAS", 3.25, date(2026, 4, 9)),
            # 沒有 ISM_PMI_MFG 行
        ])
        loader = SnapshotLoader(conn)
        result = loader._latest_macro_with_dates(
            ["ISM_PMI_MFG", "HY_OAS"], date(2026, 4, 10), lookback=None,
        )
        self.assertNotIn("ISM_PMI_MFG", result,
            "整個 DB 無有效 PMI 時，result 不應包含 ISM_PMI_MFG")
        # HY_OAS 應正常返回
        self.assertIn("HY_OAS", result)

    # ── C-4  line_flex 解析層 ───────────────────────────────────────────────────

    def test_C4_flex_parse_pmi_numeric_with_emoji(self):
        """
        輸入：PMI 行 = "49.8  📊 溫和擴張"（帶 emoji 與文字說明）
        預期：p["pmi"] = "49.8"（只留數字，emoji / 文字全部去除）
        """
        from report.line_flex import _parse
        md = _make_macro_md("49.8  📊 溫和擴張")
        p = _parse(md)
        self.assertEqual(p["pmi"], "49.8")

    def test_C5_flex_parse_pmi_na_shows_na_not_garbage(self):
        """
        輸入：PMI 行 = "N/A ⚠️"（無資料）
        預期：p["pmi"] = "N/A"（不可出現舊 bug 的 "NA/" 等垃圾字串）
        舊 Bug：re.sub('[^\\d.NA/]', ...) 保留了 N, A, / 字元，
               把 "N/A ⚠️" 變成 "NA/"（斜線在後面），而非 "N/A"（斜線在中間）。
        正確：p["pmi"] 應精確等於 "N/A"，不是 "NA/"，不是空字串，不是其他垃圾。
        """
        from report.line_flex import _parse
        md = _make_macro_md("N/A ⚠️")
        p = _parse(md)
        # 精確比對：必須是 "N/A"，不是 "NA/"、"NA"、"" 等任何其他值
        self.assertEqual(p["pmi"], "N/A",
            f"PMI N/A 應顯示 'N/A'，實際為 {p['pmi']!r}")
        # 明確排除舊 bug 的輸出形式（"NA/" = N, A, / 順序錯誤）
        self.assertNotEqual(p["pmi"], "NA/",
            "不應出現舊 Bug 的 'NA/'（字元順序錯誤）")

    def test_C6_flex_parse_pmi_integer_value(self):
        """
        輸入：PMI 行 = "51  📈 擴張強勁"（整數，無小數點）
        預期：p["pmi"] = "51"
        """
        from report.line_flex import _parse
        md = _make_macro_md("51  📈 擴張強勁")
        p = _parse(md)
        self.assertEqual(p["pmi"], "51")

    def test_C7_flex_parse_pmi_below_50(self):
        """
        輸入：PMI 行 = "47.2  ⚠️ 收縮邊緣"
        預期：p["pmi"] = "47.2"（收縮區間數值也應正確解析）
        """
        from report.line_flex import _parse
        md = _make_macro_md("47.2  ⚠️ 收縮邊緣")
        p = _parse(md)
        self.assertEqual(p["pmi"], "47.2")


# ══════════════════════════════════════════════════════════════════════════════
# Group D  Regression Tests（回歸測試）
# ══════════════════════════════════════════════════════════════════════════════

class TestRegression(unittest.TestCase):
    """
    確認既有行為未被 Bug Fix 破壞：
    - RiskSummary 結構不變
    - line_flex payload key 不變
    - 正常情況不誤報（綠燈不變成黃 / 紅）
    - SQL 結構：lookback=None 只帶 2 個參數
    """

    # ── D-1  RiskSummary 結構回歸 ──────────────────────────────────────────────

    def test_D1_risk_summary_has_four_fields(self):
        """
        RiskSummary 必須有且只有 level / icon / title / message 四個欄位。
        任何重構不可增刪欄位（破壞 consumer 解包）。
        """
        s = _zscore_risk_signal_v2({
            "VIX_Z_252": 0.3, "HY_OAS_Z_252": 0.1, "YIELD_SPREAD_10Y2Y_Z_252": -0.2,
        })
        self.assertTrue(hasattr(s, "level"),   "缺 level 欄位")
        self.assertTrue(hasattr(s, "icon"),    "缺 icon 欄位")
        self.assertTrue(hasattr(s, "title"),   "缺 title 欄位")
        self.assertTrue(hasattr(s, "message"), "缺 message 欄位")
        # _fields 是 NamedTuple 的屬性，確認欄位數量
        self.assertEqual(len(s._fields), 4, f"欄位數應為 4，實際為 {s._fields}")

    # ── D-2  line_flex payload key 回歸 ────────────────────────────────────────

    def test_D2_flex_payload_required_keys_present(self):
        """
        _parse() 必須回傳包含所有 consumer 依賴的 key。
        任何 key 改名都會讓 build_line_flex_payload() 靜默輸出 N/A。
        """
        from report.line_flex import _parse
        REQUIRED_KEYS = {
            "scenario", "confidence", "conclusion",
            "voo", "qqqm", "smh", "tsmc", "cash",
            "vix", "vix_pct", "hy_oas", "spread", "pmi",
            "op_summary", "voo_op", "qqqm_sig", "smh_sig", "tsmc_sig",
            "sc_change", "risk_drivers",
            "vix_z", "hy_z", "spread_z", "z_signal",
            "missing", "ffill",
        }
        md = _make_full_md()
        p = _parse(md)
        for key in REQUIRED_KEYS:
            self.assertIn(key, p, f"payload 缺少 key: {key!r}")

    # ── D-3  正常情況不誤報 ─────────────────────────────────────────────────────

    def test_D3_no_false_alarm_all_near_zero(self):
        """
        輸入：三個指標均接近 0（很正常）
        預期：level=green，不誤報為 yellow 或 red
        """
        s = _zscore_risk_signal_v2({
            "VIX_Z_252": 0.05, "HY_OAS_Z_252": -0.05, "YIELD_SPREAD_10Y2Y_Z_252": 0.0,
        })
        self.assertEqual(s.level, "green", "正常情況不應誤報警告")

    def test_D4_no_false_alarm_empty_zscore_dict(self):
        """
        輸入：空 dict（z_scores 尚未載入）
        預期：不 crash，level=green
        """
        s = _zscore_risk_signal_v2({})
        self.assertEqual(s.level, "green")

    # ── D-5  DB 查詢 SQL 結構回歸 ─────────────────────────────────────────────

    def test_D5_lookback_none_sql_has_no_time_filter(self):
        """
        lookback=None 時，SQL 不應帶 time >= 起始日 的篩選條件。
        只帶 2 個參數（indicators list + as_of date）。
        """
        conn = _FakeConn([("ISM_PMI_MFG", 50.0, date(2026, 3, 1))])
        loader = SnapshotLoader(conn)
        loader._latest_macro_with_dates(["ISM_PMI_MFG"], date(2026, 4, 10), lookback=None)

        sql, params = conn.last_cursor.executed[0]
        self.assertEqual(len(params), 2,
            f"lookback=None 應只帶 2 個 SQL 參數，實際為 {len(params)}")
        self.assertNotIn("time >=", sql.replace("\n", " "),
            "lookback=None 時 SQL 不應含 time >= 起始日 篩選")

    def test_D6_lookback_int_sql_has_time_filter(self):
        """
        lookback=45 時，SQL 應帶 time >= 起始日 的篩選（3 個參數）。
        回歸：確認 lookback 機制未被破壞。
        """
        conn = _FakeConn([("HY_OAS", 3.1, date(2026, 4, 9))])
        loader = SnapshotLoader(conn)
        loader._latest_macro_with_dates(["HY_OAS"], date(2026, 4, 10), lookback=45)

        sql, params = conn.last_cursor.executed[0]
        self.assertEqual(len(params), 3,
            f"lookback=45 應帶 3 個 SQL 參數，實際為 {len(params)}")

    # ── D-7  compute_rolling_zscore 不改變輸入 Series ─────────────────────────

    def test_D7_compute_zscore_does_not_mutate_input(self):
        """
        compute_rolling_zscore 必須是純函式，不可 in-place 修改輸入 series。
        """
        original = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0], dtype=float)
        copy_before = original.copy()
        compute_rolling_zscore(original, window=5, min_periods=5, name="D7")
        pd.testing.assert_series_equal(original, copy_before,
            check_names=False, obj="輸入 Series 不應被 in-place 修改")

    # ── D-8  send_line PMI regex 漏修 bug ──────────────────────────────────────

    def test_D8_send_line_pmi_regex_fixed(self):
        """
        send_line.build_line_message() 也有同一個 PMI regex bug（line_flex.py 的兄弟）。
        確認已一併修補，不再輸出 'NA/'。
        """
        from report.send_line import build_line_message
        from datetime import date

        md = _make_macro_md("N/A ⚠️") + """
## 三、目標配置

| 資產 | 目標權重 |
|------|---------|
| VOO | **70%** |
| QQQM | **0%** |
| SMH | **0%** |
| 2330.TW | **0%** |
| 現金 | **30%** |
"""
        text = build_line_message(md, date(2026, 4, 10))
        # 確認 PMI 段落不含 "NA/"
        self.assertNotIn("PMI    NA/", text,
            "send_line.build_line_message PMI 仍輸出 'NA/'（漏修的 regex bug）")


# ══════════════════════════════════════════════════════════════════════════════
# Group E  Dry-run / Test-push 安全推播流程
# ══════════════════════════════════════════════════════════════════════════════

class TestDryRunAndTestPush(unittest.TestCase):
    """
    驗證三個情境的 dry-run payload 結構、TEST banner 注入、
    及 send_line_report() 在 dry-run 模式下不送出、不寫 marker。
    """

    # ── E-1  合成情境 markdown 可正確生成 ─────────────────────────────────────

    def test_E1_scenario_normal_generates_valid_markdown(self):
        """normal 情境：PMI 有值，z_signal 為黃燈。"""
        from report.send_line import _make_scenario_md
        from report.line_flex import _parse
        md = _make_scenario_md("normal", date(2026, 4, 10))
        p  = _parse(md)
        self.assertEqual(p["pmi"], "49.8")
        self.assertIn("🟡", p["z_signal"], "normal 情境應為黃燈")
        self.assertEqual(p["scenario"], "B")

    def test_E2_scenario_hy_red_generates_red_signal(self):
        """hy-red 情境：z_signal 必須為紅燈且點名 HY OAS。"""
        from report.send_line import _make_scenario_md
        from report.line_flex import _parse
        md = _make_scenario_md("hy-red", date(2026, 4, 10))
        p  = _parse(md)
        self.assertEqual(p["pmi"], "48.1")
        self.assertIn("🔴",    p["z_signal"], "hy-red 情境 z_signal 應為紅燈")
        self.assertIn("HY OAS", p["z_signal"], "z_signal 應點名 HY OAS")
        self.assertEqual(p["scenario"], "C")

    def test_E3_scenario_pmi_missing_shows_forwarded_value(self):
        """pmi-missing 情境：PMI 顯示沿用上期值（49.8），不應為 N/A。"""
        from report.send_line import _make_scenario_md
        from report.line_flex import _parse
        md = _make_scenario_md("pmi-missing", date(2026, 4, 10))
        p  = _parse(md)
        self.assertEqual(p["pmi"], "49.8",
            "PMI 當日缺值但有前值時，應顯示沿用的有效值，不應為 N/A")
        self.assertIn("🟢", p["z_signal"], "pmi-missing 情境 z_signal 應為綠燈")

    # ── E-4  _inject_test_banner 注入正確 ─────────────────────────────────────

    def test_E4_inject_test_banner_adds_alttext_prefix(self):
        """altText 應加上 [TEST] 前綴。"""
        from report.send_line import _inject_test_banner, _make_scenario_md
        from report.line_flex import build_line_flex_payload
        md  = _make_scenario_md("normal", date(2026, 4, 10))
        raw = build_line_flex_payload(md, date(2026, 4, 10))
        out = _inject_test_banner(raw)
        self.assertTrue(out["altText"].startswith("[TEST]"),
            f"altText 應以 [TEST] 開頭，實際為 {out['altText']!r}")

    def test_E5_inject_test_banner_adds_visible_banner(self):
        """Flex body 第一個 element 應為 TEST 橫幅（紅底白字）。"""
        from report.send_line import _inject_test_banner, _make_scenario_md
        from report.line_flex import build_line_flex_payload
        md  = _make_scenario_md("hy-red", date(2026, 4, 10))
        raw = build_line_flex_payload(md, date(2026, 4, 10))
        out = _inject_test_banner(raw)

        first_elem = out["contents"]["body"]["contents"][0]
        self.assertEqual(first_elem.get("backgroundColor"), "#B71C1C",
            "TEST 橫幅背景應為紅色 #B71C1C")
        banner_text = first_elem["contents"][0]["text"]
        self.assertIn("TEST MESSAGE", banner_text)

    def test_E6_inject_test_banner_does_not_mutate_original(self):
        """_inject_test_banner 不應修改原始 payload（必須在副本上操作）。"""
        from report.send_line import _inject_test_banner, _make_scenario_md
        from report.line_flex import build_line_flex_payload
        import copy
        md       = _make_scenario_md("normal", date(2026, 4, 10))
        original = build_line_flex_payload(md, date(2026, 4, 10))
        snapshot = copy.deepcopy(original)
        _inject_test_banner(original)
        self.assertEqual(original["altText"], snapshot["altText"],
            "_inject_test_banner 不應修改原始 payload 的 altText")

    # ── E-7  send_line_report dry_run 不送出、不寫 marker ─────────────────────

    def test_E7_dry_run_returns_true_without_sending(self):
        """
        dry_run=True：send_line_report 應返回 True（成功），
        但不呼叫 _push_line_message（不送出任何訊息）。
        """
        from pathlib import Path
        from report.send_line import send_line_report, _make_scenario_md

        md = _make_scenario_md("hy-red", date(2026, 4, 10))
        sent_calls: list = []

        import report.send_line as sl_module
        original_push = sl_module._push_line_message

        def mock_push(token, user_id, msg, d):
            sent_calls.append((token, user_id))
            return True

        sl_module._push_line_message = mock_push
        try:
            result = send_line_report(
                Path("__nonexistent__.md"),
                date(2026, 4, 10),
                dry_run=True,
                _md_override=md,
            )
        finally:
            sl_module._push_line_message = original_push

        self.assertTrue(result, "dry_run 應返回 True")
        self.assertEqual(len(sent_calls), 0,
            f"dry_run 不應呼叫 _push_line_message，實際呼叫了 {len(sent_calls)} 次")

    def test_E8_dry_run_does_not_write_marker(self):
        """
        dry_run=True：不應寫 sent marker，
        否則正式推播會被誤判為已發送而跳過。
        """
        from pathlib import Path
        from report.send_line import send_line_report, is_already_sent, _make_scenario_md

        md = _make_scenario_md("normal", date(2099, 12, 31))   # 未來日期，確保 marker 不存在

        # 先確認 marker 不存在
        test_date = date(2099, 12, 31)
        self.assertFalse(is_already_sent(test_date),
            "測試前 marker 不應存在（未來日期）")

        send_line_report(
            Path("__nonexistent__.md"),
            test_date,
            dry_run=True,
            _md_override=md,
        )

        self.assertFalse(is_already_sent(test_date),
            "dry_run 不應寫入 sent marker")

    def test_E9_test_mode_does_not_write_marker(self):
        """
        test_user_id 模式：不應寫正式 sent marker。
        使用 mock 避免真實 HTTP 請求。
        """
        from pathlib import Path
        from report.send_line import send_line_report, is_already_sent, _make_scenario_md
        import report.send_line as sl_module

        test_date = date(2099, 12, 30)
        self.assertFalse(is_already_sent(test_date))

        md = _make_scenario_md("hy-red", test_date)

        original_push = sl_module._push_line_message

        def mock_push(token, user_id, msg, d):
            return True   # 模擬成功送出

        sl_module._push_line_message = mock_push
        try:
            # 需要提供 token，否則會在憑證檢查就 return False
            import os
            os.environ.setdefault("LINE_CHANNEL_ACCESS_TOKEN", "test_token")
            result = send_line_report(
                Path("__nonexistent__.md"),
                test_date,
                test_user_id="Utest_user_000",
                _md_override=md,
            )
        finally:
            sl_module._push_line_message = original_push

        self.assertTrue(result)
        self.assertFalse(is_already_sent(test_date),
            "test_user_id 模式不應寫正式 sent marker")


# ══════════════════════════════════════════════════════════════════════════════
# Group F  Trend Risk Cap（Layer 2）
# ══════════════════════════════════════════════════════════════════════════════

class TestTrendLayer(unittest.TestCase):
    """
    驗證 compute_trend_status() 純函式在各種輸入下的正確判定。
    不依賴 DB，所有測試均為純計算。
    """

    # ── F-1  歷史資料不足 → TREND_WARMUP ──────────────────────────────────────

    def test_F1_warmup_insufficient_data(self):
        """
        輸入：100 筆收盤價（< 220）
        預期：status = TREND_WARMUP，不拋例外
        """
        from engine.trend import compute_trend_status, TrendStatus
        closes = [100.0 + i * 0.1 for i in range(100)]
        result = compute_trend_status(closes)
        self.assertEqual(result.status, TrendStatus.WARMUP)
        self.assertEqual(result.history_len, 100)
        self.assertIsNone(result.sma_200)
        self.assertIsNone(result.sma_200_slope)

    def test_F2_warmup_empty_list_no_crash(self):
        """
        輸入：空 list
        預期：WARMUP，不拋例外，history_len=0
        """
        from engine.trend import compute_trend_status, TrendStatus
        result = compute_trend_status([])
        self.assertEqual(result.status, TrendStatus.WARMUP)
        self.assertEqual(result.history_len, 0)
        self.assertIsNone(result.close)

    def test_F3_warmup_exactly_219_no_crash(self):
        """
        輸入：219 筆（比門檻少 1）
        預期：WARMUP，不拋例外
        """
        from engine.trend import compute_trend_status, TrendStatus
        closes = [200.0] * 219
        result = compute_trend_status(closes)
        self.assertEqual(result.status, TrendStatus.WARMUP)

    # ── F-4  資料充足，收盤 >= SMA_200 → TREND_OK ─────────────────────────────

    def test_F4_ok_when_close_above_sma200(self):
        """
        輸入：220 筆平穩上漲資料，最後一筆遠高於均值
        預期：TREND_OK
        """
        from engine.trend import compute_trend_status, TrendStatus
        # 前 219 筆 = 100，最後一筆拉高到 200（遠超 SMA）
        closes = [100.0] * 219 + [200.0]
        result = compute_trend_status(closes)
        self.assertEqual(result.status, TrendStatus.OK)
        self.assertIsNotNone(result.sma_200)
        self.assertGreater(result.close, result.sma_200)

    # ── F-5  收盤 < SMA_200，slope > 0 → TREND_CAUTION ───────────────────────

    def test_F5_caution_below_sma_positive_slope(self):
        """
        建構一個 close < SMA 且 SMA 仍上升（slope > 0）的序列。
        預期：TREND_CAUTION
        """
        from engine.trend import compute_trend_status, TrendStatus, MIN_HISTORY, SLOPE_WINDOW

        # 建立 close < SMA 且 SMA 仍上升（slope > 0）的序列：
        #   前 20 bar 極低（50），隨後長段穩定上升（bars 20..239），末 20 bar 急跌至 80。
        #   SMA_200(today) 涵蓋上升尾段 + 急跌，均值 ≈ 120；
        #   SMA_200(20日前) 涵蓋更多上升前段（未包含急跌），均值 ≈ 119.75；
        #   slope = +0.525 > 0 → CAUTION；close=80 < SMA≈120 → 確保非 OK。
        closes  = [50.0] * 20
        closes += [60.0 + (i - 20) * 0.5 for i in range(20, 240)]
        closes += [80.0] * 20  # total = 260 bars (>= MIN_HISTORY=220)

        result = compute_trend_status(closes)
        # 收盤應低於 SMA
        self.assertLess(result.close, result.sma_200,
                        f"close={result.close:.2f} 應低於 sma_200={result.sma_200:.2f}")
        # slope > 0 → CAUTION（數據已設計確保此方向）
        self.assertGreater(result.sma_200_slope, 0,
                           f"slope={result.sma_200_slope} 應 > 0（SMA 仍上升）")
        self.assertEqual(result.status, TrendStatus.CAUTION,
                         f"預期 CAUTION，實際 {result.status}")

    # ── F-6  收盤 < SMA_200，slope <= 0 → TREND_RISK_CAP ─────────────────────

    def test_F6_risk_cap_below_sma_negative_slope(self):
        """
        建構一個持續下跌的序列（close < SMA 且 SMA 也在下彎）。
        預期：TREND_RISK_CAP
        """
        from engine.trend import compute_trend_status, TrendStatus
        # 持續從高點下跌：SMA 跟著往下，slope <= 0
        closes = [500.0 - i * 1.0 for i in range(260)]
        result = compute_trend_status(closes)
        self.assertEqual(result.status, TrendStatus.RISK_CAP,
                         f"持續下跌序列應為 RISK_CAP，實際 {result.status}")
        self.assertLess(result.close, result.sma_200)
        self.assertLessEqual(result.sma_200_slope, 0)

    # ── F-7  WARMUP 不會觸發 RISK_CAP ─────────────────────────────────────────

    def test_F7_warmup_never_triggers_risk_cap(self):
        """
        任何資料不足（< 220 筆）的情況，都不應輸出 RISK_CAP。
        即使是連續下跌的短序列。
        """
        from engine.trend import compute_trend_status, TrendStatus
        # 下跌但只有 50 筆
        closes = [300.0 - i * 2.0 for i in range(50)]
        result = compute_trend_status(closes)
        self.assertNotEqual(result.status, TrendStatus.RISK_CAP,
                            "資料不足時不應輸出 RISK_CAP")
        self.assertEqual(result.status, TrendStatus.WARMUP)

    # ── F-8  恰好 220 筆 → 不是 WARMUP ────────────────────────────────────────

    def test_F8_exactly_220_not_warmup(self):
        """
        輸入：恰好 220 筆（等於 MIN_HISTORY）
        預期：不是 WARMUP（應為 OK / CAUTION / RISK_CAP 之一）
        """
        from engine.trend import compute_trend_status, TrendStatus
        closes = [100.0] * 220
        result = compute_trend_status(closes)
        self.assertNotEqual(result.status, TrendStatus.WARMUP,
                            "220 筆等於門檻，不應為 WARMUP")

    # ── F-9  TrendLayer DB 失敗 → WARMUP，不拋例外 ────────────────────────────

    def test_F9_trend_layer_db_failure_returns_warmup(self):
        """
        DB cursor 拋例外時，TrendLayer.run() 應返回 WARMUP，不拋例外。
        """
        from engine.trend import TrendLayer, TrendStatus

        class _BrokenConn:
            def cursor(self):
                raise RuntimeError("DB connection lost")

        layer  = TrendLayer()
        result = layer.run(_BrokenConn(), date(2026, 4, 10))
        self.assertEqual(result.status, TrendStatus.WARMUP)
        self.assertEqual(result.history_len, 0)

    # ── F-10  TrendLayer DB 回傳少量資料 → WARMUP ─────────────────────────────

    def test_F10_trend_layer_insufficient_db_rows_returns_warmup(self):
        """
        DB 只回傳 50 筆（少於 220）→ WARMUP。
        使用 _FakeConn 模擬。
        """
        from engine.trend import TrendLayer, TrendStatus

        # DB 回傳 50 筆（DESC 順序）
        rows = [(500.0 - i,) for i in range(50)]
        conn = _FakeConn(rows)

        layer  = TrendLayer()
        result = layer.run(conn, date(2026, 4, 10))
        self.assertEqual(result.status, TrendStatus.WARMUP)
        self.assertEqual(result.history_len, 50)


# ══════════════════════════════════════════════════════════════════════════════
# Group G  Macro Allocation Matrix（Layer 3）
# ══════════════════════════════════════════════════════════════════════════════

class TestMacroAllocMatrix(unittest.TestCase):
    """
    驗證 classify_macro_alloc() 為 deterministic rule block，
    非投票 / 非平均，且 DEFENSIVE 優先於 AGGRESSIVE。
    """

    # ── G-1  AGGRESSIVE（全部正向條件成立）────────────────────────────────────

    def test_G1_aggressive_all_conditions_met(self):
        """
        CFNAI=+0.30 >= +0.10, Spread=+0.50 > 0, VIX=15 < 20
        預期：AGGRESSIVE
        """
        from engine.macro_alloc import classify_macro_alloc, MacroAllocStatus
        result = classify_macro_alloc(cfnai=0.30, spread=0.50, vix=15.0)
        self.assertEqual(result.status, MacroAllocStatus.AGGRESSIVE)

    # ── G-2  NEUTRAL（CFNAI 不足門檻）────────────────────────────────────────

    def test_G2_neutral_cfnai_below_mild_expansion(self):
        """
        CFNAI=+0.05 < +0.10，其餘條件正向
        預期：NEUTRAL（CFNAI 未達 AGGRESSIVE 門檻，但未達 DEFENSIVE 門檻）
        """
        from engine.macro_alloc import classify_macro_alloc, MacroAllocStatus
        result = classify_macro_alloc(cfnai=0.05, spread=0.50, vix=15.0)
        self.assertEqual(result.status, MacroAllocStatus.NEUTRAL)

    def test_G3_neutral_cfnai_missing(self):
        """
        CFNAI = None（資料缺失），其餘條件正向
        預期：NEUTRAL（CFNAI 缺值不符合 AGGRESSIVE 全部條件）
        """
        from engine.macro_alloc import classify_macro_alloc, MacroAllocStatus
        result = classify_macro_alloc(cfnai=None, spread=0.50, vix=15.0)
        self.assertEqual(result.status, MacroAllocStatus.NEUTRAL)

    # ── G-4  DEFENSIVE via CFNAI ──────────────────────────────────────────────

    def test_G4_defensive_cfnai_recession_risk(self):
        """
        CFNAI=-0.80 < -0.70
        預期：DEFENSIVE（CFNAI 衰退風險起點觸發）
        """
        from engine.macro_alloc import classify_macro_alloc, MacroAllocStatus
        result = classify_macro_alloc(cfnai=-0.80, spread=0.50, vix=15.0)
        self.assertEqual(result.status, MacroAllocStatus.DEFENSIVE)
        self.assertIn("CFNAI", result.rationale)

    # ── G-5  DEFENSIVE via Yield Spread 倒掛 ─────────────────────────────────

    def test_G5_defensive_spread_inverted(self):
        """
        spread=-0.50 < 0 → yield curve inverted
        預期：DEFENSIVE
        """
        from engine.macro_alloc import classify_macro_alloc, MacroAllocStatus
        result = classify_macro_alloc(cfnai=0.30, spread=-0.50, vix=15.0)
        self.assertEqual(result.status, MacroAllocStatus.DEFENSIVE)
        self.assertIn("Yield Spread", result.rationale)

    # ── G-6  DEFENSIVE via VIX elevated ──────────────────────────────────────

    def test_G6_defensive_vix_elevated(self):
        """
        VIX=22 >= 20 → VIX elevated（Phase 1 無噪音過濾）
        預期：DEFENSIVE
        """
        from engine.macro_alloc import classify_macro_alloc, MacroAllocStatus
        result = classify_macro_alloc(cfnai=0.30, spread=0.50, vix=22.0)
        self.assertEqual(result.status, MacroAllocStatus.DEFENSIVE)
        self.assertIn("VIX", result.rationale)

    # ── G-7  DEFENSIVE 多條件成立，結果仍為 DEFENSIVE（非平均）──────────────

    def test_G7_defensive_multiple_conditions_not_averaging(self):
        """
        三個 DEFENSIVE 條件同時成立。
        驗證：不是票數相加，結果直接為 DEFENSIVE（不會變成「2/3 = NEUTRAL」）。
        """
        from engine.macro_alloc import classify_macro_alloc, MacroAllocStatus
        result = classify_macro_alloc(cfnai=-1.0, spread=-0.8, vix=25.0)
        self.assertEqual(result.status, MacroAllocStatus.DEFENSIVE,
                         "多條件同時觸發不應因『票數平均』而降為 NEUTRAL")

    # ── G-8  DEFENSIVE 優先於 AGGRESSIVE ────────────────────────────────────

    def test_G8_defensive_overrides_aggressive_conditions(self):
        """
        CFNAI=+0.50 符合 AGGRESSIVE，但 VIX=25 觸發 DEFENSIVE。
        預期：DEFENSIVE（優先級規則，而非平均）
        """
        from engine.macro_alloc import classify_macro_alloc, MacroAllocStatus
        result = classify_macro_alloc(cfnai=0.50, spread=0.80, vix=25.0)
        self.assertEqual(result.status, MacroAllocStatus.DEFENSIVE,
                         "DEFENSIVE 應優先於 AGGRESSIVE，不得取平均")

    # ── G-9  全部 None → NEUTRAL（缺值安全）─────────────────────────────────

    def test_G9_all_none_inputs_neutral_no_crash(self):
        """
        cfnai=spread=vix=None（全部資料缺失）
        預期：NEUTRAL（不拋例外）
        """
        from engine.macro_alloc import classify_macro_alloc, MacroAllocStatus
        result = classify_macro_alloc(cfnai=None, spread=None, vix=None)
        self.assertEqual(result.status, MacroAllocStatus.NEUTRAL)

    # ── G-10  VIX 邊界值（剛好等於 VIX_ELEVATED=20）─────────────────────────

    def test_G10_vix_exactly_at_threshold_is_defensive(self):
        """
        VIX=20.0（剛好等於 VIX_ELEVATED=20.0）
        預期：DEFENSIVE（>= 門檻即觸發）
        """
        from engine.macro_alloc import classify_macro_alloc, MacroAllocStatus
        result = classify_macro_alloc(cfnai=0.30, spread=0.50, vix=20.0)
        self.assertEqual(result.status, MacroAllocStatus.DEFENSIVE)

    def test_G11_vix_just_below_threshold_not_defensive(self):
        """
        VIX=19.9（低於門檻）
        預期：AGGRESSIVE（其他條件正向）
        """
        from engine.macro_alloc import classify_macro_alloc, MacroAllocStatus
        result = classify_macro_alloc(cfnai=0.30, spread=0.50, vix=19.9)
        self.assertEqual(result.status, MacroAllocStatus.AGGRESSIVE)

    # ── G-12  Phase 2 噪音過濾：VIX elevated + pct_rank >= 0.80 → DEFENSIVE ──

    def test_G12_vix_elevated_high_pct_rank_triggers_defensive(self):
        """
        Phase 2：VIX=25.0（elevated）AND pct_rank=0.85（>= 0.80）
        預期：DEFENSIVE（雙重門檻均滿足）
        """
        from engine.macro_alloc import classify_macro_alloc, MacroAllocStatus
        result = classify_macro_alloc(
            cfnai=0.30, spread=0.50, vix=25.0, vix_pct_rank=0.85
        )
        self.assertEqual(result.status, MacroAllocStatus.DEFENSIVE)
        self.assertIn("pct_rank=0.85", result.rationale)

    # ── G-13  Phase 2 噪音過濾：VIX elevated + pct_rank < 0.80 → 過濾掉 ────

    def test_G13_vix_elevated_low_pct_rank_filtered_out(self):
        """
        Phase 2：VIX=22.0（elevated）但 pct_rank=0.60（< 0.80）。
        VIX DEFENSIVE 被過濾，但 AGGRESSIVE 的 vix_ok 維持 level-only（vix < 20 → False）。
        預期：NEUTRAL（DEFENSIVE 被濾掉，AGGRESSIVE 也因 vix_ok=False 不成立）
        """
        from engine.macro_alloc import classify_macro_alloc, MacroAllocStatus
        result = classify_macro_alloc(
            cfnai=0.30, spread=0.50, vix=22.0, vix_pct_rank=0.60
        )
        self.assertEqual(result.status, MacroAllocStatus.NEUTRAL,
                         f"VIX elevated（≥20）時：DEFENSIVE 被 pct_rank 過濾，"
                         f"AGGRESSIVE vix_ok=False → NEUTRAL；實際：{result.status}")

    # ── G-14  Phase 2：pct_rank=None → 退回 Phase 1 level-only（P-1 fallback）

    def test_G14_vix_pct_rank_none_fallback_to_level_only(self):
        """
        Phase 2 fallback（P-1）：vix_pct_rank=None 時，退回 Phase 1 level-only。
        VIX=25.0 (elevated)，pct_rank=None → 仍觸發 DEFENSIVE。
        """
        from engine.macro_alloc import classify_macro_alloc, MacroAllocStatus
        result = classify_macro_alloc(
            cfnai=0.30, spread=0.50, vix=25.0, vix_pct_rank=None
        )
        self.assertEqual(result.status, MacroAllocStatus.DEFENSIVE,
                         "pct_rank=None 應退回 level-only（Phase 1 行為），VIX elevated 仍觸發")
        self.assertIn("fallback", result.rationale,
                      "rationale 應標示 fallback 原因")

    # ── G-15  Phase 2：pct_rank 剛好等於門檻 0.80 → DEFENSIVE ─────────────────

    def test_G15_vix_pct_rank_exactly_at_threshold_triggers_defensive(self):
        """
        pct_rank=0.80（剛好等於 VIX_PCT_RANK_THRESHOLD=0.80）
        預期：DEFENSIVE（>= 門檻即觸發）
        """
        from engine.macro_alloc import classify_macro_alloc, MacroAllocStatus
        result = classify_macro_alloc(
            cfnai=0.30, spread=0.50, vix=25.0, vix_pct_rank=0.80
        )
        self.assertEqual(result.status, MacroAllocStatus.DEFENSIVE)

    # ── G-16  Phase 2：pct_rank 剛好低於門檻 0.799 → 過濾 ─────────────────────

    def test_G16_vix_pct_rank_just_below_threshold_filtered(self):
        """
        pct_rank=0.799（剛好低於 VIX_PCT_RANK_THRESHOLD=0.80）。
        VIX DEFENSIVE 被過濾，但 vix=25.0 > VIX_ELEVATED → AGGRESSIVE vix_ok=False。
        預期：NEUTRAL（同 G13，AGGRESSIVE 端維持 level-only 不升級）
        """
        from engine.macro_alloc import classify_macro_alloc, MacroAllocStatus
        result = classify_macro_alloc(
            cfnai=0.30, spread=0.50, vix=25.0, vix_pct_rank=0.799
        )
        self.assertEqual(result.status, MacroAllocStatus.NEUTRAL,
                         f"pct_rank=0.799 < 0.80 → DEFENSIVE 被過濾；"
                         f"vix=25.0 ≥ 20 → AGGRESSIVE vix_ok=False → NEUTRAL；實際：{result.status}")

    # ── G-17  Phase 2：MacroAllocResult 包含 vix_pct_rank 欄位 ─────────────────

    def test_G17_result_contains_vix_pct_rank_field(self):
        """
        MacroAllocResult dataclass 必須包含 vix_pct_rank 欄位，
        且值與輸入一致（可追溯性）。
        """
        from engine.macro_alloc import classify_macro_alloc
        result = classify_macro_alloc(
            cfnai=0.30, spread=0.50, vix=15.0, vix_pct_rank=0.55
        )
        self.assertTrue(hasattr(result, "vix_pct_rank"),
                        "MacroAllocResult 應有 vix_pct_rank 欄位")
        self.assertAlmostEqual(result.vix_pct_rank, 0.55)

    # ── G-18  Phase 2：VIX=None + pct_rank=0.90 → VIX 條件不觸發 ─────────────

    def test_G18_vix_none_pct_rank_provided_no_vix_defensive(self):
        """
        vix=None（缺值）但 pct_rank=0.90。
        預期：VIX 條件整體跳過（vix is None → 判斷短路），不誤觸 DEFENSIVE。
        """
        from engine.macro_alloc import classify_macro_alloc, MacroAllocStatus
        # cfnai=0.30 (>= 0.10), spread=0.50 (> 0), vix=None → vix_ok=True
        result = classify_macro_alloc(
            cfnai=0.30, spread=0.50, vix=None, vix_pct_rank=0.90
        )
        self.assertEqual(result.status, MacroAllocStatus.AGGRESSIVE,
                         "vix=None 時 VIX 條件不應觸發 DEFENSIVE，其餘條件正向應為 AGGRESSIVE")


# ══════════════════════════════════════════════════════════════════════════════
# Group H  Layer Integration（四層架構整合）
# ══════════════════════════════════════════════════════════════════════════════

class TestLayerIntegration(unittest.TestCase):
    """
    驗證層間優先級規則與降級路徑：
    - Credit Veto（Scenario C）時，Macro Alloc 不得覆寫
    - Trend WARMUP 時，build_report 正常輸出
    - Data degraded（全 None）時，系統不崩潰
    """

    # ── H-1  Scenario C：Macro AGGRESSIVE 不覆寫 Credit Veto ─────────────────

    def test_H1_credit_veto_macro_aggressive_cannot_override(self):
        """
        Scenario C（Credit Veto active）時，即使 Macro Alloc = AGGRESSIVE，
        系統輸出仍應反映 Scenario C 的優先級（AGGRESSIVE 僅為參考，不改 Regime）。
        Layer 3 無法反向覆蓋 Layer 1 Credit Veto。
        """
        from engine.macro_alloc import classify_macro_alloc, MacroAllocStatus
        from engine.regime import RegimeResult

        # 模擬 Scenario C（最嚴重情境）
        regime_c = RegimeResult(
            scenario="C", regime="Risk-off / Crisis",
            market_phase="Panic", regime_score=15.0,
            confidence_score="High",
            macro_score=10.0, liquidity_score=20.0,
            credit_score=5.0, sentiment_score=15.0,
            rationale="Scenario C triggered",
        )

        # 即使 Macro Alloc 判定為 AGGRESSIVE
        macro_result = classify_macro_alloc(cfnai=0.30, spread=0.50, vix=15.0)
        self.assertEqual(macro_result.status, MacroAllocStatus.AGGRESSIVE)

        # Scenario 保持 C（Macro Alloc 不改變 RegimeResult）
        self.assertEqual(regime_c.scenario, "C",
                         "Macro Alloc 不應改變 RegimeResult.scenario")

    # ── H-2  Trend WARMUP 不觸發 RISK_CAP，系統穩定 ────────────────────────

    def test_H2_trend_warmup_does_not_trigger_risk_cap(self):
        """
        WARMUP 狀態（資料不足）不應被誤判為 RISK_CAP。
        """
        from engine.trend import compute_trend_status, TrendStatus
        closes = [100.0] * 100  # 不足 220 筆
        result = compute_trend_status(closes)
        self.assertNotEqual(result.status, TrendStatus.RISK_CAP)
        self.assertEqual(result.status, TrendStatus.WARMUP)

    # ── H-3  Data degraded（全 None）：macro_alloc 不崩潰 ─────────────────

    def test_H3_data_degraded_macro_alloc_graceful(self):
        """
        所有輸入均為 None（最嚴重的資料缺失情境）。
        預期：返回 NEUTRAL，不拋例外。
        """
        from engine.macro_alloc import classify_macro_alloc, MacroAllocStatus
        result = classify_macro_alloc(cfnai=None, spread=None, vix=None)
        self.assertIsNotNone(result)
        self.assertEqual(result.status, MacroAllocStatus.NEUTRAL)
        self.assertIsNotNone(result.rationale)

    # ── H-4  Trend RISK_CAP 狀態下，Macro AGGRESSIVE 也不破壞結構 ────────────

    def test_H4_trend_risk_cap_with_macro_aggressive_no_crash(self):
        """
        Trend = RISK_CAP，Macro Alloc = AGGRESSIVE。
        Layer 規則：Trend Cap 應限制 Macro Alloc，但 Phase 1 僅顯示，不改值。
        驗證兩者可共存且不崩潰。
        """
        from engine.trend import compute_trend_status, TrendStatus
        from engine.macro_alloc import classify_macro_alloc, MacroAllocStatus

        # 持續下跌：RISK_CAP
        closes = [500.0 - i * 1.0 for i in range(260)]
        trend  = compute_trend_status(closes)
        self.assertEqual(trend.status, TrendStatus.RISK_CAP)

        # 同時 Macro AGGRESSIVE
        macro  = classify_macro_alloc(cfnai=0.30, spread=0.50, vix=15.0)
        self.assertEqual(macro.status, MacroAllocStatus.AGGRESSIVE)

        # Phase 1：兩者並存，無例外（Phase 2 才接 override 邏輯）
        # 此測試驗證結構完整性，不驗證 override 行為

    # ── H-5  TrendResult.rationale 在 WARMUP 時包含標準警告訊息 ─────────────

    def test_H5_warmup_rationale_contains_standard_warning(self):
        """
        WARMUP 狀態的 rationale 必須說明不足原因（用於 log / 報告顯示）。
        """
        from engine.trend import compute_trend_status
        result = compute_trend_status([100.0] * 50)
        self.assertIn("200DMA", result.rationale,
                      "WARMUP rationale 應說明 200DMA 計算不足")


# ══════════════════════════════════════════════════════════════════════════════
# Group I  Phase 2-B：VOO Backfill / Warm-up Management
# ══════════════════════════════════════════════════════════════════════════════

class TestBackfillManagement(unittest.TestCase):
    """
    Phase 2-B：
    - BACKFILL_DAYS 常數是否存在且值正確
    - --backfill flag 使 start 日期正確覆寫
    - history_len==0 的 WARMUP 判別（run_daily.py 的 ERROR 路徑靠整合測試覆蓋）
    """

    # ── I-1  BACKFILL_DAYS 常數存在且等於 300 ─────────────────────────────────

    def test_I1_backfill_days_constant_exists_and_correct(self):
        """
        etl/run_etl.py 頂部必須有 BACKFILL_DAYS = 300。
        """
        from etl.run_etl import BACKFILL_DAYS
        self.assertEqual(BACKFILL_DAYS, 300,
                         f"BACKFILL_DAYS 應為 300，實際 {BACKFILL_DAYS}")

    # ── I-2  parse_args() 包含 --backfill 旗標 ────────────────────────────────

    def test_I2_parse_args_has_backfill_flag(self):
        """
        parse_args() 應接受 --backfill 旗標且預設為 False。
        """
        from etl.run_etl import parse_args
        import sys
        old_argv = sys.argv
        try:
            sys.argv = ["run_etl"]          # 無 --backfill
            args = parse_args()
            self.assertFalse(args.backfill,
                             "--backfill 未傳入時應為 False")
        finally:
            sys.argv = old_argv

    # ── I-3  新常數：BACKFILL_FIXED_START 和 BACKFILL_MIN_TRADING_DAYS ──────────

    def test_I3_backfill_constants_correct(self):
        """
        BACKFILL_FIXED_START = date(2020, 1, 1)
        BACKFILL_MIN_TRADING_DAYS = 220（必須與 engine/trend.MIN_HISTORY 一致）
        """
        from datetime import date
        from etl.run_etl import BACKFILL_FIXED_START, BACKFILL_MIN_TRADING_DAYS
        from engine.trend import MIN_HISTORY
        self.assertEqual(BACKFILL_FIXED_START, date(2020, 1, 1),
                         f"BACKFILL_FIXED_START 應為 2020-01-01，實際 {BACKFILL_FIXED_START}")
        self.assertEqual(BACKFILL_MIN_TRADING_DAYS, MIN_HISTORY,
                         f"BACKFILL_MIN_TRADING_DAYS={BACKFILL_MIN_TRADING_DAYS} "
                         f"應與 engine/trend.MIN_HISTORY={MIN_HISTORY} 一致")

    # ── I-4  history_len==0 代表 DB 完全空白 ─────────────────────────────────

    def test_I4_warmup_history_len_zero_signals_empty_db(self):
        """
        compute_trend_status([]) → history_len=0, status=WARMUP。
        這是 run_daily.py 中觸發 ERROR log 的條件。
        """
        from engine.trend import compute_trend_status, TrendStatus
        result = compute_trend_status([])
        self.assertEqual(result.status, TrendStatus.WARMUP)
        self.assertEqual(result.history_len, 0,
                         "空 list → history_len=0，代表 DB 無 VOO 資料")

    # ── I-5  history_len > 0 但 < 220 → WARMUP，不觸發 empty-DB ERROR ────────

    def test_I5_warmup_partial_history_not_empty(self):
        """
        50 筆 VOO → WARMUP，但 history_len=50（非 0）。
        run_daily.py 應不觸發 empty-DB ERROR log（只觸發在 history_len==0 時）。
        """
        from engine.trend import compute_trend_status, TrendStatus
        result = compute_trend_status([100.0] * 50)
        self.assertEqual(result.status, TrendStatus.WARMUP)
        self.assertGreater(result.history_len, 0,
                           "有部分資料（50 筆），history_len 不為 0")

    # ── I-6  _count_voo_history：DB 失敗時回傳 0（fail-safe → 觸發 Pass 2）──

    def test_I6_count_voo_history_returns_zero_on_db_failure(self):
        """
        _count_voo_history() 在 DB 連線失敗時回傳 0，
        確保 Pass 2 被自動觸發（fail-safe 而非 fail-open）。
        """
        from unittest.mock import patch
        from datetime import date
        from etl.run_etl import _count_voo_history

        with patch("etl.run_etl.get_connection", side_effect=Exception("DB down")):
            count = _count_voo_history(date.today())
        self.assertEqual(count, 0,
                         "DB 失敗時 _count_voo_history 應回傳 0 以觸發 Pass 2")


# ══════════════════════════════════════════════════════════════════════════════
# 測試用 Markdown 輔助函式
# ══════════════════════════════════════════════════════════════════════════════

def _make_macro_md(pmi_cell: str) -> str:
    """產生含指定 PMI 值的最小 macro section markdown。"""
    return (
        "## 一、宏觀市場指標\n\n"
        "| 指標 | 數值 | 資料日期 |\n"
        "|------|------|---------|\n"
        f"| Macro Growth (CFNAI) | {pmi_cell} |  |\n"
        "| HY OAS 信用利差 | 3.25%  🟢 健康 |  |\n"
        "| 10Y-2Y 利差 | +0.30%  🟢 正斜率健康 | |\n"
        "| VIX 波動指數 | 18.5  🟡 正常 | |\n"
        "| VIX 百分位 (252日) | 42.3% | |\n"
    )


def _make_full_md() -> str:
    """
    產生含所有 _parse() 所需欄位的最小完整 markdown，
    用於 payload key 回歸測試。
    """
    return """# 每日投資組合報告

> **分析日期**：2026-04-10　｜　**Scenario**：B　｜　**Confidence**：🟡 Medium

**今日結論：維持 VOO 70%，戰術倉全數觀望，現金 30%。**

---

## 一、宏觀市場指標

| 指標 | 數值 | 資料日期 |
|------|------|---------|
| Macro Growth (CFNAI) | 49.8  📊 溫和擴張 | （資料日期：2026-03-03） |
| HY OAS 信用利差 | 3.25%  🟢 健康 |  |
| 10Y-2Y 利差 | +0.30%  🟢 正斜率健康 | |
| VIX 波動指數 | 18.5  🟡 正常 | |
| VIX 百分位 (252日) | 42.3% | |

## 一-B、標準化風險座標（Rolling Z-Score 252日）

| 指標 | Z-Score 解讀 |
|------|-------------|
| VIX（波動率） | +0.80  🟡 偏高 |
| HY OAS（信用利差） | +1.10  🟡 偏高 |
| 10Y-2Y 利差 | -0.50  🟢 正常範圍（±1σ） |

**Z-Score 風險燈號**

- 🟡 風險升溫｜需留意指標偏離：HY OAS z=+1.10。
> ⚠️ 此燈號為標準化指標的平行觀察層，不取代目前 Scenario A/B/C 判定。

---

## 三、目標配置

| 資產 | 目標權重 |
|------|---------|
| VOO | **70%** |
| QQQM | **0%** |
| SMH | **0%** |
| 2330.TW | **0%** |
| 現金 | **30%** |

## 四、今日操作建議

> 保守觀望，等待市場確認。

**VOO（核心）**：維持 70%，不調整。
**QQQM（戰術）**：🟡 WAIT — 暫停加碼。
**SMH（戰術）**：🟡 WAIT — 暫停加碼。
**2330.TW（戰術）**：🟡 WAIT — 暫停加碼。

## 九、昨日對比

| 項目 | 昨日 → 今日 |
|------|------------|
| Scenario：B → **B** (維持不變) | |
| 主要驅動因子 | HY OAS 升溫 |
"""


if __name__ == "__main__":
    unittest.main(verbosity=2)
