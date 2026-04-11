"""
Tests for engine/vix_sm.py
---------------------------
Cases A–H : core FSM transitions (8 cases)
Cases FT1–FT3 : forbidden transition guards (3 cases)
Case S1   : output schema validation (1 case)

Total: 12 tests
"""
import unittest

from engine.vix_sm import (
    VixState,
    VixSmConfig,
    VixSmOutput,
    evaluate_next_state,
)

CFG = VixSmConfig()   # default config (all candidate thresholds)


class TestVixSmCoreCases(unittest.TestCase):
    """Cases A–H: one test per named scenario in the design document."""

    # ── A: Normal → Caution ──────────────────────────────────────────────────

    def test_A_normal_to_caution(self):
        """VIX crosses caution_entry=20 → Normal upgrades to Caution."""
        out = evaluate_next_state(
            VixState.NORMAL, [16.0, 18.0, 21.0], hold_days=3, cfg=CFG
        )
        self.assertEqual(out.state, VixState.CAUTION)
        self.assertTrue(out.transitioned)
        self.assertEqual(out.days_in_state, 1)
        self.assertEqual(out.prev_state, VixState.NORMAL)

    # ── B: Caution holds — hysteresis prevents premature exit ────────────────

    def test_B_caution_hold_hysteresis(self):
        """VIX=19.5 is above caution_exit=18 → hysteresis, stay Caution."""
        out = evaluate_next_state(
            VixState.CAUTION, [21.0, 20.0, 19.5], hold_days=1, cfg=CFG
        )
        self.assertEqual(out.state, VixState.CAUTION)
        self.assertFalse(out.transitioned)
        self.assertEqual(out.days_in_state, 2)  # hold_days + 1

    # ── C: Caution → Normal ──────────────────────────────────────────────────

    def test_C_caution_to_normal(self):
        """VIX drops below caution_exit=18 and min_hold=1 satisfied."""
        out = evaluate_next_state(
            VixState.CAUTION, [21.0, 19.0, 17.0], hold_days=3, cfg=CFG
        )
        self.assertEqual(out.state, VixState.NORMAL)
        self.assertTrue(out.transitioned)

    # ── D: Caution → Defensive ───────────────────────────────────────────────

    def test_D_caution_to_defensive(self):
        """VIX crosses defensive_entry=28."""
        out = evaluate_next_state(
            VixState.CAUTION, [21.0, 24.0, 29.0], hold_days=2, cfg=CFG
        )
        self.assertEqual(out.state, VixState.DEFENSIVE)
        self.assertTrue(out.transitioned)

    # ── E: Defensive → Caution ───────────────────────────────────────────────

    def test_E_defensive_to_caution(self):
        """VIX drops below defensive_exit=24 and min_hold=2 satisfied."""
        out = evaluate_next_state(
            VixState.DEFENSIVE, [30.0, 27.0, 23.0], hold_days=3, cfg=CFG
        )
        self.assertEqual(out.state, VixState.CAUTION)
        self.assertTrue(out.transitioned)

    # ── F: Defensive → Panic (normal threshold) ──────────────────────────────

    def test_F_defensive_to_panic(self):
        """VIX crosses panic_entry=35 from Defensive via normal upgrade."""
        out = evaluate_next_state(
            VixState.DEFENSIVE, [29.0, 32.0, 36.0], hold_days=2, cfg=CFG
        )
        self.assertEqual(out.state, VixState.PANIC)
        self.assertTrue(out.transitioned)

    # ── G: Panic min_hold blocks exit ────────────────────────────────────────

    def test_G_panic_min_hold_blocks_exit(self):
        """VIX=27 < panic_exit=30 but hold_days=2 < panic_min_hold=3 → stay Panic."""
        out = evaluate_next_state(
            VixState.PANIC, [40.0, 35.0, 27.0], hold_days=2, cfg=CFG
        )
        self.assertEqual(out.state, VixState.PANIC)
        self.assertFalse(out.transitioned)
        self.assertIn("min_hold not met", out.reason)

    # ── H: Caution → Panic via fast-track (1d shock) ─────────────────────────

    def test_H_caution_panic_fast_track_shock(self):
        """1d shock = 31 - 21 = 10 >= panic_shock_1d=10 → fast-track to Panic."""
        out = evaluate_next_state(
            VixState.CAUTION, [21.0, 21.0, 31.0], hold_days=2, cfg=CFG
        )
        self.assertEqual(out.state, VixState.PANIC)
        self.assertTrue(out.transitioned)
        self.assertIn("fast-track", out.reason)


class TestVixSmForbiddenTransitions(unittest.TestCase):
    """FT1–FT3: verify the FSM never skips levels."""

    def test_FT1_normal_cannot_skip_to_defensive(self):
        """Normal + VIX=29 → must stop at Caution, not leap to Defensive."""
        out = evaluate_next_state(
            VixState.NORMAL, [16.0, 29.0], hold_days=3, cfg=CFG
        )
        self.assertEqual(out.state, VixState.CAUTION)
        self.assertNotEqual(out.state, VixState.DEFENSIVE)

    def test_FT2_normal_cannot_skip_to_panic(self):
        """Normal + VIX=40, large shock — fast-track MUST NOT apply from Normal."""
        out = evaluate_next_state(
            VixState.NORMAL, [16.0, 40.0], hold_days=3, cfg=CFG
        )
        self.assertEqual(out.state, VixState.CAUTION)
        self.assertNotEqual(out.state, VixState.PANIC)

    def test_FT3_panic_cannot_skip_past_defensive(self):
        """Panic + VIX=17, hold=5 (min_hold met) → Defensive only; NOT Caution/Normal."""
        out = evaluate_next_state(
            VixState.PANIC, [40.0, 30.0, 17.0], hold_days=5, cfg=CFG
        )
        self.assertEqual(out.state, VixState.DEFENSIVE)
        self.assertNotIn(out.state, (VixState.CAUTION, VixState.NORMAL))


class TestVixSmOutputSchema(unittest.TestCase):
    """S1: VixSmOutput must satisfy the required field contract."""

    def test_S1_output_schema_required_fields(self):
        """
        evaluate_next_state() output must contain:
          state (str), transitioned (bool), reason (str), days_in_state (int)
        and meet basic validity invariants.
        """
        out = evaluate_next_state(
            VixState.NORMAL, [15.0, 25.0], hold_days=1, cfg=CFG
        )

        # Field presence and types
        self.assertIsInstance(out, VixSmOutput)
        self.assertIsInstance(out.state, str)
        self.assertIsInstance(out.transitioned, bool)
        self.assertIsInstance(out.reason, str)
        self.assertIsInstance(out.days_in_state, int)
        self.assertIsInstance(out.prev_state, str)
        self.assertIsInstance(out.vix_enum, str)

        # State must be a valid VixState
        self.assertIn(out.state, VixState.ALL)
        self.assertIn(out.prev_state, VixState.ALL)

        # vix_enum must be one of the four display values
        self.assertIn(out.vix_enum, ("[Low]", "[Normal]", "[Warning]", "[Confirmed]"))

        # reason must be non-empty
        self.assertGreater(len(out.reason), 0)

        # days_in_state must be >= 1
        self.assertGreaterEqual(out.days_in_state, 1)

        # days_in_state consistency: transitioned → 1
        if out.transitioned:
            self.assertEqual(out.days_in_state, 1)


if __name__ == "__main__":
    unittest.main()
