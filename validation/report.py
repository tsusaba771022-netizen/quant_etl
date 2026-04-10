"""
Validation Report Builder
--------------------------
輸入：List[CheckResult]
輸出：
  - Markdown 字串（print 或存檔）
  - ValidationSummary（結構化彙總，供程式使用）

PASS / FAIL 邏輯：
  - 任何 FAIL 結果 → 整體 FAIL
  - WARN 結果 >= 3 → 整體 WARN
  - 其餘 → PASS
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from .checks import CheckResult, STATUS_ORDER


# ── Summary ───────────────────────────────────────────────────────────────────

@dataclass
class ValidationSummary:
    total_checks:  int
    n_pass:        int
    n_warn:        int
    n_fail:        int
    n_info:        int
    verdict:       str          # PASS | WARN | FAIL
    verdict_reason: str
    failed_checks: List[str]
    warned_checks: List[str]

    @property
    def can_backtest(self) -> bool:
        return self.verdict != "FAIL"

    @property
    def emoji(self) -> str:
        return {"PASS": "✅", "WARN": "⚠️", "FAIL": "🔴"}.get(self.verdict, "")


def summarize(results: List[CheckResult]) -> ValidationSummary:
    counts = {"PASS": 0, "WARN": 0, "FAIL": 0, "INFO": 0}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1

    failed  = [r.name for r in results if r.status == "FAIL"]
    warned  = [r.name for r in results if r.status == "WARN"]

    if failed:
        verdict = "FAIL"
        reason  = f"{len(failed)} 個嚴重問題需要修正才能進入回測"
    elif len(warned) >= 3:
        verdict = "WARN"
        reason  = f"{len(warned)} 個警告，建議修正後再回測"
    else:
        verdict = "PASS"
        reason  = "所有關鍵檢查通過，可進入回測"

    return ValidationSummary(
        total_checks  = len(results),
        n_pass        = counts["PASS"],
        n_warn        = counts["WARN"],
        n_fail        = counts["FAIL"],
        n_info        = counts["INFO"],
        verdict       = verdict,
        verdict_reason= reason,
        failed_checks = failed,
        warned_checks = warned,
    )


# ── Reporter ──────────────────────────────────────────────────────────────────

class Reporter:

    VERDICT_BANNER = {
        "PASS": "✅ **PASS — 可進入回測**",
        "WARN": "⚠️ **WARN — 建議修正警告後再回測**",
        "FAIL": "🔴 **FAIL — 必須修正以下問題後才能回測**",
    }

    # 群組標題對應（check name 前綴）
    GROUPS = [
        ("raw_market_data",       "1｜raw_market_data 行情資料"),
        ("macro_data",            "2｜macro_data 總經資料"),
        ("derived_indicators",    "3｜derived_indicators 衍生指標"),
        ("engine_regime",         "4｜engine_regime_log 市場 Regime"),
        ("engine_signal",         "4｜engine_signals 訊號"),
        ("time_alignment",        "5｜時間對齊驗證"),
    ]

    def generate(
        self,
        results: List[CheckResult],
        summary: ValidationSummary,
        as_of: Optional[str] = None,
        date_range: Optional[str] = None,
    ) -> str:
        ts  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        aof = as_of or ts

        lines: List[str] = []

        # ── Header ─────────────────────────────────────────────────────────────
        lines += [
            "# Pre-Backtest Validation Report",
            "",
            f"**生成時間**：{ts}",
            f"**分析基準日**：{aof}",
        ]
        if date_range:
            lines += [f"**資料範圍**：{date_range}"]

        lines += [
            "",
            "---",
            "## 📋 驗證總覽",
            "",
            f"| 項目 | 數值 |",
            f"|------|------|",
            f"| 總檢查數 | {summary.total_checks} |",
            f"| ✅ PASS | {summary.n_pass} |",
            f"| ⚠️  WARN | {summary.n_warn} |",
            f"| 🔴 FAIL | {summary.n_fail} |",
            f"| ℹ️  INFO | {summary.n_info} |",
            "",
            f"### {self.VERDICT_BANNER[summary.verdict]}",
            f"> {summary.verdict_reason}",
            "",
        ]

        # ── Failed / Warned 快速清單 ───────────────────────────────────────────
        if summary.failed_checks:
            lines += ["**🔴 需修正的 FAIL 項目：**"]
            for name in summary.failed_checks:
                lines += [f"- `{name}`"]
            lines += [""]

        if summary.warned_checks:
            lines += ["**⚠️  需注意的 WARN 項目：**"]
            for name in summary.warned_checks:
                lines += [f"- `{name}`"]
            lines += [""]

        # ── 詳細結果（依群組分段） ─────────────────────────────────────────────
        lines += ["---", "## 🔍 詳細驗證結果", ""]

        for prefix, title in self.GROUPS:
            group = [r for r in results if r.name.startswith(prefix)]
            if not group:
                continue
            lines += [f"### {title}", ""]
            lines += [
                "| 狀態 | 檢查項目 | 說明 |",
                "|------|---------|------|",
            ]
            for r in sorted(group, key=lambda x: STATUS_ORDER.get(x.status, 9)):
                msg = r.message.replace("|", "\\|")
                lines.append(f"| {r.emoji} {r.status} | `{r.name}` | {msg} |")
            lines += [""]

        # ── 未分類（保底） ─────────────────────────────────────────────────────
        classified = {
            r.name for prefix, _ in self.GROUPS
            for r in results if r.name.startswith(prefix)
        }
        others = [r for r in results if r.name not in classified]
        if others:
            lines += ["### 其他", ""]
            lines += [
                "| 狀態 | 檢查項目 | 說明 |",
                "|------|---------|------|",
            ]
            for r in others:
                msg = r.message.replace("|", "\\|")
                lines.append(f"| {r.emoji} {r.status} | `{r.name}` | {msg} |")
            lines += [""]

        # ── 補全建議 ──────────────────────────────────────────────────────────
        lines += self._repair_guide(summary)

        return "\n".join(lines)

    def _repair_guide(self, summary: ValidationSummary) -> List[str]:
        if summary.verdict == "PASS" and not summary.warned_checks:
            return []

        guide = ["---", "## 🔧 修復指引", ""]

        # 按問題類型給出對應指令
        by_type = {
            "raw_market_data":    [],
            "macro_data":         [],
            "derived_indicators": [],
            "engine":             [],
            "time_alignment":     [],
        }
        for name in summary.failed_checks + summary.warned_checks:
            for prefix in by_type:
                if name.startswith(prefix):
                    by_type[prefix].append(name)
                    break

        if by_type["raw_market_data"] or by_type["macro_data"]:
            guide += [
                "**行情 / 總經資料缺失** → 重新執行 ETL：",
                "```bash",
                "python -m etl.run_etl --start 2015-01-01",
                "```",
                "",
            ]
        if by_type["derived_indicators"]:
            guide += [
                "**衍生指標未計算** → 重新執行 indicators 模組：",
                "```bash",
                "python -m indicators.run_indicators --start 2015-01-01",
                "```",
                "",
            ]
        if by_type["engine"]:
            guide += [
                "**Regime / Signals 未產生** → 執行 engine（非 backtest 必要條件，但建議完成）：",
                "```bash",
                "python -m engine.run_engine",
                "```",
                "",
            ]
        if by_type["time_alignment"]:
            guide += [
                "**時間對齊問題** → 檢查 macro_data 是否有足夠歷史，"
                "並確認 FRED ETL 正確寫入 ISM_PMI_MFG / HY_OAS：",
                "```bash",
                "python -m etl.run_etl --macro-only --start 2015-01-01",
                "```",
                "",
            ]

        return guide
