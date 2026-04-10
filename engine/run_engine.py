"""
Macro Engine Entry Point
-------------------------
用法：
    python -m engine.run_engine
    python -m engine.run_engine --date 2025-04-05
    python -m engine.run_engine --no-db    # 只印報告，不寫 DB

寫入表：
    regimes   (UNIQUE on time)
    signals   (UNIQUE on asset_id, time)

報告輸出：output/macro_engine_YYYY-MM-DD.md
"""
import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Dict

from etl.db import get_connection
from .db_writer import write_regime, write_signals
from .regime import RegimeResult, RegimeEngine
from .signals import AssetSignal, SignalEngine
from .snapshot import CORE_ASSET_PROXIES, Snapshot, SnapshotLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("engine")
OUTPUT_DIR = Path(__file__).parent.parent / "output"

SIGNAL_EMOJI = {
    "BUY":      "🟢",
    "WAIT":     "🟡",
    "NO_TRADE": "🔴",
}
STRENGTH_NOTE = {
    "Conviction": "⭐⭐⭐",
    "Main":       "⭐⭐",
    "Scouting":   "⭐",
}
SCENARIO_BANNER = {
    "A":       "✅ Scenario A — 恐慌錯殺，長線加碼視窗",
    "B":       "⚡ Scenario B — 修正但未崩，保守布局",
    "C":       "🚨 Scenario C — 結構性惡化，降低曝險",
    "Neutral": "🟦 Neutral — 市場平靜，持倉觀察",
}


# ── Report ────────────────────────────────────────────────────────────────────

def _build_report(snap: Snapshot, regime: RegimeResult, signals: Dict[str, AssetSignal]) -> str:
    lines = [
        f"# 宏觀引擎報告",
        f"**分析日期**：{snap.as_of}　"
        f"**Confidence**：{regime.confidence_score}　"
        f"**Regime Score**：{regime.regime_score:.1f}/100",
        "",
        f"## {SCENARIO_BANNER.get(regime.scenario, regime.scenario)}",
        f"**Regime**：{regime.regime}　**Phase**：{regime.market_phase}",
        "",
        f"> {regime.rationale}",
        "",
    ]

    # ── 缺失指標警告 ──────────────────────────────────────────────────────────
    if regime.missing:
        lines += ["### ⚠️ 缺失指標（不影響流程，但降低 confidence）", ""]
        for m in regime.missing:
            lines.append(f"- `{m}` = N/A")
        lines += [""]

    # ── 關鍵數據 ──────────────────────────────────────────────────────────────
    lines += [
        "### 關鍵數據",
        "",
        "| 指標 | 數值 | 來源 |",
        "|------|------|------|",
        f"| VIX | {f'{snap.vix:.1f}' if snap.vix is not None else 'N/A'} | raw_market_data |",
        f"| VIX PCT RANK (252d) | {f'{snap.vix_pct_rank:.2f}' if snap.vix_pct_rank is not None else 'N/A'} | derived_indicators |",
        f"| HY OAS | {f'{snap.hy_oas:.2f}%' if snap.hy_oas is not None else 'N/A'} | macro_data |",
        f"| ISM PMI MFG | {f'{snap.ism_pmi:.1f}' if snap.ism_pmi is not None else 'N/A ⚠️'} | macro_data |",
        f"| 10Y-2Y Spread | {f'{snap.spread_10y2y:.2f}%' if snap.spread_10y2y is not None else 'N/A'} | derived_indicators |",
        "",
    ]

    # ── Sub-scores ────────────────────────────────────────────────────────────
    lines += [
        "### 維度得分",
        "",
        "| 維度 | 得分 (0~100) |",
        "|------|------------|",
        f"| Macro（含 PMI proxy） | {regime.macro_score:.1f} |",
        f"| Credit（HY OAS） | {regime.credit_score:.1f} |",
        f"| Liquidity（Yield Curve） | {regime.liquidity_score:.1f} |",
        f"| Sentiment（VIX） | {regime.sentiment_score:.1f} |",
        "",
    ]

    # ── Per-asset signals ─────────────────────────────────────────────────────
    lines += ["---", "## 核心資產訊號", ""]
    for asset, sig in signals.items():
        ad = snap.assets.get(asset)
        emoji    = SIGNAL_EMOJI.get(sig.signal_type, "")
        strength = STRENGTH_NOTE.get(sig.signal_strength, sig.signal_strength)
        lines += [
            f"### {emoji} {asset}　{sig.signal_type} / {sig.signal_strength} {strength}",
            "",
        ]
        if ad:
            close_str  = f"{ad.close:.2f}"  if ad.close  else "N/A"
            sma5_str   = f"{ad.sma_5:.2f}"  if ad.sma_5  else "N/A"
            chg1w_str  = f"{ad.chg_1w_pct:+.2f}%" if ad.chg_1w_pct is not None else "N/A"
            chg1m_str  = f"{ad.chg_1m_pct:+.2f}%" if ad.chg_1m_pct is not None else "N/A"
            fk_str     = "⚠️ 是" if sig.falling_knife else "✅ 否"
            lines += [
                f"| Price | SMA5 | Δ1W | Δ1M | Falling Knife |",
                f"|-------|------|-----|-----|--------------|",
                f"| {close_str} | {sma5_str} | {chg1w_str} | {chg1m_str} | {fk_str} |",
                "",
            ]
        lines += [f"> {sig.rationale}", ""]

    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Macro Engine")
    p.add_argument("--date",  type=date.fromisoformat, default=date.today())
    p.add_argument("--no-db", action="store_true")
    p.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    return p.parse_args()


def main():
    args  = parse_args()
    as_of = args.date
    args.output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("Macro Engine  as_of=%s  no_db=%s", as_of, args.no_db)
    logger.info("=" * 60)

    with get_connection() as conn:
        snap    = SnapshotLoader(conn).load(as_of)
        regime  = RegimeEngine().run(snap)
        signals = SignalEngine().run(snap, regime)

        report  = _build_report(snap, regime, signals)

        # Save report
        out_path = args.output_dir / f"macro_engine_{as_of}.md"
        out_path.write_text(report, encoding="utf-8")
        logger.info("Report saved: %s", out_path)
        print(report)

        # Write to DB（冪等 upsert via engine/db_writer.py）
        if not args.no_db:
            try:
                write_regime(conn, as_of, regime)
                write_signals(conn, as_of, signals, regime)
            except Exception:
                logger.exception("DB write failed (report still saved)")

    logger.info("Engine complete.")


if __name__ == "__main__":
    main()
