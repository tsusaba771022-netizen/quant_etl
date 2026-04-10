"""
Email 功能最小驗證腳本
-----------------------
用途：在正式排程前驗證兩種 email 是否能成功寄出

測試項目：
  Test 1 — 正常日報 Email（使用現有 output/ 報告）
  Test 2 — 崩潰警告 Email（人工模擬 exception）

執行方式：
  python test_email.py            # 同時執行 Test 1 + Test 2
  python test_email.py --test1    # 只測試正常日報
  python test_email.py --test2    # 只測試崩潰警告
"""
from __future__ import annotations

import argparse
import io
import sys
import traceback
from datetime import date
from pathlib import Path

# Windows UTF-8 fix
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from dotenv import load_dotenv
load_dotenv()

ROOT = Path(__file__).parent

PASS = "[OK]"
FAIL = "[FAIL]"

# ── Test 1：正常日報 Email ──────────────────────────────────────────────────────

def test_normal_report() -> bool:
    """
    找最新一份 daily_report_*.md，直接寄出。
    不需要 DB，不重新產生報告，只測試 send_report() 路徑。
    """
    print("\n" + "=" * 60)
    print("  Test 1：正常日報 Email")
    print("=" * 60)

    # 找 output/ 下最新的 daily_report
    output_dir = ROOT / "output"
    reports = sorted(output_dir.glob("daily_report_*.md"), reverse=True)
    if not reports:
        print(f"  output/ 下無 daily_report_*.md  {FAIL}")
        print(f"  -> 請先執行：python run_daily.py --no-email")
        return False

    report_path = reports[0]
    # 從檔名解析日期
    try:
        date_str = report_path.stem.replace("daily_report_", "")
        report_date = date.fromisoformat(date_str)
    except ValueError:
        report_date = date.today()

    print(f"  使用報告：{report_path.name}  (日期：{report_date})")
    print(f"  檔案大小：{report_path.stat().st_size:,} bytes")

    from report.send_email import send_report, is_already_sent

    # 若已寄過，強制重寄（--force-email 等效）
    if is_already_sent(report_date):
        print(f"  注意：{report_date} 已有寄送記錄，本測試強制重寄")

    print(f"  正在寄送... ", end="", flush=True)
    try:
        ok = send_report(report_path, report_date)
    except Exception as exc:
        print(f"  例外：{exc}  {FAIL}")
        traceback.print_exc()
        return False

    if ok:
        print(f"{PASS}")
        print()
        print(f"  預期收到信件：")
        print(f"    主旨：[量化日報] {report_date} ｜ Scenario X ｜ HY OAS X.XX%")
        print(f"    內文：HTML 格式日報（含宏觀指標、Regime 判定、配置建議）")
        print(f"    附件：{report_path.name}")
    else:
        print(f"{FAIL}")
        print(f"  -> 請檢查 .env 的 SMTP_* 設定，或確認 Gmail App Password 正確")

    return ok


# ── Test 2：崩潰警告 Email ──────────────────────────────────────────────────────

def test_crash_alert() -> bool:
    """
    人工製造一個 exception，直接呼叫 send_crash_alert()。
    模擬 run_daily.py 在 Step 3（Daily Report）發生未捕捉例外的情況。
    """
    print("\n" + "=" * 60)
    print("  Test 2：崩潰警告 Email")
    print("=" * 60)

    # 製造一個有意義的假 traceback
    fake_exc = None
    fake_tb  = None
    try:
        # 模擬 Daily Report 的典型崩潰場景
        raise RuntimeError(
            "[TEST] psycopg2.OperationalError: could not connect to server\n"
            "  Is the server running on host 'localhost' and accepting\n"
            "  TCP/IP connections on port 5432?"
        )
    except RuntimeError as exc:
        fake_exc = exc
        fake_tb  = traceback.format_exc()

    print(f"  模擬崩潰步驟：Step 3：Daily Report")
    print(f"  模擬錯誤類型：RuntimeError（DB 連線失敗）")
    print(f"  正在寄送崩潰警告... ", end="", flush=True)

    from report.send_email import send_crash_alert
    try:
        ok = send_crash_alert(
            failed_step="Step 3：Daily Report",
            exc=fake_exc,
            tb_str=fake_tb,
            run_date=date.today(),
        )
    except Exception as exc:
        print(f"  send_crash_alert 本身例外：{exc}  {FAIL}")
        traceback.print_exc()
        return False

    if ok:
        print(f"{PASS}")
        print()
        print(f"  預期收到信件：")
        print(f"    主旨：[🚨 系統崩潰警告] Quant Daily 執行失敗")
        print(f"    內文：深色背景 HTML，顯示以下資訊：")
        print(f"      - 執行時間：{date.today()}")
        print(f"      - 失敗步驟：Step 3：Daily Report")
        print(f"      - 錯誤類型：RuntimeError")
        print(f"      - 錯誤訊息：[TEST] psycopg2.OperationalError...")
        print(f"      - Traceback（綠色 monospace 字體）")
        print(f"      - 手動重跑指令提示")
    else:
        print(f"{FAIL}")
        print(f"  -> 請檢查 .env 的 SMTP_* 設定")

    return ok


# ── Main ────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Email 功能最小驗證")
    parser.add_argument("--test1", action="store_true", help="只執行 Test 1（正常日報）")
    parser.add_argument("--test2", action="store_true", help="只執行 Test 2（崩潰警告）")
    args = parser.parse_args()

    run_all = not args.test1 and not args.test2

    results = {}

    if run_all or args.test1:
        results["Test 1 正常日報"] = test_normal_report()

    if run_all or args.test2:
        results["Test 2 崩潰警告"] = test_crash_alert()

    # ── 最終摘要 ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  測試結果摘要")
    print("=" * 60)
    for name, ok in results.items():
        print(f"  {name:<20} {PASS if ok else FAIL}")
    print("=" * 60)

    all_ok = all(results.values())
    if all_ok:
        print("  全部通過，Email 系統正常運作。")
    else:
        print("  部分測試失敗，請依上方提示排查。")
    print()

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
