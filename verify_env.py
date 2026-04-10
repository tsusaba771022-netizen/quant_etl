"""
環境驗證腳本
------------
執行方式：
    python verify_env.py           # 完整驗證（含 DB 連線測試）
    python verify_env.py --no-db   # 僅驗證套件，跳過 DB

預期結果：所有項目顯示 OK，最後輸出「環境驗證通過」
"""
from __future__ import annotations

import argparse
import importlib
import importlib.metadata
import io
import sys
from typing import List, Tuple

# Windows UTF-8 終端機修正（避免 UnicodeEncodeError）
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ── 必要套件清單 ────────────────────────────────────────────────────────────────
# (import_name, pip_package_name, min_version)
REQUIRED: List[Tuple[str, str, str]] = [
    ("pandas",      "pandas",          "2.0.0"),
    ("numpy",       "numpy",           "1.24.0"),
    ("yfinance",    "yfinance",        "0.2.28"),
    ("fredapi",     "fredapi",         "0.5.1"),
    ("psycopg2",    "psycopg2-binary", "2.9.0"),
    ("dotenv",      "python-dotenv",   "1.0.0"),
    ("markdown",    "Markdown",        "3.5.0"),
]

PASS_MARK = "[OK]"
FAIL_MARK = "[FAIL]"
WARN_MARK = "[WARN]"


def _ver_tuple(ver_str: str) -> tuple:
    try:
        return tuple(int(x) for x in str(ver_str).split(".")[:3])
    except Exception:
        return (0,)


def _get_version(import_name: str, pip_name: str) -> str:
    """取得套件版本，優先用 importlib.metadata（比 __version__ 更可靠）。"""
    try:
        return importlib.metadata.version(pip_name)
    except importlib.metadata.PackageNotFoundError:
        pass
    try:
        mod = importlib.import_module(import_name)
        ver = getattr(mod, "__version__", None)
        if ver:
            return str(ver).split()[0]
    except ImportError:
        pass
    return None


def check_packages() -> bool:
    print("\n[ 1 / 3 ]  套件版本檢查")
    print(f"  {'套件':<20} {'已安裝':<14} {'最低需求':<14} 狀態")
    print(f"  {'-'*20} {'-'*14} {'-'*14} {'-'*6}")
    all_ok = True
    for import_name, pip_name, min_ver in REQUIRED:
        ver = _get_version(import_name, pip_name)
        if ver is None:
            print(f"  {pip_name:<20} {'(未安裝)':<14} {min_ver:<14} {FAIL_MARK}")
            all_ok = False
            continue
        ok = _ver_tuple(ver) >= _ver_tuple(min_ver)
        status = PASS_MARK if ok else WARN_MARK
        if not ok:
            all_ok = False
        print(f"  {pip_name:<20} {ver:<14} {min_ver:<14} {status}")
    return all_ok


def check_dotenv() -> bool:
    print("\n[ 2 / 3 ]  .env 檔案與設定檢查")
    from pathlib import Path
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        print(f"  .env 不存在  {FAIL_MARK}")
        print(f"  -> 請複製 .env.example 並填入設定：")
        print(f"     copy .env.example .env")
        return False
    from dotenv import dotenv_values
    cfg = dotenv_values(env_path)
    required_keys = [
        "PG_HOST", "PG_PORT", "PG_DBNAME", "PG_USER",
        "FRED_API_KEY", "SMTP_USER", "SMTP_PASSWORD", "REPORT_TO_EMAIL",
    ]
    all_ok = True
    for key in required_keys:
        val = cfg.get(key, "")
        if not val or "your_" in val.lower():
            status = WARN_MARK
            note = "（未設定或仍為範本值）"
            all_ok = False
        else:
            status = PASS_MARK
            note = ""
        print(f"  {key:<25} {status} {note}")
    return all_ok


def check_db() -> bool:
    print("\n[ 3 / 3 ]  PostgreSQL 連線測試")
    try:
        from dotenv import load_dotenv
        load_dotenv()
        from etl.db import get_connection
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                ver = cur.fetchone()[0]
        print(f"  連線成功  {PASS_MARK}")
        print(f"  PostgreSQL: {ver[:70]}")
        return True
    except Exception as exc:
        print(f"  連線失敗  {FAIL_MARK}")
        print(f"  錯誤：{exc}")
        print(f"  -> 確認 PostgreSQL 已啟動，並檢查 .env 的 PG_* 設定")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="quant_etl 環境驗證")
    parser.add_argument("--no-db", action="store_true", help="跳過 DB 連線測試")
    args = parser.parse_args()

    print("=" * 60)
    print("  quant_etl 環境驗證")
    print(f"  Python: {sys.version.split()[0]}  (需要 3.10+)")
    print("=" * 60)

    pkg_ok = check_packages()
    env_ok = check_dotenv()
    db_ok  = check_db() if not args.no_db else None

    print("\n" + "=" * 60)
    results = [("套件安裝", pkg_ok), (".env 設定", env_ok)]
    if db_ok is not None:
        results.append(("DB 連線", db_ok))

    all_pass = all(v for _, v in results)
    for label, ok in results:
        mark = "通過" if ok else "問題"
        print(f"  {label:<12} {PASS_MARK if ok else FAIL_MARK} {mark}")

    print("=" * 60)
    if all_pass:
        print("  環境驗證通過！可執行：python run_daily.py")
    else:
        print("  環境驗證未完全通過，請依上方提示修正後重新執行。")
    print("=" * 60)
    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
