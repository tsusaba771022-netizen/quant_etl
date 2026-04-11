"""
每日報告 Email 發送模組
-----------------------
使用 Gmail SMTP (TLS port 587) 將 daily_report_YYYY-MM-DD.md
轉為 HTML 內文發送，並附上原始 .md 檔案。

所有敏感資訊從 .env 讀取：
  SMTP_HOST        e.g. smtp.gmail.com
  SMTP_PORT        e.g. 587
  SMTP_USER        Gmail 地址
  SMTP_PASSWORD    Gmail App Password（非登入密碼）
  REPORT_TO_EMAIL  收件者（可逗號分隔多個）

Gmail App Password 設定：
  Google 帳戶 → 安全性 → 兩步驟驗證（需開啟）
  → 應用程式密碼 → 建立 → 複製 16 碼貼入 .env

用法（直接呼叫）：
  python -m report.send_email --date 2026-04-05
  python -m report.send_email  # 預設今日
"""
from __future__ import annotations

import argparse
import html as html_lib
import logging
import os
import re
import smtplib
import sys
from datetime import date, datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional

import markdown
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

OUTPUT_DIR      = Path(__file__).parent.parent / "output"
SENT_MARKER_DIR = Path(__file__).parent.parent / "logs"


def _marker_path(report_date: date) -> Path:
    return SENT_MARKER_DIR / f"email_sent_{report_date}.flag"


def is_already_sent(report_date: date) -> bool:
    """回傳 True 表示當日報告已成功寄送過（marker file 存在）。"""
    return _marker_path(report_date).exists()


def _write_sent_marker(report_date: date) -> None:
    try:
        SENT_MARKER_DIR.mkdir(exist_ok=True)
        _marker_path(report_date).touch()
    except Exception:
        pass  # 寫 marker 失敗不中斷主流程


# ── Scenario 主旨標籤 ──────────────────────────────────────────────────────────
_SCENARIO_LABEL = {
    "A": "✅ Scenario A",
    "B": "⚡ Scenario B",
    "C": "🚨 Scenario C",
    "Neutral": "🟦 Neutral",
}

# ── HTML 樣式（inline CSS，相容大多數 email client）────────────────────────────
_HTML_CSS = """
<style>
  body      { font-family: -apple-system, Arial, sans-serif; font-size: 14px;
               color: #1a1a1a; background: #f5f5f5; margin: 0; padding: 20px; }
  .card     { background: #ffffff; border-radius: 8px; padding: 24px 32px;
               max-width: 800px; margin: 0 auto;
               box-shadow: 0 1px 4px rgba(0,0,0,.12); }
  h1        { font-size: 20px; color: #0d1117; border-bottom: 2px solid #e1e4e8;
               padding-bottom: 8px; }
  h2        { font-size: 16px; color: #24292e; margin-top: 24px;
               border-left: 4px solid #0366d6; padding-left: 10px; }
  h3        { font-size: 14px; color: #444d56; margin-top: 16px; }
  table     { border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 13px; }
  th        { background: #0366d6; color: #fff; padding: 8px 12px; text-align: left; }
  td        { padding: 7px 12px; border-bottom: 1px solid #e1e4e8; }
  tr:nth-child(even) td { background: #f6f8fa; }
  blockquote{ border-left: 4px solid #e1e4e8; margin: 8px 0;
               padding: 8px 16px; color: #6a737d; background: #fafbfc; }
  code      { background: #f3f4f6; padding: 2px 5px; border-radius: 3px;
               font-family: monospace; font-size: 12px; }
  .badge-c  { display:inline-block; background:#d73a49; color:#fff;
               padding:3px 10px; border-radius:12px; font-weight:bold; }
  .badge-b  { display:inline-block; background:#f6a623; color:#fff;
               padding:3px 10px; border-radius:12px; font-weight:bold; }
  .badge-a  { display:inline-block; background:#28a745; color:#fff;
               padding:3px 10px; border-radius:12px; font-weight:bold; }
  .badge-n  { display:inline-block; background:#0366d6; color:#fff;
               padding:3px 10px; border-radius:12px; font-weight:bold; }
  hr        { border: none; border-top: 1px solid #e1e4e8; margin: 20px 0; }
  p         { line-height: 1.6; }
  ul, li    { line-height: 1.7; }
  .footer   { font-size: 12px; color: #959da5; margin-top: 20px;
               border-top: 1px solid #e1e4e8; padding-top: 12px; }
</style>
"""


def _extract_scenario(md_text: str) -> str:
    """從報告文字中擷取 Scenario（A/B/C/Neutral）。"""
    m = re.search(r"\*\*Scenario\*\*：(\w+)", md_text)
    return m.group(1) if m else "?"


def _extract_key_metrics(md_text: str) -> str:
    """擷取 HY OAS 數值，用於 email 主旨。"""
    m = re.search(r"HY OAS 信用利差 \| ([\d.]+%)", md_text)
    return f"HY OAS {m.group(1)}" if m else ""


def _md_to_html(md_text: str) -> str:
    """Markdown → HTML，加上 inline CSS 與外框卡片。"""
    body_html = markdown.markdown(
        md_text,
        extensions=["tables", "fenced_code"],
    )
    return f"""<!DOCTYPE html>
<html lang="zh-TW">
<head><meta charset="utf-8">{_HTML_CSS}</head>
<body>
  <div class="card">
    {body_html}
    <div class="footer">
      本報告由 quant_etl Baseline v1.0 自動生成 ·
      策略規則：Scenario C = HY_OAS &gt; 7.0% AND VIX &gt; 20，或 CFNAI &lt; -0.70
    </div>
  </div>
</body>
</html>"""


def build_subject(report_date: date, scenario: str, key_metric: str) -> str:
    label = _SCENARIO_LABEL.get(scenario, f"Scenario {scenario}")
    metric_part = f" ｜ {key_metric}" if key_metric else ""
    return f"[量化日報] {report_date} ｜ {label}{metric_part}"


def send_report(
    report_path: Path,
    report_date: Optional[date] = None,
) -> bool:
    """
    讀取 report_path 的 .md 檔，轉為 HTML 發送。
    回傳 True = 成功，False = 失敗。
    """
    # ── 讀取 .env 設定 ────────────────────────────────────────────────────────
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASSWORD", "")
    to_raw    = os.getenv("REPORT_TO_EMAIL", "")

    if not smtp_user or not smtp_pass:
        logger.error("SMTP_USER / SMTP_PASSWORD 未設定，請檢查 .env")
        return False
    if not to_raw:
        logger.error("REPORT_TO_EMAIL 未設定，請檢查 .env")
        return False

    to_list = [addr.strip() for addr in to_raw.split(",") if addr.strip()]

    # ── 讀取報告 ──────────────────────────────────────────────────────────────
    if not report_path.exists():
        logger.error("找不到報告檔案：%s", report_path)
        return False

    md_text  = report_path.read_text(encoding="utf-8")
    scenario = _extract_scenario(md_text)
    metric   = _extract_key_metrics(md_text)
    subject  = build_subject(report_date or date.today(), scenario, metric)
    html     = _md_to_html(md_text)

    # ── 組裝 MIME ─────────────────────────────────────────────────────────────
    msg = MIMEMultipart("mixed")
    msg["From"]    = smtp_user
    msg["To"]      = ", ".join(to_list)
    msg["Subject"] = subject

    # 1. HTML 內文
    msg.attach(MIMEText(html, "html", "utf-8"))

    # 2. .md 附件
    att = MIMEApplication(
        md_text.encode("utf-8"),
        Name=report_path.name,
    )
    att["Content-Disposition"] = f'attachment; filename="{report_path.name}"'
    msg.attach(att)

    # ── 發送 ──────────────────────────────────────────────────────────────────
    logger.info("寄送報告：%s → %s", subject, to_list)
    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, to_list, msg.as_string())
        logger.info("EMAIL_SENT  date=%s  to=%s  subject=%s",
                    report_date or date.today(), ", ".join(to_list), subject)
        _write_sent_marker(report_date or date.today())
        return True
    except smtplib.SMTPAuthenticationError as exc:
        logger.error("EMAIL_FAILED  date=%s  error=SMTPAuthenticationError  hint=%s",
                     report_date or date.today(),
                     "請確認 Gmail App Password 正確，且已開啟「兩步驟驗證→應用程式密碼」")
    except smtplib.SMTPException as exc:
        logger.error("EMAIL_FAILED  date=%s  error=SMTPException  detail=%s",
                     report_date or date.today(), exc)
    except OSError as exc:
        logger.error("EMAIL_FAILED  date=%s  error=NetworkError  detail=%s",
                     report_date or date.today(), exc)
    return False


# ── 崩潰警告 Email ────────────────────────────────────────────────────────────

def send_crash_alert(
    failed_step: str,
    exc: BaseException,
    tb_str: str,
    run_date: Optional[date] = None,
) -> bool:
    """
    發送系統崩潰警告 Email。

    主旨：[🚨 系統崩潰警告] Quant Daily 執行失敗
    內容：執行時間、失敗步驟、錯誤類型、Traceback 摘要

    若 SMTP 設定不完整 → 記錄完整 log（不拋例外，不吞 traceback）
    若 SMTP 連線失敗  → 記錄完整 log（同上）
    """
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASSWORD", "")
    to_raw    = os.getenv("REPORT_TO_EMAIL", "")

    report_date = run_date or date.today()
    now_str     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    exc_type    = type(exc).__name__
    exc_msg     = str(exc)
    # 截取最後 3000 字元（email 體積限制）
    tb_tail     = tb_str[-3000:] if len(tb_str) > 3000 else tb_str

    # 無論 SMTP 是否成功，先把完整資訊寫進 log
    logger.critical(
        "[CRASH] step=%s  type=%s  msg=%s", failed_step, exc_type, exc_msg
    )
    logger.critical("[CRASH] Traceback:\n%s", tb_str)

    if not smtp_user or not smtp_pass or not to_raw:
        logger.error(
            "[CRASH_ALERT] SMTP 未設定（SMTP_USER/SMTP_PASSWORD/REPORT_TO_EMAIL），"
            "崩潰警告 email 無法發送。請手動檢查 logs/daily_%s.log。",
            report_date,
        )
        return False

    to_list = [addr.strip() for addr in to_raw.split(",") if addr.strip()]
    subject  = "[🚨 系統崩潰警告] Quant Daily 執行失敗"

    # ── HTML 內容（全部 inline style，相容 Gmail 過濾 <style> 的問題）────────
    tb_escaped  = html_lib.escape(tb_tail)
    exc_escaped = html_lib.escape(exc_msg)

    # inline style 常數
    _S_BODY  = 'font-family:-apple-system,Arial,sans-serif;background:#1a1a1a;color:#e8e8e8;padding:20px;margin:0;'
    _S_CARD  = 'background:#2d2d2d;border-left:5px solid #d73a49;padding:16px 24px;max-width:800px;border-radius:6px;'
    _S_H2    = 'color:#d73a49;margin-top:0;font-size:18px;'
    _S_H3    = 'color:#d1d5da;font-size:14px;margin-bottom:6px;'
    _S_TABLE = 'border-collapse:collapse;width:100%;margin:12px 0;'
    _S_TD_L  = 'padding:6px 12px;border-bottom:1px solid #444;color:#959da5;white-space:nowrap;width:90px;vertical-align:top;'
    _S_TD_R  = 'padding:6px 12px;border-bottom:1px solid #444;color:#e8e8e8;font-weight:bold;font-family:monospace;word-break:break-all;'
    _S_TB    = 'background:#111;padding:14px;font-size:12px;color:#85e89d;white-space:pre-wrap;overflow-x:auto;border-radius:4px;font-family:monospace;'
    _S_HINT  = 'color:#959da5;font-size:12px;margin-top:16px;'
    _S_CODE  = 'background:#333;padding:2px 5px;border-radius:3px;font-family:monospace;'

    body_html = (
        f'<!DOCTYPE html><html lang="zh-TW"><head><meta charset="utf-8"></head>'
        f'<body style="{_S_BODY}">'
        f'<div style="{_S_CARD}">'
        f'<h2 style="{_S_H2}">&#128680; Quant Daily &#8212; 系統崩潰警告</h2>'
        f'<table style="{_S_TABLE}">'
        f'<tr><td style="{_S_TD_L}">執行時間</td><td style="{_S_TD_R}">{now_str}</td></tr>'
        f'<tr><td style="{_S_TD_L}">報告日期</td><td style="{_S_TD_R}">{report_date}</td></tr>'
        f'<tr><td style="{_S_TD_L}">失敗步驟</td><td style="{_S_TD_R}">{html_lib.escape(failed_step)}</td></tr>'
        f'<tr><td style="{_S_TD_L}">錯誤類型</td><td style="{_S_TD_R}">{exc_type}</td></tr>'
        f'<tr><td style="{_S_TD_L}">錯誤訊息</td><td style="{_S_TD_R}">{exc_escaped}</td></tr>'
        f'</table>'
        f'<h3 style="{_S_H3}">Traceback（最後 3000 字元）</h3>'
        f'<div style="{_S_TB}">{tb_escaped}</div>'
        f'<p style="{_S_HINT}">'
        f'完整 log：<code style="{_S_CODE}">logs/daily_{report_date}.log</code><br>'
        f'手動重跑：<code style="{_S_CODE}">python run_daily.py --date {report_date}</code>'
        f'</p>'
        f'</div></body></html>'
    )

    msg = MIMEMultipart("mixed")
    msg["From"]    = smtp_user
    msg["To"]      = ", ".join(to_list)
    msg["Subject"] = subject
    msg.attach(MIMEText(body_html, "html", "utf-8"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, to_list, msg.as_string())
        logger.info(
            "[CRASH_ALERT] 崩潰警告 email 已發送  step=%s  to=%s",
            failed_step, to_list,
        )
        return True
    except smtplib.SMTPAuthenticationError:
        logger.error(
            "[CRASH_ALERT] SMTP 認證失敗。請確認 Gmail App Password 正確。"
            "崩潰詳情已記錄於 log。"
        )
    except smtplib.SMTPException as smtp_exc:
        logger.error("[CRASH_ALERT] SMTP 發送失敗：%s。崩潰詳情已記錄於 log。", smtp_exc)
    except OSError as net_exc:
        logger.error("[CRASH_ALERT] 網路錯誤：%s。崩潰詳情已記錄於 log。", net_exc)
    return False


# ── CLI（直接測試用）─────────────────────────────────────────────────────────

def _parse_args():
    p = argparse.ArgumentParser(description="發送每日報告 Email")
    p.add_argument("--date", type=date.fromisoformat, default=date.today(),
                   help="報告日期（預設今日）")
    p.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    return p.parse_args()


def main():
    import io
    if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    args = _parse_args()
    path = args.output_dir / f"daily_report_{args.date}.md"
    ok   = send_report(path, args.date)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
