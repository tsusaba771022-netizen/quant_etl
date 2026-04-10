"""
LINE Webhook 處理模組 — 關鍵字查詢模式
---------------------------------------
接收 LINE platform 發來的 webhook event，
針對特定關鍵字自動回覆最新量化日報（Flex 優先，fallback 純文字）。

公開介面：
  handle_webhook(body_bytes, x_line_signature) -> tuple[str, int]
  返回 (response_body, http_status_code)，直接傳給 Flask return。

環境變數：
  LINE_CHANNEL_SECRET           Channel Secret（用於 webhook signature 驗證）
  LINE_CHANNEL_ACCESS_TOKEN     Channel Access Token（用於 reply API）
  LINE_WEBHOOK_KEYWORDS         逗號分隔關鍵字（選填，覆蓋預設清單）

Log 事件：
  LINE_REPLY_FLEX_SENT            成功以 Flex 回覆
  LINE_REPLY_FLEX_FAILED          Flex 回覆失敗（含原因）
  LINE_REPLY_TEXT_FALLBACK_SENT   Flex 失敗後 fallback 純文字成功
  LINE_REPLY_FAILED               兩種格式皆失敗
  LINE_KEYWORD_TRIGGERED          觸發關鍵字（含關鍵字與 user_id）
  LINE_WEBHOOK_REJECTED           驗證失敗
  LINE_WEBHOOK_EVENT_ERROR        單一 event 處理例外
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import re
from datetime import date
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

from report.line_flex import build_line_flex_payload
from report.send_line import build_line_message

load_dotenv()

logger = logging.getLogger(__name__)

LINE_REPLY_URL = "https://api.line.me/v2/bot/message/reply"

_OUTPUT_DIR = Path(__file__).parent.parent / "output"

# 日報觸發關鍵字
_DEFAULT_KEYWORDS: frozenset[str] = frozenset({
    "報告", "今日", "戰報", "report", "risk",
})

# 選單觸發關鍵字
_HELP_KEYWORDS: frozenset[str] = frozenset({
    "help", "幫助", "menu", "選單",
})

_HINT_TEXT = "輸入「報告」看最新日報，輸入「選單」查看所有指令。"

# ── 選單 ──────────────────────────────────────────────────────────────────────

_MENU_ALT = "AXIOM QUANT 指令選單"

def _build_menu_flex() -> dict:
    """回傳選單 Flex Message bubble。"""
    C_BG    = "#0B0F1A"
    C_HDR   = "#1565C0"
    C_LABEL = "#4D7A8C"
    C_VALUE = "#C8D8E8"
    C_DIM   = "#182535"
    C_WHITE = "#FFFFFF"

    def _t(text, **kw):
        return {"type": "text", "text": str(text), **kw}

    def _sep():
        return {"type": "box", "layout": "vertical", "contents": [],
                "height": "1px", "backgroundColor": C_DIM, "margin": "lg"}

    def _row(label, value):
        return {
            "type": "box", "layout": "horizontal",
            "paddingTop": "xs", "paddingBottom": "xs",
            "contents": [
                _t(label, color=C_LABEL, size="sm", flex=4),
                _t(value, color=C_VALUE, size="sm", flex=5, align="end", weight="bold"),
            ],
        }

    bubble = {
        "type": "bubble",
        "size": "mega",
        "styles": {
            "header": {"backgroundColor": C_HDR},
            "body":   {"backgroundColor": C_BG},
        },
        "header": {
            "type": "box", "layout": "horizontal",
            "paddingAll": "md",
            "contents": [
                _t("AXIOM QUANT", color=C_WHITE, weight="bold", size="sm", flex=5),
                _t("指令選單",     color="#FFFFFF99", size="xs", flex=4, align="end"),
            ],
        },
        "body": {
            "type": "box", "layout": "vertical", "paddingAll": "md",
            "contents": [
                # 區塊 1 — 量化日報
                _t("DAILY REPORT", color=C_LABEL, size="xxs", weight="bold"),
                {
                    "type": "box", "layout": "vertical", "margin": "sm",
                    "contents": [
                        _row("報告 ／ 今日 ／ 戰報", "查看最新日報"),
                        _row("report ／ risk",      "查看最新日報"),
                    ],
                },
                _sep(),
                # 區塊 2 — 選單
                _t("MENU", color=C_LABEL, size="xxs", weight="bold", margin="md"),
                {
                    "type": "box", "layout": "vertical", "margin": "sm",
                    "contents": [
                        _row("選單 ／ 幫助", "顯示本選單"),
                        _row("help ／ menu", "顯示本選單"),
                    ],
                },
                _sep(),
                # 說明
                _t("其他輸入均會顯示此說明。",
                   color="#3D5A6A", size="xxs", margin="md"),
            ],
        },
    }
    return {"type": "flex", "altText": _MENU_ALT, "contents": bubble}


_MENU_TEXT = (
    "【AXIOM QUANT 指令選單】\n"
    "\n"
    "📊 查看最新日報：\n"
    "  報告 ／ 今日 ／ 戰報\n"
    "  report ／ risk\n"
    "\n"
    "📋 顯示選單：\n"
    "  選單 ／ 幫助 ／ help ／ menu"
)


# ── 工具函式 ──────────────────────────────────────────────────────────────────

def _get_keywords() -> frozenset[str]:
    """回傳有效的觸發關鍵字集合（含大小寫版本）。"""
    raw = os.getenv("LINE_WEBHOOK_KEYWORDS", "").strip()
    if raw:
        return frozenset(k.strip() for k in raw.split(",") if k.strip())
    return _DEFAULT_KEYWORDS


def _verify_signature(body: bytes, signature: str, secret: str) -> bool:
    """以 HMAC-SHA256 驗證 X-Line-Signature。"""
    h = hmac.new(secret.encode("utf-8"), body, hashlib.sha256)
    expected = base64.b64encode(h.digest()).decode("utf-8")
    return hmac.compare_digest(expected, signature)


def _find_latest_report() -> Optional[tuple[Path, date]]:
    """
    掃描 output/ 目錄，回傳最新的 daily_report_YYYY-MM-DD.md。
    若找不到任何報告則回傳 None。
    """
    if not _OUTPUT_DIR.exists():
        return None
    candidates = sorted(_OUTPUT_DIR.glob("daily_report_*.md"), reverse=True)
    for p in candidates:
        m = re.search(r'daily_report_(\d{4}-\d{2}-\d{2})\.md', p.name)
        if m:
            try:
                return p, date.fromisoformat(m.group(1))
            except ValueError:
                continue
    return None


# ── 回覆底層 ──────────────────────────────────────────────────────────────────

def _call_reply_api(reply_token: str, message_obj: dict, token: str) -> bool:
    """呼叫 LINE Reply API。回傳 True = HTTP 200。"""
    try:
        resp = requests.post(
            LINE_REPLY_URL,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json; charset=UTF-8",
            },
            json={"replyToken": reply_token, "messages": [message_obj]},
            timeout=10,
        )
    except requests.Timeout:
        logger.error("LINE_REPLY_HTTP_FAILED  error=Timeout(10s)")
        return False
    except requests.RequestException as exc:
        logger.error("LINE_REPLY_HTTP_FAILED  error=%s", exc)
        return False

    if resp.status_code == 200:
        return True
    logger.warning(
        "LINE_REPLY_HTTP_FAILED  status=%s  body=%s",
        resp.status_code, resp.text[:200].replace("\n", " "),
    )
    return False


def _reply_menu(reply_token: str, token: str) -> None:
    """回覆選單：Flex 優先，失敗 fallback 純文字。"""
    try:
        flex_msg = _build_menu_flex()
        if _call_reply_api(reply_token, flex_msg, token):
            logger.info("LINE_REPLY_MENU_FLEX_SENT")
            return
    except Exception as exc:
        logger.warning("LINE_REPLY_MENU_FLEX_FAILED  error=%s", exc)

    _call_reply_api(reply_token, {"type": "text", "text": _MENU_TEXT}, token)
    logger.info("LINE_REPLY_MENU_TEXT_SENT")


def _reply_report(reply_token: str, token: str) -> None:
    """
    找最新報告，以 Flex 回覆；Flex 失敗自動 fallback 純文字。
    所有結果均記錄 log。
    """
    result = _find_latest_report()
    if result is None:
        _call_reply_api(
            reply_token,
            {"type": "text", "text": "目前尚無可用報告，請稍後再試"},
            token,
        )
        logger.warning("LINE_REPLY_FAILED  reason=no_report_found")
        return

    report_path, report_date = result

    try:
        md_text = report_path.read_text(encoding="utf-8")
    except Exception as exc:
        logger.error("LINE_REPLY_FAILED  reason=read_error  path=%s  error=%s",
                     report_path, exc)
        _call_reply_api(
            reply_token,
            {"type": "text", "text": "報告讀取失敗，請稍後再試"},
            token,
        )
        return

    # ── 嘗試 Flex ────────────────────────────────────────────────────────
    flex_ok = False
    try:
        flex_msg = build_line_flex_payload(md_text, report_date)
        if _call_reply_api(reply_token, flex_msg, token):
            logger.info("LINE_REPLY_FLEX_SENT  date=%s", report_date)
            flex_ok = True
    except Exception as exc:
        logger.warning("LINE_REPLY_FLEX_FAILED  date=%s  error=%s", report_date, exc)

    if flex_ok:
        return

    logger.warning("LINE_REPLY_FLEX_FAILED  date=%s  falling_back=text", report_date)

    # ── Fallback 純文字 ──────────────────────────────────────────────────
    try:
        text_body = build_line_message(md_text, report_date)
    except Exception as exc:
        logger.error("LINE_REPLY_FAILED  reason=build_text_error  error=%s", exc)
        return

    if _call_reply_api(reply_token, {"type": "text", "text": text_body}, token):
        logger.info("LINE_REPLY_TEXT_FALLBACK_SENT  date=%s", report_date)
    else:
        logger.error("LINE_REPLY_FAILED  date=%s  reason=both_flex_and_text_failed",
                     report_date)


# ── Event 處理 ────────────────────────────────────────────────────────────────

def _process_event(event: dict, keywords: frozenset[str], token: str) -> None:
    """處理單一 LINE webhook event。"""
    if event.get("type") != "message":
        return

    # 跳過 standby 模式（module channel 安全措置）
    if event.get("mode") == "standby":
        logger.debug("LINE_WEBHOOK_SKIP  reason=mode=standby")
        return

    if event.get("message", {}).get("type") != "text":
        return

    text        = event.get("message", {}).get("text", "").strip()
    reply_token = event.get("replyToken", "")
    user_id     = event.get("source", {}).get("userId", "unknown")

    if not reply_token:
        logger.debug("LINE_WEBHOOK_SKIP  reason=no_reply_token")
        return

    text_lower = text.lower()

    # 選單關鍵字（優先檢查）
    if text_lower in {k.lower() for k in _HELP_KEYWORDS}:
        logger.info("LINE_MENU_TRIGGERED  keyword=%r  user=%s", text, user_id)
        _reply_menu(reply_token, token)
        return

    # 日報關鍵字
    if text in keywords or text_lower in {k.lower() for k in keywords}:
        logger.info("LINE_KEYWORD_TRIGGERED  keyword=%r  user=%s", text, user_id)
        _reply_report(reply_token, token)
        return

    # 其他輸入 → 提示
    logger.debug("LINE_KEYWORD_NOT_MATCHED  text=%r  user=%s", text, user_id)
    _call_reply_api(reply_token, {"type": "text", "text": _HINT_TEXT}, token)


# ── 主入口 ────────────────────────────────────────────────────────────────────

def handle_webhook(body_bytes: bytes, signature: str) -> tuple[str, int]:
    """
    處理 LINE webhook 請求。

    Args:
        body_bytes : 原始 request body（bytes，用於 signature 驗證）
        signature  : X-Line-Signature header 值

    Returns:
        (response_body, http_status_code) — 直接傳回給 Flask
    """
    secret = os.getenv("LINE_CHANNEL_SECRET", "").strip()
    token  = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()

    if not secret:
        logger.error("LINE_WEBHOOK_ERROR  reason=LINE_CHANNEL_SECRET_not_set")
        return "Server configuration error", 500

    if not token:
        logger.error("LINE_WEBHOOK_ERROR  reason=LINE_CHANNEL_ACCESS_TOKEN_not_set")
        return "Server configuration error", 500

    # 驗證 X-Line-Signature
    if not signature:
        logger.warning("LINE_WEBHOOK_REJECTED  reason=missing_signature")
        return "Forbidden", 403

    if not _verify_signature(body_bytes, signature, secret):
        logger.warning("LINE_WEBHOOK_REJECTED  reason=invalid_signature")
        return "Forbidden", 403

    # 解析 body
    try:
        body   = json.loads(body_bytes.decode("utf-8"))
        events = body.get("events", [])
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.error("LINE_WEBHOOK_PARSE_ERROR  error=%s", exc)
        return "Bad Request", 400

    keywords = _get_keywords()

    for event in events:
        try:
            _process_event(event, keywords, token)
        except Exception as exc:
            # 單一 event 錯誤不中斷整體處理
            logger.error(
                "LINE_WEBHOOK_EVENT_ERROR  error=%s  event_type=%s",
                exc, event.get("type", "?"),
            )

    return "OK", 200
