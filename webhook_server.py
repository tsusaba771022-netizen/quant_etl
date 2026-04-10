"""
LINE Webhook 伺服器入口
-----------------------
以 Flask 監聽 LINE webhook 事件，收到指定關鍵字後自動回覆最新量化日報。

啟動方式：
  python webhook_server.py

正式部署（gunicorn）：
  gunicorn -w 1 -b 0.0.0.0:8080 "webhook_server:app"
  （LINE webhook 不需要多 worker；1 worker 即可）

環境變數（選填，均有預設值）：
  LINE_WEBHOOK_PATH   webhook 接收路徑（預設 /line/webhook）
  WEBHOOK_HOST        監聽位址（預設 0.0.0.0）
  WEBHOOK_PORT        監聽 port（預設 8080）
  WEBHOOK_DEBUG       flask debug mode，true/false（預設 false，正式環境必須 false）
"""
from __future__ import annotations

import logging
import os
import sys

from dotenv import load_dotenv
from flask import Flask, request

# 必須在所有 import 之前讀取 .env
load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── Flask App ──────────────────────────────────────────────────────────────────
app = Flask(__name__)

# load_dotenv() 已在上方呼叫，此處讀取 path 設定可取到 .env 的值
_WEBHOOK_PATH = os.getenv("LINE_WEBHOOK_PATH", "/line/webhook")


@app.route(_WEBHOOK_PATH, methods=["POST"])
def line_webhook():
    """LINE Webhook 接收端：轉交 report.webhook.handle_webhook 處理。"""
    from report.webhook import handle_webhook  # 避免循環 import / 延遲載入
    body_bytes = request.get_data()
    signature  = request.headers.get("X-Line-Signature", "")
    resp_body, status = handle_webhook(body_bytes, signature)
    return resp_body, status


@app.route("/health", methods=["GET"])
def health():
    """健康檢查端點，用於確認伺服器是否存活。"""
    return "OK", 200


# ── 直接執行 ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    host  = os.getenv("WEBHOOK_HOST", "0.0.0.0")
    port  = int(os.getenv("WEBHOOK_PORT", "8080"))
    debug = os.getenv("WEBHOOK_DEBUG", "false").lower() in ("true", "1")

    logger.info(
        "Webhook server starting  host=%s  port=%s  path=%s",
        host, port, _WEBHOOK_PATH,
    )
    # use_reloader=False 避免 debug mode 雙重啟動
    app.run(host=host, port=port, debug=debug, use_reloader=False)
