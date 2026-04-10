# quant_etl — 環境建置指南

## 前置條件

| 項目 | 需求 |
|------|------|
| Python | **3.10 以上**（建議 3.12 / 3.13） |
| PostgreSQL | **14 以上**（建議 TimescaleDB 2.x 擴充） |
| FRED API Key | 免費申請：https://fred.stlouisfed.org/docs/api/api_key.html |
| Gmail App Password | Google 帳戶 → 安全性 → 兩步驟驗證 → 應用程式密碼 |

---

## A. 建立 venv 隔離環境（Windows）

```bat
:: 1. 進入專案目錄
cd "C:\Users\USER\Desktop\我的AI工作區\quant_etl"

:: 2. 建立虛擬環境（只需執行一次）
python -m venv .venv

:: 3. 啟動虛擬環境
.venv\Scripts\activate

:: 4. 確認啟動成功（提示符應顯示 (.venv)）
python --version

:: 5. 離開虛擬環境（執行完畢後）
deactivate
```

> **注意**：每次開啟新終端機都需要重新執行 `.venv\Scripts\activate` 才能使用隔離環境。

---

## B. 安裝套件依賴

```bat
:: 確認 venv 已啟動（提示符有 (.venv)）
.venv\Scripts\activate

:: 升級 pip（避免舊版 pip 安裝失敗）
python -m pip install --upgrade pip

:: 安裝固定版本依賴
pip install -r requirements.txt
```

### requirements.txt 套件清單

| 套件 | 版本 | 用途 |
|------|------|------|
| `pandas` | 2.3.3 | 時間序列處理、資料對齊 |
| `numpy` | 2.4.3 | 數值計算 |
| `yfinance` | 1.2.0 | 市場行情下載（ETF / 股票 / VIX）|
| `fredapi` | 0.5.2 | FRED 總經資料下載（ISM PMI / HY OAS / 殖利率）|
| `psycopg2-binary` | 2.9.11 | PostgreSQL 連線 |
| `python-dotenv` | 1.2.2 | 讀取 `.env` 設定檔 |
| `Markdown` | 3.10.2 | 日報 HTML 轉換（Email 用）|

---

## C. 設定 .env

```bat
:: 複製範本
copy .env.example .env

:: 用編輯器開啟並填入真實值
notepad .env
```

`.env` 必填項目：

```dotenv
# PostgreSQL
PG_HOST=localhost
PG_PORT=5432
PG_DBNAME=quant
PG_USER=postgres
PG_PASSWORD=你的密碼

# FRED API（免費申請）
FRED_API_KEY=你的FRED_Key

# Gmail SMTP（用於日報發送 + 崩潰警告）
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=你的gmail@gmail.com
SMTP_PASSWORD=xxxx xxxx xxxx xxxx   # Gmail App Password（16碼）
REPORT_TO_EMAIL=收件者@gmail.com
```

---

## D. 初始化資料庫

```bat
:: 確認 PostgreSQL 已啟動後執行主要 DDL
psql -U postgres -d quant -f schema_main.sql

:: （選用）若使用 TimescaleDB，建立 hypertable
:: 解除 schema_main.sql 底部的注釋後重新執行
```

---

## E. 驗證環境安裝成功

```bat
:: 執行環境驗證腳本（含 DB 連線測試）
python verify_env.py

:: 若 DB 尚未啟動，可跳過 DB 測試
python verify_env.py --no-db
```

預期輸出：
```
============================================================
  quant_etl 環境驗證
  Python: 3.13.x  (需要 3.10+)
============================================================
[ 1 / 3 ]  套件版本檢查
  套件                 已安裝         最低需求       狀態
  -------------------- -------------- -------------- ------
  pandas               2.3.3          2.0.0          OK
  numpy                2.4.3          1.24.0         OK
  yfinance             1.2.0          0.2.28         OK
  fredapi              0.5.2          0.5.1          OK
  psycopg2-binary      2.9.11         2.9.0          OK
  python-dotenv        1.2.2          1.0.0          OK
  Markdown             3.10.2         3.5.0          OK

[ 2 / 3 ]  .env 檔案檢查
  PG_HOST               OK
  ...

[ 3 / 3 ]  PostgreSQL 連線測試
  連線成功  OK

============================================================
  環境驗證通過！可執行 python run_daily.py
============================================================
```

---

## F. 首次執行完整流程

```bat
:: Step 1：下載歷史資料（第一次建議拉長回溯）
python -m etl.run_etl --start 2018-01-01

:: Step 2：計算衍生指標
python -m indicators.run_indicators --start 2018-01-01

:: Step 3：驗證資料品質
python -m validation.run_validation

:: Step 4：執行每日報告（跳過 Email）
python run_daily.py --no-email
```

---

## G. 每日例行執行

```bat
.venv\Scripts\activate
python run_daily.py
```

---

## H. 常見問題

| 問題 | 解決方式 |
|------|---------|
| `psycopg2.OperationalError: could not connect` | 確認 PostgreSQL 已啟動，並檢查 `.env` 的 PG_* 設定 |
| `FRED_API_KEY not set` | 申請 FRED API Key 並填入 `.env` |
| `SMTPAuthenticationError` | 確認 Gmail App Password 正確，且 Google 帳戶已開啟「兩步驟驗證」|
| `ModuleNotFoundError` | 確認已啟動 venv 並執行 `pip install -r requirements.txt` |
| `UnicodeEncodeError` | Windows 終端機問題，run_daily.py 已內建 UTF-8 修正，通常自動處理 |
