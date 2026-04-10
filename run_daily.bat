@echo off
chcp 65001 >nul

:: 切換到 quant_etl 目錄
cd /d "%~dp0"

:: 建立 logs 資料夾（若不存在）
if not exist logs mkdir logs

set PYTHONIOENCODING=utf-8
set PYTHONPATH=%~dp0

:: 執行每日流程，輸出寫入 logs\scheduler.log
"C:\Users\USER\AppData\Local\Programs\Python\Python313\python.exe" run_daily.py >> logs\scheduler.log 2>&1
