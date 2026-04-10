@echo off
chcp 65001 > nul

:: 切換到 quant_etl 目錄
cd /d "C:\Users\USER\Desktop\我的AI工作區\quant_etl"

:: 寫入 log 標記（可選）
echo [%date% %time%] 排程啟動 >> logs\scheduler.log

:: 執行每日流程（含 Email）
"C:\Users\USER\AppData\Local\Programs\Python\Python313\python.exe" run_daily.py >> logs\scheduler.log 2>&1

echo [%date% %time%] 排程完成 >> logs\scheduler.log
