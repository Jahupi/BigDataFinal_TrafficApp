@echo off
REM Batch script to run Tableau crash-data export hourly
REM This keeps the original Tableau CSV files updated in place

cd /d "C:\Users\s558312\Documents\GitHub\BigDataFinal_TrafficApp\Tableau_Necessities"

echo [%date% %time%] Starting Tableau data update...
python export_to_tableau.py
if %errorlevel% neq 0 (
    echo ERROR: MongoDB export failed!
    exit /b 1
)

echo [%date% %time%] Tableau data updated >> export_log.txt
exit /b 0
