@echo off
REM Batch script to run Tableau export hourly
REM This keeps your CSV files updated with fresh MongoDB data

cd /d "C:\Users\s558312\Documents\GitHub\BigDataFinal_TrafficApp\Tableau_Necessities"

REM Step 1: Export fresh data from MongoDB
echo [%date% %time%] Starting Tableau data update...
python export_to_tableau.py
if %errorlevel% neq 0 (
    echo ERROR: MongoDB export failed!
    exit /b 1
)

REM Step 2: Clean the data (remove bad coordinates)
python clean_data.py

REM Step 3: Generate street segments (properly traced streets)
python create_street_segments.py
if %errorlevel% neq 0 (
    echo ERROR: Street segments generation failed!
    exit /b 1
)

REM Log the execution time
echo [%date% %time%] ✓ Tableau data updated >> export_log.txt

REM Exit gracefully
exit /b 0
