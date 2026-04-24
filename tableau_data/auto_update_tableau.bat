@echo off
REM Batch script to run Tableau export hourly
REM This keeps your CSV files updated with fresh MongoDB data

cd /d "C:\Users\s558312\Documents\GitHub\BigDataFinal_TrafficApp\tableau_data"

REM Run the export script
python export_to_tableau.py

REM Also update the line heat map data
python create_line_heatmap.py

REM Log the execution time
echo Tableau data updated at %date% %time% >> export_log.txt

REM Exit gracefully
exit /b 0
