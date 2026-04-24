import subprocess
import sys

csv_file = "crash_heat_map_data.csv"
print(f"Opening Tableau with {csv_file}...")
print("When Tableau opens, connect to the CSV file as instructed.")

# Try to open Tableau Desktop
try:
    subprocess.Popen([r"C:\Program Files\Tableau\Tableau ~\bin\tableau.exe"])
except FileNotFoundError:
    print("Tableau Desktop not found in default location.")
    print("Please open Tableau manually and connect to crash_heat_map_data.csv")
    print(f"File location: {os.path.abspath(csv_file)}")
    sys.exit(1)
