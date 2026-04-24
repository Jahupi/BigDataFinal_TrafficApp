"""
Clean the crash data by removing bad coordinates (0,0)
"""
import csv

input_file = 'crash_heat_map_data.csv'
output_file = 'crash_heat_map_data.csv'

# Read and filter
rows = []
with open(input_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Skip records with 0,0 coordinates
        if row['Latitude'].strip() == '0.0000000' and row['Longitude'].strip() == '0.0000000':
            print(f"Skipping bad record: {row['Street']}")
            continue
        rows.append(row)

# Write cleaned data
with open(output_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['Latitude', 'Longitude', 'Street', 'Incident_Count', 'Injured', 'Killed', 'Congestion_Index', 'Last_Incident_Date'])
    writer.writeheader()
    writer.writerows(rows)

print(f"✓ Cleaned! Removed bad records, kept {len(rows)} valid records")
print("✓ File is now ready for Tableau")
