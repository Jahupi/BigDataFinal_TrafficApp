"""
Create clean street lines for Tableau heat map
Each street = one clean line (no tracing, no ocean)
"""
import csv

input_file = 'street_lines_heatmap.csv'
output_file = 'street_lines_clean.csv'

# Read and create simplified version
rows = []
with open(input_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        try:
            lat = float(row['Avg_Latitude'])
            lon = float(row['Avg_Longitude'])
            
            # Only include valid NYC bounds (no ocean, no data errors)
            # NYC proper bounds: 40.5-41.0 lat, -74.3 to -73.7 lon
            if 40.50 <= lat <= 41.00 and -74.30 <= lon <= -73.70:
                rows.append({
                    'Street': row['Street'],
                    'Avg_Latitude': lat,
                    'Avg_Longitude': lon,
                    'Total_Incidents': row['Total_Incidents'],
                    'Avg_Congestion': row['Avg_Congestion'],
                    'Max_Congestion': row['Max_Congestion']
                })
        except:
            pass

# Write cleaned data
with open(output_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=[
        'Street', 'Avg_Latitude', 'Avg_Longitude', 'Total_Incidents', 'Avg_Congestion', 'Max_Congestion'
    ])
    writer.writeheader()
    writer.writerows(rows)

print(f"✓ Created clean street lines data!")
print(f"✓ {len(rows)} streets (NYC bounds only)")
print(f"✓ File: street_lines_clean.csv")
print(f"✓ Ready for Tableau!")
