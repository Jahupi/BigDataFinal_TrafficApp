"""
Create street segments for Tableau line visualization
Each street broken into segments showing congestion along its path
"""
import csv
from collections import defaultdict

# Read crash data and group by street
street_segments = defaultdict(list)

# Read from the cleaned crash heat map data
with open('crash_heat_map_data.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        try:
            street = row['Street'].strip()
            if not street or street == "Unknown":
                continue
                
            lat = float(row['Latitude'])
            lon = float(row['Longitude'])
            congestion = float(row['Congestion_Index'])
            incidents = int(row['Incident_Count'])
            
            # Only valid NYC coordinates
            if 40.50 <= lat <= 41.00 and -74.30 <= lon <= -73.70:
                street_segments[street].append({
                    'lat': lat,
                    'lon': lon,
                    'congestion': congestion,
                    'incidents': incidents
                })
        except:
            pass

# Now create segments - group nearby points on same street
output_rows = []
sequence_num = 0

for street in sorted(street_segments.keys()):
    points = street_segments[street]
    
    if len(points) == 0:
        continue
    
    # Sort points by longitude (west to east) to create continuous lines
    points = sorted(points, key=lambda p: (p['lon'], p['lat']))
    
    # Create segments: group every 5-10 points into a line segment
    segment_size = max(2, len(points) // 20)  # Divide street into ~20 segments
    
    for i in range(0, len(points), segment_size):
        segment_points = points[i:i+segment_size]
        if len(segment_points) < 2:
            continue
        
        # Calculate average for this segment
        avg_lat = sum(p['lat'] for p in segment_points) / len(segment_points)
        avg_lon = sum(p['lon'] for p in segment_points) / len(segment_points)
        avg_congestion = sum(p['congestion'] for p in segment_points) / len(segment_points)
        total_incidents = sum(p['incidents'] for p in segment_points)
        
        sequence_num += 1
        output_rows.append({
            'Street': street,
            'Latitude': avg_lat,
            'Longitude': avg_lon,
            'Congestion': avg_congestion,
            'Incidents': total_incidents,
            'Sequence': sequence_num,
            'Segment_Points': len(segment_points)
        })

# Write output
with open('street_segments.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=[
        'Street', 'Latitude', 'Longitude', 'Congestion', 'Incidents', 'Sequence', 'Segment_Points'
    ])
    writer.writeheader()
    writer.writerows(output_rows)

print(f"✓ Created street segments!")
print(f"✓ {len(street_segments)} unique streets")
print(f"✓ {len(output_rows)} total segments")
print(f"✓ Each street divided into smaller segments")
print(f"✓ File: street_segments.csv")
print(f"✓ Ready for Tableau path visualization!")
