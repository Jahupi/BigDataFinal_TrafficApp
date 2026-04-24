"""
Generate street-level line data for Tableau
Creates continuous lines for each street showing congestion patterns
"""

import csv
from collections import defaultdict

def create_street_lines_data():
    """
    Convert crash point data into street-level line segments
    """
    print("Generating street-level line data...")
    
    # Read existing crash data
    street_data = defaultdict(list)
    
    with open('crash_heat_map_data.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            street = row['Street']
            street_data[street].append({
                'Latitude': float(row['Latitude']),
                'Longitude': float(row['Longitude']),
                'Incident_Count': int(row['Incident_Count']),
                'Congestion_Index': float(row['Congestion_Index']),
                'Injured': int(row['Injured'])
            })
    
    # Create street-level lines CSV
    with open('street_lines_heatmap.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'Street', 'Avg_Latitude', 'Avg_Longitude', 
            'Total_Incidents', 'Avg_Congestion', 'Max_Congestion',
            'Total_Injured', 'Point_Count'
        ])
        writer.writeheader()
        
        for street, points in street_data.items():
            if len(points) > 0:
                total_incidents = sum(p['Incident_Count'] for p in points)
                avg_congestion = sum(p['Congestion_Index'] for p in points) / len(points)
                max_congestion = max(p['Congestion_Index'] for p in points)
                total_injured = sum(p['Injured'] for p in points)
                avg_lat = sum(p['Latitude'] for p in points) / len(points)
                avg_lon = sum(p['Longitude'] for p in points) / len(points)
                
                writer.writerow({
                    'Street': street,
                    'Avg_Latitude': round(avg_lat, 6),
                    'Avg_Longitude': round(avg_lon, 6),
                    'Total_Incidents': total_incidents,
                    'Avg_Congestion': round(avg_congestion, 2),
                    'Max_Congestion': round(max_congestion, 2),
                    'Total_Injured': total_injured,
                    'Point_Count': len(points)
                })
    
    print("✓ Created street_lines_heatmap.csv")
    print("  Use this for line-based visualization in Tableau")
    
    # Create polyline data (coordinates for drawing actual lines on map)
    print("\nGenerating polyline data for street routes...")
    
    with open('street_polylines.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'Street', 'Latitude', 'Longitude', 'Congestion_Index', 
            'Sequence', 'Incidents'
        ])
        writer.writeheader()
        
        for street, points in sorted(street_data.items()):
            # Sort points by longitude to create a line
            sorted_points = sorted(points, key=lambda p: (p['Longitude'], p['Latitude']))
            
            for seq, point in enumerate(sorted_points):
                writer.writerow({
                    'Street': street,
                    'Latitude': point['Latitude'],
                    'Longitude': point['Longitude'],
                    'Congestion_Index': point['Congestion_Index'],
                    'Sequence': seq,
                    'Incidents': point['Incident_Count']
                })
    
    print("✓ Created street_polylines.csv")
    print("  Use this to draw lines connecting points per street")

def print_instructions():
    """Print instructions for using line visualizations"""
    instructions = """
================================================================================
        HOW TO CREATE LINE-BASED HEAT MAP IN TABLEAU
================================================================================

OPTION 1: STREET SUMMARY LINES (Simplest)
==========================================
1. Open Tableau
2. Connect to: street_lines_heatmap.csv
3. Build visualization:
   - Drag [Street] to ROWS
   - Drag [Avg_Congestion] to COLOR
   - Drag [Total_Incidents] to SIZE
   - Change Mark Type to LINE
4. Result: Colored lines for each street, sized by incident count

OPTION 2: POLYLINE ROUTES (Most Detailed)
==========================================
1. Open Tableau
2. Connect to: street_polylines.csv
3. Build visualization:
   - Drag [Latitude] to ROWS
   - Drag [Longitude] to COLUMNS
   - Drag [Street] to DETAIL
   - Drag [Sequence] to PATH (in Marks)
   - Drag [Congestion_Index] to COLOR
   - Change Mark Type to LINE
4. Result: Actual street routes drawn as colored lines on map

OPTION 3: DENSITY CONTOUR LINES (Heat Mapping)
===============================================
With original crash_heat_map_data.csv:
1. Drag [Latitude] to ROWS
2. Drag [Longitude] to COLUMNS
3. Drag [Congestion_Index] to COLOR
4. Change Mark Type to DENSITY (from dropdown)
5. Result: Contour heat lines showing congestion zones

================================================================================

COMPARISON:
- Option 1: Simple, each street is one line (best for street performance)
- Option 2: Detailed routes (best for understanding street patterns)
- Option 3: Scientific contours (best for overall heat patterns)

Color Scheme: Red = High Congestion, Yellow = Medium, Green = Low

================================================================================
"""
    print(instructions)

if __name__ == '__main__':
    print("Creating street-level line data for Tableau...\n")
    create_street_lines_data()
    print_instructions()
    print("\nNew CSV files ready to use!")
    print("- street_lines_heatmap.csv")
    print("- street_polylines.csv")
