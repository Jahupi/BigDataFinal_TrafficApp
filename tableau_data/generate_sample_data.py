"""
Sample Tableau Heat Map Generator for Testing
This script generates sample NYC street congestion data for Tableau if you don't have
enough real data yet. It creates realistic-looking heat map data centered on Manhattan.
"""

import csv
import random
import math

# NYC bounds (approximate)
NYC_BOUNDS = {
    'min_lat': 40.5774,
    'max_lat': 40.9176,
    'min_lon': -74.2591,
    'max_lon': -73.7004
}

# Manhattan neighborhoods with realistic incident patterns
MANHATTAN_STREETS = [
    "5th Avenue", "Broadway", "Park Avenue", "Madison Avenue", "Lexington Avenue",
    "3rd Avenue", "2nd Avenue", "1st Avenue", "Avenue A", "Avenue B", "Avenue C",
    "9th Avenue", "10th Avenue", "11th Avenue", "12th Avenue",
    "West Side Highway", "FDR Drive", "Houston Street", "Canal Street",
    "14th Street", "23rd Street", "34th Street", "42nd Street", "59th Street",
    "72nd Street", "86th Street", "96th Street", "110th Street",
    "Chambers Street", "Delancey Street", "Spring Street", "Bleecker Street",
    "Washington Square", "Union Square", "Madison Square", "Times Square"
]

def generate_sample_heatmap_data(num_locations=200, num_hours=24):
    """Generate realistic sample data for heat map testing"""
    
    # Generate crash heat map data
    print("Generating sample crash heat map data...")
    with open('crash_heat_map_data.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'Latitude', 'Longitude', 'Street', 'Incident_Count', 
            'Injured', 'Killed', 'Congestion_Index', 'Last_Incident_Date'
        ])
        writer.writeheader()
        
        for i in range(num_locations):
            # Generate random coordinates within NYC bounds
            lat = random.uniform(NYC_BOUNDS['min_lat'], NYC_BOUNDS['max_lat'])
            lon = random.uniform(NYC_BOUNDS['min_lon'], NYC_BOUNDS['max_lon'])
            
            # Manhattan has more incidents (bias towards center)
            if 40.7089 < lat < 40.8082 and -74.0113 < lon < -73.9355:
                incident_count = random.randint(50, 500)
            else:
                incident_count = random.randint(5, 100)
            
            street = random.choice(MANHATTAN_STREETS)
            injured = random.randint(0, incident_count // 5)
            killed = random.randint(0, max(1, incident_count // 50))
            congestion_index = min(100, (incident_count / 500) * 100)
            
            writer.writerow({
                'Latitude': round(lat, 6),
                'Longitude': round(lon, 6),
                'Street': street,
                'Incident_Count': incident_count,
                'Injured': injured,
                'Killed': killed,
                'Congestion_Index': round(congestion_index, 2),
                'Last_Incident_Date': '2024-03-20'
            })
    
    print(f"✓ Generated {num_locations} sample locations in crash_heat_map_data.csv")
    
    # Generate hourly congestion data
    print("Generating sample hourly congestion data...")
    with open('hourly_congestion_data.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'Latitude', 'Longitude', 'Street', 'Hour', 
            'Incident_Count', 'Total_Injured'
        ])
        writer.writeheader()
        
        for i in range(num_locations):
            lat = random.uniform(NYC_BOUNDS['min_lat'], NYC_BOUNDS['max_lat'])
            lon = random.uniform(NYC_BOUNDS['min_lon'], NYC_BOUNDS['max_lon'])
            street = random.choice(MANHATTAN_STREETS)
            
            for hour in range(num_hours):
                # Rush hours (7-9am, 4-7pm) have more incidents
                if hour in [7, 8, 9, 16, 17, 18]:
                    incident_count = random.randint(5, 30)
                else:
                    incident_count = random.randint(0, 10)
                
                total_injured = random.randint(0, incident_count)
                
                writer.writerow({
                    'Latitude': round(lat, 6),
                    'Longitude': round(lon, 6),
                    'Street': street,
                    'Hour': hour,
                    'Incident_Count': incident_count,
                    'Total_Injured': total_injured
                })
    
    print(f"✓ Generated hourly data for {num_locations} locations across {num_hours} hours")
    
    # Generate street summary
    print("Generating sample street summary...")
    streets_data = {}
    
    for _ in range(num_locations):
        street = random.choice(MANHATTAN_STREETS)
        if street not in streets_data:
            lat = random.uniform(40.7089, 40.8082)
            lon = random.uniform(-74.0113, -73.9355)
            streets_data[street] = {
                'lat': lat,
                'lon': lon,
                'incidents': 0,
                'injured': 0,
                'killed': 0
            }
        
        streets_data[street]['incidents'] += random.randint(10, 200)
        streets_data[street]['injured'] += random.randint(0, 20)
        streets_data[street]['killed'] += random.randint(0, 5)
    
    with open('street_summary.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'Street', 'Latitude', 'Longitude', 'Total_Incidents', 
            'Total_Injured', 'Total_Killed', 'Danger_Level'
        ])
        writer.writeheader()
        
        # Sort by incident count
        sorted_streets = sorted(streets_data.items(), key=lambda x: x[1]['incidents'], reverse=True)
        max_incidents = sorted_streets[0][1]['incidents'] if sorted_streets else 1
        
        for street, data in sorted_streets:
            danger_level = min(5, max(1, int((data['incidents'] / max_incidents) * 5)))
            
            writer.writerow({
                'Street': street,
                'Latitude': round(data['lat'], 6),
                'Longitude': round(data['lon'], 6),
                'Total_Incidents': data['incidents'],
                'Total_Injured': data['injured'],
                'Total_Killed': data['killed'],
                'Danger_Level': danger_level
            })
    
    print(f"✓ Generated summary for {len(streets_data)} unique streets")
    print("\n✓ Sample data generation complete!")
    print("  Files ready for import into Tableau")
    print("  This data is centered on Manhattan for realistic visualization")

if __name__ == '__main__':
    generate_sample_heatmap_data()
