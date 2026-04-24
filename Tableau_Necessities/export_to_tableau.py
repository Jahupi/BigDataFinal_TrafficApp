"""
Export MongoDB traffic data to CSV format for Tableau heat map visualization.
This script extracts crash and congestion data and formats it for Tableau.
"""

import os
import json
import csv
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables
load_dotenv('../secrets.env')

# MongoDB connection
mongodb_uri = os.getenv('MONGODB_URI')
if not mongodb_uri:
    raise ValueError("MONGODB_URI not found in secrets.env")

client = MongoClient(mongodb_uri)
db = client['traffic_data']
collection = db['crashes']

def export_crash_data_for_heatmap():
    """
    Export crash data aggregated by location for heat map visualization.
    This shows density of incidents at each location.
    """
    print("Exporting crash data for heat map...")
    
    # Aggregate data by location
    pipeline = [
        {
            '$match': {
                'latitude': {'$exists': True, '$ne': None},
                'longitude': {'$exists': True, '$ne': None}
            }
        },
        {
            '$group': {
                '_id': {
                    'latitude': '$latitude',
                    'longitude': '$longitude',
                    'street': {'$ifNull': ['$on_street_name', 'Unknown']}
                },
                'incident_count': {'$sum': 1},
                'number_of_persons_injured': {'$sum': {
                    '$cond': [
                        {'$isNumber': '$number_of_persons_injured'},
                        '$number_of_persons_injured',
                        0
                    ]
                }},
                'number_of_persons_killed': {'$sum': {
                    '$cond': [
                        {'$isNumber': '$number_of_persons_killed'},
                        '$number_of_persons_killed',
                        0
                    ]
                }},
                'last_incident': {'$max': '$crash_date'}
            }
        },
        {
            '$sort': {'incident_count': -1}
        }
    ]
    
    results = list(collection.aggregate(pipeline))
    
    if not results:
        print("No data found in MongoDB!")
        return
    
    # Write to CSV
    csv_filename = 'crash_heat_map_data.csv'
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Latitude', 'Longitude', 'Street', 'Incident_Count', 'Injured', 'Killed', 'Congestion_Index', 'Last_Incident_Date']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        
        for row in results:
            # Calculate congestion index (0-100 scale)
            # Higher incidents = higher congestion
            congestion_index = min(100, (row['incident_count'] / len(results)) * 100 * 10)
            
            writer.writerow({
                'Latitude': row['_id']['latitude'],
                'Longitude': row['_id']['longitude'],
                'Street': row['_id']['street'] or 'Unknown',
                'Incident_Count': row['incident_count'],
                'Injured': row['number_of_persons_injured'],
                'Killed': row['number_of_persons_killed'],
                'Congestion_Index': round(congestion_index, 2),
                'Last_Incident_Date': row['last_incident'] if row['last_incident'] else ''
            })
    
    print(f"✓ Exported {len(results)} locations to {csv_filename}")
    print(f"  File location: {os.path.abspath(csv_filename)}")

def export_hourly_congestion_by_street():
    """
    Export congestion data aggregated by street and hour for time-based analysis.
    """
    print("Exporting hourly congestion data by street...")
    
    pipeline = [
        {
            '$match': {
                'latitude': {'$exists': True, '$ne': None},
                'longitude': {'$exists': True, '$ne': None},
                'crash_date': {'$exists': True}
            }
        },
        {
            '$group': {
                '_id': {
                    'latitude': '$latitude',
                    'longitude': '$longitude',
                    'street': {'$ifNull': ['$on_street_name', 'Unknown']},
                    'hour': {
                        '$hour': {
                            '$dateFromString': {
                                'dateString': '$crash_date',
                                'onError': None
                            }
                        }
                    }
                },
                'incident_count': {'$sum': 1},
                'total_injured': {'$sum': {
                    '$cond': [
                        {'$isNumber': '$number_of_persons_injured'},
                        '$number_of_persons_injured',
                        0
                    ]
                }}
            }
        }
    ]
    
    results = list(collection.aggregate(pipeline))
    
    if not results:
        print("No hourly data found!")
        return
    
    # Write to CSV
    csv_filename = 'hourly_congestion_data.csv'
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Latitude', 'Longitude', 'Street', 'Hour', 'Incident_Count', 'Total_Injured']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        
        for row in results:
            writer.writerow({
                'Latitude': row['_id']['latitude'],
                'Longitude': row['_id']['longitude'],
                'Street': row['_id']['street'] or 'Unknown',
                'Hour': row['_id']['hour'],
                'Incident_Count': row['incident_count'],
                'Total_Injured': row['total_injured']
            })
    
    print(f"✓ Exported {len(results)} time-location records to {csv_filename}")
    print(f"  File location: {os.path.abspath(csv_filename)}")

def export_street_level_summary():
    """
    Export summary statistics by street for easier filtering in Tableau.
    """
    print("Exporting street-level summary...")
    
    pipeline = [
        {
            '$match': {
                'latitude': {'$exists': True, '$ne': None},
                'longitude': {'$exists': True, '$ne': None},
                'on_street_name': {'$exists': True, '$ne': None}
            }
        },
        {
            '$group': {
                '_id': '$on_street_name',
                'avg_latitude': {'$avg': '$latitude'},
                'avg_longitude': {'$avg': '$longitude'},
                'total_incidents': {'$sum': 1},
                'total_injured': {'$sum': {
                    '$cond': [
                        {'$isNumber': '$number_of_persons_injured'},
                        '$number_of_persons_injured',
                        0
                    ]
                }},
                'total_killed': {'$sum': {
                    '$cond': [
                        {'$isNumber': '$number_of_persons_killed'},
                        '$number_of_persons_killed',
                        0
                    ]
                }}
            }
        },
        {
            '$sort': {'total_incidents': -1}
        },
        {
            '$limit': 500  # Top 500 streets
        }
    ]
    
    results = list(collection.aggregate(pipeline))
    
    if not results:
        print("No street data found!")
        return
    
    # Write to CSV
    csv_filename = 'street_summary.csv'
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Street', 'Latitude', 'Longitude', 'Total_Incidents', 'Total_Injured', 'Total_Killed', 'Danger_Level']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        
        max_incidents = results[0]['total_incidents'] if results else 1
        
        for row in results:
            # Danger level: 1-5 scale
            danger_level = min(5, max(1, int((row['total_incidents'] / max_incidents) * 5)))
            
            writer.writerow({
                'Street': row['_id'],
                'Latitude': row['avg_latitude'],
                'Longitude': row['avg_longitude'],
                'Total_Incidents': row['total_incidents'],
                'Total_Injured': row['total_injured'],
                'Total_Killed': row['total_killed'],
                'Danger_Level': danger_level
            })
    
    print(f"✓ Exported {len(results)} streets to {csv_filename}")
    print(f"  File location: {os.path.abspath(csv_filename)}")

if __name__ == '__main__':
    try:
        export_crash_data_for_heatmap()
        export_hourly_congestion_by_street()
        export_street_level_summary()
        print("\n✓ All exports completed successfully!")
        print("CSV files are ready to import into Tableau.")
    except Exception as e:
        print(f"Error during export: {e}")
    finally:
        client.close()
