import requests
from dotenv import load_dotenv
import os
import re
import pickle
import json
from confluent_kafka import Producer
import time
from datetime import datetime, timedelta

# Load environment variables from secrets.env
load_dotenv('secrets.env')

# Kafka producers config for local Kafka
crashes_conf = {
    'bootstrap.servers': 'localhost:9092',
}
speeds_conf = {
    'bootstrap.servers': 'localhost:9093',
}

crashes_producer = Producer(crashes_conf)
speeds_producer = Producer(speeds_conf)


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# File to store last update time
LAST_UPDATE_CRASHES = 'crashes_last_update.txt'
LAST_UPDATE_SPEEDS = 'speeds_last_update.txt'

def get_last_update(update_file):
    if os.path.exists(update_file):
        with open(update_file, 'r', encoding='utf-8') as f:
            return f.read().strip()
    else:
        # Default to a recent date for testing
        return '2024-03-20T00:00:00'

def set_last_update(update_file, timestamp):
    with open(update_file, 'w') as f:
        f.write(timestamp)

# Get the CRASH API key (which is the URL)
crash_url = os.getenv('CRASH_API_KEY')
speed_url = os.getenv('SPEED_API_KEY')  

if crash_url and speed_url:
    # Extract the dataset ID from the crash URL
    match_crash = re.search(r'/views/([a-z0-9-]+)/', crash_url)
    match_speed = re.search(r'/views/([a-z0-9-]+)/', speed_url)
    
    if match_crash and match_speed:
        crash_dataset_id = match_crash.group(1)
        speed_dataset_id = match_speed.group(1)
        
        while True:  # Run indefinitely, updating hourly
            # Handle crashes data
            last_crash_update = get_last_update(LAST_UPDATE_CRASHES)
            crash_api_url = f'https://data.cityofnewyork.us/resource/{crash_dataset_id}.json?$where=crash_date >= \'{last_crash_update}\'&$limit=50000'
            try:
                crash_response = requests.get(crash_api_url)
                crash_response.raise_for_status()
                crash_data = crash_response.json()
                
                if crash_data:
                    print(f"Fetched {len(crash_data)} new crash records since {last_crash_update}")
                    for record in crash_data:
                        crashes_producer.produce('crashes', key=str(record.get('collision_id', 'unknown')), value=json.dumps(record), callback=delivery_report)
                    crashes_producer.flush()
                    
                    current_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                    set_last_update(LAST_UPDATE_CRASHES, current_time)
                    print(f"Updated crashes last_update to {current_time}")
                else:
                    print(f"No new crash data since {last_crash_update}")
                    
            except requests.exceptions.RequestException as e:
                print(f"Error fetching crash data: {e}")
            
            # Handle speeds data
            last_speed_update = get_last_update(LAST_UPDATE_SPEEDS)
            # Use order by to get the most recent records first
            speed_api_url = f'https://data.cityofnewyork.us/resource/{speed_dataset_id}.json?$order=data_as_of'
            try:
                speed_response = requests.get(speed_api_url, timeout=30)
                speed_response.raise_for_status()
                speed_data = speed_response.json()
                
                if speed_data and isinstance(speed_data, list):
                    print(f"Fetched {len(speed_data)} speed records")
                    for record in speed_data:
                        speeds_producer.produce('speeds', key=str(record.get('id', 'unknown')), value=json.dumps(record), callback=delivery_report)
                    speeds_producer.flush()
                    
                    current_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                    set_last_update(LAST_UPDATE_SPEEDS, current_time)
                    print(f"Updated speeds last_update to {current_time}")
                else:
                    print(f"No speed data returned")
                    
            except requests.exceptions.Timeout:
                print(f"Error: Speed API request timed out after 30 seconds")
            except requests.exceptions.RequestException as e:
                print(f"Error fetching speed data: {e}")
            
            # Sleep for 1 hour before next fetch
            print("Sleeping for 1 hour...")
            time.sleep(3600)
    else:
        print("Could not extract dataset IDs from URLs")
else:
    print("CRASH_API_KEY or SPEED_API_KEY not found in secrets.env")
