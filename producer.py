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

# Kafka producer config for local Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',
}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# File to store last update time
LAST_UPDATE_FILE = 'last_update.txt'

def get_last_update():
    if os.path.exists(LAST_UPDATE_FILE):
        with open(LAST_UPDATE_FILE, 'r', encoding='utf-8') as f:
            return f.read().strip()
    else:
        # Default to a recent date for testing
        return '2024-03-20T00:00:00'

def set_last_update(timestamp):
    with open(LAST_UPDATE_FILE, 'w') as f:
        f.write(timestamp)

# Get the CRASH API key (which is the URL)
crash_url = os.getenv('CRASH_API_KEY')

if crash_url:
    # Extract the dataset ID from the URL
    # URL format: https://data.cityofnewyork.us/api/v3/views/{id}/query.json
    match = re.search(r'/views/([a-z0-9-]+)/', crash_url)
    if match:
        dataset_id = match.group(1)
        
        while True:  # Run indefinitely, updating hourly
            last_update = get_last_update()
            # Construct the SODA API URL with date filter
            api_url = f'https://data.cityofnewyork.us/resource/{dataset_id}.json?$where=crash_date >= \'{last_update}\'&$limit=50000'
            try:
                # Fetch the new data from the API
                response = requests.get(api_url)
                response.raise_for_status()  # Raise an error for bad status codes
                
                # Parse the JSON data
                data = response.json()
                
                if data:  # Only produce if there's new data
                    print(f"Fetched {len(data)} new crash records since {last_update}")
                    
                    # Produce each record to Kafka topic 'crashes'
                    for record in data:
                        producer.produce('crashes', key=str(record.get('collision_id', 'unknown')), value=json.dumps(record), callback=delivery_report)
                    
                    # Wait for all messages to be delivered
                    producer.flush()
                    
                    # Update last update time to the current time
                    current_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                    set_last_update(current_time)
                    print(f"Updated last_update to {current_time}")
                else:
                    print(f"No new data since {last_update}")
                
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data: {e}")
            
            # Sleep for 1 hour (3600 seconds) before next fetch
            print("Sleeping for 1 hour...")
            time.sleep(3600)
    else:
        print("Could not extract dataset ID from URL")
else:
    print("CRASH_API_KEY not found in secrets.env")
