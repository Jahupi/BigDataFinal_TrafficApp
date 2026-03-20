import requests
from dotenv import load_dotenv
import os
import re
import pickle
import json
from confluent_kafka import Producer

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

# Get the CRASH API key (which is the URL)
crash_url = os.getenv('CRASH_API_KEY')

if crash_url:
    # Extract the dataset ID from the URL
    # URL format: https://data.cityofnewyork.us/api/v3/views/{id}/query.json
    match = re.search(r'/views/([a-z0-9-]+)/', crash_url)
    if match:
        dataset_id = match.group(1)
        # Construct the correct SODA API URL
        api_url = f'https://data.cityofnewyork.us/resource/{dataset_id}.json?$limit=50000'
        try:
            # Fetch the data from the API
            response = requests.get(api_url)
            response.raise_for_status()  # Raise an error for bad status codes
            
            # Parse the JSON data
            data = response.json()
            
            # Produce each record to Kafka topic 'crashes'
            for record in data:
                producer.produce('crashes', key=str(record.get('collision_id', 'unknown')), value=json.dumps(record), callback=delivery_report)
            
            # Wait for all messages to be delivered
            producer.flush()
            
            # Also pickle the dataset for backup
            with open('crashes.pkl', 'wb') as f:
                pickle.dump(data, f)
            
            print(f"Fetched, produced to Kafka, and pickled {len(data)} crash records")
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
    else:
        print("Could not extract dataset ID from URL")
else:
    print("CRASH_API_KEY not found in secrets.env")
