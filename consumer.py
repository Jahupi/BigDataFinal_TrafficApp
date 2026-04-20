from confluent_kafka import Consumer, KafkaError, KafkaException
from pymongo import MongoClient
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('secrets.env')

# Kafka consumer config (connects to both brokers)
conf = {
    'bootstrap.servers': 'localhost:9092,localhost:9093',  # Connect to both Kafka brokers
    'group.id': 'traffic-consumer-group',  # Unique group for your consumer
    'auto.offset.reset': 'earliest'  # Start from the beginning if no offset exists
}
consumer = Consumer(conf)

# MongoDB connection
mongodb_uri = os.getenv('MONGODB_URI')
if not mongodb_uri:
    raise ValueError("MONGODB_URI not found in secrets.env")
client = MongoClient(mongodb_uri)
db = client['traffic_data']  # Replace with your database name
crashes_collection = db['crashes']  # Collection for crash data
speeds_collection = db['speeds']   # Collection for speed data

# Subscribe to both topics
consumer.subscribe(['crashes', 'speeds'])

try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Poll for messages
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            # Deserialize the message value (JSON string)
            record = json.loads(msg.value().decode('utf-8'))
            topic = msg.topic()

            # Route to appropriate MongoDB collection based on topic
            if topic == 'crashes':
                try:
                    crashes_collection.insert_one(record)
                    print(f"Inserted crash record with collision_id: {record.get('collision_id', 'unknown')}")
                except Exception as e:
                    print(f"Error inserting crash into MongoDB: {e}")
            
            elif topic == 'speeds':
                try:
                    speeds_collection.insert_one(record)
                    print(f"Inserted speed record with id: {record.get('id', 'unknown')}")
                except Exception as e:
                    print(f"Error inserting speed into MongoDB: {e}")

except KeyboardInterrupt:
    print("Consumer interrupted")
finally:
    consumer.close()
    client.close()