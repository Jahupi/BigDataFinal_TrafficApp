# BigDataFinal_TrafficApp

This project fetches traffic crash data from the NYC Open Data API hourly, produces new records to a local Kafka topic, and consumes them into MongoDB Atlas for analysis.

## Setup

1. Ensure Docker Desktop is installed and running.
2. Start Kafka locally: `docker compose up -d`
3. Install dependencies: `pip install -r requirements.txt`
4. Set up MongoDB Atlas connection (see MongoDB section below).
5. Run the producer in the background: `python producer.py` (it will fetch new data hourly).
6. In a separate terminal, run the consumer: `python consumer.py` (leave it running to consume new messages).

The producer runs continuously, checking for new crash records since the last update (stored in `last_update.txt`) and producing only new data to Kafka hourly. The consumer reads from Kafka and inserts into MongoDB.

## Kafka

- Local Kafka runs on `localhost:9092`.
- Topic: `crashes` (auto-created).
- Consumer group: `traffic-consumer-group`.

## MongoDB

- Connected to MongoDB Atlas.
- Database: `traffic_data`
- Collection: `crashes`
- Connection URI stored in `secrets.env` as `MONGODB_URI`.
- Use the MongoDB VS Code extension to browse and query the data directly in VS Code.

## Data Sources

## Kafka

- Local Kafka runs on `localhost:9092`.
- Topic: `crashes` (auto-created).
- Consumer group: `traffic-consumer-group`.

## MongoDB

- Connected to MongoDB Atlas.
- Database: `traffic_data`
- Collection: `crashes`
- Connection URI stored in `secrets.env` as `MONGODB_URI`.
- Use the MongoDB VS Code extension to browse and query the data directly in VS Code.

##
Tools:
• Data ingestion: Kafka (real-time)
• Storage: MongoDB
• Analysis: Spark SQL
• Visualization: Tableau


## Data Sources

- [Collisions and accident](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about_data)
- [Traffic speeds on streets](https://data.cityofnewyork.us/Transportation/DOT-Traffic-Speeds-NBE/i4gi-tjb9/about_data)
- [Weather Crash Data](https://huggingface.co/datasets/xx103/NYC_Motor_Vehicle_Collisions_and_Weather_Dataset)
- [Traffic Speed on a local NYC Street](https://data.cityofnewyork.us/Transportation/EZ-Pass-Readers-July-2024-current/6a2s-2t65/about_data)

(a) Use past and present traffic speeds to determine whether there is currently congestion by comparing the speeds and times against each other.
~~(b) Use past and present accident/collision information to analyze
which routes/streets are the most dangerous. Show as a bar chart
with the top 10 most dangerous intersections/streets.~~
(c) Generate a heatmap of every day traffic to show the averages of
each street day to day.
(d) Analyze the relationship between traffic congestion and accident
frequency to determine whether higher congestion directly increases
collision probability