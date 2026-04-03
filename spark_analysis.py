from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

# Load secrets from .env
load_dotenv(dotenv_path="secrets.env")

MONGODB_URI = os.getenv("MONGODB_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

if not MONGODB_URI or not MONGO_DB or not MONGO_COLLECTION:
    raise ValueError("Missing one of MONGODB_URI, MONGO_DB, MONGO_COLLECTION in secrets.env")

# Path to the built Spark connector
CONNECTOR_JAR = os.getenv("SPARK_JARS")

# Name can be anything
spark_builder = SparkSession.builder.appName("TrafficAppMongoAnalysis")

# Connect the jar
if os.path.exists(CONNECTOR_JAR):
    spark_builder = spark_builder.config("spark.jars", CONNECTOR_JAR)

# Connect read and write to MongoDB
spark_builder = spark_builder.config("spark.mongodb.read.connection.uri", MONGODB_URI)
spark_builder = spark_builder.config("spark.mongodb.write.connection.uri", MONGODB_URI)

#Create the builder and session
spark = spark_builder.getOrCreate()

print("Spark session created")

# Spark session can be succesfully created, but something below here breaks

# .load from this creation is breaking, breakpoint on line 44 for time being
df = spark.read.format("mongo") \
    .option("database", MONGO_DB) \
    .option("collection", MONGO_COLLECTION) \
    .load() #Error here