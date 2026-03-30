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

# Path to the built Spark connector JAR (after compilation)
CONNECTOR_JAR = os.getenv("SPARK_JARS")
# Update CONNECTOR_JAR to the actual jar name you get after build (or pass via SPARK_JARS env)

spark_builder = SparkSession.builder.appName("TrafficAppMongoAnalysis")

# Either set spark.jars in builder or use environment variable SPARK_JARS
if os.path.exists(CONNECTOR_JAR):
    spark_builder = spark_builder.config("spark.jars", CONNECTOR_JAR)

spark_builder = spark_builder.config("spark.mongodb.read.connection.uri", MONGODB_URI)
spark_builder = spark_builder.config("spark.mongodb.write.connection.uri", MONGODB_URI)

spark = spark_builder.getOrCreate()

print("Spark session created")

# Read from MongoDB
df = spark.read.format("mongo") \
    .option("database", MONGO_DB) \
    .option("collection", MONGO_COLLECTION) \
    .load()

print("Read collection: {}.{}".format(MONGO_DB, MONGO_COLLECTION))

# Basic data inspection
print("Schema:")
df.printSchema()
print("Sample rows:")
df.show(10, truncate=False)

# Example transform: count per a field (update to your field names)
if "road_id" in df.columns:
    counts = df.groupBy("road_id").count().orderBy("count", ascending=False)
    counts.show(20)
else:
    print("NOTE: 'road_id' not present; skipping groupBy example")

spark.stop()
print("Spark stop")
