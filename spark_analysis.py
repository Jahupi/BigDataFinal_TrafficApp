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

# Print version info
mongo_connector_jar = "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"

# Name can be anything
spark_builder = SparkSession.builder.appName("TrafficAppMongoAnalysis")

# Connect the jar using packages to get dependencies
spark_builder = spark_builder.config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0")

# Connect read and write to MongoDB
spark_builder = spark_builder.config("spark.mongodb.read.connection.uri", MONGODB_URI)
spark_builder = spark_builder.config("spark.mongodb.write.connection.uri", MONGODB_URI)
spark_builder = spark_builder.config("spark.driver.extraJavaOptions", "-Dlog4j.logger.org.apache.spark.SparkEnv=WARN")

#Create the builder and session
spark = spark_builder.getOrCreate()

print("Spark session created")

# Spark session can be succesfully created, but something below here breaks

# .load from this creation is breaking, breakpoint on line 44 for time being
df = spark.read.format("mongodb") \
    .option("database", MONGO_DB) \
    .option("collection", MONGO_COLLECTION) \
    .load() #Error here

print("DataFrame Schema:")
df.printSchema()

# Anything after this is just tests, you can successfully grab
# info using spark
filtered_df = df.filter(df.number_of_persons_killed == "3") \
    .orderBy("_id")

print("Records where number_of_persons_killed = 3, sorted by _id:")
filtered_df.show(truncate=False)

spark.stop()