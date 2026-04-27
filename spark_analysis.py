from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv
import os
from urllib.parse import quote
import sys
import logging
from io import StringIO

# Suppress Java/Scala logging
logging.getLogger('py4j').setLevel(logging.ERROR)
logging.getLogger('pyspark').setLevel(logging.WARN)

# Load secrets from .env
load_dotenv(dotenv_path="secrets.env")

MONGODB_URI = os.getenv("MONGODB_URI")
MONGO_DB = "traffic_data"
# URL-encode database name to handle spaces
MONGO_DB_ENCODED = quote(MONGO_DB, safe='')

# Choose which collection to analyze: "crashes" or "speeds"
MONGO_COLLECTION = "speeds"  # Change this to "speeds" to analyze speeds instead

if not MONGODB_URI or not MONGO_DB or not MONGO_COLLECTION:
    raise ValueError("Missing one of MONGODB_URI, MONGO_DB, MONGO_COLLECTION in secrets.env")

# Print version info
mongo_connector_jar = "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"

# Name can be anything
spark_builder = SparkSession.builder.appName("TrafficAppMongoAnalysis")

# Connect the jar using packages to get dependencies
spark_builder = spark_builder.config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0")

# Enhanced MongoDB connection configuration for Atlas stability
spark_builder = spark_builder.config("spark.mongodb.read.connection.uri", MONGODB_URI)
spark_builder = spark_builder.config("spark.mongodb.write.connection.uri", MONGODB_URI)

# Add connection pool and timeouts for Atlas
spark_builder = spark_builder.config("spark.mongodb.read.connection.maxPoolSize", "64")
spark_builder = spark_builder.config("spark.mongodb.read.connection.minPoolSize", "1")
spark_builder = spark_builder.config("spark.mongodb.read.connection.maxIdleTime", "60000")
spark_builder = spark_builder.config("spark.mongodb.read.connection.maxWaitTime", "120000")
spark_builder = spark_builder.config("spark.mongodb.read.serverSelectionTimeout", "5000")

# Logging configuration
spark_builder = spark_builder.config("spark.driver.extraJavaOptions", "-Dlog4j.logger.org.apache.spark.SparkEnv=ERROR -Dlog4j.logger.org.apache.spark.ShutdownHookManager=OFF")
spark_builder = spark_builder.config("spark.executor.extraJavaOptions", "-Dlog4j.logger.org.apache.spark.executor.Executor=WARN")

#Create the builder and session
spark = spark_builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Spark session created")

# Verify connection is working
try:
    print(f"Attempting to read from MongoDB:")
    print(f"  URI: {MONGODB_URI}")
    print(f"  Database: {MONGO_DB}")
    print(f"  Collection: {MONGO_COLLECTION}")
    
    # Read from MongoDB with explicit connection options
    df = spark.read.format("mongodb") \
        .option("database", MONGO_DB_ENCODED) \
        .option("collection", MONGO_COLLECTION) \
        .option("spark.mongodb.read.connection.uri", MONGODB_URI) \
        .load()
    
    print(f"Successfully connected! Found {df.count()} records")
    
except Exception as e:
    print(f"ERROR connecting to MongoDB: {e}")
    spark.stop()
    raise

print("DataFrame Schema:")
df.printSchema()

# Export results to files for better readability
output_dir = "spark_analysis_output"
os.makedirs(output_dir, exist_ok=True)

# Drop the location struct column since CSV doesn't support nested types
df_for_csv = df.drop("location")

# Export full dataset to CSV
csv_output = os.path.join(output_dir, "crashes_data.csv")
print(f"\nExporting all {df.count()} records to: {csv_output}")
df_for_csv.coalesce(1).write.mode("overwrite").csv(csv_output, header=True)

# Export sample data (first 100 records)
sample_output = os.path.join(output_dir, "crashes_sample.csv")
print(f"Exporting sample (first 100 records) to: {sample_output}")
df_for_csv.limit(100).coalesce(1).write.mode("overwrite").csv(sample_output, header=True)

# Also export to JSON for better structure support
json_output = os.path.join(output_dir, "crashes_data.json")
print(f"Exporting all records to JSON: {json_output}")
df.coalesce(1).write.mode("overwrite").json(json_output)

# Anything after this is just tests, you can successfully grab
# info using spark

print(f"\n{'='*80}")
print(f"ANALYZING: {MONGO_COLLECTION.upper()} COLLECTION")
print(f"{'='*80}\n")

if MONGO_COLLECTION == "speeds":
    # CONGESTION ANALYSIS: Compare current speeds to determine congestion
    print("\n[TRAFFIC CONGESTION ANALYSIS]")
    print("-" * 80)
    print("Comparing current speeds to historical averages to identify congestion...\n")
    
    try:
        # Calculate average speed per street
        avg_speeds = df.groupBy("link_name").agg(
            F.avg(F.col("speed").cast("double")).alias("avg_speed"),
            F.count("*").alias("num_observations"),
            F.min(F.col("speed").cast("double")).alias("min_speed"),
            F.max(F.col("speed").cast("double")).alias("max_speed")
        ).orderBy(F.col("avg_speed"))
        
        # Define congestion threshold: 70% of average speed or below 15 mph
        avg_speeds = avg_speeds.withColumn(
            "congestion_threshold",
            F.when(F.col("avg_speed") * 0.7 > 15, F.col("avg_speed") * 0.7).otherwise(F.lit(15.0))
        )
        
        # Flag congested streets
        congested_streets = avg_speeds.filter(
            (F.col("avg_speed") < 20) | (F.col("min_speed") < 10)
        ).orderBy(F.col("avg_speed"))
        
        print("[RED] CONGESTED STREETS (Current avg speed < 20 mph):")
        print("=" * 80)
        congested_streets.select(
            F.col("link_name"),
            F.round(F.col("avg_speed"), 2).alias("avg_speed_mph"),
            F.col("min_speed").alias("min_speed_mph"),
            F.col("max_speed").alias("max_speed_mph"),
            F.col("num_observations")
        ).show(20, truncate=False)
        
        # Export congestion analysis
        congestion_output = os.path.join(output_dir, "congestion_analysis.csv")
        congested_streets.select(
            F.col("link_name"),
            F.round(F.col("avg_speed"), 2).alias("avg_speed_mph"),
            F.col("min_speed").alias("min_speed_mph"),
            F.col("max_speed").alias("max_speed_mph"),
            F.col("congestion_threshold"),
            F.col("num_observations")
        ).coalesce(1).write.mode("overwrite").csv(congestion_output, header=True)
        print(f"\n[OK] Congestion analysis exported to: {congestion_output}\n")
        
        # Top streets by average speed (best flowing)
        print("\n[GREEN] BEST FLOWING STREETS (Highest average speeds):")
        print("=" * 80)
        best_streets = avg_speeds.orderBy(F.col("avg_speed").desc())
        best_streets.select(
            F.col("link_name"),
            F.round(F.col("avg_speed"), 2).alias("avg_speed_mph"),
            F.col("num_observations")
        ).limit(10).show(truncate=False)
        
        # Export best flowing streets
        best_output = os.path.join(output_dir, "best_flowing_streets.csv")
        best_streets.select(
            F.col("link_name"),
            F.round(F.col("avg_speed"), 2).alias("avg_speed_mph"),
            F.col("num_observations")
        ).coalesce(1).write.mode("overwrite").csv(best_output, header=True)
        print(f"\n[OK] Best flowing streets exported to: {best_output}\n")
        
    except Exception as e:
        print(f"Error during congestion analysis: {e}")
        print("\nShowing first 10 speed records instead:")
        df.limit(10).show(10, truncate=False)

elif MONGO_COLLECTION == "crashes":
    # CRASH DATA ANALYSIS
    print("\n[CRASH DATA ANALYSIS]")
    print("-" * 80)
    
    try:
        # Analysis (b): Find the top 10 most dangerous streets
        print("\n[ANALYSIS B] MOST DANGEROUS ROUTES/STREETS (Top 10)")
        print("=" * 80)
        print("Analyzing accident/collision information to identify most dangerous streets...\n")
        
        # Group by street and calculate danger metrics
        danger_analysis = df.groupBy("on_street_name").agg(
            F.count("*").alias("total_crashes"),
            F.sum(F.col("number_of_persons_killed").cast("int")).alias("total_killed"),
            F.sum(F.col("number_of_persons_injured").cast("int")).alias("total_injured"),
            F.sum(F.col("number_of_cyclist_killed").cast("int")).alias("cyclists_killed"),
            F.sum(F.col("number_of_motorist_killed").cast("int")).alias("motorists_killed"),
            F.sum(F.col("number_of_pedestrians_killed").cast("int")).alias("pedestrians_killed"),
            F.avg(F.col("number_of_persons_killed").cast("int")).alias("avg_killed_per_crash"),
            F.avg(F.col("number_of_persons_injured").cast("int")).alias("avg_injured_per_crash")
        ).filter(F.col("on_street_name").isNotNull())
        
        # Calculate danger score: weighted combination of fatalities and injuries
        danger_analysis = danger_analysis.withColumn(
            "danger_score",
            (F.col("total_killed") * 10) + (F.col("total_injured") * 1)  # Fatalities weighted 10x
        ).orderBy(F.col("danger_score").desc())
        
        # Show top 10 most dangerous streets
        top_dangerous = danger_analysis.limit(10)
        top_dangerous.select(
            F.col("on_street_name").alias("street"),
            F.col("total_crashes"),
            F.col("total_killed"),
            F.col("total_injured"),
            F.round(F.col("avg_killed_per_crash"), 2).alias("avg_killed_per_crash"),
            F.round(F.col("danger_score"), 0).alias("danger_score")
        ).show(10, truncate=False)
        
        # Export top dangerous streets
        dangerous_output = os.path.join(output_dir, "top_10_dangerous_streets.csv")
        top_dangerous.select(
            F.col("on_street_name").alias("street"),
            F.col("total_crashes"),
            F.col("total_killed"),
            F.col("total_injured"),
            F.col("cyclists_killed"),
            F.col("motorists_killed"),
            F.col("pedestrians_killed"),
            F.round(F.col("avg_killed_per_crash"), 2).alias("avg_killed_per_crash"),
            F.round(F.col("avg_injured_per_crash"), 2).alias("avg_injured_per_crash"),
            F.round(F.col("danger_score"), 0).alias("danger_score")
        ).coalesce(1).write.mode("overwrite").csv(dangerous_output, header=True)
        print(f"\n[OK] Top 10 dangerous streets exported to: {dangerous_output}\n")
        
        # Analysis (d): Analyze relationship between crash frequency and potential congestion
        print("\n[ANALYSIS D] CRASH FREQUENCY & CONGESTION CORRELATION")
        print("=" * 80)
        print("Analyzing crash frequency by street to identify congestion hotspots...\n")
        
        # Calculate crash frequency metrics per street
        crash_frequency = df.groupBy("on_street_name").agg(
            F.count("*").alias("crash_frequency"),
            F.countDistinct("crash_date").alias("days_with_crashes"),
            F.round(F.count("*") / F.countDistinct("crash_date"), 2).alias("avg_crashes_per_day"),
            F.sum(F.col("number_of_persons_killed").cast("int")).alias("total_fatalities"),
            F.sum(F.col("number_of_persons_injured").cast("int")).alias("total_injuries")
        ).filter(F.col("on_street_name").isNotNull())
        
        # Calculate congestion index: streets with high crash frequency likely have congestion
        crash_frequency = crash_frequency.withColumn(
            "congestion_indicator",
            F.when(F.col("avg_crashes_per_day") > 5, "CRITICAL - Very High Frequency")
            .when(F.col("avg_crashes_per_day") > 2, "HIGH - Elevated Frequency")
            .when(F.col("avg_crashes_per_day") > 0.5, "MODERATE - Regular Crashes")
            .otherwise("LOW - Infrequent Crashes")
        ).orderBy(F.col("crash_frequency").desc())
        
        # Show streets with highest crash frequency (likely congestion zones)
        high_frequency = crash_frequency.filter(F.col("crash_frequency") > 10)
        print("STREETS WITH HIGH CRASH FREQUENCY (Potential Congestion Zones):")
        high_frequency.select(
            F.col("on_street_name").alias("street"),
            F.col("crash_frequency"),
            F.col("avg_crashes_per_day"),
            F.col("total_fatalities"),
            F.col("total_injuries"),
            F.col("congestion_indicator")
        ).limit(15).show(15, truncate=False)
        
        # Export crash frequency analysis
        frequency_output = os.path.join(output_dir, "crash_frequency_congestion_analysis.csv")
        crash_frequency.select(
            F.col("on_street_name").alias("street"),
            F.col("crash_frequency"),
            F.col("days_with_crashes"),
            F.col("avg_crashes_per_day"),
            F.col("total_fatalities"),
            F.col("total_injuries"),
            F.col("congestion_indicator")
        ).coalesce(1).write.mode("overwrite").csv(frequency_output, header=True)
        print(f"\n[OK] Crash frequency analysis exported to: {frequency_output}\n")
        
        # Summary insights
        print("\n[INSIGHTS]")
        print("=" * 80)
        print("To fully answer question (d):")
        print("1. Change MONGO_COLLECTION to 'speeds' and run again to get congestion data")
        print("2. Compare 'danger_score' streets with low speed streets")
        print("3. Streets appearing in both datasets confirm congestion-collision relationship")
        print("=" * 80)
        
    except Exception as e:
        print(f"Error during crash analysis: {e}")
        print("\nShowing first 10 crash records instead:")
        df.limit(10).show(10, truncate=False)

print("\n[OK] Analysis complete! Check the 'spark_analysis_output' folder for results.")
print(f"Analysis type: {MONGO_COLLECTION.upper()}")
print("="*80)

# Suppress stderr during Spark shutdown to hide temp cleanup errors
sys.stderr = open(os.devnull, 'w')
spark.stop()
sys.stderr = sys.__stderr__