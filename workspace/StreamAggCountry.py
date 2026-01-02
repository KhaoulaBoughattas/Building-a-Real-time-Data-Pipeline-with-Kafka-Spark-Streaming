# StreamAggCountry.py
# ==========================================================
# PURPOSE:
# Spark Structured Streaming job consumes real-time
# user/order events from Kafka, aggregates order quantities
# by country, and stores the results in MongoDB.
# ==========================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as spark_sum
from pyspark.sql.types import StructType, StringType, IntegerType

# ==========================================================
# Kafka Configuration
# ==========================================================
KAFKA_TOPIC = "topicA"
KAFKA_BOOTSTRAP = "kafka:9092"

# ==========================================================
# Schema Definition
# ==========================================================
schema = StructType() \
    .add("id", StringType()) \
    .add("first_name", StringType()) \
    .add("last_name", StringType()) \
    .add("country", StringType()) \
    .add("product_name", StringType()) \
    .add("gender", StringType()) \
    .add("quantity", StringType())

# ==========================================================
# Spark Session Initialization
# ==========================================================
spark = SparkSession.builder \
    .appName("StreamAggCountry") \
    .config("spark.jars", "/opt/spark/jars/mongo-spark-connector_2.12-10.2.2.jar,/opt/spark/jars/mongodb-driver-sync-4.9.0.jar") \
    .config("spark.mongodb.output.uri", "mongodb://root:pass@mongo:27017/Analysis.CountryAmount?authSource=admin") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================================
# Reading Stream from Kafka
# ==========================================================
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# ==========================================================
# JSON Parsing and Data Cleaning
# ==========================================================
parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("quantity", col("quantity").cast(IntegerType()))

# ==========================================================
# Streaming Aggregation
# ==========================================================
agg = parsed.groupBy("country") \
    .agg(spark_sum("quantity").alias("total_order_amount"))

# ==========================================================
# Writing Results to MongoDB
# ==========================================================
query = agg.writeStream \
    .format("mongodb") \
    .option("database", "Analysis") \
    .option("collection", "CountryAmount") \
    .option("checkpointLocation", "/tmp/checkpoint_country") \
    .outputMode("complete") \
    .start()

# ==========================================================
# Stream Execution
# ==========================================================
query.awaitTermination()
