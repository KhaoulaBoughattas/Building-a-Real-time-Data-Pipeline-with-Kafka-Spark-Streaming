# StreamAggGender.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as spark_sum
from pyspark.sql.types import StructType, StringType, IntegerType

KAFKA_TOPIC = "topicA"
KAFKA_BOOTSTRAP = "kafka:9092"

schema = StructType() \
    .add("id", StringType()) \
    .add("first_name", StringType()) \
    .add("last_name", StringType()) \
    .add("country", StringType()) \
    .add("product_name", StringType()) \
    .add("gender", StringType()) \
    .add("quantity", StringType())

spark = SparkSession.builder \
    .appName("StreamAggGender") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("quantity", col("quantity").cast(IntegerType()))

agg = parsed.groupBy("gender") \
    .agg(spark_sum("quantity").alias("total_order_amount"))

query = agg.writeStream \
    .format("mongodb") \
    .option(
        "spark.mongodb.connection.uri",
        "mongodb://root:pass@mongo:27017/Analysis.GenderAmount?authSource=admin"
    ) \
    .option("checkpointLocation", "/tmp/checkpoint_gender") \
    .outputMode("complete") \
    .start()

query.awaitTermination()
