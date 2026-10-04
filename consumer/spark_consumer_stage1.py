"""
PySpark Structured Streaming Consumer -- Stage 1 (verification)
------------------------------------------------------------------
Reads messages in real time from the 'transactions_topic' Kafka topic
and prints them to the console, so we can confirm the full pipeline
(HDFS -> Kafka producer -> PySpark consumer) actually works end-to-end
before adding feature engineering and ML predictions on top.

How to run (from Windows cmd, all on ONE line; the JARs are in the jars folder,
mounted at /tmp/jars):

    docker exec -it spark-master /opt/spark/bin/spark-submit --jars /tmp/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar,/tmp/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar,/tmp/jars/kafka-clients-3.4.1.jar,/tmp/jars/commons-pool2-2.11.1.jar /tmp/consumer/spark_consumer_stage1.py

This runs INSIDE the spark-master container, so it connects to Kafka
via "kafka:9092" (the internal Docker network address) -- NOT
localhost:29092 (that's only for connections from your Windows host).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType
)

# Schema must match the JSON fields sent by the Kafka producer
transaction_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("amount", DoubleType()),
    StructField("merchant_category", StringType()),
    StructField("device_type", StringType()),
    StructField("card_present", BooleanType()),
    StructField("customer_age", IntegerType()),
    StructField("customer_tenure_days", IntegerType()),
    StructField("avg_txn_amount_last_30d", DoubleType()),
    StructField("transactions_last_1h", IntegerType()),
    StructField("distance_from_home_km", DoubleType()),
    StructField("txn_city", StringType()),
    StructField("is_fraud", IntegerType()),
])

spark = SparkSession.builder \
    .appName("FraudDetectionConsumerStage1") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read raw messages from Kafka as a streaming DataFrame
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transactions_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Kafka messages arrive as raw bytes in a "value" column -- parse the JSON
parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), transaction_schema).alias("data")
).select("data.*")

# For this verification stage: just print each micro-batch to the console
query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 10) \
    .start()

print("Consumer started. Reading from 'transactions_topic'... (Ctrl+C to stop)")
query.awaitTermination()
