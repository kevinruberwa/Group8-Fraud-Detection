"""
PySpark Structured Streaming Consumer -- Stage 2 (cleaning + feature engineering)
---------------------------------------------------------------------------------
Builds on Stage 1: reads transactions in real time from the
'transactions_topic' Kafka topic, cleans them, and adds derived
features that the MLlib model (Step 4) will use for fraud prediction.

Derived features (each maps to a real-world fraud signal):
  - amount_to_avg_ratio   : amount / avg_txn_amount_last_30d
                            (spending far above the customer's normal level)
  - hour_of_day           : hour extracted from the transaction timestamp
  - is_high_risk_merchant : 1 if merchant is jewelry or electronics
                            (easy-to-resell goods, favoured by fraudsters)
  - is_far_from_home      : 1 if distance_from_home_km > 500
                            (card used far from where the customer lives)
  - is_high_velocity      : 1 if transactions_last_1h >= 3
                            (many transactions in a short burst)
  - card_present_int      : card_present as 0/1 (numeric, for MLlib)

How to run (from Windows cmd, all on ONE line):

    docker exec -it spark-master /opt/spark/bin/spark-submit --driver-memory 512m --executor-memory 768m --jars /tmp/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar,/tmp/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar,/tmp/jars/kafka-clients-3.4.1.jar,/tmp/jars/commons-pool2-2.11.1.jar /tmp/consumer/spark_consumer_stage2.py

This runs INSIDE the spark-master container, so it connects to Kafka
via "kafka:9092" (the internal Docker network address) -- NOT
localhost:29092 (that's only for connections from your Windows host).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, hour, when, round as spark_round
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

HIGH_RISK_MERCHANTS = ["jewelry", "electronics"]
FAR_FROM_HOME_KM = 500
HIGH_VELOCITY_TXNS = 3

spark = SparkSession.builder \
    .appName("FraudDetectionConsumerStage2") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---- 1. Read raw messages from Kafka ----
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transactions_topic") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), transaction_schema).alias("data")
).select("data.*")

# ---- 2. Cleaning ----
# Drop malformed messages (failed JSON parse -> nulls) and invalid amounts,
# and convert the timestamp string into a real timestamp type.
clean_stream = parsed_stream \
    .dropna(subset=["transaction_id", "timestamp", "amount", "avg_txn_amount_last_30d"]) \
    .filter(col("amount") > 0) \
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .dropna(subset=["event_time"])

# ---- 3. Feature engineering ----
features_stream = clean_stream \
    .withColumn(
        "amount_to_avg_ratio",
        # guard against divide-by-zero when a customer's 30-day average is 0
        when(col("avg_txn_amount_last_30d") > 0,
             spark_round(col("amount") / col("avg_txn_amount_last_30d"), 2))
        .otherwise(spark_round(col("amount"), 2))
    ) \
    .withColumn("hour_of_day", hour(col("event_time"))) \
    .withColumn("is_high_risk_merchant",
                when(col("merchant_category").isin(HIGH_RISK_MERCHANTS), 1).otherwise(0)) \
    .withColumn("is_far_from_home",
                when(col("distance_from_home_km") > FAR_FROM_HOME_KM, 1).otherwise(0)) \
    .withColumn("is_high_velocity",
                when(col("transactions_last_1h") >= HIGH_VELOCITY_TXNS, 1).otherwise(0)) \
    .withColumn("card_present_int", col("card_present").cast("int"))

# For this test stage: show the key columns + new features in the console
display_cols = [
    "transaction_id", "amount", "merchant_category", "distance_from_home_km",
    "transactions_last_1h", "amount_to_avg_ratio", "hour_of_day",
    "is_high_risk_merchant", "is_far_from_home", "is_high_velocity",
    "card_present_int", "is_fraud",
]

query = features_stream.select(*display_cols).writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 15) \
    .start()

print("Stage 2 consumer started. Cleaning + feature engineering on 'transactions_topic'... (Ctrl+C to stop)")
query.awaitTermination()
