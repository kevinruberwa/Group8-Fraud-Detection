"""
PySpark Structured Streaming Consumer -- Stage 3 (live predictions -> MySQL)
-----------------------------------------------------------------------------
The full real-time pipeline:

  Kafka 'transactions_topic'
      -> clean + feature engineering (same logic as Stage 2 and training)
      -> trained MLlib model loaded from HDFS (/fraud_project/models/fraud_model)
      -> fraud probability + prediction + risk level for every transaction
      -> written into MySQL table fraud_db.fraud_predictions (read by Django)

Risk levels (from the model's fraud probability):
  HIGH   : probability >= 0.80
  MEDIUM : probability >= 0.50   (the model's default "flag as fraud" threshold)
  LOW    : below 0.50

Progress is checkpointed in HDFS, so if this job is stopped and restarted it
continues where it left off instead of re-inserting the same transactions.

How to run (from Windows cmd, all on ONE line):

    docker exec -it spark-master /opt/spark/bin/spark-submit --driver-memory 512m --executor-memory 768m --jars /tmp/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar,/tmp/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar,/tmp/jars/kafka-clients-3.4.1.jar,/tmp/jars/commons-pool2-2.11.1.jar,/tmp/jars/mysql-connector-j-8.0.33.jar /tmp/consumer/spark_consumer_stage3.py
"""

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, hour, lit, round as spark_round, to_timestamp, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType
)

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "transactions_topic"
HDFS_MODEL_PATH = "hdfs://namenode:9000/fraud_project/models/fraud_model"
CHECKPOINT_PATH = "hdfs://namenode:9000/fraud_project/checkpoints/stage3"

MYSQL_URL = "jdbc:mysql://mysql:3306/fraud_db?useSSL=false&allowPublicKeyRetrieval=true"
MYSQL_TABLE = "fraud_predictions"
MYSQL_PROPS = {"user": "bde_user", "password": "bde_pw", "driver": "com.mysql.cj.jdbc.Driver"}

# Same thresholds as Stage 2 and the training script
HIGH_RISK_MERCHANTS = ["jewelry", "electronics"]
FAR_FROM_HOME_KM = 500
HIGH_VELOCITY_TXNS = 3

HIGH_RISK_PROB = 0.80
MEDIUM_RISK_PROB = 0.50

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

# Columns written to MySQL (must match scripts/create_tables.sql)
OUTPUT_COLS = [
    "transaction_id", "customer_id", "event_time", "amount", "merchant_category",
    "device_type", "txn_city", "card_present", "distance_from_home_km",
    "transactions_last_1h", "amount_to_avg_ratio", "hour_of_day",
    "is_high_risk_merchant", "is_far_from_home", "is_high_velocity",
    "fraud_probability", "prediction", "risk_level", "is_fraud",
]


def clean_and_add_features(df):
    """Cleaning + feature engineering -- identical logic to Stage 2 and train_fraud_model.py."""
    return df \
        .dropna(subset=["transaction_id", "timestamp", "amount", "avg_txn_amount_last_30d"]) \
        .filter(col("amount") > 0) \
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .dropna(subset=["event_time"]) \
        .withColumn(
            "amount_to_avg_ratio",
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


def score(df, model):
    """Apply the trained pipeline and turn its output into dashboard-friendly columns."""
    fraud_prob = vector_to_array(col("probability"))[1]   # probability of class 1 (fraud)
    return model.transform(df) \
        .withColumn("fraud_probability", spark_round(fraud_prob, 4)) \
        .withColumn("prediction", col("prediction").cast("int")) \
        .withColumn(
            "risk_level",
            when(col("fraud_probability") >= HIGH_RISK_PROB, lit("HIGH"))
            .when(col("fraud_probability") >= MEDIUM_RISK_PROB, lit("MEDIUM"))
            .otherwise(lit("LOW"))
        ) \
        .withColumn("transaction_id", col("transaction_id").cast("long")) \
        .withColumn("customer_id", col("customer_id").cast("long")) \
        .select(*OUTPUT_COLS)


def write_batch_to_mysql(batch_df, batch_id):
    """Called by Spark for every micro-batch: write the scored rows into MySQL."""
    batch_df = batch_df.withColumn("batch_id", lit(batch_id)).persist()
    rows = batch_df.count()
    if rows == 0:
        batch_df.unpersist()
        return

    flagged = batch_df.filter(col("prediction") == 1).count()
    high = batch_df.filter(col("risk_level") == "HIGH").count()
    actual = batch_df.filter(col("is_fraud") == 1).count()

    batch_df.write.jdbc(url=MYSQL_URL, table=MYSQL_TABLE, mode="append", properties=MYSQL_PROPS)

    print(f"Batch {batch_id}: {rows} transactions scored -> MySQL | "
          f"flagged: {flagged} (HIGH risk: {high}) | actual frauds in batch: {actual}")
    batch_df.unpersist()


def main():
    spark = SparkSession.builder \
        .appName("FraudDetectionConsumerStage3") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Loading trained model from HDFS: {HDFS_MODEL_PATH}")
    model = PipelineModel.load(HDFS_MODEL_PATH)

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 2000) \
        .load()

    parsed = raw_stream.select(
        from_json(col("value").cast("string"), transaction_schema).alias("data")
    ).select("data.*")

    scored = score(clean_and_add_features(parsed), model)

    query = scored.writeStream \
        .foreachBatch(write_batch_to_mysql) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime="5 seconds") \
        .start()

    print(f"Stage 3 consumer started. Scoring '{KAFKA_TOPIC}' in real time -> MySQL {MYSQL_TABLE}... (Ctrl+C to stop)")
    query.awaitTermination()


if __name__ == "__main__":
    main()
