"""
MLlib Model Training -- Step 4 (fraud prediction model)
--------------------------------------------------------
Trains a fraud detection model on HISTORICAL data stored in HDFS,
as the project guidelines require ("Train the model using historical
data stored in HDFS").

What this script does:
  1. Reads transactions.csv from HDFS (hdfs://namenode:9000/fraud_project/data/)
  2. Takes a random sample (default 5% ~ 675K rows) -- Docker only has 2-3GB
     of memory on this laptop, so training on all 13.5M rows would crash it
  3. Cleans the data and builds the SAME features as the Stage 2 streaming
     consumer (so the model sees identical inputs when it runs on live data)
  4. Handles class imbalance (fraud is only a few % of rows) with class weights
  5. Trains two MLlib models: Logistic Regression and Random Forest
  6. Evaluates both on a 20% test set (AUC-ROC, AUC-PR, precision, recall, F1)
  7. Saves the better model (full pipeline) to HDFS at
     /fraud_project/models/fraud_model -- the streaming consumer will load it
  8. Writes all metrics to /tmp/models/training_metrics.json (for the report)

How to run (from Windows cmd, all on ONE line):

    docker exec -it spark-master /opt/spark/bin/spark-submit --driver-memory 512m --executor-memory 768m /tmp/models/train_fraud_model.py --sample-fraction 0.05

TIP: stop kafka, zookeeper and mysql first (docker stop kafka zookeeper mysql)
-- training doesn't use them, and it frees memory for Spark.

No Kafka JARs are needed here -- this is a batch job reading from HDFS.
"""

import argparse
import json
import os
import time

from pyspark import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_timestamp, when, round as spark_round
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType
)

HDFS_DATA_PATH = "hdfs://namenode:9000/fraud_project/data/transactions.csv"
HDFS_MODEL_PATH = "hdfs://namenode:9000/fraud_project/models/fraud_model"
LOCAL_METRICS_PATH = "/tmp/models/training_metrics.json"

# Same thresholds as the Stage 2 streaming consumer
HIGH_RISK_MERCHANTS = ["jewelry", "electronics"]
FAR_FROM_HOME_KM = 500
HIGH_VELOCITY_TXNS = 3

# Same columns as the CSV / Kafka messages
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

CATEGORICAL_COLS = ["merchant_category", "device_type"]
NUMERIC_FEATURES = [
    "amount", "avg_txn_amount_last_30d", "amount_to_avg_ratio",
    "distance_from_home_km", "transactions_last_1h", "hour_of_day",
    "customer_age", "customer_tenure_days",
    "is_high_risk_merchant", "is_far_from_home", "is_high_velocity",
    "card_present_int",
]


def clean_and_add_features(df):
    """Cleaning + feature engineering -- identical logic to spark_consumer_stage2.py."""
    return df \
        .dropna(subset=["transaction_id", "timestamp", "amount", "avg_txn_amount_last_30d", "is_fraud"]) \
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


def build_preprocessing_stages():
    """Turn text categories into numbers and combine everything into one 'features' vector."""
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in CATEGORICAL_COLS
    ]
    encoder = OneHotEncoder(
        inputCols=[f"{c}_idx" for c in CATEGORICAL_COLS],
        outputCols=[f"{c}_vec" for c in CATEGORICAL_COLS],
        handleInvalid="keep",
    )
    assembler = VectorAssembler(
        inputCols=NUMERIC_FEATURES + [f"{c}_vec" for c in CATEGORICAL_COLS],
        outputCol="features",
        handleInvalid="skip",
    )
    return indexers + [encoder, assembler]


def evaluate(predictions, model_name):
    """AUC-ROC, AUC-PR, and confusion-matrix based metrics at the default 0.5 threshold."""
    auc_roc = BinaryClassificationEvaluator(
        labelCol="is_fraud", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    ).evaluate(predictions)
    auc_pr = BinaryClassificationEvaluator(
        labelCol="is_fraud", rawPredictionCol="rawPrediction", metricName="areaUnderPR"
    ).evaluate(predictions)

    rows = predictions.groupBy("is_fraud", "prediction").count().collect()
    cm = {(int(r["is_fraud"]), int(r["prediction"])): r["count"] for r in rows}
    tp, fp = cm.get((1, 1), 0), cm.get((0, 1), 0)
    tn, fn = cm.get((0, 0), 0), cm.get((1, 0), 0)

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    accuracy = (tp + tn) / max(tp + tn + fp + fn, 1)

    metrics = {
        "model": model_name,
        "auc_roc": round(auc_roc, 4),
        "auc_pr": round(auc_pr, 4),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "accuracy": round(accuracy, 4),
        "confusion_matrix": {"TP": tp, "FP": fp, "TN": tn, "FN": fn},
    }

    print(f"\n===== {model_name} =====")
    print(f"AUC-ROC  : {metrics['auc_roc']}")
    print(f"AUC-PR   : {metrics['auc_pr']}")
    print(f"Precision: {metrics['precision']}   (of transactions flagged as fraud, how many really were)")
    print(f"Recall   : {metrics['recall']}   (of all real frauds, how many we caught)")
    print(f"F1       : {metrics['f1']}")
    print(f"Accuracy : {metrics['accuracy']}   (misleading on imbalanced data -- don't rely on it)")
    print(f"Confusion matrix -> TP={tp:,}  FP={fp:,}  TN={tn:,}  FN={fn:,}")
    return metrics


def feature_names(predictions):
    """Recover readable feature names from the assembled vector (for RF feature importance)."""
    attrs = predictions.schema["features"].metadata["ml_attr"]["attrs"]
    idx_to_name = {}
    for group in attrs.values():
        for a in group:
            idx_to_name[a["idx"]] = a["name"]
    return [idx_to_name[i] for i in range(len(idx_to_name))]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample-fraction", type=float, default=0.05,
                        help="Fraction of the full dataset to train on (0.05 = ~675K rows)")
    parser.add_argument("--rf-trees", type=int, default=30)
    parser.add_argument("--rf-depth", type=int, default=8)
    args = parser.parse_args()

    start = time.time()

    spark = SparkSession.builder \
        .appName("FraudDetectionModelTraining") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ---- 1-3. Load from HDFS, sample, clean, feature engineering ----
    print(f"Reading historical data from HDFS: {HDFS_DATA_PATH}")
    raw = spark.read.csv(HDFS_DATA_PATH, header=True, schema=transaction_schema)

    # Keep ONLY the columns the model needs before caching -- IDs, timestamps and
    # city names are dropped, which cuts memory use a lot on a small machine
    data = clean_and_add_features(raw.sample(fraction=args.sample_fraction, seed=42)) \
        .select(*(NUMERIC_FEATURES + CATEGORICAL_COLS + ["is_fraud"]))
    data = data.persist(StorageLevel.MEMORY_AND_DISK)

    total = data.count()
    frauds = data.filter(col("is_fraud") == 1).count()
    fraud_rate = frauds / total
    print(f"Sampled rows: {total:,} | Fraud rows: {frauds:,} | Fraud rate: {fraud_rate:.2%}")

    # ---- 4. Class weights: make each fraud row count more, so the model
    #         doesn't just learn "everything is legit" (which would already be ~95% accurate)
    data = data.withColumn(
        "class_weight",
        when(col("is_fraud") == 1, 0.5 / fraud_rate).otherwise(0.5 / (1 - fraud_rate))
    )

    train, test = data.randomSplit([0.8, 0.2], seed=42)
    print(f"Train rows: {train.count():,} | Test rows: {test.count():,}")

    # ---- 5. Train both models ----
    prep = build_preprocessing_stages()

    lr = LogisticRegression(labelCol="is_fraud", featuresCol="features",
                            weightCol="class_weight", maxIter=50)
    rf = RandomForestClassifier(labelCol="is_fraud", featuresCol="features",
                                weightCol="class_weight", numTrees=args.rf_trees,
                                maxDepth=args.rf_depth, seed=42)

    results = {}
    models = {}
    for name, clf in [("LogisticRegression", lr), ("RandomForest", rf)]:
        t0 = time.time()
        print(f"\nTraining {name}...")
        model = Pipeline(stages=prep + [clf]).fit(train)
        preds = model.transform(test)
        metrics = evaluate(preds, name)
        metrics["training_seconds"] = round(time.time() - t0, 1)
        print(f"Time: {metrics['training_seconds']}s")
        results[name] = metrics
        models[name] = (model, preds)

    # ---- Random Forest feature importance (great for the report's Findings section) ----
    rf_model, rf_preds = models["RandomForest"]
    importances = rf_model.stages[-1].featureImportances.toArray()
    names = feature_names(rf_preds)
    ranked = sorted(zip(names, importances), key=lambda x: x[1], reverse=True)
    print("\nTop 10 features (Random Forest importance):")
    for n, v in ranked[:10]:
        print(f"  {n:<35} {v:.4f}")

    # ---- 6-7. Pick the best model by AUC-PR (the right metric for rare fraud) and save it ----
    best_name = max(results, key=lambda k: results[k]["auc_pr"])
    best_model = models[best_name][0]
    print(f"\nBest model (by AUC-PR): {best_name}")
    best_model.write().overwrite().save(HDFS_MODEL_PATH)
    print(f"Saved best model pipeline to: {HDFS_MODEL_PATH}")

    # ---- 8. Save metrics for the report ----
    summary = {
        "sample_fraction": args.sample_fraction,
        "sampled_rows": total,
        "fraud_rows": frauds,
        "fraud_rate": round(fraud_rate, 4),
        "features_used": NUMERIC_FEATURES + CATEGORICAL_COLS,
        "models": results,
        "rf_feature_importance_top10": [{"feature": n, "importance": round(float(v), 4)} for n, v in ranked[:10]],
        "best_model": best_name,
        "model_path": HDFS_MODEL_PATH,
        "total_runtime_seconds": round(time.time() - start, 1),
    }
    os.makedirs(os.path.dirname(LOCAL_METRICS_PATH), exist_ok=True)
    with open(LOCAL_METRICS_PATH, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"Metrics written to: {LOCAL_METRICS_PATH}")
    print(f"\nDone in {summary['total_runtime_seconds']}s")

    data.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
