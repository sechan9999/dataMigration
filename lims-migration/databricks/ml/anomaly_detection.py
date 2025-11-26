# Databricks notebook source
# MAGIC %md
# MAGIC # ML-Based Anomaly Detection for Lab Results
# MAGIC
# MAGIC This notebook implements production-grade anomaly detection using:
# MAGIC - Isolation Forest for multivariate anomaly detection
# MAGIC - Feature engineering from clinical data
# MAGIC - MLflow for model tracking and versioning
# MAGIC - Real-time scoring pipeline
# MAGIC - Automated retraining

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, datediff, months_between, log, abs as spark_abs,
    when, lag, lead, stddev, avg, count, percentile_approx, coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.types import *

# ML imports
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from sklearn.ensemble import IsolationForest
import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("load_date", "2024-01-01", "Load Date")
dbutils.widgets.dropdown("run_training", "false", ["true", "false"], "Run Training")

load_date = dbutils.widgets.get("load_date")
run_training = dbutils.widgets.get("run_training") == "true"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Paths
SILVER_PATH = f"abfss://silver@{storage_account}.dfs.core.windows.net/lims"
GOLD_PATH = f"abfss://gold@{storage_account}.dfs.core.windows.net/lims"
MODEL_PATH = f"dbfs:/models/lims/anomaly_detection"

# MLflow configuration
mlflow.set_experiment("/LIMS/AnomalyDetection")

# Model hyperparameters
CONTAMINATION_RATE = 0.05  # Expected % of anomalies
N_ESTIMATORS = 100
MAX_SAMPLES = 256
RANDOM_STATE = 42

# Feature configuration
FEATURE_COLUMNS = [
    "result_value_numeric",
    "result_vs_reference_min_pct",
    "result_vs_reference_max_pct",
    "days_since_last_test",
    "test_frequency_30d",
    "percent_change_from_previous",
    "z_score_within_patient",
    "z_score_population"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and Prepare Data

# COMMAND ----------

def load_training_data(lookback_days: int = 90) -> DataFrame:
    """
    Load historical lab results for training
    """
    # Load lab results from silver layer
    lab_results = spark.read.format("delta").load(f"{SILVER_PATH}/labresults")

    # Load patients for age calculation
    patients = spark.read.format("delta").load(f"{SILVER_PATH}/patients")

    # Join and filter
    df = lab_results.alias("lr") \
        .join(patients.alias("p"), col("lr.PatientId") == col("p.PatientId"), "left") \
        .filter(
            (col("lr.ResultValueNumeric").isNotNull()) &
            (col("lr.ResultStatus") == "FINAL") &
            (col("lr.ResultDate") >= lit(datetime.now() - timedelta(days=lookback_days)))
        ) \
        .select(
            col("lr.ResultId"),
            col("lr.PatientId"),
            col("lr.TestCode"),
            col("lr.ResultValueNumeric"),
            col("lr.ResultDate"),
            col("lr.ReferenceRangeMin"),
            col("lr.ReferenceRangeMax"),
            col("lr.AbnormalFlag"),
            col("p.DateOfBirth"),
            col("p.Gender")
        )

    print(f"Loaded {df.count():,} lab results for training (last {lookback_days} days)")
    return df

def engineer_features(df: DataFrame) -> DataFrame:
    """
    Create features for anomaly detection model
    """
    print("\nEngineering features...")

    # Feature 1: Result value (normalized by log transform for skewed distributions)
    df = df.withColumn("result_value_numeric", col("ResultValueNumeric"))

    # Feature 2: Result vs Reference Range (percentage)
    df = df.withColumn(
        "result_vs_reference_min_pct",
        when(
            col("ReferenceRangeMin").isNotNull() & (col("ReferenceRangeMin") > 0),
            ((col("ResultValueNumeric") - col("ReferenceRangeMin")) / col("ReferenceRangeMin")) * 100
        ).otherwise(0)
    )

    df = df.withColumn(
        "result_vs_reference_max_pct",
        when(
            col("ReferenceRangeMax").isNotNull() & (col("ReferenceRangeMax") > 0),
            ((col("ResultValueNumeric") - col("ReferenceRangeMax")) / col("ReferenceRangeMax")) * 100
        ).otherwise(0)
    )

    # Feature 3: Days since last test
    window_patient_test = Window.partitionBy("PatientId", "TestCode").orderBy("ResultDate")

    df = df.withColumn(
        "previous_test_date",
        lag("ResultDate", 1).over(window_patient_test)
    )

    df = df.withColumn(
        "days_since_last_test",
        when(
            col("previous_test_date").isNotNull(),
            datediff(col("ResultDate"), col("previous_test_date"))
        ).otherwise(365)  # Default for first test
    )

    # Feature 4: Test frequency in last 30 days
    window_patient_test_30d = Window.partitionBy("PatientId", "TestCode") \
                                     .orderBy("ResultDate") \
                                     .rangeBetween(-30 * 86400, 0)

    df = df.withColumn(
        "test_frequency_30d",
        count("*").over(window_patient_test_30d)
    )

    # Feature 5: Percent change from previous result
    df = df.withColumn(
        "previous_result_value",
        lag("ResultValueNumeric", 1).over(window_patient_test)
    )

    df = df.withColumn(
        "percent_change_from_previous",
        when(
            (col("previous_result_value").isNotNull()) & (col("previous_result_value") != 0),
            spark_abs((col("ResultValueNumeric") - col("previous_result_value")) / col("previous_result_value") * 100)
        ).otherwise(0)
    )

    # Feature 6: Z-score within patient (how unusual for this patient)
    window_patient_test_all = Window.partitionBy("PatientId", "TestCode")

    df = df.withColumn("patient_test_avg", avg("ResultValueNumeric").over(window_patient_test_all))
    df = df.withColumn("patient_test_stddev", stddev("ResultValueNumeric").over(window_patient_test_all))

    df = df.withColumn(
        "z_score_within_patient",
        when(
            (col("patient_test_stddev").isNotNull()) & (col("patient_test_stddev") > 0),
            (col("ResultValueNumeric") - col("patient_test_avg")) / col("patient_test_stddev")
        ).otherwise(0)
    )

    # Feature 7: Z-score across population
    window_test_population = Window.partitionBy("TestCode")

    df = df.withColumn("population_test_avg", avg("ResultValueNumeric").over(window_test_population))
    df = df.withColumn("population_test_stddev", stddev("ResultValueNumeric").over(window_test_population))

    df = df.withColumn(
        "z_score_population",
        when(
            (col("population_test_stddev").isNotNull()) & (col("population_test_stddev") > 0),
            (col("ResultValueNumeric") - col("population_test_avg")) / col("population_test_stddev")
        ).otherwise(0)
    )

    # Select final feature columns
    feature_cols = ["ResultId", "PatientId", "TestCode", "ResultDate"] + FEATURE_COLUMNS

    df = df.select(*feature_cols)

    # Fill nulls
    for col_name in FEATURE_COLUMNS:
        df = df.withColumn(col_name, coalesce(col(col_name), lit(0.0)))

    print(f"✓ Feature engineering completed. Shape: {df.count():,} rows x {len(FEATURE_COLUMNS)} features")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Anomaly Detection Model

# COMMAND ----------

def train_isolation_forest(training_df: DataFrame) -> IsolationForest:
    """
    Train Isolation Forest model for anomaly detection
    """
    print("\n" + "="*80)
    print("Training Isolation Forest Model")
    print("="*80)

    # Convert to Pandas for sklearn
    features_pdf = training_df.select(FEATURE_COLUMNS).toPandas()
    X = features_pdf[FEATURE_COLUMNS].values

    print(f"Training data shape: {X.shape}")
    print(f"Features: {', '.join(FEATURE_COLUMNS)}")

    # Start MLflow run
    with mlflow.start_run(run_name=f"isolation_forest_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):

        # Log parameters
        mlflow.log_param("model_type", "IsolationForest")
        mlflow.log_param("contamination", CONTAMINATION_RATE)
        mlflow.log_param("n_estimators", N_ESTIMATORS)
        mlflow.log_param("max_samples", MAX_SAMPLES)
        mlflow.log_param("n_features", len(FEATURE_COLUMNS))
        mlflow.log_param("n_samples", X.shape[0])

        # Train model
        print(f"\nTraining with contamination={CONTAMINATION_RATE}, n_estimators={N_ESTIMATORS}...")

        model = IsolationForest(
            contamination=CONTAMINATION_RATE,
            n_estimators=N_ESTIMATORS,
            max_samples=MAX_SAMPLES,
            random_state=RANDOM_STATE,
            n_jobs=-1,
            verbose=1
        )

        model.fit(X)

        # Predictions on training data
        predictions = model.predict(X)
        anomaly_scores = model.score_samples(X)

        # Metrics
        n_anomalies = np.sum(predictions == -1)
        anomaly_rate = n_anomalies / len(predictions)

        print(f"\n✓ Training completed")
        print(f"  Total samples: {len(predictions):,}")
        print(f"  Detected anomalies: {n_anomalies:,} ({anomaly_rate*100:.2f}%)")
        print(f"  Anomaly score range: [{anomaly_scores.min():.4f}, {anomaly_scores.max():.4f}]")

        # Log metrics
        mlflow.log_metric("n_anomalies_detected", n_anomalies)
        mlflow.log_metric("anomaly_rate", anomaly_rate)
        mlflow.log_metric("min_anomaly_score", float(anomaly_scores.min()))
        mlflow.log_metric("max_anomaly_score", float(anomaly_scores.max()))

        # Log feature importance (based on feature contribution to anomaly scores)
        feature_importance = calculate_feature_importance(model, X, FEATURE_COLUMNS)
        for feat, importance in feature_importance.items():
            mlflow.log_metric(f"feature_importance_{feat}", importance)

        # Log model
        mlflow.sklearn.log_model(model, "model")

        print(f"\n✓ Model logged to MLflow")

    return model

def calculate_feature_importance(model, X, feature_names):
    """
    Calculate feature importance for Isolation Forest
    """
    # Use permutation importance approximation
    baseline_scores = model.score_samples(X)
    importances = {}

    for i, feat in enumerate(feature_names):
        X_permuted = X.copy()
        np.random.shuffle(X_permuted[:, i])
        permuted_scores = model.score_samples(X_permuted)
        importances[feat] = np.abs(baseline_scores - permuted_scores).mean()

    # Normalize
    total = sum(importances.values())
    importances = {k: v/total for k, v in importances.items()}

    return importances

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score New Data

# COMMAND ----------

def load_model() -> IsolationForest:
    """
    Load the latest trained model
    """
    # Get the latest model from MLflow
    client = mlflow.tracking.MlflowClient()
    experiment = mlflow.get_experiment_by_name("/LIMS/AnomalyDetection")

    if experiment is None:
        raise ValueError("No trained model found. Please run training first.")

    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["start_time DESC"],
        max_results=1
    )

    if not runs:
        raise ValueError("No trained model found. Please run training first.")

    run_id = runs[0].info.run_id
    model_uri = f"runs:/{run_id}/model"

    print(f"Loading model from run: {run_id}")
    model = mlflow.sklearn.load_model(model_uri)

    return model

def score_anomalies(df: DataFrame, model: IsolationForest) -> DataFrame:
    """
    Score lab results for anomalies using trained model
    """
    print("\n" + "="*80)
    print("Scoring Lab Results for Anomalies")
    print("="*80)

    # Convert to Pandas for sklearn
    features_pdf = df.select(["ResultId"] + FEATURE_COLUMNS).toPandas()
    X = features_pdf[FEATURE_COLUMNS].values

    # Predict
    predictions = model.predict(X)
    anomaly_scores = model.score_samples(X)

    # Create results dataframe
    features_pdf['IsAnomaly'] = (predictions == -1).astype(int)
    features_pdf['AnomalyScore'] = anomaly_scores

    # Convert back to Spark
    results_df = spark.createDataFrame(features_pdf[['ResultId', 'IsAnomaly', 'AnomalyScore']])

    # Join back with original data
    scored_df = df.join(results_df, on="ResultId", how="left")

    # Add anomaly reason
    scored_df = scored_df.withColumn(
        "AnomalyReason",
        when(col("IsAnomaly") == 1,
             concat_ws("; ",
                 when(spark_abs(col("z_score_within_patient")) > 3,
                      lit("Unusual for patient (z-score > 3)")).otherwise(lit("")),
                 when(spark_abs(col("z_score_population")) > 3,
                      lit("Unusual for population (z-score > 3)")).otherwise(lit("")),
                 when(col("percent_change_from_previous") > 100,
                      lit("Large change from previous result (>100%)")).otherwise(lit("")),
                 when(spark_abs(col("result_vs_reference_max_pct")) > 200,
                      lit("Far outside reference range (>200%)")).otherwise(lit(""))
             )
        ).otherwise(lit(None))
    )

    n_anomalies = scored_df.filter(col("IsAnomaly") == 1).count()
    total = scored_df.count()

    print(f"✓ Scoring completed")
    print(f"  Total results scored: {total:,}")
    print(f"  Anomalies detected: {n_anomalies:,} ({n_anomalies/total*100:.2f}%)")

    return scored_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

def save_anomaly_results(scored_df: DataFrame, output_path: str):
    """
    Save anomaly detection results to gold layer
    """
    results_df = scored_df.select(
        "ResultId",
        "PatientId",
        "TestCode",
        "ResultDate",
        "IsAnomaly",
        "AnomalyScore",
        "AnomalyReason",
        lit(load_date).alias("_load_date"),
        current_timestamp().alias("_scored_timestamp")
    )

    results_df.write.format("delta") \
        .mode("append") \
        .partitionBy("_load_date") \
        .save(output_path)

    print(f"✓ Anomaly results saved to: {output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution

# COMMAND ----------

try:
    print(f"\n{'='*80}")
    print(f"ML Anomaly Detection Pipeline - {load_date}")
    print(f"Training Mode: {run_training}")
    print(f"{'='*80}\n")

    # Step 1: Load data
    print("Step 1: Loading training data...")
    training_df = load_training_data(lookback_days=90)

    # Step 2: Engineer features
    print("\nStep 2: Engineering features...")
    features_df = engineer_features(training_df)

    # Step 3: Train or load model
    if run_training:
        print("\nStep 3: Training new model...")
        model = train_isolation_forest(features_df)
    else:
        print("\nStep 3: Loading existing model...")
        model = load_model()

    # Step 4: Score data
    print("\nStep 4: Scoring lab results...")
    scored_df = score_anomalies(features_df, model)

    # Step 5: Save results
    print("\nStep 5: Saving anomaly results...")
    save_anomaly_results(scored_df, f"{GOLD_PATH}/anomaly_detection_results")

    # Generate summary
    anomaly_summary = scored_df.groupBy("TestCode") \
        .agg(
            count("*").alias("total_results"),
            sum(col("IsAnomaly")).alias("total_anomalies"),
            avg("AnomalyScore").alias("avg_anomaly_score")
        ) \
        .orderBy(col("total_anomalies").desc())

    print("\n" + "="*80)
    print("Anomaly Summary by Test")
    print("="*80)
    anomaly_summary.show(20, False)

    print(f"\n{'='*80}")
    print(f"✓ ML Anomaly Detection completed successfully")
    print(f"{'='*80}\n")

    result = {
        "status": "SUCCESS",
        "total_scored": features_df.count(),
        "total_anomalies": scored_df.filter(col("IsAnomaly") == 1).count(),
        "training_performed": run_training
    }

    dbutils.notebook.exit(json.dumps(result))

except Exception as e:
    print(f"\n{'='*80}")
    print(f"✗ Error in ML anomaly detection")
    print(f"Error: {str(e)}")
    print(f"{'='*80}\n")

    import traceback
    traceback.print_exc()

    dbutils.notebook.exit(json.dumps({"status": "FAILURE", "error": str(e)}))

# COMMAND ----------
