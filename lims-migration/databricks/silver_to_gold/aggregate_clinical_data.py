# Databricks notebook source
# MAGIC %md
# MAGIC # Silver to Gold - Clinical Data Aggregation
# MAGIC
# MAGIC This notebook aggregates validated clinical data from Silver layer to Gold layer:
# MAGIC - Patient lab summaries
# MAGIC - Test utilization metrics
# MAGIC - Quality metrics by laboratory
# MAGIC - Temporal trends and patterns

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, max as spark_max, min as spark_min,
    count, sum as spark_sum, avg, stddev, first, last,
    datediff, months_between, when, row_number
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("load_date", "2024-01-01", "Load Date")
load_date = dbutils.widgets.get("load_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

SILVER_PATH = f"abfss://silver@{storage_account}.dfs.core.windows.net/lims"
GOLD_PATH = f"abfss://gold@{storage_account}.dfs.core.windows.net/lims"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Data

# COMMAND ----------

lab_results = spark.read.format("delta").load(f"{SILVER_PATH}/labresults")
patients = spark.read.format("delta").load(f"{SILVER_PATH}/patients")
test_master = spark.read.format("delta").load(f"{SILVER_PATH}/testmaster")

print(f"Loaded {lab_results.count():,} lab results from silver layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregation 1: Patient Lab Summary

# COMMAND ----------

def create_patient_lab_summary(lab_results_df: DataFrame) -> DataFrame:
    """
    Create aggregated patient-test summary
    """
    print("\n" + "="*80)
    print("Creating Patient Lab Summary")
    print("="*80)

    summary = lab_results_df.groupBy("PatientId", "TestCode").agg(
        count("*").alias("TotalTests"),
        spark_sum(when(col("AbnormalFlag").isin(["H", "L", "HH", "LL"]), 1).otherwise(0)).alias("TotalAbnormal"),
        spark_sum(when(col("CriticalFlag") == True, 1).otherwise(0)).alias("TotalCritical"),
        spark_sum(when(col("IsAnomaly") == True, 1).otherwise(0)).alias("TotalAnomalies"),
        spark_min("ResultValueNumeric").alias("MinValue"),
        spark_max("ResultValueNumeric").alias("MaxValue"),
        avg("ResultValueNumeric").alias("AvgValue"),
        stddev("ResultValueNumeric").alias("StdDevValue"),
        spark_min("ResultDate").alias("FirstTestDate"),
        spark_max("ResultDate").alias("LastTestDate")
    )

    # Get current and previous values
    window_spec = Window.partitionBy("PatientId", "TestCode").orderBy(col("LastTestDate").desc())

    result_window = lab_results_df.select(
        "PatientId",
        "TestCode",
        "ResultDate",
        "ResultValueNumeric",
        row_number().over(window_spec).alias("rn")
    ).filter(col("rn").isin([1, 2]))

    current_values = result_window.filter(col("rn") == 1).select(
        col("PatientId"),
        col("TestCode"),
        col("ResultValueNumeric").alias("CurrentValue")
    )

    previous_values = result_window.filter(col("rn") == 2).select(
        col("PatientId"),
        col("TestCode"),
        col("ResultValueNumeric").alias("PreviousValue")
    )

    # Join everything
    summary = summary \
        .join(current_values, ["PatientId", "TestCode"], "left") \
        .join(previous_values, ["PatientId", "TestCode"], "left")

    # Calculate trend
    summary = summary.withColumn(
        "PercentChange",
        when(
            (col("PreviousValue").isNotNull()) & (col("PreviousValue") != 0),
            ((col("CurrentValue") - col("PreviousValue")) / col("PreviousValue") * 100)
        )
    )

    summary = summary.withColumn(
        "TrendDirection",
        when(col("PercentChange") > 10, "Increasing")
        .when(col("PercentChange") < -10, "Decreasing")
        .otherwise("Stable")
    )

    summary = summary.withColumn("LastUpdatedDate", current_timestamp())

    print(f"✓ Created patient lab summary: {summary.count():,} records")

    return summary

# Save patient lab summary
patient_summary = create_patient_lab_summary(lab_results)
patient_summary.write.format("delta") \
    .mode("overwrite") \
    .save(f"{GOLD_PATH}/patient_lab_summary")

print("✓ Patient lab summary saved to gold layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregation 2: Daily Quality Metrics

# COMMAND ----------

def create_daily_quality_metrics(lab_results_df: DataFrame) -> DataFrame:
    """
    Create daily quality metrics by laboratory and test
    """
    print("\n" + "="*80)
    print("Creating Daily Quality Metrics")
    print("="*80)

    # Aggregate by date, laboratory, and test
    daily_metrics = lab_results_df.groupBy(
        col("ResultDate").cast("date").alias("MetricDate"),
        "PerformingLabId",
        "TestCode"
    ).agg(
        count("*").alias("TotalTests"),
        spark_sum(when(col("CriticalFlag") == True, 1).otherwise(0)).alias("CriticalValues"),
        spark_sum(when(col("IsAnomaly") == True, 1).otherwise(0)).alias("AnomalousResults"),
        spark_sum(when(col("ReferenceRangeValid") == False, 1).otherwise(0)).alias("ValidationFailures"),
        avg("TurnaroundTimeMinutes").alias("AvgTurnaroundTimeMinutes"),
        spark_sum(when(col("ResultValueNumeric").isNull(), 1).otherwise(0)).alias("MissingResults")
    )

    # Calculate percentages
    daily_metrics = daily_metrics \
        .withColumn("CriticalValueRate", col("CriticalValues") / col("TotalTests") * 100) \
        .withColumn("AnomalyRate", col("AnomalousResults") / col("TotalTests") * 100) \
        .withColumn("ValidationFailureRate", col("ValidationFailures") / col("TotalTests") * 100) \
        .withColumn("CompletenessRate", (col("TotalTests") - col("MissingResults")) / col("TotalTests") * 100)

    daily_metrics = daily_metrics.withColumn("SnapshotDate", current_timestamp())

    print(f"✓ Created daily quality metrics: {daily_metrics.count():,} records")

    return daily_metrics

# Save daily quality metrics
quality_metrics = create_daily_quality_metrics(lab_results)
quality_metrics.write.format("delta") \
    .mode("append") \
    .partitionBy("MetricDate") \
    .save(f"{GOLD_PATH}/daily_quality_metrics")

print("✓ Daily quality metrics saved to gold layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregation 3: Test Utilization

# COMMAND ----------

def create_test_utilization(lab_results_df: DataFrame, test_master_df: DataFrame) -> DataFrame:
    """
    Create test utilization metrics
    """
    print("\n" + "="*80)
    print("Creating Test Utilization Metrics")
    print("="*80)

    utilization = lab_results_df.groupBy("TestCode").agg(
        count("*").alias("TotalOrders"),
        count(col("ResultValueNumeric")).alias("CompletedResults"),
        count(col("PatientId").alias("UniquePatients")),
        avg("TurnaroundTimeMinutes").alias("AvgTurnaroundTime"),
        spark_sum(when(col("CriticalFlag") == True, 1).otherwise(0)).alias("CriticalCount")
    )

    # Join with test master
    utilization = utilization.join(test_master_df, "TestCode", "left")

    # Calculate utilization rate
    utilization = utilization.withColumn(
        "CompletionRate",
        col("CompletedResults") / col("TotalOrders") * 100
    )

    utilization = utilization.withColumn("CalculatedDate", lit(load_date))

    print(f"✓ Created test utilization: {utilization.count():,} records")

    return utilization

# Save test utilization
test_utilization = create_test_utilization(lab_results, test_master)
test_utilization.write.format("delta") \
    .mode("overwrite") \
    .save(f"{GOLD_PATH}/test_utilization")

print("✓ Test utilization saved to gold layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregation 4: Critical Value Alerts

# COMMAND ----------

def aggregate_critical_alerts(lab_results_df: DataFrame) -> DataFrame:
    """
    Aggregate critical value alerts for review
    """
    print("\n" + "="*80)
    print("Aggregating Critical Value Alerts")
    print("="*80)

    critical_results = lab_results_df.filter(col("CriticalFlag") == True) \
        .select(
            "ResultId",
            "PatientId",
            "TestCode",
            "ResultValueNumeric",
            "ResultDate",
            "PerformingLabId",
            "ReferenceRangeMin",
            "ReferenceRangeMax"
        )

    critical_results = critical_results.withColumn(
        "Severity",
        when(
            (col("ResultValueNumeric") < col("ReferenceRangeMin") * 0.5) |
            (col("ResultValueNumeric") > col("ReferenceRangeMax") * 2),
            "Critical"
        ).otherwise("High")
    )

    critical_results = critical_results.withColumn("AlertGeneratedDate", current_timestamp())

    print(f"✓ Aggregated {critical_results.count():,} critical alerts")

    return critical_results

# Save critical alerts
critical_alerts = aggregate_critical_alerts(lab_results)
critical_alerts.write.format("delta") \
    .mode("append") \
    .save(f"{GOLD_PATH}/critical_value_alerts")

print("✓ Critical alerts saved to gold layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*80)
print("Silver to Gold Aggregation Summary")
print("="*80)

summary_stats = {
    "patient_lab_summaries": patient_summary.count(),
    "daily_quality_metrics": quality_metrics.count(),
    "test_utilization": test_utilization.count(),
    "critical_alerts": critical_alerts.count()
}

for metric, value in summary_stats.items():
    print(f"{metric}: {value:,}")

print("\n" + "="*80)
print("✓ Silver to Gold aggregation completed successfully")
print("="*80)

result = {
    "status": "SUCCESS",
    "summary": summary_stats
}

dbutils.notebook.exit(json.dumps(result))

# COMMAND ----------
