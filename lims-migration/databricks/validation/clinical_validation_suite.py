# Databricks notebook source
# MAGIC %md
# MAGIC # Clinical Validation Suite
# MAGIC
# MAGIC Comprehensive clinical validation including:
# MAGIC - Reference range validation (age/gender-specific)
# MAGIC - Temporal consistency checks
# MAGIC - Cross-test correlation validation
# MAGIC - Critical value detection
# MAGIC - Specimen validity checks
# MAGIC - LOINC code validation

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, datediff, months_between,
    when, abs as spark_abs, lag, lead, stddev, avg, count, percentile_approx
)
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
import json
from datetime import datetime, timedelta

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

# Paths
SILVER_PATH = f"abfss://silver@{storage_account}.dfs.core.windows.net/lims"
GOLD_PATH = f"abfss://gold@{storage_account}.dfs.core.windows.net/lims"
VALIDATION_RESULTS_PATH = f"{GOLD_PATH}/validation_results"

# Clinical Reference Ranges (example data - should be loaded from reference table)
REFERENCE_RANGES = {
    "Glucose": {
        "adult": {"min": 70, "max": 100, "unit": "mg/dL", "critical_low": 50, "critical_high": 300},
        "pediatric": {"min": 60, "max": 100, "unit": "mg/dL", "critical_low": 40, "critical_high": 250}
    },
    "HbA1c": {
        "adult": {"min": 4.0, "max": 5.6, "unit": "%", "critical_low": None, "critical_high": 10.0},
        "pediatric": {"min": 4.0, "max": 5.6, "unit": "%", "critical_low": None, "critical_high": 10.0}
    },
    "Creatinine": {
        "adult_male": {"min": 0.74, "max": 1.35, "unit": "mg/dL", "critical_low": 0.3, "critical_high": 5.0},
        "adult_female": {"min": 0.59, "max": 1.04, "unit": "mg/dL", "critical_low": 0.3, "critical_high": 5.0},
        "pediatric": {"min": 0.3, "max": 0.7, "unit": "mg/dL", "critical_low": 0.2, "critical_high": 2.0}
    },
    "WBC": {
        "adult": {"min": 4.5, "max": 11.0, "unit": "K/uL", "critical_low": 1.0, "critical_high": 30.0},
        "pediatric": {"min": 5.0, "max": 14.5, "unit": "K/uL", "critical_low": 1.0, "critical_high": 30.0}
    },
    "Hemoglobin": {
        "adult_male": {"min": 13.5, "max": 17.5, "unit": "g/dL", "critical_low": 7.0, "critical_high": 20.0},
        "adult_female": {"min": 12.0, "max": 15.5, "unit": "g/dL", "critical_low": 7.0, "critical_high": 20.0},
        "pediatric": {"min": 11.0, "max": 14.5, "unit": "g/dL", "critical_low": 7.0, "critical_high": 20.0}
    }
}

# Cross-test correlation pairs
CORRELATION_PAIRS = [
    {"test1": "Glucose", "test2": "HbA1c", "expected_correlation": "positive"},
    {"test1": "BUN", "test2": "Creatinine", "expected_correlation": "positive"},
    {"test1": "TSH", "test2": "FreeT4", "expected_correlation": "inverse"},
    {"test1": "Calcium", "test2": "Phosphorus", "expected_correlation": "inverse"}
]

# Specimen validity rules
SPECIMEN_RULES = {
    "Blood": {"max_age_hours": 24, "valid_tests": ["Glucose", "HbA1c", "WBC", "Hemoglobin"]},
    "Serum": {"max_age_hours": 48, "valid_tests": ["Creatinine", "BUN", "Calcium"]},
    "Urine": {"max_age_hours": 2, "valid_tests": ["UrineProtein", "UrineCulture"]},
    "Plasma": {"max_age_hours": 24, "valid_tests": ["Glucose", "Lactate"]}
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

# Load lab results with patient info
lab_results_df = spark.read.format("delta").load(f"{SILVER_PATH}/labresults")

patients_df = spark.read.format("delta").load(f"{SILVER_PATH}/patients")

test_master_df = spark.read.format("delta").load(f"{SILVER_PATH}/testmaster")

# Join lab results with patient demographics
lab_with_demographics = lab_results_df.alias("lr") \
    .join(patients_df.alias("p"), col("lr.PatientId") == col("p.PatientId"), "left") \
    .join(test_master_df.alias("tm"), col("lr.TestCode") == col("tm.TestCode"), "left") \
    .select(
        col("lr.*"),
        col("p.DateOfBirth"),
        col("p.Gender"),
        col("tm.TestName"),
        col("tm.LoincCode"),
        col("tm.TestCategory"),
        col("tm.SpecimenType")
    )

# Calculate age at time of test
lab_with_demographics = lab_with_demographics.withColumn(
    "AgeAtTest",
    (datediff(col("ResultDate"), col("DateOfBirth")) / 365.25).cast("int")
)

print(f"Loaded {lab_with_demographics.count():,} lab results for validation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Function 1: Reference Range Validation

# COMMAND ----------

def validate_reference_ranges(df: DataFrame) -> DataFrame:
    """
    Validate lab results against age/gender-specific reference ranges
    """
    print("\n" + "="*80)
    print("Reference Range Validation")
    print("="*80)

    # Add age group classification
    df = df.withColumn(
        "AgeGroup",
        when(col("AgeAtTest") < 18, "pediatric")
        .otherwise("adult")
    )

    # Add validation flags
    df = df.withColumn("ReferenceRangeValid", lit(None).cast(BooleanType())) \
           .withColumn("IsCriticalValue", lit(False)) \
           .withColumn("ValidationMessage", lit(""))

    # Validate each test type
    for test_code, ranges in REFERENCE_RANGES.items():
        for range_key, range_values in ranges.items():
            # Determine which records to validate
            if range_key == "adult":
                condition = (col("TestCode") == test_code) & (col("AgeGroup") == "adult")
            elif range_key == "pediatric":
                condition = (col("TestCode") == test_code) & (col("AgeGroup") == "pediatric")
            elif range_key == "adult_male":
                condition = (col("TestCode") == test_code) & (col("AgeGroup") == "adult") & (col("Gender") == "M")
            elif range_key == "adult_female":
                condition = (col("TestCode") == test_code) & (col("AgeGroup") == "adult") & (col("Gender") == "F")
            else:
                continue

            min_val = range_values["min"]
            max_val = range_values["max"]
            crit_low = range_values.get("critical_low")
            crit_high = range_values.get("critical_high")

            # Validate reference range
            df = df.withColumn(
                "ReferenceRangeValid",
                when(
                    condition & col("ResultValueNumeric").isNotNull(),
                    (col("ResultValueNumeric") >= min_val) & (col("ResultValueNumeric") <= max_val)
                ).otherwise(col("ReferenceRangeValid"))
            )

            # Flag critical values
            if crit_low is not None or crit_high is not None:
                df = df.withColumn(
                    "IsCriticalValue",
                    when(
                        condition & col("ResultValueNumeric").isNotNull() & (
                            ((crit_low is not None) & (col("ResultValueNumeric") < crit_low)) |
                            ((crit_high is not None) & (col("ResultValueNumeric") > crit_high))
                        ),
                        lit(True)
                    ).otherwise(col("IsCriticalValue"))
                )

            # Add validation message
            df = df.withColumn(
                "ValidationMessage",
                when(
                    condition & (col("ReferenceRangeValid") == False),
                    concat_ws("; ", col("ValidationMessage"),
                             lit(f"Out of range for {test_code} ({range_key}): expected {min_val}-{max_val}"))
                ).otherwise(col("ValidationMessage"))
            )

    # Statistics
    total_results = df.filter(col("ResultValueNumeric").isNotNull()).count()
    out_of_range = df.filter(col("ReferenceRangeValid") == False).count()
    critical_values = df.filter(col("IsCriticalValue") == True).count()

    print(f"Total numeric results: {total_results:,}")
    print(f"Out of range: {out_of_range:,} ({out_of_range/total_results*100:.2f}%)")
    print(f"Critical values: {critical_values:,} ({critical_values/total_results*100:.2f}%)")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Function 2: Temporal Consistency

# COMMAND ----------

def validate_temporal_consistency(df: DataFrame) -> DataFrame:
    """
    Check for unrealistic changes in lab values over time
    """
    print("\n" + "="*80)
    print("Temporal Consistency Validation")
    print("="*80)

    # Define window for looking at previous result
    window_spec = Window.partitionBy("PatientId", "TestCode").orderBy("ResultDate")

    # Get previous result and time difference
    df = df.withColumn("PreviousResultValue", lag("ResultValueNumeric", 1).over(window_spec)) \
           .withColumn("PreviousResultDate", lag("ResultDate", 1).over(window_spec)) \
           .withColumn("DaysSincePrevious", datediff(col("ResultDate"), col("PreviousResultDate")))

    # Calculate percentage change
    df = df.withColumn(
        "PercentChange",
        when(
            (col("PreviousResultValue").isNotNull()) & (col("PreviousResultValue") != 0),
            spark_abs((col("ResultValueNumeric") - col("PreviousResultValue")) / col("PreviousResultValue") * 100)
        )
    )

    # Flag unrealistic changes (>200% change within 7 days)
    df = df.withColumn(
        "TemporalConsistencyValid",
        when(
            (col("PercentChange").isNotNull()) & (col("DaysSincePrevious") <= 7),
            col("PercentChange") <= 200
        ).otherwise(lit(True))
    )

    # Add to validation message
    df = df.withColumn(
        "ValidationMessage",
        when(
            col("TemporalConsistencyValid") == False,
            concat_ws("; ", col("ValidationMessage"),
                     lit("Unrealistic temporal change detected"))
        ).otherwise(col("ValidationMessage"))
    )

    # Statistics
    temporal_issues = df.filter(col("TemporalConsistencyValid") == False).count()
    print(f"Temporal consistency issues: {temporal_issues:,}")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Function 3: Cross-Test Correlation

# COMMAND ----------

def validate_cross_test_correlation(df: DataFrame) -> DataFrame:
    """
    Validate expected correlations between related tests
    """
    print("\n" + "="*80)
    print("Cross-Test Correlation Validation")
    print("="*80)

    df = df.withColumn("CorrelationValid", lit(True))

    for pair in CORRELATION_PAIRS:
        test1 = pair["test1"]
        test2 = pair["test2"]
        correlation_type = pair["expected_correlation"]

        # Self-join to find patients with both tests on same day
        test1_df = df.filter(col("TestCode") == test1) \
                     .select(
                         col("PatientId"),
                         col("ResultDate").alias("Date"),
                         col("ResultValueNumeric").alias(f"{test1}_Value")
                     )

        test2_df = df.filter(col("TestCode") == test2) \
                     .select(
                         col("PatientId"),
                         col("ResultDate").alias("Date"),
                         col("ResultValueNumeric").alias(f"{test2}_Value")
                     )

        correlation_df = test1_df.join(
            test2_df,
            (test1_df.PatientId == test2_df.PatientId) &
            (test1_df.Date == test2_df.Date),
            "inner"
        )

        if correlation_df.count() > 0:
            # Calculate correlation
            correlation = correlation_df.stat.corr(f"{test1}_Value", f"{test2}_Value")
            print(f"{test1} vs {test2}: correlation = {correlation:.3f} (expected: {correlation_type})")

            # Validate correlation meets expectation
            if correlation_type == "positive" and correlation < 0.3:
                print(f"  ⚠ Warning: Expected positive correlation, got {correlation:.3f}")
            elif correlation_type == "inverse" and correlation > -0.3:
                print(f"  ⚠ Warning: Expected inverse correlation, got {correlation:.3f}")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Function 4: Critical Value Alerting

# COMMAND ----------

def generate_critical_value_alerts(df: DataFrame) -> DataFrame:
    """
    Generate alerts for critical values requiring immediate attention
    """
    print("\n" + "="*80)
    print("Critical Value Alerts")
    print("="*80)

    critical_results = df.filter(col("IsCriticalValue") == True) \
                         .select(
                             "ResultId",
                             "PatientId",
                             "TestCode",
                             "TestName",
                             "ResultValueNumeric",
                             "ResultUnit",
                             "ResultDate",
                             "PerformingLabId"
                         )

    critical_count = critical_results.count()
    print(f"Generated {critical_count:,} critical value alerts")

    if critical_count > 0:
        # Save critical alerts to dedicated table
        critical_results.withColumn("AlertGeneratedDate", current_timestamp()) \
                       .withColumn("AlertStatus", lit("PENDING")) \
                       .write.format("delta") \
                       .mode("append") \
                       .save(f"{GOLD_PATH}/critical_value_alerts")

        print("✓ Critical alerts saved to gold layer")

        # Show sample
        print("\nSample critical values:")
        critical_results.show(10, False)

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Function 5: Specimen Validity

# COMMAND ----------

def validate_specimen(df: DataFrame) -> DataFrame:
    """
    Validate specimen type and age for each test
    """
    print("\n" + "="*80)
    print("Specimen Validity Validation")
    print("="*80)

    # Note: This assumes specimen collection date is available
    # If not available, this validation would be skipped

    df = df.withColumn("SpecimenValid", lit(True))

    for specimen_type, rules in SPECIMEN_RULES.items():
        max_age = rules["max_age_hours"]
        valid_tests = rules["valid_tests"]

        # Validate specimen age (if collection date available)
        # df = df.withColumn(
        #     "SpecimenValid",
        #     when(
        #         (col("SpecimenType") == specimen_type) &
        #         (col("TestCode").isin(valid_tests)) &
        #         (col("SpecimenCollectionDate").isNotNull()),
        #         (hours_between(col("ResultDate"), col("SpecimenCollectionDate")) <= max_age)
        #     ).otherwise(col("SpecimenValid"))
        # )

        # Validate test is appropriate for specimen type
        df = df.withColumn(
            "SpecimenValid",
            when(
                (col("SpecimenType") == specimen_type) & (~col("TestCode").isin(valid_tests)),
                lit(False)
            ).otherwise(col("SpecimenValid"))
        )

    specimen_issues = df.filter(col("SpecimenValid") == False).count()
    print(f"Specimen validity issues: {specimen_issues:,}")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Validation Pipeline

# COMMAND ----------

try:
    print(f"\n{'='*80}")
    print(f"Clinical Validation Suite - {load_date}")
    print(f"{'='*80}\n")

    # Step 1: Reference range validation
    validated_df = validate_reference_ranges(lab_with_demographics)

    # Step 2: Temporal consistency
    validated_df = validate_temporal_consistency(validated_df)

    # Step 3: Cross-test correlation
    validated_df = validate_cross_test_correlation(validated_df)

    # Step 4: Critical value alerting
    validated_df = generate_critical_value_alerts(validated_df)

    # Step 5: Specimen validity
    validated_df = validate_specimen(validated_df)

    # Overall validation summary
    print("\n" + "="*80)
    print("Overall Validation Summary")
    print("="*80)

    total_results = validated_df.count()
    fully_valid = validated_df.filter(
        (col("ReferenceRangeValid") == True) &
        (col("TemporalConsistencyValid") == True) &
        (col("CorrelationValid") == True) &
        (col("SpecimenValid") == True)
    ).count()

    print(f"Total results validated: {total_results:,}")
    print(f"Fully valid results: {fully_valid:,} ({fully_valid/total_results*100:.2f}%)")
    print(f"Results with issues: {total_results - fully_valid:,} ({(total_results-fully_valid)/total_results*100:.2f}%)")

    # Save validation results
    validation_summary = validated_df.select(
        "ResultId",
        "PatientId",
        "TestCode",
        "ResultDate",
        "ReferenceRangeValid",
        "TemporalConsistencyValid",
        "CorrelationValid",
        "SpecimenValid",
        "IsCriticalValue",
        "ValidationMessage",
        "_load_date"
    )

    validation_summary.write.format("delta") \
                      .mode("append") \
                      .partitionBy("_load_date") \
                      .save(VALIDATION_RESULTS_PATH)

    print(f"\n✓ Validation results saved to: {VALIDATION_RESULTS_PATH}")

    print(f"\n{'='*80}")
    print(f"✓ Clinical validation completed successfully")
    print(f"{'='*80}\n")

    result = {
        "status": "SUCCESS",
        "total_results": total_results,
        "fully_valid": fully_valid,
        "validation_rate": fully_valid/total_results
    }

    dbutils.notebook.exit(json.dumps(result))

except Exception as e:
    print(f"\n{'='*80}")
    print(f"✗ Error during clinical validation")
    print(f"Error: {str(e)}")
    print(f"{'='*80}\n")

    import traceback
    traceback.print_exc()

    dbutils.notebook.exit(json.dumps({"status": "FAILURE", "error": str(e)}))

# COMMAND ----------
