# Databricks notebook source
# MAGIC %md
# MAGIC # Lab Data Quality Framework
# MAGIC
# MAGIC Comprehensive validation rules specific to laboratory data:
# MAGIC - Physiological plausibility (delta checks)
# MAGIC - Unit consistency validation
# MAGIC - Sample integrity checks
# MAGIC - Test-specific business rules
# MAGIC - LOINC code validation
# MAGIC - Critical value protocols

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, abs as spark_abs, datediff,
    lag, lead, concat_ws, regexp_extract, upper, trim
)
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
import json
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

SILVER_PATH = f"abfss://silver@{storage_account}.dfs.core.windows.net/lims"
GOLD_PATH = f"abfss://gold@{storage_account}.dfs.core.windows.net/lims"
QUALITY_RESULTS_PATH = f"{GOLD_PATH}/data_quality_results"

# Lab-specific validation rules
LAB_VALIDATION_RULES = {
    "GLUCOSE": {
        "physiological_range": {"min": 0, "max": 700},  # mg/dL
        "delta_check_percent": 50,  # Max 50% change within 24 hours
        "critical_low": 50,
        "critical_high": 300,
        "common_units": ["mg/dL", "mmol/L"],
        "loinc_codes": ["2345-7", "2339-0"],
        "required_specimen": ["Blood", "Serum", "Plasma"]
    },
    "HBA1C": {
        "physiological_range": {"min": 2.0, "max": 18.0},  # %
        "delta_check_percent": 20,  # Max 20% change within 90 days
        "critical_high": 10.0,
        "common_units": ["%", "mmol/mol"],
        "loinc_codes": ["4548-4", "17856-6"],
        "required_specimen": ["Blood"]
    },
    "CREATININE": {
        "physiological_range": {"min": 0.1, "max": 25.0},  # mg/dL
        "delta_check_percent": 50,
        "critical_low": 0.3,
        "critical_high": 5.0,
        "common_units": ["mg/dL", "μmol/L"],
        "loinc_codes": ["2160-0"],
        "required_specimen": ["Serum", "Plasma"]
    },
    "WBC": {
        "physiological_range": {"min": 0, "max": 100.0},  # K/uL
        "delta_check_percent": 100,
        "critical_low": 1.0,
        "critical_high": 30.0,
        "common_units": ["K/uL", "10^9/L"],
        "loinc_codes": ["6690-2"],
        "required_specimen": ["Blood"]
    },
    "HEMOGLOBIN": {
        "physiological_range": {"min": 3.0, "max": 25.0},  # g/dL
        "delta_check_percent": 25,
        "critical_low": 7.0,
        "critical_high": 20.0,
        "common_units": ["g/dL", "g/L"],
        "loinc_codes": ["718-7"],
        "required_specimen": ["Blood"]
    },
    "POTASSIUM": {
        "physiological_range": {"min": 1.5, "max": 9.0},  # mEq/L
        "delta_check_percent": 30,
        "critical_low": 2.5,
        "critical_high": 6.5,
        "common_units": ["mEq/L", "mmol/L"],
        "loinc_codes": ["2823-3"],
        "required_specimen": ["Serum", "Plasma"],
        "hemolysis_check": True  # Flag if hemolyzed
    },
    "TROPONIN": {
        "physiological_range": {"min": 0, "max": 500000},  # ng/L
        "delta_check_percent": 200,  # Can increase rapidly in MI
        "critical_high": 14,  # ng/mL
        "common_units": ["ng/mL", "ng/L"],
        "loinc_codes": ["6598-7", "10839-9"],
        "required_specimen": ["Serum", "Plasma"],
        "time_sensitive": True,  # Report immediately
        "serial_testing": True  # Often ordered serially
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Rule 1: Physiological Plausibility

# COMMAND ----------

def validate_physiological_plausibility(df: DataFrame) -> DataFrame:
    """
    Validate that results fall within physiologically possible ranges
    """
    print("\n" + "="*80)
    print("Physiological Plausibility Validation")
    print("="*80)

    df = df.withColumn("PhysiologicallyPlausible", lit(True))
    df = df.withColumn("PlausibilityIssues", lit(""))

    for test_code, rules in LAB_VALIDATION_RULES.items():
        phys_range = rules.get("physiological_range")

        if phys_range:
            df = df.withColumn(
                "PhysiologicallyPlausible",
                when(
                    (col("TestCode") == test_code) &
                    col("ResultValueNumeric").isNotNull() &
                    (
                        (col("ResultValueNumeric") < phys_range["min"]) |
                        (col("ResultValueNumeric") > phys_range["max"])
                    ),
                    lit(False)
                ).otherwise(col("PhysiologicallyPlausible"))
            )

            df = df.withColumn(
                "PlausibilityIssues",
                when(
                    (col("TestCode") == test_code) &
                    (col("PhysiologicallyPlausible") == False),
                    concat_ws("; ", col("PlausibilityIssues"),
                             lit(f"Value outside physiological range {phys_range['min']}-{phys_range['max']}"))
                ).otherwise(col("PlausibilityIssues"))
            )

    implausible_count = df.filter(col("PhysiologicallyPlausible") == False).count()
    total_count = df.filter(col("ResultValueNumeric").isNotNull()).count()

    print(f"Total numeric results: {total_count:,}")
    print(f"Physiologically implausible: {implausible_count:,} ({implausible_count/total_count*100:.2f}%)")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Rule 2: Delta Check (Temporal Plausibility)

# COMMAND ----------

def validate_delta_check(df: DataFrame) -> DataFrame:
    """
    Validate that changes between consecutive results are plausible
    """
    print("\n" + "="*80)
    print("Delta Check Validation")
    print("="*80)

    window_spec = Window.partitionBy("PatientId", "TestCode").orderBy("ResultDate")

    df = df.withColumn("PreviousResult", lag("ResultValueNumeric", 1).over(window_spec))
    df = df.withColumn("PreviousResultDate", lag("ResultDate", 1).over(window_spec))
    df = df.withColumn("HoursSincePrevious",
                      datediff(col("ResultDate"), col("PreviousResultDate")) * 24)

    df = df.withColumn("PercentChange",
        when(
            (col("PreviousResult").isNotNull()) & (col("PreviousResult") != 0),
            spark_abs((col("ResultValueNumeric") - col("PreviousResult")) / col("PreviousResult") * 100)
        )
    )

    df = df.withColumn("DeltaCheckPass", lit(True))
    df = df.withColumn("DeltaCheckIssues", lit(""))

    # Apply test-specific delta check rules
    for test_code, rules in LAB_VALIDATION_RULES.items():
        max_change = rules.get("delta_check_percent")
        time_window = 24  # Default 24 hours

        if max_change:
            df = df.withColumn(
                "DeltaCheckPass",
                when(
                    (col("TestCode") == test_code) &
                    (col("HoursSincePrevious") <= time_window) &
                    (col("PercentChange") > max_change),
                    lit(False)
                ).otherwise(col("DeltaCheckPass"))
            )

            df = df.withColumn(
                "DeltaCheckIssues",
                when(
                    (col("TestCode") == test_code) & (col("DeltaCheckPass") == False),
                    concat_ws("; ", col("DeltaCheckIssues"),
                             lit(f"Change of {col('PercentChange')}% exceeds {max_change}% threshold within {time_window}h"))
                ).otherwise(col("DeltaCheckIssues"))
            )

    delta_failures = df.filter(col("DeltaCheckPass") == False).count()
    print(f"Delta check failures: {delta_failures:,}")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Rule 3: Unit Consistency

# COMMAND ----------

def validate_unit_consistency(df: DataFrame) -> DataFrame:
    """
    Validate that result units match expected units for each test
    """
    print("\n" + "="*80)
    print("Unit Consistency Validation")
    print("="*80)

    df = df.withColumn("UnitValid", lit(True))
    df = df.withColumn("UnitIssues", lit(""))

    for test_code, rules in LAB_VALIDATION_RULES.items():
        expected_units = rules.get("common_units", [])

        if expected_units:
            df = df.withColumn(
                "UnitValid",
                when(
                    (col("TestCode") == test_code) &
                    col("ResultUnit").isNotNull() &
                    (~col("ResultUnit").isin(expected_units)),
                    lit(False)
                ).otherwise(col("UnitValid"))
            )

            df = df.withColumn(
                "UnitIssues",
                when(
                    (col("TestCode") == test_code) & (col("UnitValid") == False),
                    concat_ws("; ", col("UnitIssues"),
                             lit(f"Unexpected unit. Expected: {', '.join(expected_units)}"))
                ).otherwise(col("UnitIssues"))
            )

    unit_issues = df.filter(col("UnitValid") == False).count()
    print(f"Unit consistency issues: {unit_issues:,}")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Rule 4: LOINC Code Validation

# COMMAND ----------

def validate_loinc_codes(df: DataFrame, test_master_df: DataFrame) -> DataFrame:
    """
    Validate that LOINC codes are assigned and valid
    """
    print("\n" + "="*80)
    print("LOINC Code Validation")
    print("="*80)

    # Join with test master to get LOINC codes
    df = df.alias("lr").join(
        test_master_df.alias("tm").select("TestCode", "LoincCode"),
        col("lr.TestCode") == col("tm.TestCode"),
        "left"
    ).select("lr.*", col("tm.LoincCode").alias("AssignedLoincCode"))

    df = df.withColumn("LoincValid", lit(True))
    df = df.withColumn("LoincIssues", lit(""))

    # Check if LOINC code is assigned
    df = df.withColumn(
        "LoincValid",
        when(col("AssignedLoincCode").isNull(), lit(False))
        .otherwise(col("LoincValid"))
    )

    df = df.withColumn(
        "LoincIssues",
        when(
            col("LoincValid") == False,
            lit("LOINC code not assigned to test")
        ).otherwise(col("LoincIssues"))
    )

    # Validate LOINC code format (e.g., "2345-7")
    df = df.withColumn(
        "LoincFormatValid",
        when(
            col("AssignedLoincCode").isNotNull(),
            col("AssignedLoincCode").rlike("^\\d{4,5}-\\d$")
        ).otherwise(lit(True))
    )

    df = df.withColumn(
        "LoincIssues",
        when(
            col("LoincFormatValid") == False,
            concat_ws("; ", col("LoincIssues"), lit("Invalid LOINC code format"))
        ).otherwise(col("LoincIssues"))
    )

    loinc_issues = df.filter(col("LoincValid") == False).count()
    print(f"LOINC validation issues: {loinc_issues:,}")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Rule 5: Specimen Integrity

# COMMAND ----------

def validate_specimen_integrity(df: DataFrame) -> DataFrame:
    """
    Validate specimen type and quality indicators
    """
    print("\n" + "="*80)
    print("Specimen Integrity Validation")
    print("="*80)

    df = df.withColumn("SpecimenIntegrityValid", lit(True))
    df = df.withColumn("SpecimenIssues", lit(""))

    # Validate specimen type for test
    for test_code, rules in LAB_VALIDATION_RULES.items():
        required_specimens = rules.get("required_specimen", [])

        if required_specimens:
            df = df.withColumn(
                "SpecimenIntegrityValid",
                when(
                    (col("TestCode") == test_code) &
                    col("SpecimenType").isNotNull() &
                    (~col("SpecimenType").isin(required_specimens)),
                    lit(False)
                ).otherwise(col("SpecimenIntegrityValid"))
            )

            df = df.withColumn(
                "SpecimenIssues",
                when(
                    (col("TestCode") == test_code) & (col("SpecimenIntegrityValid") == False),
                    concat_ws("; ", col("SpecimenIssues"),
                             lit(f"Invalid specimen type. Expected: {', '.join(required_specimens)}"))
                ).otherwise(col("SpecimenIssues"))
            )

    # Check for hemolysis flag (critical for certain tests)
    df = df.withColumn(
        "HemolysisImpact",
        when(
            col("TestCode").isin(["POTASSIUM", "LDH", "AST"]) &
            (col("SpecimenQuality") == "Hemolyzed"),
            lit("Result may be falsely elevated due to hemolysis")
        )
    )

    specimen_issues = df.filter(col("SpecimenIntegrityValid") == False).count()
    print(f"Specimen integrity issues: {specimen_issues:,}")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Rule 6: Critical Value Protocol

# COMMAND ----------

def validate_critical_value_protocol(df: DataFrame) -> DataFrame:
    """
    Validate that critical values follow proper protocols
    """
    print("\n" + "="*80)
    print("Critical Value Protocol Validation")
    print("="*80)

    df = df.withColumn("CriticalProtocolCompliant", lit(True))
    df = df.withColumn("ProtocolIssues", lit(""))

    # Check if critical results have acknowledgment timestamp
    # (This would require additional metadata in production)

    # Identify time-sensitive tests
    time_sensitive_tests = [code for code, rules in LAB_VALIDATION_RULES.items()
                           if rules.get("time_sensitive", False)]

    df = df.withColumn(
        "TimeSensitive",
        when(col("TestCode").isin(time_sensitive_tests), lit(True))
        .otherwise(lit(False))
    )

    # Flag critical values for immediate notification
    df = df.withColumn(
        "RequiresImmediateNotification",
        when(
            col("CriticalFlag") == True,
            lit(True)
        ).otherwise(lit(False))
    )

    critical_count = df.filter(col("RequiresImmediateNotification") == True).count()
    print(f"Results requiring immediate notification: {critical_count:,}")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Quality Assessment

# COMMAND ----------

def assess_overall_quality(df: DataFrame) -> Dict:
    """
    Calculate overall data quality scores
    """
    print("\n" + "="*80)
    print("Overall Data Quality Assessment")
    print("="*80)

    total_results = df.count()

    # Calculate quality dimensions
    completeness = df.filter(col("ResultValueNumeric").isNotNull()).count() / total_results

    physiological_validity = df.filter(col("PhysiologicallyPlausible") == True).count() / total_results

    delta_check_pass_rate = df.filter(
        (col("PreviousResult").isNotNull()) & (col("DeltaCheckPass") == True)
    ).count() / df.filter(col("PreviousResult").isNotNull()).count()

    unit_conformity = df.filter(col("UnitValid") == True).count() / total_results

    loinc_coverage = df.filter(col("LoincValid") == True).count() / total_results

    specimen_integrity = df.filter(col("SpecimenIntegrityValid") == True).count() / total_results

    # Calculate composite quality score
    quality_score = (
        completeness * 0.20 +
        physiological_validity * 0.25 +
        delta_check_pass_rate * 0.20 +
        unit_conformity * 0.15 +
        loinc_coverage * 0.10 +
        specimen_integrity * 0.10
    )

    quality_metrics = {
        "total_results": total_results,
        "completeness": round(completeness, 4),
        "physiological_validity": round(physiological_validity, 4),
        "delta_check_pass_rate": round(delta_check_pass_rate, 4),
        "unit_conformity": round(unit_conformity, 4),
        "loinc_coverage": round(loinc_coverage, 4),
        "specimen_integrity": round(specimen_integrity, 4),
        "overall_quality_score": round(quality_score, 4)
    }

    print("\nQuality Metrics:")
    for metric, value in quality_metrics.items():
        if metric != "total_results":
            print(f"  {metric}: {value:.2%}")
        else:
            print(f"  {metric}: {value:,}")

    print(f"\n✓ Overall Quality Score: {quality_score:.2%}")

    return quality_metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution

# COMMAND ----------

try:
    print(f"\n{'='*80}")
    print(f"Lab Data Quality Framework")
    print(f"{'='*80}\n")

    # Load data
    lab_results = spark.read.format("delta").load(f"{SILVER_PATH}/labresults")
    test_master = spark.read.format("delta").load(f"{SILVER_PATH}/testmaster")

    # Run validations
    validated_df = validate_physiological_plausibility(lab_results)
    validated_df = validate_delta_check(validated_df)
    validated_df = validate_unit_consistency(validated_df)
    validated_df = validate_loinc_codes(validated_df, test_master)
    validated_df = validate_specimen_integrity(validated_df)
    validated_df = validate_critical_value_protocol(validated_df)

    # Assess overall quality
    quality_metrics = assess_overall_quality(validated_df)

    # Save quality results
    quality_summary = validated_df.select(
        "ResultId",
        "TestCode",
        "PhysiologicallyPlausible",
        "PlausibilityIssues",
        "DeltaCheckPass",
        "DeltaCheckIssues",
        "UnitValid",
        "UnitIssues",
        "LoincValid",
        "LoincIssues",
        "SpecimenIntegrityValid",
        "SpecimenIssues",
        "RequiresImmediateNotification"
    )

    quality_summary.write.format("delta") \
        .mode("overwrite") \
        .save(QUALITY_RESULTS_PATH)

    print(f"\n✓ Quality results saved to: {QUALITY_RESULTS_PATH}")

    result = {
        "status": "SUCCESS",
        "quality_metrics": quality_metrics
    }

    dbutils.notebook.exit(json.dumps(result))

except Exception as e:
    print(f"\n{'='*80}")
    print(f"✗ Error in data quality validation")
    print(f"Error: {str(e)}")
    print(f"{'='*80}\n")

    import traceback
    traceback.print_exc()

    dbutils.notebook.exit(json.dumps({"status": "FAILURE", "error": str(e)}))

# COMMAND ----------
