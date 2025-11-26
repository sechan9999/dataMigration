# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver - Incremental Load Processing
# MAGIC
# MAGIC This notebook processes incremental loads from Bronze layer to Silver layer with:
# MAGIC - Schema validation
# MAGIC - Data type standardization
# MAGIC - Deduplication
# MAGIC - Basic data quality checks
# MAGIC - Delta Lake optimization

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, hash, md5, concat_ws,
    when, trim, upper, to_date, to_timestamp, coalesce
)
from pyspark.sql.types import *
from delta.tables import DeltaTable
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("table_name", "LabResults", "Table Name")
dbutils.widgets.text("load_date", "2024-01-01", "Load Date")
dbutils.widgets.text("bronze_path", "", "Bronze Layer Path")

table_name = dbutils.widgets.get("table_name")
load_date = dbutils.widgets.get("load_date")
bronze_path = dbutils.widgets.get("bronze_path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Storage paths
BRONZE_PATH = f"abfss://bronze@{storage_account}.dfs.core.windows.net/lims/{table_name.lower()}"
SILVER_PATH = f"abfss://silver@{storage_account}.dfs.core.windows.net/lims/{table_name.lower()}"
CHECKPOINT_PATH = f"abfss://silver@{storage_account}.dfs.core.windows.net/_checkpoints/lims/{table_name.lower()}"

# Schema definitions for each table
SCHEMA_DEFINITIONS = {
    "LabResults": StructType([
        StructField("ResultId", LongType(), False),
        StructField("PatientId", LongType(), False),
        StructField("OrderId", LongType(), False),
        StructField("TestCode", StringType(), False),
        StructField("ResultValue", StringType(), True),
        StructField("ResultValueNumeric", DoubleType(), True),
        StructField("ResultUnit", StringType(), True),
        StructField("ReferenceRangeMin", DoubleType(), True),
        StructField("ReferenceRangeMax", DoubleType(), True),
        StructField("AbnormalFlag", StringType(), True),
        StructField("ResultStatus", StringType(), True),
        StructField("ResultDate", TimestampType(), False),
        StructField("PerformingLabId", IntegerType(), True),
        StructField("TechnicianId", StringType(), True),
        StructField("SpecimenId", LongType(), True),
        StructField("Comments", StringType(), True),
        StructField("CreatedDate", TimestampType(), False),
        StructField("ModifiedDate", TimestampType(), False)
    ]),
    "Patients": StructType([
        StructField("PatientId", LongType(), False),
        StructField("MRN", StringType(), False),
        StructField("FirstName", StringType(), False),
        StructField("LastName", StringType(), False),
        StructField("DateOfBirth", DateType(), False),
        StructField("Gender", StringType(), True),
        StructField("Race", StringType(), True),
        StructField("Ethnicity", StringType(), True),
        StructField("CreatedDate", TimestampType(), False),
        StructField("ModifiedDate", TimestampType(), False)
    ]),
    "TestMaster": StructType([
        StructField("TestCode", StringType(), False),
        StructField("TestName", StringType(), False),
        StructField("LoincCode", StringType(), True),
        StructField("TestCategory", StringType(), True),
        StructField("SpecimenType", StringType(), True),
        StructField("ResultType", StringType(), True),
        StructField("TurnaroundTimeMinutes", IntegerType(), True),
        StructField("IsActive", BooleanType(), False),
        StructField("CreatedDate", TimestampType(), False),
        StructField("ModifiedDate", TimestampType(), False)
    ])
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def read_bronze_data(bronze_path: str, load_date: str) -> DataFrame:
    """
    Read data from bronze layer for the specified load date
    """
    try:
        df = spark.read.format("parquet").load(bronze_path)
        print(f"✓ Read {df.count():,} records from bronze layer")
        return df
    except Exception as e:
        print(f"✗ Error reading bronze data: {str(e)}")
        raise

def validate_and_standardize_schema(df: DataFrame, table_name: str) -> DataFrame:
    """
    Validate and standardize the schema according to table definition
    """
    if table_name not in SCHEMA_DEFINITIONS:
        print(f"⚠ No schema definition found for {table_name}, proceeding with source schema")
        return df

    target_schema = SCHEMA_DEFINITIONS[table_name]

    # Add missing columns with null values
    for field in target_schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, lit(None).cast(field.dataType))

    # Cast columns to target types
    for field in target_schema.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))

    # Select only schema columns in correct order
    df = df.select([field.name for field in target_schema.fields])

    print(f"✓ Schema validated and standardized for {table_name}")
    return df

def standardize_data(df: DataFrame, table_name: str) -> DataFrame:
    """
    Apply data standardization rules
    """
    # Trim string columns
    string_columns = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col_name in string_columns:
        df = df.withColumn(col_name, trim(col(col_name)))

    # Table-specific standardization
    if table_name == "LabResults":
        df = df.withColumn("AbnormalFlag", upper(col("AbnormalFlag"))) \
               .withColumn("ResultStatus", upper(col("ResultStatus")))

    elif table_name == "Patients":
        df = df.withColumn("Gender", upper(col("Gender"))) \
               .withColumn("MRN", upper(trim(col("MRN"))))

    print(f"✓ Data standardization completed")
    return df

def add_technical_columns(df: DataFrame) -> DataFrame:
    """
    Add technical metadata columns
    """
    df = df.withColumn("_load_timestamp", current_timestamp()) \
           .withColumn("_load_date", lit(load_date)) \
           .withColumn("_source_file", lit(bronze_path))

    # Add hash for change detection
    hash_columns = [c for c in df.columns if not c.startswith("_")]
    df = df.withColumn("_row_hash", md5(concat_ws("||", *[coalesce(col(c).cast("string"), lit("")) for c in hash_columns])))

    print(f"✓ Technical columns added")
    return df

def deduplicate_data(df: DataFrame, table_name: str) -> DataFrame:
    """
    Remove duplicate records based on primary key
    """
    primary_keys = {
        "LabResults": ["ResultId"],
        "Patients": ["PatientId"],
        "TestMaster": ["TestCode"],
        "Specimens": ["SpecimenId"],
        "Orders": ["OrderId"]
    }

    if table_name not in primary_keys:
        print(f"⚠ No primary key defined for {table_name}, skipping deduplication")
        return df

    pk_columns = primary_keys[table_name]
    initial_count = df.count()

    # Keep latest record based on ModifiedDate
    from pyspark.sql.window import Window
    import pyspark.sql.functions as F

    window_spec = Window.partitionBy(*pk_columns).orderBy(col("ModifiedDate").desc())
    df = df.withColumn("_row_num", F.row_number().over(window_spec)) \
           .filter(col("_row_num") == 1) \
           .drop("_row_num")

    final_count = df.count()
    duplicates_removed = initial_count - final_count

    if duplicates_removed > 0:
        print(f"⚠ Removed {duplicates_removed:,} duplicate records")
    else:
        print(f"✓ No duplicates found")

    return df

def perform_quality_checks(df: DataFrame, table_name: str) -> dict:
    """
    Perform basic data quality checks
    """
    checks = {}

    # Record count
    checks['record_count'] = df.count()

    # Null counts for required columns
    primary_keys = {
        "LabResults": ["ResultId", "PatientId", "TestCode", "ResultDate"],
        "Patients": ["PatientId", "MRN", "DateOfBirth"],
        "TestMaster": ["TestCode", "TestName"]
    }

    if table_name in primary_keys:
        for col_name in primary_keys[table_name]:
            null_count = df.filter(col(col_name).isNull()).count()
            checks[f'{col_name}_null_count'] = null_count
            if null_count > 0:
                print(f"⚠ Found {null_count:,} null values in required column {col_name}")

    # Value ranges for LabResults
    if table_name == "LabResults":
        checks['negative_numeric_values'] = df.filter(col("ResultValueNumeric") < 0).count()
        checks['invalid_reference_ranges'] = df.filter(
            (col("ReferenceRangeMin").isNotNull()) &
            (col("ReferenceRangeMax").isNotNull()) &
            (col("ReferenceRangeMin") > col("ReferenceRangeMax"))
        ).count()

    print(f"✓ Quality checks completed: {json.dumps(checks, indent=2)}")
    return checks

def merge_to_silver(df: DataFrame, silver_path: str, table_name: str):
    """
    Merge data to silver layer using Delta Lake MERGE
    """
    primary_keys = {
        "LabResults": ["ResultId"],
        "Patients": ["PatientId"],
        "TestMaster": ["TestCode"],
        "Specimens": ["SpecimenId"],
        "Orders": ["OrderId"]
    }

    if table_name not in primary_keys:
        raise ValueError(f"Primary key not defined for {table_name}")

    pk_columns = primary_keys[table_name]

    # Check if silver table exists
    if DeltaTable.isDeltaTable(spark, silver_path):
        print(f"✓ Silver table exists, performing MERGE operation")

        silver_table = DeltaTable.forPath(spark, silver_path)

        # Build merge condition
        merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in pk_columns])

        # Perform merge
        silver_table.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenMatchedUpdate(
            condition = "source._row_hash != target._row_hash",
            set = {col: f"source.{col}" for col in df.columns}
        ).whenNotMatchedInsertAll().execute()

        print(f"✓ MERGE completed successfully")
    else:
        print(f"✓ Silver table does not exist, creating new table")
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(silver_path)
        print(f"✓ Silver table created successfully")

def optimize_delta_table(silver_path: str):
    """
    Optimize Delta table and clean up old versions
    """
    silver_table = DeltaTable.forPath(spark, silver_path)

    # Optimize (compaction)
    silver_table.optimize().executeCompaction()
    print(f"✓ Table optimized (compaction)")

    # Z-order if applicable
    # silver_table.optimize().executeZOrderBy("PatientId", "ResultDate")

    # Vacuum old versions (keep 7 days)
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    silver_table.vacuum(168)  # 7 days in hours
    print(f"✓ Table vacuumed (old versions cleaned)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Processing Logic

# COMMAND ----------

try:
    print(f"\n{'='*80}")
    print(f"Processing {table_name} - Bronze to Silver")
    print(f"Load Date: {load_date}")
    print(f"{'='*80}\n")

    # Step 1: Read bronze data
    print("Step 1: Reading bronze data...")
    df = read_bronze_data(bronze_path if bronze_path else BRONZE_PATH, load_date)

    # Step 2: Validate and standardize schema
    print("\nStep 2: Validating and standardizing schema...")
    df = validate_and_standardize_schema(df, table_name)

    # Step 3: Standardize data
    print("\nStep 3: Standardizing data...")
    df = standardize_data(df, table_name)

    # Step 4: Add technical columns
    print("\nStep 4: Adding technical columns...")
    df = add_technical_columns(df)

    # Step 5: Deduplicate
    print("\nStep 5: Deduplicating data...")
    df = deduplicate_data(df, table_name)

    # Step 6: Quality checks
    print("\nStep 6: Performing quality checks...")
    quality_checks = perform_quality_checks(df, table_name)

    # Step 7: Merge to silver
    print("\nStep 7: Merging to silver layer...")
    merge_to_silver(df, SILVER_PATH, table_name)

    # Step 8: Optimize
    print("\nStep 8: Optimizing Delta table...")
    optimize_delta_table(SILVER_PATH)

    print(f"\n{'='*80}")
    print(f"✓ Successfully processed {table_name} to Silver layer")
    print(f"{'='*80}\n")

    dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "checks": quality_checks}))

except Exception as e:
    print(f"\n{'='*80}")
    print(f"✗ Error processing {table_name}")
    print(f"Error: {str(e)}")
    print(f"{'='*80}\n")

    dbutils.notebook.exit(json.dumps({"status": "FAILURE", "error": str(e)}))

# COMMAND ----------
