# Databricks notebook source
# MAGIC %md
# MAGIC # Enqueue Migration Tasks
# MAGIC
# MAGIC This notebook demonstrates how to enqueue migration tasks into the delay queue.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Creates migration tasks for CSV → Delta Lake migration
# MAGIC 2. Enqueues tasks with priority and metadata
# MAGIC 3. Validates task creation

# COMMAND ----------

import sys
sys.path.append("/dbfs/src")

from delay_queue.orchestrator import create_orchestrator
from models.task import TaskPriority

# Initialize orchestrator
CONFIG_PATH = "/dbfs/config/queue_config.yaml"
orchestrator = create_orchestrator(spark, CONFIG_PATH)

print("✓ Orchestrator initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: Enqueue a Single Migration Task

# COMMAND ----------

# Define migration parameters
task_id = orchestrator.enqueue_migration_task(
    task_name="Migrate Customer Data",
    source_csv_path="/mnt/legacy/csv_exports/customers.csv",
    target_delta_path="/mnt/datalake/migrated_data/customers",
    table_name="customers",
    source_row_count=1000000,  # Optional: pre-calculated row count
    partition_columns=["region", "signup_date"],
    priority=TaskPriority.HIGH,
    business_date="2024-01-15",
    data_owner="data_engineering_team"
)

print(f"✓ Task enqueued with ID: {task_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2: Batch Enqueue Multiple Tasks

# COMMAND ----------

# Define multiple migration tasks
migration_tasks = [
    {
        "task_name": "Migrate Orders Data",
        "source_csv_path": "/mnt/legacy/csv_exports/orders.csv",
        "target_delta_path": "/mnt/datalake/migrated_data/orders",
        "table_name": "orders",
        "partition_columns": ["order_date"],
        "priority": TaskPriority.CRITICAL
    },
    {
        "task_name": "Migrate Products Data",
        "source_csv_path": "/mnt/legacy/csv_exports/products.csv",
        "target_delta_path": "/mnt/datalake/migrated_data/products",
        "table_name": "products",
        "partition_columns": ["category"],
        "priority": TaskPriority.NORMAL
    },
    {
        "task_name": "Migrate Transactions Data",
        "source_csv_path": "/mnt/legacy/csv_exports/transactions.csv",
        "target_delta_path": "/mnt/datalake/migrated_data/transactions",
        "table_name": "transactions",
        "partition_columns": ["transaction_date"],
        "priority": TaskPriority.HIGH
    }
]

# Enqueue all tasks
task_ids = []
for task in migration_tasks:
    task_id = orchestrator.enqueue_migration_task(**task)
    task_ids.append(task_id)
    print(f"✓ Enqueued: {task['task_name']} (ID: {task_id})")

print(f"\n✓ Total tasks enqueued: {len(task_ids)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 3: Enqueue with Custom Configuration

# COMMAND ----------

# Enqueue task with custom reconciliation configuration
task_id = orchestrator.enqueue_migration_task(
    task_name="Migrate Financial Transactions (High Validation)",
    source_csv_path="/mnt/legacy/csv_exports/financial_transactions.csv",
    target_delta_path="/mnt/datalake/migrated_data/financial_transactions",
    table_name="financial_transactions",
    partition_columns=["transaction_date", "account_type"],
    priority=TaskPriority.CRITICAL,
    # Custom configuration
    custom_config={
        "validate_schema": True,
        "expected_columns": [
            "transaction_id",
            "account_id",
            "transaction_date",
            "amount",
            "currency",
            "account_type"
        ],
        "key_sum_columns": ["amount"],  # Critical for financial data
        "enable_encryption": True
    },
    tags={
        "department": "finance",
        "compliance": "sox",
        "sensitivity": "high"
    }
)

print(f"✓ High-priority financial task enqueued: {task_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 4: Healthcare Data Migration with Validation

# COMMAND ----------

# Healthcare data requires strict validation
task_id = orchestrator.enqueue_migration_task(
    task_name="Migrate Patient Records (HIPAA Compliant)",
    source_csv_path="/mnt/legacy/csv_exports/patient_records.csv",
    target_delta_path="/mnt/datalake/migrated_data/patient_records",
    table_name="patient_records",
    partition_columns=["admission_date"],
    priority=TaskPriority.CRITICAL,
    custom_config={
        "validate_schema": True,
        "expected_columns": [
            "patient_id",
            "admission_date",
            "diagnosis_code",
            "treatment_cost",
            "insurance_id"
        ],
        "key_sum_columns": ["treatment_cost"],  # Validate total costs
        "enable_phi_protection": True
    },
    tags={
        "department": "healthcare",
        "compliance": "hipaa",
        "sensitivity": "critical"
    }
)

print(f"✓ Healthcare migration task enqueued: {task_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Current Queue Status

# COMMAND ----------

status = orchestrator.get_queue_status()

print("Current Queue Status:")
print("=" * 70)
print(f"Total Tasks: {status['statistics'].get('total_tasks', 0)}")
print(f"Pending: {status['statistics'].get('by_status', {}).get('PENDING', 0)}")
print(f"Ready: {status['statistics'].get('by_status', {}).get('READY', 0)}")
print(f"Processing: {status['statistics'].get('by_status', {}).get('PROCESSING', 0)}")
print(f"Retry Scheduled: {status['statistics'].get('by_status', {}).get('RETRY_SCHEDULED', 0)}")
print(f"Completed: {status['statistics'].get('by_status', {}).get('COMPLETED', 0)}")
print(f"Failed: {status['statistics'].get('by_status', {}).get('FAILED', 0)}")
print(f"Dead Letter: {status['statistics'].get('by_status', {}).get('DEAD_LETTER', 0)}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Queue Contents (Last 10 Tasks)

# COMMAND ----------

from pyspark.sql.functions import col

queue_df = spark.read.format("delta").load("/mnt/datalake/delay_queue/tasks")

# Display recent tasks
display(
    queue_df
    .select(
        "task_id",
        "task_name",
        "status",
        "priority",
        "created_at",
        "updated_at"
    )
    .orderBy(col("created_at").desc())
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Tasks Enqueued!
# MAGIC
# MAGIC Your migration tasks are now in the queue. Next steps:
# MAGIC
# MAGIC 1. **Start Processing**: Run notebook `03_process_queue.py` to start a worker
# MAGIC 2. **Monitor Progress**: Use notebook `04_monitoring_dashboard.py`
# MAGIC 3. **View Results**: Check reconciliation results after completion

# COMMAND ----------


