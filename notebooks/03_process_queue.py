# Databricks notebook source
# MAGIC %md
# MAGIC # Process Queue - Start Worker
# MAGIC
# MAGIC This notebook starts a worker that processes tasks from the delay queue.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Starts a queue worker
# MAGIC 2. Processes tasks with exponential backoff retry
# MAGIC 3. Runs reconciliation on completed migrations
# MAGIC 4. Handles failures and dead letter queue
# MAGIC
# MAGIC **⚠️ Important:**
# MAGIC - This notebook runs continuously until stopped
# MAGIC - You can run multiple instances for parallel processing
# MAGIC - Monitor progress in notebook `04_monitoring_dashboard.py`

# COMMAND ----------

import sys
sys.path.append("/dbfs/src")

from delay_queue.orchestrator import create_orchestrator

# Initialize orchestrator
CONFIG_PATH = "/dbfs/config/queue_config.yaml"
orchestrator = create_orchestrator(spark, CONFIG_PATH)

print("✓ Worker initialized")
print(f"✓ Worker ID: {orchestrator.worker_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Processing configuration
MAX_ITERATIONS = None  # None = run forever, or set to number (e.g., 100)
CONTINUOUS_MODE = True  # True = keep polling, False = process once and stop
BATCH_SIZE = 10  # Process up to 10 tasks per batch

print("Worker Configuration:")
print(f"  Max Iterations: {MAX_ITERATIONS or 'Unlimited'}")
print(f"  Continuous Mode: {CONTINUOUS_MODE}")
print(f"  Batch Size: {BATCH_SIZE}")
print(f"  Polling Interval: {orchestrator.polling_interval}s")
print(f"  Visibility Timeout: {orchestrator.visibility_timeout}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Queue Before Processing

# COMMAND ----------

status = orchestrator.get_queue_status()

print("Queue Status Before Processing:")
print("=" * 70)
print(f"Total Tasks: {status['statistics'].get('total_tasks', 0)}")
print(f"Pending/Ready: {status['statistics'].get('by_status', {}).get('PENDING', 0) + status['statistics'].get('by_status', {}).get('READY', 0)}")
print(f"Processing: {status['statistics'].get('by_status', {}).get('PROCESSING', 0)}")
print(f"Retry Scheduled: {status['statistics'].get('by_status', {}).get('RETRY_SCHEDULED', 0)}")
print(f"Completed: {status['statistics'].get('by_status', {}).get('COMPLETED', 0)}")
print(f"Failed: {status['statistics'].get('by_status', {}).get('FAILED', 0)}")
print(f"Dead Letter: {status['statistics'].get('by_status', {}).get('DEAD_LETTER', 0)}")
print("=" * 70)
print(f"Health Status: {status['health_metrics'].health_status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Start Processing Queue
# MAGIC
# MAGIC This cell will run continuously. Click **Cancel** to stop the worker.

# COMMAND ----------

import logging

# Configure logging for better visibility
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

print("🚀 Starting queue worker...")
print("=" * 70)
print("Press 'Cancel' to stop the worker")
print("=" * 70)

try:
    # Start processing
    orchestrator.start_processing(
        max_iterations=MAX_ITERATIONS,
        continuous=CONTINUOUS_MODE
    )
except KeyboardInterrupt:
    print("\n⚠️ Worker stopped by user")
except Exception as e:
    print(f"\n❌ Worker stopped due to error: {str(e)}")
    raise

print("\n✓ Worker shutdown complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Queue After Processing

# COMMAND ----------

status = orchestrator.get_queue_status()

print("Queue Status After Processing:")
print("=" * 70)
print(f"Total Tasks: {status['statistics'].get('total_tasks', 0)}")
print(f"Pending/Ready: {status['statistics'].get('by_status', {}).get('PENDING', 0) + status['statistics'].get('by_status', {}).get('READY', 0)}")
print(f"Processing: {status['statistics'].get('by_status', {}).get('PROCESSING', 0)}")
print(f"Retry Scheduled: {status['statistics'].get('by_status', {}).get('RETRY_SCHEDULED', 0)}")
print(f"Completed: {status['statistics'].get('by_status', {}).get('COMPLETED', 0)}")
print(f"Failed: {status['statistics'].get('by_status', {}).get('FAILED', 0)}")
print(f"Dead Letter: {status['statistics'].get('by_status', {}).get('DEAD_LETTER', 0)}")
print("=" * 70)

# Show alerts if any
if status['alerts']:
    print("\n⚠️ Active Alerts:")
    for alert in status['alerts']:
        print(f"  [{alert['severity']}] {alert['message']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Recent Task History

# COMMAND ----------

from pyspark.sql.functions import col

queue_df = spark.read.format("delta").load("/mnt/datalake/delay_queue/tasks")

# Display recently completed tasks
print("Recently Completed Tasks:")
display(
    queue_df
    .filter(col("status") == "COMPLETED")
    .select(
        "task_id",
        "task_name",
        "status",
        "execution_time_seconds",
        "completed_at",
        "reconciliation_result_json"
    )
    .orderBy(col("completed_at").desc())
    .limit(10)
)

# COMMAND ----------

# Display failed tasks
print("\nFailed/Retry Scheduled Tasks:")
display(
    queue_df
    .filter(col("status").isin(["FAILED", "DEAD_LETTER", "RETRY_SCHEDULED"]))
    .select(
        "task_id",
        "task_name",
        "status",
        "retry_metadata_json",
        "updated_at"
    )
    .orderBy(col("updated_at").desc())
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Reconciliation Results

# COMMAND ----------

# Display reconciliation results for completed tasks
recon_df = spark.read.format("delta").load("/mnt/datalake/delay_queue/reconciliation_results")

display(
    recon_df
    .select(
        "reconciliation_id",
        "status",
        "row_count_match",
        "source_row_count",
        "target_row_count",
        "checksum_match",
        "key_sums_match",
        "validated_at"
    )
    .orderBy(col("validated_at").desc())
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Processing Complete!
# MAGIC
# MAGIC Next steps:
# MAGIC
# MAGIC 1. **Monitor System**: Use notebook `04_monitoring_dashboard.py`
# MAGIC 2. **Review Failures**: Check tasks in RETRY_SCHEDULED or DEAD_LETTER status
# MAGIC 3. **Verify Data**: Review reconciliation results to ensure data integrity

# COMMAND ----------


