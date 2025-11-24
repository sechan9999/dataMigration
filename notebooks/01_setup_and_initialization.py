# Databricks notebook source
# MAGIC %md
# MAGIC # Delay Queue Setup and Initialization
# MAGIC
# MAGIC This notebook sets up the Delay Queue system with exponential backoff for Azure Databricks.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Azure Databricks workspace
# MAGIC - Delta Lake enabled
# MAGIC - Storage mounted at `/mnt/datalake`
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Installs required dependencies
# MAGIC 2. Configures storage paths
# MAGIC 3. Initializes Delta tables
# MAGIC 4. Validates setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Dependencies

# COMMAND ----------

# MAGIC %pip install pyyaml pydantic tenacity --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# Configuration paths
CONFIG_PATH = "/dbfs/config/queue_config.yaml"
SOURCE_CODE_PATH = "/dbfs/src"

# Verify configuration exists
import os

if not os.path.exists(CONFIG_PATH):
    print(f"⚠️  Configuration file not found at {CONFIG_PATH}")
    print("Please upload the config/queue_config.yaml to DBFS")
else:
    print(f"✓ Configuration file found at {CONFIG_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Upload Source Code to DBFS

# COMMAND ----------

# MAGIC %md
# MAGIC Run this cell to check if source code is uploaded:

# COMMAND ----------

import os

expected_files = [
    "/dbfs/src/models/task.py",
    "/dbfs/src/delay_queue/queue_manager.py",
    "/dbfs/src/delay_queue/backoff_strategy.py",
    "/dbfs/src/delay_queue/job_executor.py",
    "/dbfs/src/delay_queue/orchestrator.py",
    "/dbfs/src/utils/reconciliation.py",
    "/dbfs/src/utils/monitoring.py"
]

missing_files = []
for file_path in expected_files:
    if os.path.exists(file_path):
        print(f"✓ {file_path}")
    else:
        print(f"✗ {file_path}")
        missing_files.append(file_path)

if missing_files:
    print(f"\n⚠️  Missing {len(missing_files)} files. Please upload source code to DBFS.")
else:
    print("\n✓ All source files found!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Initialize Delta Tables

# COMMAND ----------

import sys
sys.path.append("/dbfs/src")

from delay_queue.orchestrator import create_orchestrator

# Create orchestrator (this will initialize all Delta tables)
orchestrator = create_orchestrator(spark, CONFIG_PATH)

print("✓ Delay Queue system initialized successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify Setup

# COMMAND ----------

# Check queue status
status = orchestrator.get_queue_status()

print("Queue Status:")
print("-" * 50)
print(f"Total Tasks: {status['statistics'].get('total_tasks', 0)}")
print(f"Pending: {status['statistics'].get('by_status', {}).get('PENDING', 0)}")
print(f"Processing: {status['statistics'].get('by_status', {}).get('PROCESSING', 0)}")
print(f"Completed: {status['statistics'].get('by_status', {}).get('COMPLETED', 0)}")
print(f"Failed: {status['statistics'].get('by_status', {}).get('FAILED', 0)}")
print(f"Dead Letter: {status['statistics'].get('by_status', {}).get('DEAD_LETTER', 0)}")
print("-" * 50)
print(f"Health Status: {status['health_metrics'].health_status}")
print(f"Active Alerts: {len(status['alerts'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Test Backoff Strategy

# COMMAND ----------

from delay_queue.backoff_strategy import BackoffConfig, ExponentialBackoffStrategy

# Create backoff strategy
config = BackoffConfig(
    initial_delay_seconds=60,
    max_delay_seconds=3600,
    exponential_base=2,
    max_retries=5,
    jitter_factor=0.1
)

strategy = ExponentialBackoffStrategy(config)

# Display retry schedule
print("Exponential Backoff Schedule:")
print("-" * 70)
print(f"{'Attempt':<10} {'Delay (sec)':<15} {'Delay (min)':<15} {'Cumulative (min)':<15}")
print("-" * 70)

for entry in strategy.get_retry_schedule():
    print(f"{entry['attempt']:<10} {entry['delay_seconds']:<15.2f} {entry['delay_minutes']:<15.2f} {entry['cumulative_minutes']:<15.2f}")

print("-" * 70)
print(f"Total retry time: {strategy.estimate_total_retry_time() / 60:.2f} minutes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Setup Complete!
# MAGIC
# MAGIC Your Delay Queue system is now ready. Next steps:
# MAGIC
# MAGIC 1. **Enqueue Tasks**: Use notebook `02_enqueue_migration_tasks.py`
# MAGIC 2. **Start Worker**: Use notebook `03_process_queue.py`
# MAGIC 3. **Monitor System**: Use notebook `04_monitoring_dashboard.py`

# COMMAND ----------


