# Databricks notebook source
# MAGIC %md
# MAGIC # Monitoring Dashboard
# MAGIC
# MAGIC Real-time monitoring and analytics for the delay queue system.
# MAGIC
# MAGIC **What this notebook provides:**
# MAGIC 1. Queue health metrics
# MAGIC 2. Performance analytics
# MAGIC 3. Failure analysis
# MAGIC 4. Reconciliation validation results
# MAGIC 5. System alerts

# COMMAND ----------

import sys
sys.path.append("/dbfs/src")

from delay_queue.orchestrator import create_orchestrator
from pyspark.sql.functions import col, count, avg, sum as spark_sum, max as spark_max
import json

# Initialize orchestrator
CONFIG_PATH = "/dbfs/config/queue_config.yaml"
orchestrator = create_orchestrator(spark, CONFIG_PATH)

print("✓ Monitoring dashboard initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Current Queue Status

# COMMAND ----------

status = orchestrator.get_queue_status()
metrics = status['health_metrics']

print("=" * 70)
print("QUEUE HEALTH STATUS")
print("=" * 70)
print(f"🟢 Health: {metrics.health_status}")
print(f"📅 Timestamp: {metrics.timestamp}")
print()
print("TASK COUNTS:")
print(f"  Total Tasks: {metrics.total_tasks}")
print(f"  ⏳ Pending: {metrics.pending_tasks}")
print(f"  ⚙️  Processing: {metrics.processing_tasks}")
print(f"  🔄 Retry Scheduled: {metrics.retry_scheduled_tasks}")
print(f"  ✅ Completed: {metrics.completed_tasks}")
print(f"  ❌ Failed: {metrics.failed_tasks}")
print(f"  💀 Dead Letter: {metrics.dead_letter_tasks}")
print()
print("PERFORMANCE METRICS:")
print(f"  Avg Retry Count: {metrics.avg_retry_count:.2f}")
print(f"  Avg Execution Time: {metrics.avg_execution_time_seconds:.2f}s")
print(f"  Oldest Pending Task: {metrics.oldest_pending_task_age_minutes:.0f} minutes")
print()
print("ALERTS:")
print(f"  Tasks in DLQ (last hour): {metrics.tasks_in_dlq_last_hour}")
print(f"  Consecutive Failures: {metrics.consecutive_failures}")
print("=" * 70)

# COMMAND ----------

# Show active alerts
if status['alerts']:
    print("\n⚠️ ACTIVE ALERTS:")
    print("=" * 70)
    for alert in status['alerts']:
        severity_icon = "🔴" if alert['severity'] == 'CRITICAL' else "⚠️"
        print(f"{severity_icon} [{alert['severity']}] {alert['type']}")
        print(f"   {alert['message']}")
        print(f"   Threshold: {alert['threshold']}, Actual: {alert['actual']}")
        print()
else:
    print("\n✅ No active alerts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Queue Metrics Visualization

# COMMAND ----------

# Load queue data
queue_df = spark.read.format("delta").load("/mnt/datalake/delay_queue/tasks")

# Task distribution by status
status_dist = queue_df.groupBy("status").count().orderBy(col("count").desc())

print("Task Distribution by Status:")
display(status_dist)

# COMMAND ----------

# Task distribution by priority
priority_dist = queue_df.groupBy("priority").count().orderBy("priority")

print("Task Distribution by Priority:")
display(priority_dist)

# COMMAND ----------

# Execution time statistics for completed tasks
exec_stats = queue_df.filter(col("status") == "COMPLETED") \
    .select(
        avg("execution_time_seconds").alias("avg_exec_time"),
        spark_max("execution_time_seconds").alias("max_exec_time"),
        count("*").alias("completed_count")
    )

print("Execution Time Statistics:")
display(exec_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Retry Analysis

# COMMAND ----------

from pyspark.sql.functions import get_json_object

# Retry count distribution
retry_analysis = queue_df.select(
    "task_id",
    "task_name",
    "status",
    get_json_object("retry_metadata_json", "$.retry_count").cast("int").alias("retry_count"),
    get_json_object("retry_metadata_json", "$.last_error").alias("last_error")
).filter(col("retry_count") > 0)

print("Tasks with Retries:")
display(retry_analysis.orderBy(col("retry_count").desc()))

# COMMAND ----------

# Retry statistics by status
retry_stats = queue_df.select(
    "status",
    get_json_object("retry_metadata_json", "$.retry_count").cast("int").alias("retry_count")
).groupBy("status").agg(
    avg("retry_count").alias("avg_retries"),
    spark_max("retry_count").alias("max_retries"),
    count("*").alias("task_count")
)

print("Retry Statistics by Status:")
display(retry_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Reconciliation Results Analysis

# COMMAND ----------

# Load reconciliation results
recon_df = spark.read.format("delta").load("/mnt/datalake/delay_queue/reconciliation_results")

# Success/failure distribution
recon_status_dist = recon_df.groupBy("status").count()

print("Reconciliation Status Distribution:")
display(recon_status_dist)

# COMMAND ----------

# Detailed reconciliation results
recon_details = recon_df.select(
    "reconciliation_id",
    "status",
    "row_count_match",
    "source_row_count",
    "target_row_count",
    "checksum_match",
    "key_sums_match",
    "validated_at"
).orderBy(col("validated_at").desc())

print("Recent Reconciliation Results:")
display(recon_details.limit(20))

# COMMAND ----------

# Failed reconciliations
failed_recon = recon_df.filter(col("status") != "SUCCESS") \
    .select(
        "reconciliation_id",
        "status",
        "source_row_count",
        "target_row_count",
        "discrepancies_json",
        "validated_at"
    )

if failed_recon.count() > 0:
    print("⚠️ Failed Reconciliations:")
    display(failed_recon)
else:
    print("✅ All reconciliations passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Historical Metrics (Last 24 Hours)

# COMMAND ----------

# Load historical metrics
metrics_df = spark.read.format("delta").load("/mnt/datalake/delay_queue/metrics")

from datetime import datetime, timedelta
cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()

recent_metrics = metrics_df.filter(col("timestamp") >= cutoff) \
    .orderBy(col("timestamp").desc())

print("Health Status Over Time:")
display(
    recent_metrics.select(
        "timestamp",
        "health_status",
        "total_tasks",
        "pending_tasks",
        "completed_tasks",
        "failed_tasks",
        "dead_letter_tasks"
    )
)

# COMMAND ----------

# Performance trend
print("Performance Trend:")
display(
    recent_metrics.select(
        "timestamp",
        "avg_execution_time_seconds",
        "avg_retry_count",
        "consecutive_failures"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💀 Dead Letter Queue Analysis

# COMMAND ----------

# Analyze dead letter queue
dlq_tasks = queue_df.filter(col("status") == "DEAD_LETTER") \
    .select(
        "task_id",
        "task_name",
        get_json_object("retry_metadata_json", "$.retry_count").cast("int").alias("retry_count"),
        get_json_object("retry_metadata_json", "$.last_error").alias("last_error"),
        get_json_object("retry_metadata_json", "$.last_error_timestamp").alias("error_timestamp")
    ).orderBy(col("error_timestamp").desc())

dlq_count = dlq_tasks.count()

if dlq_count > 0:
    print(f"⚠️ Dead Letter Queue contains {dlq_count} tasks:")
    display(dlq_tasks)
else:
    print("✅ Dead Letter Queue is empty!")

# COMMAND ----------

# Error analysis for DLQ
if dlq_count > 0:
    # Group by error type
    error_analysis = queue_df.filter(col("status") == "DEAD_LETTER") \
        .select(
            get_json_object("retry_metadata_json", "$.last_error").alias("error_type")
        ).groupBy("error_type").count().orderBy(col("count").desc())

    print("Error Types in DLQ:")
    display(error_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Task Detail Lookup

# COMMAND ----------

# Widget for task ID lookup
dbutils.widgets.text("task_id", "", "Task ID to Lookup")
task_id_lookup = dbutils.widgets.get("task_id")

if task_id_lookup:
    task_detail = queue_df.filter(col("task_id") == task_id_lookup).first()

    if task_detail:
        print("=" * 70)
        print(f"TASK DETAILS: {task_id_lookup}")
        print("=" * 70)
        print(f"Task Name: {task_detail.task_name}")
        print(f"Status: {task_detail.status}")
        print(f"Priority: {task_detail.priority}")
        print(f"Created: {task_detail.created_at}")
        print(f"Updated: {task_detail.updated_at}")
        print(f"Started: {task_detail.started_at}")
        print(f"Completed: {task_detail.completed_at}")
        print(f"Execution Time: {task_detail.execution_time_seconds}s")
        print()
        print("Migration Metadata:")
        print(task_detail.migration_metadata_json)
        print()
        print("Retry Metadata:")
        print(task_detail.retry_metadata_json)
        print()
        if task_detail.reconciliation_result_json:
            print("Reconciliation Result:")
            print(task_detail.reconciliation_result_json)
    else:
        print(f"❌ Task {task_id_lookup} not found")
else:
    print("Enter a Task ID in the widget above to view details")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Maintenance Operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup Old Completed Tasks

# COMMAND ----------

# Cleanup tasks older than 30 days (uncomment to run)
# retention_days = 30
# deleted_count = orchestrator.cleanup_old_tasks(retention_days)
# print(f"✓ Cleaned up {deleted_count} tasks older than {retention_days} days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Summary Report

# COMMAND ----------

def generate_summary_report():
    """Generate comprehensive summary report"""
    status = orchestrator.get_queue_status()
    metrics = status['health_metrics']

    total = metrics.total_tasks
    completed = metrics.completed_tasks
    failed = metrics.failed_tasks + metrics.dead_letter_tasks

    success_rate = (completed / total * 100) if total > 0 else 0
    failure_rate = (failed / total * 100) if total > 0 else 0

    print("=" * 70)
    print("DELAY QUEUE SYSTEM - SUMMARY REPORT")
    print("=" * 70)
    print(f"Generated: {datetime.utcnow().isoformat()}")
    print()
    print("OVERALL HEALTH:")
    print(f"  Status: {metrics.health_status}")
    print(f"  Active Alerts: {len(status['alerts'])}")
    print()
    print("TASK STATISTICS:")
    print(f"  Total Tasks: {total}")
    print(f"  Success Rate: {success_rate:.1f}%")
    print(f"  Failure Rate: {failure_rate:.1f}%")
    print(f"  Avg Execution Time: {metrics.avg_execution_time_seconds:.2f}s")
    print(f"  Avg Retries: {metrics.avg_retry_count:.2f}")
    print()
    print("CURRENT STATE:")
    print(f"  Pending: {metrics.pending_tasks}")
    print(f"  Processing: {metrics.processing_tasks}")
    print(f"  Retry Scheduled: {metrics.retry_scheduled_tasks}")
    print(f"  Completed: {completed}")
    print(f"  Failed: {failed}")
    print()
    print("SYSTEM HEALTH:")
    print(f"  Consecutive Failures: {metrics.consecutive_failures}")
    print(f"  DLQ Tasks (last hour): {metrics.tasks_in_dlq_last_hour}")
    print(f"  Oldest Pending: {metrics.oldest_pending_task_age_minutes:.0f} min")
    print("=" * 70)

generate_summary_report()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Dashboard Complete!
# MAGIC
# MAGIC This dashboard provides real-time monitoring of your delay queue system.
# MAGIC
# MAGIC **Recommended Actions:**
# MAGIC - Check this dashboard regularly during migrations
# MAGIC - Investigate any tasks in the Dead Letter Queue
# MAGIC - Review failed reconciliations immediately
# MAGIC - Monitor consecutive failures and take action if threshold exceeded

# COMMAND ----------


