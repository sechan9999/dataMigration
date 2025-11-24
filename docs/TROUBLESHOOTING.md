# Troubleshooting Guide

## Common Issues and Solutions

### 1. Tasks Stuck in PROCESSING State

**Symptoms**:
- Tasks remain in PROCESSING state indefinitely
- No progress in queue

**Causes**:
- Worker crashed during processing
- Lock timeout too short
- Long-running migrations

**Solutions**:

1. **Check lock expiration**:
```python
# In monitoring dashboard
queue_df = spark.read.format("delta").load("/mnt/datalake/delay_queue/tasks")
stuck_tasks = queue_df.filter(
    (col("status") == "PROCESSING") &
    (col("locked_until") < current_timestamp())
)
display(stuck_tasks)
```

2. **Increase visibility timeout**:
```yaml
queue:
  visibility_timeout_seconds: 1800  # Increase to 30 minutes
```

3. **Manual unlock** (emergency only):
```python
from delta import DeltaTable

delta_table = DeltaTable.forPath(spark, "/mnt/datalake/delay_queue/tasks")
delta_table.update(
    condition=col("task_id") == "stuck-task-id",
    set={"status": lit("READY"), "locked_by": None, "locked_until": None}
)
```

### 2. High Failure Rate

**Symptoms**:
- Many tasks in FAILED or DEAD_LETTER status
- Low success rate in monitoring

**Diagnostic Steps**:

1. **Check error patterns**:
```python
# Group failures by error type
error_analysis = queue_df.filter(col("status") == "FAILED") \
    .select(get_json_object("retry_metadata_json", "$.last_error").alias("error")) \
    .groupBy("error").count().orderBy(col("count").desc())
display(error_analysis)
```

2. **Review dead letter queue**:
```python
dlq_tasks = queue_df.filter(col("status") == "DEAD_LETTER")
display(dlq_tasks.select("task_id", "task_name", "retry_metadata_json"))
```

**Common Causes & Solutions**:

#### Network Errors
```yaml
# Increase network retry attempts
network:
  max_network_retries: 8
  network_backoff_seconds: [2, 4, 8, 16, 32, 64]
```

#### Data Quality Issues
```python
# Add schema validation
custom_config = {
    "validate_schema": True,
    "expected_columns": ["col1", "col2", "col3"]
}
```

#### Resource Exhaustion
```yaml
# Reduce parallel processing
queue:
  batch_size: 5  # Reduce from 10
migration:
  max_parallel_migrations: 3  # Reduce from 5
```

### 3. Reconciliation Failures

**Symptoms**:
- Tasks marked as FAILED due to reconciliation
- Row count or checksum mismatches

**Diagnostic Steps**:

1. **Check reconciliation results**:
```python
recon_df = spark.read.format("delta").load("/mnt/datalake/delay_queue/reconciliation_results")
failed = recon_df.filter(col("status") != "SUCCESS")
display(failed.select("reconciliation_id", "discrepancies_json"))
```

2. **Investigate specific failure**:
```python
import json

result = failed.first()
discrepancies = json.loads(result.discrepancies_json)
print("Discrepancies:", discrepancies)
```

**Common Causes & Solutions**:

#### Row Count Mismatch
- **Cause**: Source data changed during migration
- **Solution**: Re-run migration with updated source

#### Checksum Mismatch
- **Cause**: Data type conversions, encoding issues
- **Solution**: Review schema inference, specify explicit schema

#### Key Sum Mismatch
- **Cause**: Floating-point precision, null handling
- **Solution**: Adjust tolerance threshold
```yaml
reconciliation:
  tolerance_threshold: 0.001  # Increase if needed
```

### 4. Queue Depth Growing

**Symptoms**:
- Pending tasks increasing
- Processing can't keep up

**Diagnostic Steps**:

1. **Check queue metrics**:
```python
status = orchestrator.get_queue_status()
print(f"Pending: {status['statistics'].get('by_status', {}).get('PENDING', 0)}")
print(f"Processing: {status['statistics'].get('by_status', {}).get('PROCESSING', 0)}")
```

2. **Analyze execution times**:
```python
queue_df.filter(col("status") == "COMPLETED") \
    .select(avg("execution_time_seconds"), max("execution_time_seconds"))
```

**Solutions**:

1. **Scale horizontally**:
```python
# Start multiple workers
# Worker 1 (Notebook 1)
orchestrator1 = create_orchestrator(spark, config)
orchestrator1.start_processing()

# Worker 2 (Notebook 2)
orchestrator2 = create_orchestrator(spark, config)
orchestrator2.start_processing()
```

2. **Optimize batch size**:
```yaml
queue:
  batch_size: 20  # Increase if workers can handle it
```

3. **Prioritize critical tasks**:
```python
# Enqueue with higher priority
orchestrator.enqueue_migration_task(
    ...,
    priority=TaskPriority.CRITICAL
)
```

### 5. Slow Migrations

**Symptoms**:
- High average execution time
- Tasks timing out

**Diagnostic Steps**:

1. **Profile execution**:
```python
# Check execution times by table
queue_df.filter(col("status") == "COMPLETED") \
    .select(
        get_json_object("migration_metadata_json", "$.table_name").alias("table"),
        "execution_time_seconds"
    ).groupBy("table").agg(
        avg("execution_time_seconds").alias("avg_time"),
        max("execution_time_seconds").alias("max_time")
    ).orderBy(col("avg_time").desc())
```

**Solutions**:

1. **Enable optimizations**:
```yaml
migration:
  enable_optimization: true
  chunk_size_mb: 100
```

2. **Add partitioning**:
```python
# For large tables
orchestrator.enqueue_migration_task(
    ...,
    partition_columns=["date", "region"]
)
```

3. **Increase cluster size**:
- Use larger Databricks cluster
- More executors and cores

### 6. Memory Errors

**Symptoms**:
- OutOfMemoryError exceptions
- Worker crashes

**Solutions**:

1. **Increase executor memory**:
```python
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.driver.memory", "8g")
```

2. **Process in chunks**:
```yaml
migration:
  chunk_size_mb: 50  # Reduce chunk size
```

3. **Disable schema inference caching**:
```python
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
```

### 7. Dead Letter Queue Growing

**Symptoms**:
- Increasing DLQ tasks
- CRITICAL alerts

**Action Plan**:

1. **Review all DLQ tasks**:
```python
dlq = queue_df.filter(col("status") == "DEAD_LETTER")
display(dlq.select("task_id", "task_name", "retry_metadata_json"))
```

2. **Categorize errors**:
```python
# Group by error type
dlq.select(
    get_json_object("retry_metadata_json", "$.last_error").alias("error")
).groupBy("error").count()
```

3. **Fix and re-enqueue**:
```python
# After fixing issue, create new task
orchestrator.enqueue_migration_task(
    task_name="Retry: Original Task Name",
    ...
)
```

4. **Clean up DLQ**:
```python
# After manual resolution
delta_table = DeltaTable.forPath(spark, "/mnt/datalake/delay_queue/tasks")
delta_table.delete(condition=col("status") == "DEAD_LETTER")
```

### 8. Configuration Issues

**Symptoms**:
- FileNotFoundError for config
- Invalid configuration errors

**Solutions**:

1. **Verify config location**:
```bash
databricks fs ls dbfs:/config/queue_config.yaml
```

2. **Validate YAML syntax**:
```python
import yaml

with open("/dbfs/config/queue_config.yaml", "r") as f:
    config = yaml.safe_load(f)
print("Config valid:", config)
```

3. **Check storage mounts**:
```python
display(dbutils.fs.mounts())
```

### 9. Performance Degradation

**Symptoms**:
- System slowing down over time
- Increasing execution times

**Solutions**:

1. **Optimize Delta tables**:
```python
# Optimize queue table
spark.sql("OPTIMIZE delta.`/mnt/datalake/delay_queue/tasks`")

# Vacuum old versions (be careful!)
spark.sql("VACUUM delta.`/mnt/datalake/delay_queue/tasks` RETAIN 168 HOURS")
```

2. **Clean old tasks**:
```python
orchestrator.cleanup_old_tasks(retention_days=7)
```

3. **Compact metrics table**:
```python
spark.sql("OPTIMIZE delta.`/mnt/datalake/delay_queue/metrics`")
```

### 10. Monitoring Dashboard Not Loading

**Symptoms**:
- Dashboard queries timing out
- Metrics not displaying

**Solutions**:

1. **Check table existence**:
```python
spark.read.format("delta").load("/mnt/datalake/delay_queue/tasks")
spark.read.format("delta").load("/mnt/datalake/delay_queue/metrics")
```

2. **Rebuild metrics**:
```python
# Force new metrics collection
metrics = orchestrator.monitor.collect_metrics()
orchestrator.monitor.save_metrics(metrics)
```

3. **Reduce lookback window**:
```python
# In dashboard
dashboard = orchestrator.get_dashboard_data(lookback_hours=6)  # Reduce from 24
```

## Emergency Procedures

### Stop All Processing

```python
# In each worker notebook
# Click "Cancel" or interrupt execution

# Or kill from code
import os
import signal
os.kill(os.getpid(), signal.SIGTERM)
```

### Reset Queue State

```python
# WARNING: Only in emergency situations
# This resets all PROCESSING tasks to READY

delta_table = DeltaTable.forPath(spark, "/mnt/datalake/delay_queue/tasks")
delta_table.update(
    condition=col("status") == "PROCESSING",
    set={
        "status": lit("READY"),
        "locked_by": None,
        "locked_until": None
    }
)
```

### Full System Reset

```python
# WARNING: This deletes all queue data
# Only use for testing/development

spark.sql("DROP TABLE IF EXISTS delta.`/mnt/datalake/delay_queue/tasks`")
spark.sql("DROP TABLE IF EXISTS delta.`/mnt/datalake/delay_queue/metrics`")
spark.sql("DROP TABLE IF EXISTS delta.`/mnt/datalake/delay_queue/audit_logs`")

# Reinitialize
orchestrator = create_orchestrator(spark, config_path)
```

## Getting Help

If you're still experiencing issues:

1. **Collect diagnostics**:
   - Queue statistics
   - Recent error messages
   - Configuration files
   - Execution logs

2. **Check logs**:
   - Databricks cluster logs
   - Spark driver logs
   - Worker notebook outputs

3. **Review documentation**:
   - [Architecture](ARCHITECTURE.md)
   - [Configuration Guide](CONFIGURATION.md)
   - [API Reference](API_REFERENCE.md)

4. **Contact support**:
   - GitHub Issues
   - Email support team
   - Internal helpdesk
