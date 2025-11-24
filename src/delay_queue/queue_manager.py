"""
Delay Queue Manager using Delta Lake for ACID transactions
Provides durable, distributed queue with exactly-once processing guarantees
"""

import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
from delta import DeltaTable

from ..models.task import MigrationTask, TaskStatus, TaskPriority
from .backoff_strategy import ExponentialBackoffStrategy, BackoffConfig


logger = logging.getLogger(__name__)


class DelayQueueManager:
    """
    Production-grade delay queue backed by Delta Lake.

    Features:
    - ACID transactions ensure no task is processed twice
    - Distributed locking prevents concurrent processing
    - Automatic retry with exponential backoff
    - Dead letter queue for permanently failed tasks
    - Audit logging for compliance
    """

    def __init__(
        self,
        spark: SparkSession,
        queue_table_path: str,
        audit_log_path: str,
        backoff_strategy: ExponentialBackoffStrategy
    ):
        """
        Initialize delay queue manager.

        Args:
            spark: Active Spark session
            queue_table_path: Delta Lake path for queue table
            audit_log_path: Delta Lake path for audit logs
            backoff_strategy: Strategy for calculating retry delays
        """
        self.spark = spark
        self.queue_table_path = queue_table_path
        self.audit_log_path = audit_log_path
        self.backoff_strategy = backoff_strategy

        self._initialize_tables()

    def _initialize_tables(self):
        """Initialize Delta Lake tables if they don't exist"""
        logger.info("Initializing delay queue tables...")

        # Check if queue table exists
        try:
            DeltaTable.forPath(self.spark, self.queue_table_path)
            logger.info(f"Queue table exists at {self.queue_table_path}")
        except Exception:
            logger.info(f"Creating new queue table at {self.queue_table_path}")
            self._create_queue_table()

        # Check if audit log exists
        try:
            DeltaTable.forPath(self.spark, self.audit_log_path)
            logger.info(f"Audit log exists at {self.audit_log_path}")
        except Exception:
            logger.info(f"Creating new audit log at {self.audit_log_path}")
            self._create_audit_table()

    def _create_queue_table(self):
        """Create initial queue table with optimized schema"""
        empty_df = self.spark.createDataFrame([], schema="""
            task_id STRING,
            task_name STRING,
            status STRING,
            priority INT,
            created_at STRING,
            updated_at STRING,
            started_at STRING,
            completed_at STRING,
            execution_time_seconds DOUBLE,
            locked_until STRING,
            locked_by STRING,
            migration_metadata_json STRING,
            retry_metadata_json STRING,
            reconciliation_result_json STRING,
            tags_json STRING,
            custom_config_json STRING
        """)

        empty_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(self.queue_table_path)

        # Optimize for query patterns
        delta_table = DeltaTable.forPath(self.spark, self.queue_table_path)
        self.spark.sql(f"""
            ALTER TABLE delta.`{self.queue_table_path}`
            SET TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.checkpoint.writeStatsAsStruct' = 'true',
                'delta.enableChangeDataFeed' = 'true'
            )
        """)

        logger.info("Queue table created successfully")

    def _create_audit_table(self):
        """Create audit log table for compliance"""
        empty_df = self.spark.createDataFrame([], schema="""
            audit_id STRING,
            task_id STRING,
            action STRING,
            old_status STRING,
            new_status STRING,
            actor STRING,
            timestamp STRING,
            details_json STRING
        """)

        empty_df.write.format("delta") \
            .mode("overwrite") \
            .save(self.audit_log_path)

        logger.info("Audit log table created successfully")

    def enqueue_task(self, task: MigrationTask) -> bool:
        """
        Add a new task to the queue.

        Args:
            task: Migration task to enqueue

        Returns:
            True if successfully enqueued
        """
        try:
            task_dict = task.to_dict()
            task_df = self.spark.createDataFrame([task_dict])

            task_df.write.format("delta") \
                .mode("append") \
                .save(self.queue_table_path)

            self._audit_log(
                task.task_id,
                "ENQUEUE",
                None,
                task.status.value,
                "SYSTEM",
                {"task_name": task.task_name}
            )

            logger.info(f"Task {task.task_id} enqueued successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to enqueue task {task.task_id}: {str(e)}")
            return False

    def dequeue_ready_tasks(
        self,
        batch_size: int = 10,
        worker_id: str = "worker-1",
        visibility_timeout_seconds: int = 900
    ) -> List[MigrationTask]:
        """
        Dequeue tasks that are ready for processing.

        Uses optimistic concurrency control via Delta Lake transactions
        to ensure no task is processed twice.

        Args:
            batch_size: Maximum number of tasks to dequeue
            worker_id: Identifier for the worker processing tasks
            visibility_timeout_seconds: Lock duration in seconds

        Returns:
            List of tasks ready for processing
        """
        try:
            current_time = datetime.utcnow()
            lock_expiry = (current_time + timedelta(seconds=visibility_timeout_seconds)).isoformat()

            # Load queue
            queue_df = self.spark.read.format("delta").load(self.queue_table_path)

            # Find ready tasks (PENDING, READY, or expired RETRY_SCHEDULED)
            ready_tasks_df = queue_df.filter(
                (
                    (col("status") == TaskStatus.PENDING.value) |
                    (col("status") == TaskStatus.READY.value) |
                    (
                        (col("status") == TaskStatus.RETRY_SCHEDULED.value) &
                        (col("locked_until") < current_time.isoformat())
                    )
                ) &
                (
                    (col("locked_until").isNull()) |
                    (col("locked_until") < current_time.isoformat())
                )
            ).orderBy(
                col("priority").asc(),  # Higher priority first (1 = CRITICAL)
                col("created_at").asc()  # FIFO within priority
            ).limit(batch_size)

            # Collect task IDs to lock
            ready_tasks = ready_tasks_df.collect()

            if not ready_tasks:
                logger.debug("No ready tasks found in queue")
                return []

            task_ids = [row.task_id for row in ready_tasks]

            # Acquire locks using Delta Lake transaction
            delta_table = DeltaTable.forPath(self.spark, self.queue_table_path)

            delta_table.update(
                condition=col("task_id").isin(task_ids) &
                          (
                              (col("locked_until").isNull()) |
                              (col("locked_until") < current_time.isoformat())
                          ),
                set={
                    "status": lit(TaskStatus.PROCESSING.value),
                    "locked_by": lit(worker_id),
                    "locked_until": lit(lock_expiry),
                    "started_at": lit(current_time.isoformat()),
                    "updated_at": lit(current_time.isoformat())
                }
            )

            # Read back locked tasks to confirm
            locked_df = self.spark.read.format("delta").load(self.queue_table_path) \
                .filter(
                    (col("task_id").isin(task_ids)) &
                    (col("locked_by") == worker_id)
                )

            locked_tasks = locked_df.collect()

            # Convert to MigrationTask objects
            tasks = []
            for row in locked_tasks:
                try:
                    task = MigrationTask.from_dict(row.asDict())
                    tasks.append(task)

                    self._audit_log(
                        task.task_id,
                        "DEQUEUE",
                        row.status,
                        TaskStatus.PROCESSING.value,
                        worker_id,
                        {"batch_size": batch_size}
                    )
                except Exception as e:
                    logger.error(f"Failed to deserialize task {row.task_id}: {str(e)}")

            logger.info(f"Dequeued {len(tasks)} tasks for processing")
            return tasks

        except Exception as e:
            logger.error(f"Failed to dequeue tasks: {str(e)}")
            return []

    def update_task_status(
        self,
        task_id: str,
        new_status: TaskStatus,
        **kwargs
    ) -> bool:
        """
        Update task status and related fields.

        Args:
            task_id: ID of task to update
            new_status: New status for task
            **kwargs: Additional fields to update

        Returns:
            True if successfully updated
        """
        try:
            delta_table = DeltaTable.forPath(self.spark, self.queue_table_path)

            # Get current status for audit
            old_task = self.spark.read.format("delta").load(self.queue_table_path) \
                .filter(col("task_id") == task_id) \
                .first()

            if not old_task:
                logger.warning(f"Task {task_id} not found")
                return False

            # Build update set
            update_set = {
                "status": lit(new_status.value),
                "updated_at": lit(datetime.utcnow().isoformat())
            }

            # Add optional fields
            for key, value in kwargs.items():
                if value is not None:
                    update_set[key] = lit(value)

            # Execute update
            delta_table.update(
                condition=col("task_id") == task_id,
                set=update_set
            )

            self._audit_log(
                task_id,
                "STATUS_UPDATE",
                old_task.status,
                new_status.value,
                "SYSTEM",
                kwargs
            )

            logger.info(f"Task {task_id} status updated to {new_status.value}")
            return True

        except Exception as e:
            logger.error(f"Failed to update task {task_id}: {str(e)}")
            return False

    def mark_task_completed(
        self,
        task: MigrationTask,
        execution_time_seconds: float
    ) -> bool:
        """Mark task as completed with results"""
        return self.update_task_status(
            task.task_id,
            TaskStatus.COMPLETED,
            completed_at=datetime.utcnow().isoformat(),
            execution_time_seconds=execution_time_seconds,
            reconciliation_result_json=task.to_dict().get("reconciliation_result_json"),
            locked_by=None,
            locked_until=None
        )

    def mark_task_failed(
        self,
        task: MigrationTask,
        error: str
    ) -> bool:
        """
        Mark task as failed and schedule retry or move to DLQ.

        Args:
            task: Failed task
            error: Error message

        Returns:
            True if successfully updated
        """
        try:
            # Check if should retry
            if self.backoff_strategy.should_retry(task.retry_metadata.retry_count):
                # Schedule retry with exponential backoff
                backoff_seconds = self.backoff_strategy.calculate_backoff(
                    task.retry_metadata.retry_count
                )

                task.schedule_retry(error, backoff_seconds)

                logger.info(
                    f"Task {task.task_id} scheduled for retry {task.retry_metadata.retry_count} "
                    f"in {backoff_seconds:.2f} seconds"
                )

                return self.update_task_status(
                    task.task_id,
                    TaskStatus.RETRY_SCHEDULED,
                    retry_metadata_json=task.to_dict()["retry_metadata_json"],
                    locked_by=None,
                    locked_until=None
                )
            else:
                # Exceeded max retries, move to dead letter queue
                task.mark_failed(error)

                logger.error(
                    f"Task {task.task_id} permanently failed after "
                    f"{task.retry_metadata.retry_count} retries"
                )

                return self.update_task_status(
                    task.task_id,
                    TaskStatus.DEAD_LETTER,
                    retry_metadata_json=task.to_dict()["retry_metadata_json"],
                    locked_by=None,
                    locked_until=None
                )

        except Exception as e:
            logger.error(f"Failed to mark task {task.task_id} as failed: {str(e)}")
            return False

    def get_queue_statistics(self) -> Dict[str, Any]:
        """Get current queue statistics"""
        try:
            queue_df = self.spark.read.format("delta").load(self.queue_table_path)

            stats = {
                "total_tasks": queue_df.count(),
                "by_status": {}
            }

            # Count by status
            status_counts = queue_df.groupBy("status").count().collect()
            for row in status_counts:
                stats["by_status"][row.status] = row["count"]

            # Average retry count
            avg_retries = queue_df.selectExpr(
                "AVG(JSON_EXTRACT(retry_metadata_json, '$.retry_count')) as avg_retries"
            ).first()

            stats["avg_retries"] = avg_retries["avg_retries"] if avg_retries else 0

            return stats

        except Exception as e:
            logger.error(f"Failed to get queue statistics: {str(e)}")
            return {}

    def _audit_log(
        self,
        task_id: str,
        action: str,
        old_status: Optional[str],
        new_status: str,
        actor: str,
        details: Dict[str, Any]
    ):
        """Write audit log entry"""
        try:
            import uuid
            import json

            audit_entry = {
                "audit_id": str(uuid.uuid4()),
                "task_id": task_id,
                "action": action,
                "old_status": old_status,
                "new_status": new_status,
                "actor": actor,
                "timestamp": datetime.utcnow().isoformat(),
                "details_json": json.dumps(details)
            }

            audit_df = self.spark.createDataFrame([audit_entry])
            audit_df.write.format("delta").mode("append").save(self.audit_log_path)

        except Exception as e:
            logger.warning(f"Failed to write audit log: {str(e)}")

    def cleanup_old_completed_tasks(self, retention_days: int = 30) -> int:
        """
        Remove old completed tasks to manage storage costs.

        Args:
            retention_days: Keep completed tasks for this many days

        Returns:
            Number of tasks deleted
        """
        try:
            cutoff_date = (datetime.utcnow() - timedelta(days=retention_days)).isoformat()

            delta_table = DeltaTable.forPath(self.spark, self.queue_table_path)

            # Count tasks to delete
            tasks_to_delete = self.spark.read.format("delta").load(self.queue_table_path) \
                .filter(
                    (col("status") == TaskStatus.COMPLETED.value) &
                    (col("completed_at") < cutoff_date)
                ).count()

            # Delete old completed tasks
            delta_table.delete(
                condition=(col("status") == TaskStatus.COMPLETED.value) &
                          (col("completed_at") < cutoff_date)
            )

            logger.info(f"Cleaned up {tasks_to_delete} old completed tasks")
            return tasks_to_delete

        except Exception as e:
            logger.error(f"Failed to cleanup old tasks: {str(e)}")
            return 0
