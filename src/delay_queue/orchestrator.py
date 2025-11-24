"""
Main Orchestrator for Delay Queue System
Coordinates queue processing, monitoring, and task execution
"""

import logging
import time
from typing import Dict, Any
from pyspark.sql import SparkSession
import yaml

from ..models.task import MigrationTask, TaskStatus
from .queue_manager import DelayQueueManager
from .job_executor import MigrationJobExecutor
from .backoff_strategy import ExponentialBackoffStrategy, BackoffConfig
from ..utils.reconciliation import ReconciliationValidator
from ..utils.monitoring import QueueMonitor, HealthCheckRunner, AlertConfig


logger = logging.getLogger(__name__)


class DelayQueueOrchestrator:
    """
    Main orchestrator for the delay queue system.

    Coordinates:
    - Queue management
    - Task execution
    - Retry logic with exponential backoff
    - Monitoring and health checks
    - Reconciliation validation
    """

    def __init__(self, spark: SparkSession, config_path: str):
        """
        Initialize orchestrator with configuration.

        Args:
            spark: Active Spark session
            config_path: Path to YAML configuration file
        """
        self.spark = spark

        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        logger.info("Initializing Delay Queue Orchestrator")

        # Initialize backoff strategy
        backoff_config = BackoffConfig(
            initial_delay_seconds=self.config["backoff"]["initial_delay_seconds"],
            max_delay_seconds=self.config["backoff"]["max_delay_seconds"],
            exponential_base=self.config["backoff"]["exponential_base"],
            max_retries=self.config["backoff"]["max_retries"],
            jitter_factor=self.config["backoff"]["jitter_factor"]
        )
        self.backoff_strategy = ExponentialBackoffStrategy(backoff_config)

        # Initialize queue manager
        self.queue_manager = DelayQueueManager(
            spark=self.spark,
            queue_table_path=self.config["delta"]["queue_table_path"],
            audit_log_path=self.config["delta"]["audit_log_path"],
            backoff_strategy=self.backoff_strategy
        )

        # Initialize reconciliation validator
        self.validator = ReconciliationValidator(
            spark=self.spark,
            tolerance_threshold=self.config["reconciliation"]["tolerance_threshold"],
            required_checks=self.config["reconciliation"]["required_checks"]
        )

        # Initialize job executor
        self.job_executor = MigrationJobExecutor(
            spark=self.spark,
            reconciliation_validator=self.validator,
            config=self.config
        )

        # Initialize monitoring
        alert_config = AlertConfig(
            max_consecutive_failures=self.config["monitoring"]["alert_on_consecutive_failures"]
        )
        self.monitor = QueueMonitor(
            spark=self.spark,
            queue_table_path=self.config["delta"]["queue_table_path"],
            metrics_table_path=self.config["monitoring"]["metrics_table_path"],
            alert_config=alert_config
        )

        self.health_checker = HealthCheckRunner(
            monitor=self.monitor,
            check_interval_seconds=self.config["monitoring"]["health_check_interval_seconds"]
        )

        # Worker configuration
        self.worker_id = f"worker-{int(time.time())}"
        self.batch_size = self.config["queue"]["batch_size"]
        self.polling_interval = self.config["queue"]["polling_interval_seconds"]
        self.visibility_timeout = self.config["queue"]["visibility_timeout_seconds"]

        logger.info(f"Orchestrator initialized with worker_id={self.worker_id}")

    def start_processing(self, max_iterations: int = None, continuous: bool = True):
        """
        Start processing tasks from the queue.

        Args:
            max_iterations: Maximum number of processing iterations (None for unlimited)
            continuous: If True, run continuously; if False, run once
        """
        logger.info("Starting delay queue processing")

        iteration = 0

        while True:
            iteration += 1

            if max_iterations and iteration > max_iterations:
                logger.info(f"Reached max iterations ({max_iterations}), stopping")
                break

            try:
                # Run health check
                if iteration % 10 == 1:  # Every 10 iterations
                    self._run_health_check()

                # Dequeue ready tasks
                tasks = self.queue_manager.dequeue_ready_tasks(
                    batch_size=self.batch_size,
                    worker_id=self.worker_id,
                    visibility_timeout_seconds=self.visibility_timeout
                )

                if not tasks:
                    logger.info("No ready tasks found, waiting...")
                    if not continuous:
                        break
                    time.sleep(self.polling_interval)
                    continue

                logger.info(f"Processing {len(tasks)} tasks")

                # Process each task
                for task in tasks:
                    self._process_task(task)

                # Short sleep between batches
                time.sleep(5)

            except KeyboardInterrupt:
                logger.info("Received interrupt signal, shutting down gracefully")
                break

            except Exception as e:
                logger.error(f"Error in processing loop: {str(e)}")
                time.sleep(self.polling_interval)

        logger.info("Queue processing stopped")

    def _process_task(self, task: MigrationTask):
        """
        Process a single migration task.

        Args:
            task: Task to process
        """
        logger.info(f"Processing task {task.task_id}: {task.task_name}")
        start_time = time.time()

        try:
            # Execute migration
            success, reconciliation_result, error = self.job_executor.execute_migration(task)

            execution_time = time.time() - start_time

            if success:
                # Mark as completed
                task.mark_completed(reconciliation_result, execution_time)
                self.queue_manager.mark_task_completed(task, execution_time)

                logger.info(
                    f"Task {task.task_id} completed successfully in {execution_time:.2f}s"
                )

            else:
                # Mark as failed and schedule retry
                self.queue_manager.mark_task_failed(task, error or "Unknown error")

                if task.status == TaskStatus.RETRY_SCHEDULED:
                    logger.warning(
                        f"Task {task.task_id} failed, scheduled for retry "
                        f"{task.retry_metadata.retry_count}/{task.retry_metadata.max_retries}"
                    )
                else:
                    logger.error(
                        f"Task {task.task_id} permanently failed and moved to DLQ"
                    )

        except Exception as e:
            logger.error(f"Unexpected error processing task {task.task_id}: {str(e)}")

            # Attempt to mark as failed
            try:
                self.queue_manager.mark_task_failed(task, str(e))
            except Exception as inner_e:
                logger.error(f"Failed to mark task as failed: {str(inner_e)}")

    def _run_health_check(self):
        """Run system health check"""
        try:
            health_result = self.health_checker.run_health_check()

            logger.info(
                f"Health check: status={health_result['health_status']}, "
                f"alerts={len(health_result['alerts'])}"
            )

            # Log critical alerts
            for alert in health_result["alerts"]:
                if alert["severity"] == "CRITICAL":
                    logger.error(f"CRITICAL: {alert['message']}")

        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")

    def enqueue_migration_task(
        self,
        task_name: str,
        source_csv_path: str,
        target_delta_path: str,
        table_name: str,
        **kwargs
    ) -> str:
        """
        Convenience method to enqueue a new migration task.

        Args:
            task_name: Name of the task
            source_csv_path: Path to source CSV
            target_delta_path: Path to target Delta table
            table_name: Name of the table
            **kwargs: Additional metadata

        Returns:
            Task ID
        """
        import uuid
        from ..models.task import MigrationMetadata, TaskPriority

        task_id = str(uuid.uuid4())

        metadata = MigrationMetadata(
            source_csv_path=source_csv_path,
            target_delta_path=target_delta_path,
            table_name=table_name,
            **kwargs
        )

        task = MigrationTask(
            task_id=task_id,
            task_name=task_name,
            migration_metadata=metadata,
            priority=kwargs.get("priority", TaskPriority.NORMAL)
        )

        success = self.queue_manager.enqueue_task(task)

        if success:
            logger.info(f"Task {task_id} enqueued successfully")
            return task_id
        else:
            logger.error(f"Failed to enqueue task {task_id}")
            return None

    def get_queue_status(self) -> Dict[str, Any]:
        """
        Get current queue status and statistics.

        Returns:
            Queue status dictionary
        """
        stats = self.queue_manager.get_queue_statistics()
        metrics = self.monitor.collect_metrics()
        alerts = self.monitor.generate_alerts(metrics)

        return {
            "statistics": stats,
            "health_metrics": metrics,
            "alerts": alerts,
            "worker_id": self.worker_id
        }

    def get_dashboard_data(self, lookback_hours: int = 24) -> Dict[str, Any]:
        """
        Get dashboard data for visualization.

        Args:
            lookback_hours: Hours of historical data

        Returns:
            Dashboard data
        """
        return self.monitor.get_metrics_dashboard(lookback_hours)

    def cleanup_old_tasks(self, retention_days: int = 30) -> int:
        """
        Clean up old completed tasks.

        Args:
            retention_days: Days to retain completed tasks

        Returns:
            Number of tasks cleaned up
        """
        return self.queue_manager.cleanup_old_completed_tasks(retention_days)


def create_orchestrator(spark: SparkSession, config_path: str = None) -> DelayQueueOrchestrator:
    """
    Factory function to create orchestrator instance.

    Args:
        spark: Active Spark session
        config_path: Path to config file (defaults to standard location)

    Returns:
        Configured orchestrator instance
    """
    if config_path is None:
        config_path = "/dbfs/config/queue_config.yaml"

    return DelayQueueOrchestrator(spark, config_path)
