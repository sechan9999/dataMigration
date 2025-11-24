"""
Monitoring and Alerting System for Delay Queue
Tracks system health, performance metrics, and anomalies
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max as spark_max, min as spark_min


logger = logging.getLogger(__name__)


@dataclass
class QueueHealthMetrics:
    """Health metrics for the delay queue system"""
    timestamp: str
    total_tasks: int
    pending_tasks: int
    processing_tasks: int
    retry_scheduled_tasks: int
    completed_tasks: int
    failed_tasks: int
    dead_letter_tasks: int
    avg_retry_count: float
    avg_execution_time_seconds: float
    oldest_pending_task_age_minutes: float
    tasks_in_dlq_last_hour: int
    consecutive_failures: int
    health_status: str  # HEALTHY, DEGRADED, CRITICAL


@dataclass
class AlertConfig:
    """Configuration for alerting thresholds"""
    max_pending_tasks: int = 100
    max_dlq_tasks_per_hour: int = 5
    max_consecutive_failures: int = 3
    max_task_age_minutes: int = 1440  # 24 hours
    min_success_rate: float = 0.95


class QueueMonitor:
    """
    Monitors delay queue health and generates alerts.

    Tracks:
    - Queue depth and processing rate
    - Failure rates and patterns
    - Dead letter queue growth
    - Task age and latency
    - System performance
    """

    def __init__(
        self,
        spark: SparkSession,
        queue_table_path: str,
        metrics_table_path: str,
        alert_config: AlertConfig = None
    ):
        """
        Initialize queue monitor.

        Args:
            spark: Active Spark session
            queue_table_path: Path to queue Delta table
            metrics_table_path: Path to metrics Delta table
            alert_config: Alert threshold configuration
        """
        self.spark = spark
        self.queue_table_path = queue_table_path
        self.metrics_table_path = metrics_table_path
        self.alert_config = alert_config or AlertConfig()

        self._initialize_metrics_table()

    def _initialize_metrics_table(self):
        """Initialize metrics storage table"""
        try:
            self.spark.read.format("delta").load(self.metrics_table_path)
            logger.info(f"Metrics table exists at {self.metrics_table_path}")
        except Exception:
            logger.info(f"Creating metrics table at {self.metrics_table_path}")
            empty_df = self.spark.createDataFrame([], schema="""
                timestamp STRING,
                total_tasks INT,
                pending_tasks INT,
                processing_tasks INT,
                retry_scheduled_tasks INT,
                completed_tasks INT,
                failed_tasks INT,
                dead_letter_tasks INT,
                avg_retry_count DOUBLE,
                avg_execution_time_seconds DOUBLE,
                oldest_pending_task_age_minutes DOUBLE,
                tasks_in_dlq_last_hour INT,
                consecutive_failures INT,
                health_status STRING,
                metrics_json STRING
            """)
            empty_df.write.format("delta").mode("overwrite").save(self.metrics_table_path)

    def collect_metrics(self) -> QueueHealthMetrics:
        """
        Collect current queue health metrics.

        Returns:
            QueueHealthMetrics with current system state
        """
        logger.info("Collecting queue health metrics")

        try:
            queue_df = self.spark.read.format("delta").load(self.queue_table_path)

            # Count tasks by status
            status_counts = self._get_status_counts(queue_df)

            # Calculate average retry count
            avg_retry = self._get_avg_retry_count(queue_df)

            # Calculate average execution time
            avg_exec_time = self._get_avg_execution_time(queue_df)

            # Find oldest pending task
            oldest_pending_age = self._get_oldest_pending_age(queue_df)

            # Count DLQ tasks in last hour
            dlq_last_hour = self._get_recent_dlq_count(queue_df)

            # Count consecutive failures
            consecutive_failures = self._get_consecutive_failures(queue_df)

            # Determine health status
            health_status = self._determine_health_status(
                status_counts,
                dlq_last_hour,
                consecutive_failures,
                oldest_pending_age
            )

            metrics = QueueHealthMetrics(
                timestamp=datetime.utcnow().isoformat(),
                total_tasks=status_counts.get("total", 0),
                pending_tasks=status_counts.get("PENDING", 0) + status_counts.get("READY", 0),
                processing_tasks=status_counts.get("PROCESSING", 0),
                retry_scheduled_tasks=status_counts.get("RETRY_SCHEDULED", 0),
                completed_tasks=status_counts.get("COMPLETED", 0),
                failed_tasks=status_counts.get("FAILED", 0),
                dead_letter_tasks=status_counts.get("DEAD_LETTER", 0),
                avg_retry_count=avg_retry,
                avg_execution_time_seconds=avg_exec_time,
                oldest_pending_task_age_minutes=oldest_pending_age,
                tasks_in_dlq_last_hour=dlq_last_hour,
                consecutive_failures=consecutive_failures,
                health_status=health_status
            )

            logger.info(f"Metrics collected: health_status={health_status}")
            return metrics

        except Exception as e:
            logger.error(f"Failed to collect metrics: {str(e)}")
            return QueueHealthMetrics(
                timestamp=datetime.utcnow().isoformat(),
                total_tasks=0,
                pending_tasks=0,
                processing_tasks=0,
                retry_scheduled_tasks=0,
                completed_tasks=0,
                failed_tasks=0,
                dead_letter_tasks=0,
                avg_retry_count=0.0,
                avg_execution_time_seconds=0.0,
                oldest_pending_task_age_minutes=0.0,
                tasks_in_dlq_last_hour=0,
                consecutive_failures=0,
                health_status="UNKNOWN"
            )

    def _get_status_counts(self, queue_df) -> Dict[str, int]:
        """Count tasks by status"""
        status_counts = {"total": queue_df.count()}

        for row in queue_df.groupBy("status").count().collect():
            status_counts[row.status] = row["count"]

        return status_counts

    def _get_avg_retry_count(self, queue_df) -> float:
        """Calculate average retry count"""
        try:
            result = queue_df.selectExpr(
                "AVG(CAST(JSON_EXTRACT(retry_metadata_json, '$.retry_count') AS INT)) as avg_retries"
            ).first()
            return float(result["avg_retries"] or 0.0)
        except Exception:
            return 0.0

    def _get_avg_execution_time(self, queue_df) -> float:
        """Calculate average execution time for completed tasks"""
        try:
            result = queue_df.filter(
                (col("status") == "COMPLETED") &
                col("execution_time_seconds").isNotNull()
            ).agg(
                avg("execution_time_seconds").alias("avg_time")
            ).first()
            return float(result["avg_time"] or 0.0)
        except Exception:
            return 0.0

    def _get_oldest_pending_age(self, queue_df) -> float:
        """Get age of oldest pending task in minutes"""
        try:
            oldest = queue_df.filter(
                col("status").isin(["PENDING", "READY", "RETRY_SCHEDULED"])
            ).agg(
                spark_min("created_at").alias("oldest")
            ).first()

            if oldest and oldest["oldest"]:
                oldest_time = datetime.fromisoformat(oldest["oldest"])
                age_minutes = (datetime.utcnow() - oldest_time).total_seconds() / 60
                return age_minutes
            return 0.0
        except Exception:
            return 0.0

    def _get_recent_dlq_count(self, queue_df) -> int:
        """Count tasks moved to DLQ in last hour"""
        try:
            one_hour_ago = (datetime.utcnow() - timedelta(hours=1)).isoformat()

            dlq_count = queue_df.filter(
                (col("status") == "DEAD_LETTER") &
                (col("updated_at") >= one_hour_ago)
            ).count()

            return dlq_count
        except Exception:
            return 0

    def _get_consecutive_failures(self, queue_df) -> int:
        """Count consecutive task failures (last N tasks)"""
        try:
            # Get last 10 completed tasks ordered by completion time
            recent_tasks = queue_df.filter(
                col("status").isin(["COMPLETED", "FAILED", "DEAD_LETTER"])
            ).orderBy(
                col("updated_at").desc()
            ).limit(10).select("status").collect()

            consecutive = 0
            for task in recent_tasks:
                if task.status in ["FAILED", "DEAD_LETTER"]:
                    consecutive += 1
                else:
                    break

            return consecutive
        except Exception:
            return 0

    def _determine_health_status(
        self,
        status_counts: Dict[str, int],
        dlq_last_hour: int,
        consecutive_failures: int,
        oldest_pending_age: float
    ) -> str:
        """
        Determine overall system health status.

        Returns:
            Health status: HEALTHY, DEGRADED, or CRITICAL
        """
        # Critical conditions
        if consecutive_failures >= self.alert_config.max_consecutive_failures:
            return "CRITICAL"

        if dlq_last_hour >= self.alert_config.max_dlq_tasks_per_hour:
            return "CRITICAL"

        # Degraded conditions
        pending_count = status_counts.get("PENDING", 0) + status_counts.get("READY", 0)
        if pending_count >= self.alert_config.max_pending_tasks:
            return "DEGRADED"

        if oldest_pending_age >= self.alert_config.max_task_age_minutes:
            return "DEGRADED"

        # Calculate success rate
        total_completed = status_counts.get("COMPLETED", 0)
        total_failed = status_counts.get("FAILED", 0) + status_counts.get("DEAD_LETTER", 0)

        if total_completed + total_failed > 0:
            success_rate = total_completed / (total_completed + total_failed)
            if success_rate < self.alert_config.min_success_rate:
                return "DEGRADED"

        return "HEALTHY"

    def save_metrics(self, metrics: QueueHealthMetrics):
        """
        Save metrics to Delta table for historical tracking.

        Args:
            metrics: Metrics to save
        """
        try:
            import json

            metrics_dict = asdict(metrics)
            metrics_dict["metrics_json"] = json.dumps(metrics_dict)

            metrics_df = self.spark.createDataFrame([metrics_dict])

            metrics_df.write.format("delta") \
                .mode("append") \
                .save(self.metrics_table_path)

            logger.info(f"Metrics saved to {self.metrics_table_path}")

        except Exception as e:
            logger.error(f"Failed to save metrics: {str(e)}")

    def generate_alerts(self, metrics: QueueHealthMetrics) -> List[Dict[str, Any]]:
        """
        Generate alerts based on current metrics.

        Args:
            metrics: Current health metrics

        Returns:
            List of alert dictionaries
        """
        alerts = []

        # Dead Letter Queue alert
        if metrics.tasks_in_dlq_last_hour >= self.alert_config.max_dlq_tasks_per_hour:
            alerts.append({
                "severity": "CRITICAL",
                "type": "DLQ_THRESHOLD_EXCEEDED",
                "message": f"{metrics.tasks_in_dlq_last_hour} tasks moved to DLQ in last hour",
                "threshold": self.alert_config.max_dlq_tasks_per_hour,
                "actual": metrics.tasks_in_dlq_last_hour
            })

        # Consecutive failures alert
        if metrics.consecutive_failures >= self.alert_config.max_consecutive_failures:
            alerts.append({
                "severity": "CRITICAL",
                "type": "CONSECUTIVE_FAILURES",
                "message": f"{metrics.consecutive_failures} consecutive task failures",
                "threshold": self.alert_config.max_consecutive_failures,
                "actual": metrics.consecutive_failures
            })

        # Queue depth alert
        if metrics.pending_tasks >= self.alert_config.max_pending_tasks:
            alerts.append({
                "severity": "WARNING",
                "type": "QUEUE_DEPTH_HIGH",
                "message": f"{metrics.pending_tasks} tasks pending in queue",
                "threshold": self.alert_config.max_pending_tasks,
                "actual": metrics.pending_tasks
            })

        # Task age alert
        if metrics.oldest_pending_task_age_minutes >= self.alert_config.max_task_age_minutes:
            alerts.append({
                "severity": "WARNING",
                "type": "TASK_AGE_HIGH",
                "message": f"Oldest pending task is {metrics.oldest_pending_task_age_minutes:.0f} minutes old",
                "threshold": self.alert_config.max_task_age_minutes,
                "actual": metrics.oldest_pending_task_age_minutes
            })

        # Success rate alert
        total_completed = metrics.completed_tasks
        total_failed = metrics.failed_tasks + metrics.dead_letter_tasks

        if total_completed + total_failed > 0:
            success_rate = total_completed / (total_completed + total_failed)
            if success_rate < self.alert_config.min_success_rate:
                alerts.append({
                    "severity": "WARNING",
                    "type": "LOW_SUCCESS_RATE",
                    "message": f"Success rate is {success_rate:.2%}",
                    "threshold": self.alert_config.min_success_rate,
                    "actual": success_rate
                })

        return alerts

    def get_metrics_dashboard(self, lookback_hours: int = 24) -> Dict[str, Any]:
        """
        Generate dashboard data for visualization.

        Args:
            lookback_hours: Hours of historical data to include

        Returns:
            Dashboard data dictionary
        """
        try:
            cutoff_time = (datetime.utcnow() - timedelta(hours=lookback_hours)).isoformat()

            metrics_df = self.spark.read.format("delta") \
                .load(self.metrics_table_path) \
                .filter(col("timestamp") >= cutoff_time) \
                .orderBy(col("timestamp").desc())

            metrics_history = []
            for row in metrics_df.collect():
                metrics_history.append(row.asDict())

            # Current metrics
            current_metrics = self.collect_metrics()

            # Calculate trends
            dashboard = {
                "current": asdict(current_metrics),
                "history": metrics_history,
                "trends": self._calculate_trends(metrics_history),
                "alerts": self.generate_alerts(current_metrics)
            }

            return dashboard

        except Exception as e:
            logger.error(f"Failed to generate dashboard: {str(e)}")
            return {}

    def _calculate_trends(self, history: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate metric trends from historical data"""
        if len(history) < 2:
            return {}

        # Simple trend calculation (latest vs first in window)
        latest = history[0]
        oldest = history[-1]

        trends = {
            "pending_tasks_trend": latest.get("pending_tasks", 0) - oldest.get("pending_tasks", 0),
            "completed_tasks_trend": latest.get("completed_tasks", 0) - oldest.get("completed_tasks", 0),
            "dlq_tasks_trend": latest.get("dead_letter_tasks", 0) - oldest.get("dead_letter_tasks", 0),
        }

        return trends


class HealthCheckRunner:
    """
    Runs periodic health checks on the delay queue system.
    """

    def __init__(
        self,
        monitor: QueueMonitor,
        check_interval_seconds: int = 300
    ):
        """
        Initialize health check runner.

        Args:
            monitor: Queue monitor instance
            check_interval_seconds: Seconds between health checks
        """
        self.monitor = monitor
        self.check_interval_seconds = check_interval_seconds

    def run_health_check(self) -> Dict[str, Any]:
        """
        Run complete health check.

        Returns:
            Health check results
        """
        logger.info("Running health check")

        # Collect metrics
        metrics = self.monitor.collect_metrics()

        # Save metrics
        self.monitor.save_metrics(metrics)

        # Generate alerts
        alerts = self.monitor.generate_alerts(metrics)

        # Log alerts
        for alert in alerts:
            if alert["severity"] == "CRITICAL":
                logger.error(f"CRITICAL ALERT: {alert['message']}")
            else:
                logger.warning(f"WARNING ALERT: {alert['message']}")

        health_check_result = {
            "timestamp": datetime.utcnow().isoformat(),
            "health_status": metrics.health_status,
            "metrics": asdict(metrics),
            "alerts": alerts,
            "next_check_in_seconds": self.check_interval_seconds
        }

        return health_check_result
