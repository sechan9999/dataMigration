"""
Example Usage of Delay Queue System

This file demonstrates how to use the delay queue system
for various migration scenarios.
"""

from pyspark.sql import SparkSession
from src.delay_queue.orchestrator import create_orchestrator
from src.models.task import TaskPriority, MigrationMetadata, MigrationTask
import uuid


def example_1_simple_migration(orchestrator):
    """
    Example 1: Simple CSV to Delta migration
    """
    print("Example 1: Simple Migration")
    print("=" * 70)

    task_id = orchestrator.enqueue_migration_task(
        task_name="Migrate Customer Data",
        source_csv_path="/mnt/legacy/customers.csv",
        target_delta_path="/mnt/datalake/customers",
        table_name="customers",
        priority=TaskPriority.NORMAL
    )

    print(f"✓ Task enqueued: {task_id}")
    return task_id


def example_2_batch_migration(orchestrator):
    """
    Example 2: Batch migration of multiple tables
    """
    print("\nExample 2: Batch Migration")
    print("=" * 70)

    tables = ["customers", "orders", "products", "transactions"]
    task_ids = []

    for table in tables:
        task_id = orchestrator.enqueue_migration_task(
            task_name=f"Migrate {table.title()} Table",
            source_csv_path=f"/mnt/legacy/{table}.csv",
            target_delta_path=f"/mnt/datalake/{table}",
            table_name=table,
            priority=TaskPriority.NORMAL
        )
        task_ids.append(task_id)
        print(f"✓ Enqueued: {table} ({task_id})")

    return task_ids


def example_3_partitioned_migration(orchestrator):
    """
    Example 3: Migration with partitioning
    """
    print("\nExample 3: Partitioned Migration")
    print("=" * 70)

    task_id = orchestrator.enqueue_migration_task(
        task_name="Migrate Sales Data (Partitioned)",
        source_csv_path="/mnt/legacy/sales.csv",
        target_delta_path="/mnt/datalake/sales",
        table_name="sales",
        partition_columns=["year", "month", "region"],
        priority=TaskPriority.HIGH
    )

    print(f"✓ Task enqueued with partitioning: {task_id}")
    return task_id


def example_4_high_priority_financial_migration(orchestrator):
    """
    Example 4: High-priority financial data migration
    """
    print("\nExample 4: Financial Data Migration (High Priority)")
    print("=" * 70)

    task_id = orchestrator.enqueue_migration_task(
        task_name="Migrate Financial Transactions",
        source_csv_path="/mnt/legacy/financial_transactions.csv",
        target_delta_path="/mnt/datalake/financial_transactions",
        table_name="financial_transactions",
        partition_columns=["transaction_date"],
        priority=TaskPriority.CRITICAL,
        # Custom configuration for financial data
        custom_config={
            "validate_schema": True,
            "expected_columns": [
                "transaction_id",
                "account_id",
                "transaction_date",
                "amount",
                "currency"
            ]
        },
        tags={
            "department": "finance",
            "compliance": "sox",
            "sensitivity": "high"
        }
    )

    print(f"✓ Critical financial task enqueued: {task_id}")
    return task_id


def example_5_healthcare_migration_with_validation(orchestrator):
    """
    Example 5: Healthcare data with strict validation
    """
    print("\nExample 5: Healthcare Data Migration (HIPAA)")
    print("=" * 70)

    task_id = orchestrator.enqueue_migration_task(
        task_name="Migrate Patient Records",
        source_csv_path="/mnt/legacy/patient_records.csv",
        target_delta_path="/mnt/datalake/patient_records",
        table_name="patient_records",
        partition_columns=["admission_year", "hospital_id"],
        priority=TaskPriority.CRITICAL,
        custom_config={
            "validate_schema": True,
            "expected_columns": [
                "patient_id",
                "admission_date",
                "diagnosis_code",
                "treatment_cost"
            ],
            "enable_phi_protection": True
        },
        tags={
            "department": "healthcare",
            "compliance": "hipaa"
        }
    )

    print(f"✓ Healthcare migration task enqueued: {task_id}")
    return task_id


def example_6_monitor_queue_status(orchestrator):
    """
    Example 6: Monitor queue status
    """
    print("\nExample 6: Monitor Queue Status")
    print("=" * 70)

    status = orchestrator.get_queue_status()

    print(f"Total Tasks: {status['statistics'].get('total_tasks', 0)}")
    print(f"Pending: {status['statistics'].get('by_status', {}).get('PENDING', 0)}")
    print(f"Processing: {status['statistics'].get('by_status', {}).get('PROCESSING', 0)}")
    print(f"Completed: {status['statistics'].get('by_status', {}).get('COMPLETED', 0)}")
    print(f"Failed: {status['statistics'].get('by_status', {}).get('FAILED', 0)}")
    print(f"Health Status: {status['health_metrics'].health_status}")

    if status['alerts']:
        print("\n⚠️ Active Alerts:")
        for alert in status['alerts']:
            print(f"  [{alert['severity']}] {alert['message']}")


def example_7_process_queue_once(orchestrator):
    """
    Example 7: Process queue once (non-continuous)
    """
    print("\nExample 7: Process Queue Once")
    print("=" * 70)

    orchestrator.start_processing(
        max_iterations=1,
        continuous=False
    )

    print("✓ Processing cycle complete")


def example_8_continuous_processing(orchestrator):
    """
    Example 8: Start continuous queue processing
    """
    print("\nExample 8: Start Continuous Processing")
    print("=" * 70)
    print("Starting continuous worker (Ctrl+C to stop)...")

    try:
        orchestrator.start_processing(
            max_iterations=None,  # Unlimited
            continuous=True
        )
    except KeyboardInterrupt:
        print("\n✓ Worker stopped by user")


def example_9_get_dashboard_data(orchestrator):
    """
    Example 9: Get dashboard data for visualization
    """
    print("\nExample 9: Get Dashboard Data")
    print("=" * 70)

    dashboard = orchestrator.get_dashboard_data(lookback_hours=24)

    print(f"Current Health: {dashboard['current']['health_status']}")
    print(f"Historical Records: {len(dashboard['history'])}")
    print(f"Active Alerts: {len(dashboard['alerts'])}")

    if dashboard['trends']:
        print("\nTrends (24h):")
        print(f"  Pending Tasks: {dashboard['trends'].get('pending_tasks_trend', 0):+d}")
        print(f"  Completed Tasks: {dashboard['trends'].get('completed_tasks_trend', 0):+d}")
        print(f"  DLQ Tasks: {dashboard['trends'].get('dlq_tasks_trend', 0):+d}")


def example_10_cleanup_old_tasks(orchestrator):
    """
    Example 10: Cleanup old completed tasks
    """
    print("\nExample 10: Cleanup Old Tasks")
    print("=" * 70)

    retention_days = 30
    deleted_count = orchestrator.cleanup_old_tasks(retention_days)

    print(f"✓ Cleaned up {deleted_count} tasks older than {retention_days} days")


def main():
    """
    Main function to run all examples
    """
    print("=" * 70)
    print("DELAY QUEUE SYSTEM - USAGE EXAMPLES")
    print("=" * 70)

    # Initialize Spark and orchestrator
    spark = SparkSession.builder \
        .appName("DelayQueueExamples") \
        .getOrCreate()

    orchestrator = create_orchestrator(
        spark,
        config_path="/dbfs/config/queue_config.yaml"
    )

    # Run examples
    example_1_simple_migration(orchestrator)
    example_2_batch_migration(orchestrator)
    example_3_partitioned_migration(orchestrator)
    example_4_high_priority_financial_migration(orchestrator)
    example_5_healthcare_migration_with_validation(orchestrator)
    example_6_monitor_queue_status(orchestrator)

    # These would typically be run separately
    # example_7_process_queue_once(orchestrator)
    # example_8_continuous_processing(orchestrator)

    example_9_get_dashboard_data(orchestrator)
    # example_10_cleanup_old_tasks(orchestrator)

    print("\n" + "=" * 70)
    print("✓ All examples completed!")
    print("=" * 70)


if __name__ == "__main__":
    main()
