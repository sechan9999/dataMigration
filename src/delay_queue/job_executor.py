"""
Migration Job Executor with Network Resilience and Retry Logic
Handles the actual migration work with comprehensive error handling
"""

import logging
import time
import traceback
from typing import Optional, Dict, Any
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import socket

from ..models.task import MigrationTask, ReconciliationResult
from ..utils.reconciliation import ReconciliationValidator
from .backoff_strategy import NetworkBackoffStrategy


logger = logging.getLogger(__name__)


class NetworkException(Exception):
    """Exception for network-related errors"""
    pass


class DataValidationException(Exception):
    """Exception for data validation errors"""
    pass


class MigrationJobExecutor:
    """
    Executes migration jobs with comprehensive error handling.

    Features:
    - Network resilience with retry logic
    - Automatic rollback on failure (Delta ACID)
    - Data validation and reconciliation
    - Detailed error reporting
    """

    def __init__(
        self,
        spark: SparkSession,
        reconciliation_validator: ReconciliationValidator,
        config: Dict[str, Any]
    ):
        """
        Initialize migration job executor.

        Args:
            spark: Active Spark session
            reconciliation_validator: Validator for data integrity
            config: Configuration dictionary
        """
        self.spark = spark
        self.validator = reconciliation_validator
        self.config = config

        # Network retry configuration
        self.network_strategy = NetworkBackoffStrategy(
            config.get("network", {}).get("network_backoff_seconds", [2, 4, 8, 16])
        )

        self.connection_timeout = config.get("network", {}).get("connection_timeout_seconds", 300)
        self.read_timeout = config.get("network", {}).get("read_timeout_seconds", 600)

        # Migration settings
        self.enable_optimization = config.get("migration", {}).get("enable_optimization", True)
        self.chunk_size_mb = config.get("migration", {}).get("chunk_size_mb", 100)

    def execute_migration(self, task: MigrationTask) -> tuple[bool, Optional[ReconciliationResult], Optional[str]]:
        """
        Execute migration task with full error handling.

        Args:
            task: Migration task to execute

        Returns:
            Tuple of (success, reconciliation_result, error_message)
        """
        logger.info(f"Starting migration for task {task.task_id}: {task.task_name}")
        start_time = time.time()

        try:
            # Step 1: Validate source data exists
            self._validate_source_exists(task)

            # Step 2: Perform migration with network retry
            self._migrate_data_with_retry(task)

            # Step 3: Optimize target Delta table
            if self.enable_optimization:
                self._optimize_delta_table(task)

            # Step 4: Run reconciliation validation
            reconciliation_result = self._run_reconciliation(task)

            # Step 5: Verify reconciliation succeeded
            if reconciliation_result.status != "SUCCESS":
                error_msg = f"Reconciliation failed: {reconciliation_result.discrepancies}"
                logger.error(error_msg)

                # Rollback migration (delete target data)
                self._rollback_migration(task)

                return False, reconciliation_result, error_msg

            execution_time = time.time() - start_time
            logger.info(
                f"Migration completed successfully for {task.task_id} "
                f"in {execution_time:.2f} seconds"
            )

            return True, reconciliation_result, None

        except NetworkException as e:
            error_msg = f"Network error during migration: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            return False, None, error_msg

        except DataValidationException as e:
            error_msg = f"Data validation error: {str(e)}"
            logger.error(error_msg)
            self._rollback_migration(task)
            return False, None, error_msg

        except Exception as e:
            error_msg = f"Unexpected error during migration: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            self._rollback_migration(task)
            return False, None, error_msg

    def _validate_source_exists(self, task: MigrationTask):
        """
        Validate source CSV file exists and is readable.

        Args:
            task: Migration task

        Raises:
            DataValidationException: If source validation fails
        """
        logger.info(f"Validating source: {task.migration_metadata.source_csv_path}")

        try:
            # Try to read schema without loading all data
            source_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(task.migration_metadata.source_csv_path)

            # Validate schema exists
            if not source_df.schema:
                raise DataValidationException("Source CSV has no schema")

            # Get row count estimate (cached in metadata if available)
            if task.migration_metadata.source_row_count is None:
                task.migration_metadata.source_row_count = source_df.count()
                logger.info(f"Source row count: {task.migration_metadata.source_row_count}")

        except Exception as e:
            raise DataValidationException(f"Source validation failed: {str(e)}")

    def _migrate_data_with_retry(self, task: MigrationTask):
        """
        Migrate data from CSV to Delta with network retry logic.

        Args:
            task: Migration task

        Raises:
            NetworkException: If migration fails after retries
        """
        logger.info(f"Starting data migration with network retry")

        retry_count = 0
        last_error = None

        while retry_count <= self.network_strategy.max_retries:
            try:
                # Attempt migration
                self._perform_migration(task)
                logger.info("Data migration completed successfully")
                return

            except Exception as e:
                last_error = str(e)

                # Check if it's a network error
                if self._is_network_error(e):
                    if retry_count < self.network_strategy.max_retries:
                        backoff = self.network_strategy.calculate_backoff(retry_count)
                        logger.warning(
                            f"Network error on attempt {retry_count + 1}/{self.network_strategy.max_retries + 1}: {str(e)}. "
                            f"Retrying in {backoff} seconds..."
                        )
                        time.sleep(backoff)
                        retry_count += 1
                    else:
                        logger.error(f"Network error after {retry_count + 1} attempts: {str(e)}")
                        raise NetworkException(f"Migration failed after {retry_count + 1} network retries: {last_error}")
                else:
                    # Non-network error, don't retry
                    logger.error(f"Non-network error during migration: {str(e)}")
                    raise

        raise NetworkException(f"Migration failed after {retry_count} retries: {last_error}")

    def _perform_migration(self, task: MigrationTask):
        """
        Perform actual data migration from CSV to Delta.

        Uses Delta Lake ACID transactions - if this fails at 99%,
        Delta automatically rolls back to 0%, preventing corrupt data.

        Args:
            task: Migration task
        """
        logger.info(f"Reading source CSV: {task.migration_metadata.source_csv_path}")

        # Read source CSV
        source_df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("mode", "FAILFAST") \
            .csv(task.migration_metadata.source_csv_path)

        # Apply schema validation if configured
        if task.custom_config.get("validate_schema"):
            self._validate_schema(source_df, task)

        logger.info(f"Writing to Delta Lake: {task.migration_metadata.target_delta_path}")

        # Write to Delta Lake with ACID guarantees
        writer = source_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true")

        # Add partitioning if specified
        if task.migration_metadata.partition_columns:
            writer = writer.partitionBy(*task.migration_metadata.partition_columns)

        # Enable optimized writes
        if self.enable_optimization:
            writer = writer.option("optimizeWrite", "true") \
                           .option("autoCompact", "true")

        # Execute write (ACID transaction - all or nothing)
        writer.save(task.migration_metadata.target_delta_path)

        logger.info("Delta Lake write completed successfully")

    def _validate_schema(self, df: DataFrame, task: MigrationTask):
        """
        Validate dataframe schema against expected schema.

        Args:
            df: Source dataframe
            task: Migration task

        Raises:
            DataValidationException: If schema validation fails
        """
        expected_columns = task.custom_config.get("expected_columns", [])

        if expected_columns:
            actual_columns = set(df.columns)
            expected_columns_set = set(expected_columns)

            missing = expected_columns_set - actual_columns
            extra = actual_columns - expected_columns_set

            if missing or extra:
                raise DataValidationException(
                    f"Schema mismatch - missing: {missing}, extra: {extra}"
                )

    def _optimize_delta_table(self, task: MigrationTask):
        """
        Optimize Delta table for query performance.

        Args:
            task: Migration task
        """
        logger.info(f"Optimizing Delta table: {task.migration_metadata.target_delta_path}")

        try:
            # Run OPTIMIZE command
            self.spark.sql(f"""
                OPTIMIZE delta.`{task.migration_metadata.target_delta_path}`
            """)

            # Z-ORDER by partition columns if specified
            if task.migration_metadata.partition_columns:
                zorder_cols = ", ".join(task.migration_metadata.partition_columns[:3])  # Limit to 3
                self.spark.sql(f"""
                    OPTIMIZE delta.`{task.migration_metadata.target_delta_path}`
                    ZORDER BY ({zorder_cols})
                """)

            logger.info("Delta table optimization completed")

        except Exception as e:
            # Optimization failure is not critical
            logger.warning(f"Delta optimization failed (non-critical): {str(e)}")

    def _run_reconciliation(self, task: MigrationTask) -> ReconciliationResult:
        """
        Run reconciliation validation to ensure data integrity.

        Never consider migration complete until reconciliation returns SUCCESS.

        Args:
            task: Migration task

        Returns:
            ReconciliationResult with validation details
        """
        logger.info("Running reconciliation validation")

        # Get key sum columns from config
        key_sum_columns = self.config.get("reconciliation", {}).get(
            "key_sum_columns",
            []
        )

        # Perform validation
        result = self.validator.validate_migration(
            task.migration_metadata,
            key_sum_columns
        )

        logger.info(f"Reconciliation result: {result.status}")

        if result.status == "SUCCESS":
            logger.info(
                f"✓ Row counts match: {result.source_row_count} == {result.target_row_count}"
            )
            if result.key_sum_results:
                for col_name, col_result in result.key_sum_results.items():
                    if col_result.get("match"):
                        logger.info(
                            f"✓ {col_name} sum match: {col_result.get('source_sum')} == {col_result.get('target_sum')}"
                        )
        else:
            logger.error(f"✗ Reconciliation failed: {result.discrepancies}")

        return result

    def _rollback_migration(self, task: MigrationTask):
        """
        Rollback failed migration by deleting target data.

        Delta Lake's ACID properties ensure this is safe.

        Args:
            task: Migration task
        """
        logger.warning(f"Rolling back migration for {task.task_id}")

        try:
            # Delete target Delta table
            self.spark.sql(f"""
                DROP TABLE IF EXISTS delta.`{task.migration_metadata.target_delta_path}`
            """)

            # Clean up storage location
            from delta import DeltaTable
            try:
                delta_table = DeltaTable.forPath(self.spark, task.migration_metadata.target_delta_path)
                delta_table.delete()
            except Exception:
                pass  # Table might not exist

            logger.info("Migration rollback completed")

        except Exception as e:
            logger.error(f"Rollback failed: {str(e)}")

    def _is_network_error(self, exception: Exception) -> bool:
        """
        Determine if exception is network-related.

        Args:
            exception: Exception to check

        Returns:
            True if network error
        """
        error_str = str(exception).lower()

        network_indicators = [
            "timeout",
            "connection",
            "network",
            "socket",
            "unreachable",
            "reset by peer",
            "broken pipe",
            "connection refused",
            "no route to host",
            "temporary failure",
            "name resolution",
            "getaddrinfo failed"
        ]

        # Check exception type
        if isinstance(exception, (
            socket.timeout,
            socket.error,
            ConnectionError,
            TimeoutError
        )):
            return True

        # Check error message
        return any(indicator in error_str for indicator in network_indicators)


class BatchMigrationExecutor:
    """
    Executes multiple migrations in parallel with resource management.
    """

    def __init__(
        self,
        spark: SparkSession,
        job_executor: MigrationJobExecutor,
        max_parallel: int = 5
    ):
        """
        Initialize batch migration executor.

        Args:
            spark: Active Spark session
            job_executor: Job executor for individual tasks
            max_parallel: Maximum parallel migrations
        """
        self.spark = spark
        self.job_executor = job_executor
        self.max_parallel = max_parallel

    def execute_batch(self, tasks: list[MigrationTask]) -> Dict[str, Any]:
        """
        Execute batch of migration tasks.

        Args:
            tasks: List of tasks to execute

        Returns:
            Batch execution results
        """
        logger.info(f"Starting batch execution of {len(tasks)} tasks")

        results = {
            "total": len(tasks),
            "successful": 0,
            "failed": 0,
            "task_results": {}
        }

        # Simple sequential execution (parallel can be added with ThreadPoolExecutor)
        for task in tasks:
            success, reconciliation_result, error = self.job_executor.execute_migration(task)

            results["task_results"][task.task_id] = {
                "success": success,
                "reconciliation_status": reconciliation_result.status if reconciliation_result else "N/A",
                "error": error
            }

            if success:
                results["successful"] += 1
            else:
                results["failed"] += 1

        logger.info(
            f"Batch execution completed: {results['successful']} successful, "
            f"{results['failed']} failed"
        )

        return results
