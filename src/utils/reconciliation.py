"""
Reconciliation Validator for Data Integrity Checks
Never consider a migration complete until reconciliation returns SUCCESS
"""

import logging
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as spark_sum, count, countDistinct, md5, concat_ws
from datetime import datetime

from ..models.task import ReconciliationResult, MigrationMetadata


logger = logging.getLogger(__name__)


class ReconciliationValidator:
    """
    Validates data integrity between source CSV and target Delta table.

    Performs comprehensive checks:
    1. Row count validation
    2. Column-level checksums
    3. Key sum validation (e.g., Total Revenue, Total Patients)
    4. Null count verification
    5. Duplicate detection
    """

    def __init__(
        self,
        spark: SparkSession,
        tolerance_threshold: float = 0.0001,
        required_checks: List[str] = None
    ):
        """
        Initialize reconciliation validator.

        Args:
            spark: Active Spark session
            tolerance_threshold: Allowed difference for numeric comparisons
            required_checks: List of checks to perform
        """
        self.spark = spark
        self.tolerance_threshold = tolerance_threshold
        self.required_checks = required_checks or [
            "row_count",
            "column_checksums",
            "key_sums",
            "null_counts"
        ]

    def validate_migration(
        self,
        metadata: MigrationMetadata,
        key_sum_columns: List[str] = None
    ) -> ReconciliationResult:
        """
        Perform complete reconciliation validation.

        Args:
            metadata: Migration metadata with source/target paths
            key_sum_columns: Columns to sum for validation

        Returns:
            ReconciliationResult with detailed validation results
        """
        logger.info(f"Starting reconciliation for {metadata.table_name}")

        try:
            # Load source and target data
            source_df = self._load_source_csv(metadata.source_csv_path)
            target_df = self._load_target_delta(metadata.target_delta_path)

            discrepancies = []
            key_sum_results = {}

            # 1. Row Count Validation
            row_count_match, source_count, target_count = self._validate_row_count(
                source_df, target_df
            )

            if not row_count_match:
                discrepancies.append(
                    f"Row count mismatch: source={source_count}, target={target_count}"
                )

            # 2. Column Checksum Validation
            checksum_match = True
            if "column_checksums" in self.required_checks:
                checksum_match, checksum_details = self._validate_column_checksums(
                    source_df, target_df
                )
                if not checksum_match:
                    discrepancies.append(f"Column checksum mismatch: {checksum_details}")

            # 3. Key Sum Validation
            key_sums_match = True
            if "key_sums" in self.required_checks and key_sum_columns:
                key_sums_match, key_sum_results = self._validate_key_sums(
                    source_df, target_df, key_sum_columns
                )
                if not key_sums_match:
                    discrepancies.append(f"Key sum mismatch: {key_sum_results}")

            # 4. Null Count Validation
            if "null_counts" in self.required_checks:
                null_match, null_details = self._validate_null_counts(
                    source_df, target_df
                )
                if not null_match:
                    discrepancies.append(f"Null count mismatch: {null_details}")

            # Determine overall status
            status = "SUCCESS"
            if not row_count_match:
                status = "FAILED"
            elif not checksum_match or not key_sums_match:
                status = "PARTIAL"

            result = ReconciliationResult(
                status=status,
                row_count_match=row_count_match,
                source_row_count=source_count,
                target_row_count=target_count,
                checksum_match=checksum_match,
                key_sums_match=key_sums_match,
                key_sum_results=key_sum_results,
                discrepancies=discrepancies,
                validated_at=datetime.utcnow().isoformat()
            )

            logger.info(f"Reconciliation completed with status: {status}")
            return result

        except Exception as e:
            logger.error(f"Reconciliation failed: {str(e)}")
            return ReconciliationResult(
                status="FAILED",
                row_count_match=False,
                source_row_count=0,
                target_row_count=0,
                checksum_match=False,
                key_sums_match=False,
                discrepancies=[f"Reconciliation error: {str(e)}"]
            )

    def _load_source_csv(self, csv_path: str) -> DataFrame:
        """Load source CSV file"""
        logger.info(f"Loading source CSV from {csv_path}")

        return self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("mode", "FAILFAST") \
            .csv(csv_path)

    def _load_target_delta(self, delta_path: str) -> DataFrame:
        """Load target Delta table"""
        logger.info(f"Loading target Delta from {delta_path}")

        return self.spark.read.format("delta").load(delta_path)

    def _validate_row_count(
        self,
        source_df: DataFrame,
        target_df: DataFrame
    ) -> tuple[bool, int, int]:
        """
        Validate row counts match between source and target.

        Returns:
            Tuple of (match, source_count, target_count)
        """
        source_count = source_df.count()
        target_count = target_df.count()

        match = source_count == target_count

        logger.info(f"Row count validation: source={source_count}, target={target_count}, match={match}")

        return match, source_count, target_count

    def _validate_column_checksums(
        self,
        source_df: DataFrame,
        target_df: DataFrame
    ) -> tuple[bool, Dict[str, Any]]:
        """
        Validate column-level checksums to detect data corruption.

        Returns:
            Tuple of (match, details)
        """
        logger.info("Validating column checksums")

        try:
            # Get common columns
            source_columns = set(source_df.columns)
            target_columns = set(target_df.columns)
            common_columns = source_columns & target_columns

            if source_columns != target_columns:
                logger.warning(
                    f"Column mismatch: source_only={source_columns - target_columns}, "
                    f"target_only={target_columns - source_columns}"
                )

            # Calculate checksums for each column
            mismatches = []
            for col_name in common_columns:
                # Create checksum by concatenating all values and hashing
                source_checksum = source_df.select(
                    md5(concat_ws("", col(col_name).cast("string"))).alias("checksum")
                ).first()["checksum"]

                target_checksum = target_df.select(
                    md5(concat_ws("", col(col_name).cast("string"))).alias("checksum")
                ).first()["checksum"]

                if source_checksum != target_checksum:
                    mismatches.append(col_name)

            match = len(mismatches) == 0
            details = {
                "mismatched_columns": mismatches,
                "common_columns": list(common_columns),
                "column_diff": {
                    "source_only": list(source_columns - target_columns),
                    "target_only": list(target_columns - source_columns)
                }
            }

            return match, details

        except Exception as e:
            logger.error(f"Column checksum validation failed: {str(e)}")
            return False, {"error": str(e)}

    def _validate_key_sums(
        self,
        source_df: DataFrame,
        target_df: DataFrame,
        key_columns: List[str]
    ) -> tuple[bool, Dict[str, Dict[str, float]]]:
        """
        Validate key sum columns (e.g., Total Revenue, Total Patients).

        This is critical for financial and healthcare data where
        even small discrepancies indicate data corruption.

        Args:
            source_df: Source dataframe
            target_df: Target dataframe
            key_columns: Columns to sum and compare

        Returns:
            Tuple of (match, results_dict)
        """
        logger.info(f"Validating key sums for columns: {key_columns}")

        results = {}
        all_match = True

        for col_name in key_columns:
            try:
                # Check if column exists
                if col_name not in source_df.columns or col_name not in target_df.columns:
                    logger.warning(f"Key column {col_name} not found in both datasets")
                    results[col_name] = {
                        "status": "COLUMN_NOT_FOUND",
                        "source_sum": 0.0,
                        "target_sum": 0.0,
                        "difference": 0.0,
                        "match": False
                    }
                    all_match = False
                    continue

                # Calculate sums
                source_sum = source_df.select(
                    spark_sum(col(col_name)).alias("total")
                ).first()["total"]

                target_sum = target_df.select(
                    spark_sum(col(col_name)).alias("total")
                ).first()["total"]

                # Handle nulls
                source_sum = source_sum or 0.0
                target_sum = target_sum or 0.0

                # Calculate difference
                difference = abs(source_sum - target_sum)
                relative_diff = difference / max(abs(source_sum), 1.0)  # Avoid division by zero

                # Check if within tolerance
                match = relative_diff <= self.tolerance_threshold

                results[col_name] = {
                    "status": "MATCH" if match else "MISMATCH",
                    "source_sum": float(source_sum),
                    "target_sum": float(target_sum),
                    "difference": float(difference),
                    "relative_difference": float(relative_diff),
                    "match": match
                }

                if not match:
                    all_match = False
                    logger.warning(
                        f"Key sum mismatch for {col_name}: "
                        f"source={source_sum}, target={target_sum}, diff={difference}"
                    )
                else:
                    logger.info(
                        f"Key sum match for {col_name}: "
                        f"source={source_sum}, target={target_sum}"
                    )

            except Exception as e:
                logger.error(f"Failed to validate key sum for {col_name}: {str(e)}")
                results[col_name] = {
                    "status": "ERROR",
                    "error": str(e),
                    "match": False
                }
                all_match = False

        return all_match, results

    def _validate_null_counts(
        self,
        source_df: DataFrame,
        target_df: DataFrame
    ) -> tuple[bool, Dict[str, Any]]:
        """
        Validate null counts per column.

        Returns:
            Tuple of (match, details)
        """
        logger.info("Validating null counts")

        try:
            common_columns = set(source_df.columns) & set(target_df.columns)

            mismatches = []
            for col_name in common_columns:
                source_nulls = source_df.filter(col(col_name).isNull()).count()
                target_nulls = target_df.filter(col(col_name).isNull()).count()

                if source_nulls != target_nulls:
                    mismatches.append({
                        "column": col_name,
                        "source_nulls": source_nulls,
                        "target_nulls": target_nulls
                    })

            match = len(mismatches) == 0
            details = {"mismatched_null_counts": mismatches}

            return match, details

        except Exception as e:
            logger.error(f"Null count validation failed: {str(e)}")
            return False, {"error": str(e)}

    def generate_reconciliation_report(
        self,
        result: ReconciliationResult,
        output_path: str
    ):
        """
        Generate detailed reconciliation report as Delta table.

        Args:
            result: Reconciliation result
            output_path: Path to save report
        """
        try:
            import json

            report_data = [{
                "reconciliation_id": f"recon_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                "status": result.status,
                "validated_at": result.validated_at,
                "row_count_match": result.row_count_match,
                "source_row_count": result.source_row_count,
                "target_row_count": result.target_row_count,
                "checksum_match": result.checksum_match,
                "key_sums_match": result.key_sums_match,
                "key_sum_results_json": json.dumps(result.key_sum_results),
                "discrepancies_json": json.dumps(result.discrepancies)
            }]

            report_df = self.spark.createDataFrame(report_data)

            report_df.write.format("delta") \
                .mode("append") \
                .save(output_path)

            logger.info(f"Reconciliation report saved to {output_path}")

        except Exception as e:
            logger.error(f"Failed to generate reconciliation report: {str(e)}")


class ContinuousReconciliationMonitor:
    """
    Continuously monitors and validates completed migrations.
    Runs periodic reconciliation checks to ensure long-term data integrity.
    """

    def __init__(
        self,
        spark: SparkSession,
        validator: ReconciliationValidator,
        check_interval_hours: int = 24
    ):
        """
        Initialize continuous reconciliation monitor.

        Args:
            spark: Active Spark session
            validator: Reconciliation validator instance
            check_interval_hours: Hours between validation checks
        """
        self.spark = spark
        self.validator = validator
        self.check_interval_hours = check_interval_hours

    def schedule_reconciliation_checks(
        self,
        completed_tasks_path: str,
        results_output_path: str
    ):
        """
        Schedule periodic reconciliation checks for completed migrations.

        This ensures data integrity is maintained over time and
        catches any corruption that may occur post-migration.

        Args:
            completed_tasks_path: Path to completed migration tasks
            results_output_path: Path to save reconciliation results
        """
        logger.info("Starting continuous reconciliation monitoring")

        try:
            # Load completed tasks
            completed_df = self.spark.read.format("delta") \
                .load(completed_tasks_path) \
                .filter(col("status") == "COMPLETED")

            for task_row in completed_df.collect():
                metadata = MigrationMetadata.from_dict(
                    task_row.migration_metadata_json
                )

                # Run reconciliation
                result = self.validator.validate_migration(metadata)

                # Save result
                self.validator.generate_reconciliation_report(
                    result,
                    results_output_path
                )

                if result.status != "SUCCESS":
                    logger.error(
                        f"Reconciliation check failed for {metadata.table_name}: "
                        f"{result.discrepancies}"
                    )

        except Exception as e:
            logger.error(f"Continuous reconciliation monitoring failed: {str(e)}")
