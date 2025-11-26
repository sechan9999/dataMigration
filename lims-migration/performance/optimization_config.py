#!/usr/bin/env python3
"""
Performance Optimization Configuration
Implements partitioning, clustering, and indexing strategies for optimal performance
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth
from delta.tables import DeltaTable
import json


class PerformanceOptimizer:
    """
    Manages performance optimization for LIMS data pipeline
    """

    def __init__(self, spark: SparkSession):
        """Initialize optimizer"""
        self.spark = spark

        # Configure Spark for optimal Delta Lake performance
        self.configure_spark_settings()

    def configure_spark_settings(self):
        """
        Configure Spark settings for optimal performance
        """
        print("\n" + "="*80)
        print("Configuring Spark Performance Settings")
        print("="*80)

        spark_config = {
            # Delta Lake optimizations
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.delta.autoCompact.enabled": "true",
            "spark.databricks.delta.optimizeWrite.binSize": "512",
            "spark.databricks.delta.autoCompact.minNumFiles": "10",

            # Adaptive Query Execution
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",

            # Dynamic partition pruning
            "spark.sql.optimizer.dynamicPartitionPruning.enabled": "true",

            # File size optimization
            "spark.sql.files.maxPartitionBytes": "134217728",  # 128 MB
            "spark.sql.files.openCostInBytes": "4194304",  # 4 MB

            # Shuffle optimization
            "spark.sql.shuffle.partitions": "200",
            "spark.sql.adaptive.shuffle.targetPostShuffleInputSize": "67108864",  # 64 MB

            # Broadcast join threshold
            "spark.sql.autoBroadcastJoinThreshold": "10485760",  # 10 MB

            # Z-Ordering configuration
            "spark.databricks.delta.zorder.useDeltaPath": "true",

            # Cache optimization
            "spark.sql.inMemoryColumnarStorage.compressed": "true",
            "spark.sql.inMemoryColumnarStorage.batchSize": "10000"
        }

        for key, value in spark_config.items():
            self.spark.conf.set(key, value)
            print(f"  ✓ {key} = {value}")

        print("✓ Spark configuration complete")

    def create_partitioned_tables(self, storage_account: str):
        """
        Create Delta Lake tables with optimal partitioning strategies
        """
        print("\n" + "="*80)
        print("Creating Partitioned Delta Tables")
        print("="*80)

        # Define partitioning strategies per layer
        table_configs = {
            "bronze": {
                "labresults": {
                    "path": f"abfss://bronze@{storage_account}.dfs.core.windows.net/lims/labresults",
                    "partition_columns": ["_load_date"],
                    "clustering_columns": None,
                    "optimization": "Partition by load date for time-based queries"
                },
                "patients": {
                    "path": f"abfss://bronze@{storage_account}.dfs.core.windows.net/lims/patients",
                    "partition_columns": ["_load_date"],
                    "clustering_columns": None,
                    "optimization": "Partition by load date"
                }
            },
            "silver": {
                "labresults": {
                    "path": f"abfss://silver@{storage_account}.dfs.core.windows.net/lims/labresults",
                    "partition_columns": ["result_date_year", "result_date_month"],
                    "clustering_columns": ["TestCode", "PatientId"],
                    "optimization": "Partition by test date (year/month), cluster by TestCode and PatientId"
                },
                "patients": {
                    "path": f"abfss://silver@{storage_account}.dfs.core.windows.net/lims/patients",
                    "partition_columns": None,  # Small dimension table - no partitioning
                    "clustering_columns": ["PatientId"],
                    "optimization": "Small dimension - cluster by PatientId only"
                }
            },
            "gold": {
                "patient_lab_summary": {
                    "path": f"abfss://gold@{storage_account}.dfs.core.windows.net/lims/patient_lab_summary",
                    "partition_columns": None,  # Aggregated table - typically small
                    "clustering_columns": ["PatientId", "TestCode"],
                    "optimization": "Cluster by PatientId and TestCode for patient-test queries"
                },
                "daily_quality_metrics": {
                    "path": f"abfss://gold@{storage_account}.dfs.core.windows.net/lims/daily_quality_metrics",
                    "partition_columns": ["metric_year", "metric_month"],
                    "clustering_columns": ["PerformingLabId"],
                    "optimization": "Partition by metric date, cluster by laboratory"
                }
            }
        }

        return table_configs

    def optimize_table_with_zorder(self, table_path: str, zorder_columns: list):
        """
        Optimize Delta table with Z-Ordering
        """
        print(f"\nOptimizing table: {table_path}")
        print(f"Z-Ordering by: {', '.join(zorder_columns)}")

        # Read Delta table
        delta_table = DeltaTable.forPath(self.spark, table_path)

        # Optimize with Z-Order
        delta_table.optimize().executeZOrderBy(*zorder_columns)

        print(f"  ✓ Z-Ordering completed")

        # Get table statistics
        stats = self.spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
        print(f"  Files: {stats['numFiles']}")
        print(f"  Size: {stats['sizeInBytes'] / (1024**3):.2f} GB")

    def create_sql_indexes(self) -> str:
        """
        Generate SQL DDL for optimal indexing strategy
        """
        index_ddl = """
-- =====================================================
-- Performance Optimization - SQL Database Indexes
-- =====================================================

-- =====================================================
-- FactLabResult: Columnstore + Rowstore Indexes
-- =====================================================

-- Primary columnstore index (already exists from table creation)
-- Optimal for analytical queries scanning large data volumes

-- Rowstore indexes for selective lookups
CREATE NONCLUSTERED INDEX IX_FactLabResult_PatientKey_ResultDate
ON fact.FactLabResult(PatientKey, ResultDateKey)
INCLUDE (TestKey, ResultValueNumeric, CriticalFlag)
WITH (DATA_COMPRESSION = PAGE);

CREATE NONCLUSTERED INDEX IX_FactLabResult_TestKey_ResultDate
ON fact.FactLabResult(TestKey, ResultDateKey)
INCLUDE (PatientKey, ResultValueNumeric, IsOutOfRange)
WITH (DATA_COMPRESSION = PAGE);

CREATE NONCLUSTERED INDEX IX_FactLabResult_LaboratoryKey_ResultDate
ON fact.FactLabResult(LaboratoryKey, ResultDateKey)
INCLUDE (TestKey, TurnaroundTimeMinutes)
WITH (DATA_COMPRESSION = PAGE);

-- Filtered indexes for high-value queries
CREATE NONCLUSTERED INDEX IX_FactLabResult_Critical
ON fact.FactLabResult(ResultDateKey, PatientKey, TestKey)
INCLUDE (ResultValueNumeric, CriticalFlag)
WHERE CriticalFlag = 1
WITH (DATA_COMPRESSION = PAGE);

CREATE NONCLUSTERED INDEX IX_FactLabResult_Anomalies
ON fact.FactLabResult(ResultDateKey, PatientKey, TestKey)
INCLUDE (ResultValueNumeric, AnomalyScore, AnomalyReason)
WHERE IsAnomaly = 1
WITH (DATA_COMPRESSION = PAGE);

-- =====================================================
-- DimPatient: Optimize for lookups
-- =====================================================

CREATE NONCLUSTERED INDEX IX_DimPatient_MRN_Current
ON dim.DimPatient(MRN)
INCLUDE (FirstName, LastName, DateOfBirth, Gender)
WHERE IsCurrent = 1
WITH (DATA_COMPRESSION = PAGE);

-- =====================================================
-- DimTest: Optimize for hierarchy drill-down
-- =====================================================

CREATE NONCLUSTERED INDEX IX_DimTest_Hierarchy
ON dim.DimTest(TestCategoryHierarchyLevel1, TestCategoryHierarchyLevel2, TestCategoryHierarchyLevel3)
INCLUDE (TestCode, TestName)
WITH (DATA_COMPRESSION = PAGE);

CREATE NONCLUSTERED INDEX IX_DimTest_LoincCode
ON dim.DimTest(LoincCode)
INCLUDE (TestCode, TestName)
WHERE LoincCode IS NOT NULL
WITH (DATA_COMPRESSION = PAGE);

-- =====================================================
-- Audit Tables: Optimize for compliance queries
-- =====================================================

CREATE NONCLUSTERED INDEX IX_DataAccessLog_PHI_User_Date
ON audit.DataAccessLog(UserId, AccessTimestamp DESC)
INCLUDE (TableName, RecordIdentifier)
WHERE PHIAccessed = 1
WITH (DATA_COMPRESSION = PAGE);

CREATE NONCLUSTERED INDEX IX_DataModificationLog_Table_Date
ON audit.DataModificationLog(TableName, ModificationTimestamp DESC)
INCLUDE (UserId, ColumnName, OldValue, NewValue)
WITH (DATA_COMPRESSION = PAGE);

-- =====================================================
-- Table Partitioning for FactLabResult
-- =====================================================

-- Create partition function (monthly partitions)
CREATE PARTITION FUNCTION PF_LabResults_Monthly (INT)
AS RANGE RIGHT FOR VALUES (
    20230101, 20230201, 20230301, 20230401, 20230501, 20230601,
    20230701, 20230801, 20230901, 20231001, 20231101, 20231201,
    20240101, 20240201, 20240301, 20240401, 20240501, 20240601,
    20240701, 20240801, 20240901, 20241001, 20241101, 20241201,
    20250101
);

-- Create partition scheme
CREATE PARTITION SCHEME PS_LabResults_Monthly
AS PARTITION PF_LabResults_Monthly
ALL TO ([PRIMARY]);

-- Note: To apply partitioning to existing table, requires rebuild:
-- CREATE TABLE fact.FactLabResult_Partitioned (... same schema ...)
-- ON PS_LabResults_Monthly(ResultDateKey);
-- Then copy data and switch tables

-- =====================================================
-- Statistics Update Strategy
-- =====================================================

-- Create stored procedure for statistics maintenance
CREATE OR ALTER PROCEDURE dbo.usp_UpdateStatistics
AS
BEGIN
    SET NOCOUNT ON;

    -- Update statistics on fact tables with FULLSCAN
    UPDATE STATISTICS fact.FactLabResult WITH FULLSCAN;
    UPDATE STATISTICS fact.FactTestOrder WITH FULLSCAN;
    UPDATE STATISTICS fact.FactQualityMetrics WITH FULLSCAN;

    -- Update statistics on dimension tables
    UPDATE STATISTICS dim.DimPatient WITH FULLSCAN;
    UPDATE STATISTICS dim.DimTest WITH FULLSCAN;
    UPDATE STATISTICS dim.DimDate WITH FULLSCAN;
    UPDATE STATISTICS dim.DimLaboratory WITH FULLSCAN;

    PRINT '✓ Statistics updated successfully';
END;
GO

-- Schedule to run daily
-- EXEC msdb.dbo.sp_add_job @job_name = 'Daily_Statistics_Update';
-- EXEC msdb.dbo.sp_add_jobstep @job_name = 'Daily_Statistics_Update',
--      @step_name = 'Update Stats', @command = 'EXEC dbo.usp_UpdateStatistics';

-- =====================================================
-- Index Maintenance Strategy
-- =====================================================

CREATE OR ALTER PROCEDURE dbo.usp_IndexMaintenance
    @FragmentationThreshold FLOAT = 30.0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @SchemaName NVARCHAR(128);
    DECLARE @TableName NVARCHAR(128);
    DECLARE @IndexName NVARCHAR(128);
    DECLARE @Fragmentation FLOAT;

    -- Cursor to iterate through fragmented indexes
    DECLARE index_cursor CURSOR FOR
    SELECT
        s.name AS SchemaName,
        t.name AS TableName,
        i.name AS IndexName,
        ips.avg_fragmentation_in_percent AS Fragmentation
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
    INNER JOIN sys.tables t ON ips.object_id = t.object_id
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    WHERE ips.avg_fragmentation_in_percent > @FragmentationThreshold
      AND ips.page_count > 1000  -- Only defrag indexes with > 1000 pages
      AND i.name IS NOT NULL;

    OPEN index_cursor;
    FETCH NEXT FROM index_cursor INTO @SchemaName, @TableName, @IndexName, @Fragmentation;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @Fragmentation > 50.0
        BEGIN
            -- Rebuild if fragmentation > 50%
            SET @SQL = N'ALTER INDEX ' + QUOTENAME(@IndexName) +
                      N' ON ' + QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@TableName) +
                      N' REBUILD WITH (ONLINE = ON, DATA_COMPRESSION = PAGE)';
            PRINT 'Rebuilding: ' + @SchemaName + '.' + @TableName + '.' + @IndexName;
        END
        ELSE
        BEGIN
            -- Reorganize if fragmentation 30-50%
            SET @SQL = N'ALTER INDEX ' + QUOTENAME(@IndexName) +
                      N' ON ' + QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@TableName) +
                      N' REORGANIZE';
            PRINT 'Reorganizing: ' + @SchemaName + '.' + @TableName + '.' + @IndexName;
        END

        EXEC sp_executesql @SQL;

        FETCH NEXT FROM index_cursor INTO @SchemaName, @TableName, @IndexName, @Fragmentation;
    END

    CLOSE index_cursor;
    DEALLOCATE index_cursor;

    PRINT '✓ Index maintenance completed';
END;
GO

PRINT '✓ Performance optimization indexes and procedures created';
GO
"""
        return index_ddl

    def generate_performance_monitoring_queries(self) -> Dict[str, str]:
        """
        Generate performance monitoring queries
        """
        queries = {
            "delta_table_stats": """
-- Delta Lake table statistics
SELECT
    table_name,
    num_files,
    size_in_bytes / (1024*1024*1024) AS size_gb,
    num_partitions,
    last_modified
FROM (
    DESCRIBE DETAIL delta.`abfss://silver@storage.dfs.core.windows.net/lims/labresults`
)
            """,

            "sql_query_performance": """
-- Top 10 slowest queries
SELECT TOP 10
    qs.total_elapsed_time / qs.execution_count / 1000000.0 AS avg_elapsed_sec,
    qs.execution_count,
    qs.total_logical_reads / qs.execution_count AS avg_logical_reads,
    SUBSTRING(qt.text, (qs.statement_start_offset/2)+1,
        ((CASE qs.statement_end_offset
            WHEN -1 THEN DATALENGTH(qt.text)
            ELSE qs.statement_end_offset
        END - qs.statement_start_offset)/2) + 1) AS query_text
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
WHERE qt.text LIKE '%FactLabResult%'
ORDER BY avg_elapsed_sec DESC;
            """,

            "index_usage": """
-- Index usage statistics
SELECT
    OBJECT_NAME(s.object_id) AS TableName,
    i.name AS IndexName,
    s.user_seeks,
    s.user_scans,
    s.user_lookups,
    s.user_updates,
    s.last_user_seek,
    s.last_user_scan
FROM sys.dm_db_index_usage_stats s
INNER JOIN sys.indexes i ON s.object_id = i.object_id AND s.index_id = i.index_id
WHERE OBJECTPROPERTY(s.object_id, 'IsUserTable') = 1
ORDER BY s.user_seeks + s.user_scans + s.user_lookups DESC;
            """,

            "missing_indexes": """
-- Missing index recommendations
SELECT
    d.statement AS TableName,
    d.equality_columns,
    d.inequality_columns,
    d.included_columns,
    s.avg_total_user_cost * s.avg_user_impact * (s.user_seeks + s.user_scans) AS improvement_measure
FROM sys.dm_db_missing_index_details d
INNER JOIN sys.dm_db_missing_index_groups g ON d.index_handle = g.index_handle
INNER JOIN sys.dm_db_missing_index_group_stats s ON g.index_group_handle = s.group_handle
WHERE d.database_id = DB_ID()
ORDER BY improvement_measure DESC;
            """
        }

        return queries

    def generate_performance_optimization_guide(self) -> Dict[str, Any]:
        """
        Generate comprehensive performance optimization guide
        """
        guide = {
            "delta_lake_optimization": {
                "partitioning_strategy": {
                    "bronze_layer": "Partition by load date (_load_date) for time-based archival and queries",
                    "silver_layer": "Partition by business date (result_date_year/month) and cluster by high-cardinality columns (TestCode, PatientId)",
                    "gold_layer": "Partition aggregated tables by metric date, use clustering for dimensional attributes"
                },
                "file_sizing": {
                    "target_file_size": "128 MB - 512 MB per file",
                    "action": "Enable auto-optimize write and auto-compact",
                    "command": "spark.databricks.delta.optimizeWrite.enabled = true"
                },
                "zorder_clustering": {
                    "labresults": ["TestCode", "PatientId"],
                    "quality_metrics": ["PerformingLabId", "TestCode"],
                    "frequency": "Weekly for active partitions, monthly for historical"
                },
                "vacuum_strategy": {
                    "retention": "7 days for silver/gold, 30 days for bronze",
                    "frequency": "Weekly",
                    "command": "VACUUM delta.`path` RETAIN 168 HOURS"
                }
            },
            "sql_database_optimization": {
                "indexing": {
                    "columnstore": "Primary index for fact tables (analytical workload)",
                    "rowstore": "Secondary indexes for selective lookups (operational queries)",
                    "filtered_indexes": "For high-value predicates (CriticalFlag=1, IsAnomaly=1)"
                },
                "partitioning": {
                    "fact_tables": "Monthly partitions on ResultDateKey",
                    "benefit": "Partition elimination reduces I/O by 90%+ for time-range queries",
                    "sliding_window": "Archive old partitions to cheaper storage"
                },
                "statistics": {
                    "update_frequency": "Daily with FULLSCAN for fact tables",
                    "importance": "Critical for query optimizer cardinality estimates"
                },
                "compression": {
                    "page_compression": "All rowstore indexes",
                    "columnstore_archival": "Older partitions use archival compression"
                }
            },
            "query_optimization": {
                "best_practices": [
                    "Always filter on partition columns (ResultDateKey) when possible",
                    "Use covering indexes to avoid key lookups",
                    "Leverage columnstore for aggregation queries",
                    "Use NOLOCK hint for reporting queries (READ_COMMITTED_SNAPSHOT ON)",
                    "Parameterize queries to enable plan reuse"
                ],
                "anti_patterns": [
                    "Avoid SELECT * - specify columns explicitly",
                    "Avoid functions on indexed columns in WHERE clause",
                    "Avoid DISTINCT when GROUP BY achieves same result",
                    "Avoid excessive JOINs - denormalize for read-heavy workloads"
                ]
            },
            "monitoring": {
                "key_metrics": [
                    "Query execution time (p50, p95, p99)",
                    "Index fragmentation (rebuild if >30%)",
                    "Delta table file count (optimize if >1000 files)",
                    "Cache hit ratio (target >90%)"
                ],
                "alerting_thresholds": {
                    "query_timeout": "> 30 seconds",
                    "pipeline_duration": "> 4 hours",
                    "index_fragmentation": "> 50%",
                    "disk_io_latency": "> 10 ms"
                }
            }
        }

        return guide


def main():
    """Main execution"""
    from pathlib import Path

    # Initialize Spark session (for Databricks environment)
    spark = SparkSession.builder \
        .appName("LIMS Performance Optimization") \
        .getOrCreate()

    optimizer = PerformanceOptimizer(spark)

    # Generate optimization artifacts
    output_dir = Path(__file__).parent / 'optimization_artifacts'
    output_dir.mkdir(exist_ok=True)

    # Export SQL index DDL
    with open(output_dir / 'sql_indexes.sql', 'w') as f:
        f.write(optimizer.create_sql_indexes())

    # Export monitoring queries
    with open(output_dir / 'monitoring_queries.json', 'w') as f:
        json.dump(optimizer.generate_performance_monitoring_queries(), f, indent=2)

    # Export optimization guide
    with open(output_dir / 'optimization_guide.json', 'w') as f:
        json.dump(optimizer.generate_performance_optimization_guide(), f, indent=2)

    print("\n" + "="*80)
    print("✓ Performance Optimization Configuration Complete")
    print("="*80)


if __name__ == "__main__":
    main()
