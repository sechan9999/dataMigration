/*
    Enhanced CDC Strategy - SQL Server Change Data Capture
    Enables CDC on source LIMS tables for automatic change tracking
*/

-- =====================================================
-- Enable CDC on Database (if not already enabled)
-- =====================================================
USE [LIMS_Production];
GO

-- Check if CDC is enabled
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'LIMS_Production' AND is_cdc_enabled = 1)
BEGIN
    EXEC sys.sp_cdc_enable_db;
    PRINT '✓ CDC enabled on database';
END
ELSE
BEGIN
    PRINT '✓ CDC already enabled on database';
END
GO

-- =====================================================
-- Enable CDC on LabResults Table
-- =====================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'dbo' AND t.name = 'LabResults' AND t.is_tracked_by_cdc = 1)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'LabResults',
        @role_name = NULL, -- NULL = public access
        @supports_net_changes = 1, -- Enable net changes query
        @capture_instance = N'dbo_LabResults',
        @captured_column_list = NULL; -- Capture all columns

    PRINT '✓ CDC enabled on dbo.LabResults';
END
ELSE
BEGIN
    PRINT '✓ CDC already enabled on dbo.LabResults';
END
GO

-- =====================================================
-- Enable CDC on Patients Table
-- =====================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'dbo' AND t.name = 'Patients' AND t.is_tracked_by_cdc = 1)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'Patients',
        @role_name = NULL,
        @supports_net_changes = 1,
        @capture_instance = N'dbo_Patients',
        @captured_column_list = NULL;

    PRINT '✓ CDC enabled on dbo.Patients';
END
GO

-- =====================================================
-- Enable CDC on TestMaster Table
-- =====================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'dbo' AND t.name = 'TestMaster' AND t.is_tracked_by_cdc = 1)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'TestMaster',
        @role_name = NULL,
        @supports_net_changes = 1,
        @capture_instance = N'dbo_TestMaster',
        @captured_column_list = NULL;

    PRINT '✓ CDC enabled on dbo.TestMaster';
END
GO

-- =====================================================
-- Enable CDC on Specimens Table
-- =====================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'dbo' AND t.name = 'Specimens' AND t.is_tracked_by_cdc = 1)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'Specimens',
        @role_name = NULL,
        @supports_net_changes = 1,
        @capture_instance = N'dbo_Specimens',
        @captured_column_list = NULL;

    PRINT '✓ CDC enabled on dbo.Specimens';
END
GO

-- =====================================================
-- Enable CDC on Orders Table
-- =====================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables t
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'dbo' AND t.name = 'Orders' AND t.is_tracked_by_cdc = 1)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'Orders',
        @role_name = NULL,
        @supports_net_changes = 1,
        @capture_instance = N'dbo_Orders',
        @captured_column_list = NULL;

    PRINT '✓ CDC enabled on dbo.Orders';
END
GO

-- =====================================================
-- Create Helper View for CDC Changes
-- =====================================================
CREATE OR ALTER VIEW dbo.vw_LabResults_CDC_Changes
AS
SELECT
    __$start_lsn AS ChangeLSN,
    __$seqval AS SequenceValue,
    __$operation AS Operation, -- 1=Delete, 2=Insert, 3=Before Update, 4=After Update
    CASE __$operation
        WHEN 1 THEN 'Delete'
        WHEN 2 THEN 'Insert'
        WHEN 3 THEN 'Update (Before)'
        WHEN 4 THEN 'Update (After)'
    END AS OperationType,
    __$update_mask AS UpdateMask,
    ResultId,
    PatientId,
    OrderId,
    TestCode,
    ResultValue,
    ResultValueNumeric,
    ResultDate,
    ModifiedDate
FROM cdc.dbo_LabResults_CT
WHERE __$operation IN (2, 4); -- Only inserts and after-update images
GO

-- =====================================================
-- Create Stored Procedure to Query CDC Changes
-- =====================================================
CREATE OR ALTER PROCEDURE dbo.usp_GetLabResultsCDCChanges
    @FromLSN binary(10) = NULL,
    @ToLSN binary(10) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Get LSN bounds if not provided
    IF @FromLSN IS NULL
        SET @FromLSN = sys.fn_cdc_get_min_lsn('dbo_LabResults');

    IF @ToLSN IS NULL
        SET @ToLSN = sys.fn_cdc_get_max_lsn();

    -- Query net changes (one row per key, latest version)
    SELECT
        c.ResultId,
        c.PatientId,
        c.OrderId,
        c.TestCode,
        c.ResultValue,
        c.ResultValueNumeric,
        c.ResultDate,
        c.ModifiedDate,
        c.__$operation AS OperationCode,
        CASE c.__$operation
            WHEN 1 THEN 'Delete'
            WHEN 2 THEN 'Insert'
            WHEN 4 THEN 'Update'
        END AS OperationType,
        c.__$start_lsn AS ChangeLSN
    FROM cdc.fn_cdc_get_net_changes_dbo_LabResults(@FromLSN, @ToLSN, 'all') c
    ORDER BY c.__$start_lsn;
END;
GO

-- =====================================================
-- Create Stored Procedure to Get LSN from Timestamp
-- =====================================================
CREATE OR ALTER PROCEDURE dbo.usp_GetLSNFromTimestamp
    @Timestamp DATETIME2,
    @LSN binary(10) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    SET @LSN = sys.fn_cdc_map_time_to_lsn('smallest greater than or equal', @Timestamp);

    SELECT @LSN AS LSN, @Timestamp AS Timestamp;
END;
GO

-- =====================================================
-- Create CDC Cleanup Job (Manual execution)
-- =====================================================
CREATE OR ALTER PROCEDURE dbo.usp_CleanupCDCTables
    @RetentionDays INT = 7
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @CleanupDate DATETIME = DATEADD(DAY, -@RetentionDays, GETDATE());

    PRINT 'Cleaning up CDC tables older than ' + CAST(@RetentionDays AS VARCHAR(10)) + ' days...';

    -- Note: SQL Server Agent job typically handles this automatically
    -- This procedure is for manual cleanup if needed

    EXEC sys.sp_cdc_cleanup_change_table
        @capture_instance = 'dbo_LabResults',
        @low_water_mark = NULL, -- NULL = use default retention
        @threshold = 5000; -- Max rows to delete per transaction

    PRINT '✓ CDC cleanup completed';
END;
GO

-- =====================================================
-- Create CDC Monitoring View
-- =====================================================
CREATE OR ALTER VIEW dbo.vw_CDC_Monitoring
AS
SELECT
    t.name AS SourceTable,
    ct.capture_instance AS CaptureInstance,
    ct.start_lsn AS StartLSN,
    ct.create_date AS CDCEnabledDate,
    CASE WHEN ct.has_drop_pending = 1 THEN 'Drop Pending' ELSE 'Active' END AS Status,
    (SELECT COUNT(*)
     FROM sys.tables st
     WHERE st.object_id = ct.object_id
     AND st.is_tracked_by_cdc = 1) AS IsCDCEnabled
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN cdc.change_tables ct ON t.object_id = ct.source_object_id
WHERE s.name = 'dbo';
GO

-- =====================================================
-- Verify CDC Configuration
-- =====================================================
PRINT '';
PRINT '========================================';
PRINT 'CDC Configuration Summary';
PRINT '========================================';

-- Check database-level CDC
SELECT
    name AS DatabaseName,
    is_cdc_enabled AS CDCEnabled,
    CASE is_cdc_enabled
        WHEN 1 THEN '✓ Enabled'
        ELSE '✗ Disabled'
    END AS Status
FROM sys.databases
WHERE name = 'LIMS_Production';

-- Check table-level CDC
PRINT '';
PRINT 'CDC-Enabled Tables:';
SELECT * FROM dbo.vw_CDC_Monitoring;

-- Show example CDC query
PRINT '';
PRINT 'Example CDC Query:';
PRINT 'EXEC dbo.usp_GetLabResultsCDCChanges @FromLSN = NULL, @ToLSN = NULL;';
PRINT '';
PRINT '✓ CDC setup completed successfully';
GO
