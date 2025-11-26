/*
    LIMS Analytics Database - ETL Control Tables
    Tables for managing ETL processes and watermarks
*/

-- =====================================================
-- ETL: Watermark Table
-- =====================================================
CREATE TABLE etl.Watermark (
    WatermarkId INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    TableName VARCHAR(100) NOT NULL UNIQUE,
    WatermarkValue DATETIME2 NOT NULL,
    LastProcessedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    ProcessedRowCount BIGINT NOT NULL DEFAULT 0,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETDATE()
);

-- Initialize watermarks for key tables
INSERT INTO etl.Watermark (TableName, WatermarkValue) VALUES
('LabResults', '1900-01-01'),
('Patients', '1900-01-01'),
('TestMaster', '1900-01-01'),
('Specimens', '1900-01-01'),
('Orders', '1900-01-01');
GO

-- =====================================================
-- ETL: Pipeline Run Log
-- =====================================================
CREATE TABLE etl.PipelineRunLog (
    LogId BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    PipelineName VARCHAR(200) NOT NULL,
    RunId VARCHAR(100) NOT NULL,
    TableName VARCHAR(100),
    StartTime DATETIME2 NOT NULL DEFAULT GETDATE(),
    EndTime DATETIME2 NULL,
    Status VARCHAR(50) NOT NULL, -- Running, Success, Failed
    RowsCopied BIGINT NULL,
    DataRead BIGINT NULL,
    ExecutionDurationSeconds INT NULL,
    ErrorMessage VARCHAR(MAX) NULL,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE()
);

CREATE NONCLUSTERED INDEX IX_PipelineRunLog_PipelineName ON etl.PipelineRunLog(PipelineName);
CREATE NONCLUSTERED INDEX IX_PipelineRunLog_Status ON etl.PipelineRunLog(Status);
CREATE NONCLUSTERED INDEX IX_PipelineRunLog_StartTime ON etl.PipelineRunLog(StartTime);
GO

-- =====================================================
-- ETL: Data Quality Results
-- =====================================================
CREATE TABLE etl.DataQualityResults (
    QualityCheckId BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    CheckDate DATE NOT NULL,
    TableName VARCHAR(100) NOT NULL,
    CheckType VARCHAR(100) NOT NULL, -- Completeness, Validity, Consistency, etc.
    CheckName VARCHAR(200) NOT NULL,
    ExpectedValue VARCHAR(500),
    ActualValue VARCHAR(500),
    PassedFlag BIT NOT NULL,
    Severity VARCHAR(20) NOT NULL, -- Info, Warning, Error, Critical
    Details VARCHAR(MAX),
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE()
);

CREATE NONCLUSTERED INDEX IX_DataQualityResults_CheckDate ON etl.DataQualityResults(CheckDate);
CREATE NONCLUSTERED INDEX IX_DataQualityResults_TableName ON etl.DataQualityResults(TableName);
CREATE NONCLUSTERED INDEX IX_DataQualityResults_PassedFlag ON etl.DataQualityResults(PassedFlag) WHERE PassedFlag = 0;
GO

-- =====================================================
-- ETL: Reconciliation Results
-- =====================================================
CREATE TABLE etl.ReconciliationResults (
    ReconciliationId BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    ReconciliationDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    SourceTable VARCHAR(100) NOT NULL,
    TargetTable VARCHAR(100) NOT NULL,
    SourceRowCount BIGINT NOT NULL,
    TargetRowCount BIGINT NOT NULL,
    RowCountMatch BIT NOT NULL,
    KeySumColumns VARCHAR(500),
    SourceKeySum DECIMAL(38,2),
    TargetKeySum DECIMAL(38,2),
    KeySumMatch BIT,
    ReconciliationStatus VARCHAR(50) NOT NULL, -- SUCCESS, FAILURE
    Details VARCHAR(MAX),
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE()
);

CREATE NONCLUSTERED INDEX IX_ReconciliationResults_ReconciliationDate ON etl.ReconciliationResults(ReconciliationDate);
CREATE NONCLUSTERED INDEX IX_ReconciliationResults_Status ON etl.ReconciliationResults(ReconciliationStatus);
GO

PRINT 'ETL tables created successfully';
GO
