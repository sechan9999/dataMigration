/*
    LIMS Analytics Database - Fact Tables
    Star schema fact tables for clinical analytics
*/

-- =====================================================
-- Fact: Lab Results
-- =====================================================
CREATE TABLE fact.FactLabResult (
    LabResultKey BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    -- Dimension keys
    PatientKey INT NOT NULL,
    TestKey INT NOT NULL,
    OrderDateKey INT NOT NULL,
    ResultDateKey INT NOT NULL,
    ResultTimeKey INT NOT NULL,
    LaboratoryKey INT NOT NULL,
    OrderingPhysicianKey INT NULL,
    -- Degenerate dimensions (high cardinality)
    ResultId BIGINT NOT NULL,
    OrderId BIGINT NOT NULL,
    SpecimenId BIGINT NULL,
    -- Measures
    ResultValueNumeric FLOAT NULL,
    ReferenceRangeMin FLOAT NULL,
    ReferenceRangeMax FLOAT NULL,
    TurnaroundTimeMinutes INT NULL,
    -- Flags and attributes
    ResultValue VARCHAR(500),
    ResultUnit VARCHAR(50),
    AbnormalFlag VARCHAR(10),
    CriticalFlag BIT NOT NULL DEFAULT 0,
    ResultStatus VARCHAR(50),
    SpecimenType VARCHAR(50),
    -- Calculated measures
    IsOutOfRange AS CASE
        WHEN ResultValueNumeric IS NOT NULL
             AND ReferenceRangeMin IS NOT NULL
             AND ReferenceRangeMax IS NOT NULL
             AND (ResultValueNumeric < ReferenceRangeMin OR ResultValueNumeric > ReferenceRangeMax)
        THEN 1
        ELSE 0
    END PERSISTED,
    PercentOfReferenceRangeMax AS CASE
        WHEN ResultValueNumeric IS NOT NULL AND ReferenceRangeMax IS NOT NULL AND ReferenceRangeMax > 0
        THEN (ResultValueNumeric / ReferenceRangeMax) * 100
        ELSE NULL
    END PERSISTED,
    -- Anomaly detection
    AnomalyScore FLOAT NULL,
    IsAnomaly BIT NOT NULL DEFAULT 0,
    AnomalyReason VARCHAR(500) NULL,
    -- Validation flags
    ReferenceRangeValid BIT NULL,
    TemporalConsistencyValid BIT NULL,
    SpecimenValid BIT NULL,
    ValidationMessage VARCHAR(1000) NULL,
    -- Audit columns
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    -- Foreign keys
    CONSTRAINT FK_FactLabResult_DimPatient FOREIGN KEY (PatientKey) REFERENCES dim.DimPatient(PatientKey),
    CONSTRAINT FK_FactLabResult_DimTest FOREIGN KEY (TestKey) REFERENCES dim.DimTest(TestKey),
    CONSTRAINT FK_FactLabResult_DimDate_Order FOREIGN KEY (OrderDateKey) REFERENCES dim.DimDate(DateKey),
    CONSTRAINT FK_FactLabResult_DimDate_Result FOREIGN KEY (ResultDateKey) REFERENCES dim.DimDate(DateKey),
    CONSTRAINT FK_FactLabResult_DimTime FOREIGN KEY (ResultTimeKey) REFERENCES dim.DimTime(TimeKey),
    CONSTRAINT FK_FactLabResult_DimLaboratory FOREIGN KEY (LaboratoryKey) REFERENCES dim.DimLaboratory(LaboratoryKey),
    CONSTRAINT FK_FactLabResult_DimPhysician FOREIGN KEY (OrderingPhysicianKey) REFERENCES dim.DimPhysician(PhysicianKey)
);

-- Columnstore index for analytical queries
CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_FactLabResult
ON fact.FactLabResult (
    PatientKey, TestKey, OrderDateKey, ResultDateKey, LaboratoryKey,
    ResultValueNumeric, TurnaroundTimeMinutes, IsOutOfRange, CriticalFlag, IsAnomaly
);

-- Traditional indexes for common queries
CREATE NONCLUSTERED INDEX IX_FactLabResult_PatientKey ON fact.FactLabResult(PatientKey);
CREATE NONCLUSTERED INDEX IX_FactLabResult_TestKey ON fact.FactLabResult(TestKey);
CREATE NONCLUSTERED INDEX IX_FactLabResult_ResultDateKey ON fact.FactLabResult(ResultDateKey);
CREATE NONCLUSTERED INDEX IX_FactLabResult_CriticalFlag ON fact.FactLabResult(CriticalFlag) WHERE CriticalFlag = 1;
CREATE NONCLUSTERED INDEX IX_FactLabResult_IsAnomaly ON fact.FactLabResult(IsAnomaly) WHERE IsAnomaly = 1;
CREATE UNIQUE NONCLUSTERED INDEX UQ_FactLabResult_ResultId ON fact.FactLabResult(ResultId);
GO

-- =====================================================
-- Fact: Test Orders (aggregated)
-- =====================================================
CREATE TABLE fact.FactTestOrder (
    TestOrderKey BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    -- Dimension keys
    PatientKey INT NOT NULL,
    TestKey INT NOT NULL,
    OrderDateKey INT NOT NULL,
    LaboratoryKey INT NOT NULL,
    OrderingPhysicianKey INT NULL,
    -- Degenerate dimensions
    OrderId BIGINT NOT NULL,
    -- Measures
    TotalOrderCost DECIMAL(10,2) NULL,
    TurnaroundTimeMinutes INT NULL,
    -- Status flags
    OrderStatus VARCHAR(50),
    IsStat BIT NOT NULL DEFAULT 0,
    IsRecollect BIT NOT NULL DEFAULT 0,
    -- Audit columns
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    -- Foreign keys
    CONSTRAINT FK_FactTestOrder_DimPatient FOREIGN KEY (PatientKey) REFERENCES dim.DimPatient(PatientKey),
    CONSTRAINT FK_FactTestOrder_DimTest FOREIGN KEY (TestKey) REFERENCES dim.DimTest(TestKey),
    CONSTRAINT FK_FactTestOrder_DimDate FOREIGN KEY (OrderDateKey) REFERENCES dim.DimDate(DateKey),
    CONSTRAINT FK_FactTestOrder_DimLaboratory FOREIGN KEY (LaboratoryKey) REFERENCES dim.DimLaboratory(LaboratoryKey),
    CONSTRAINT FK_FactTestOrder_DimPhysician FOREIGN KEY (OrderingPhysicianKey) REFERENCES dim.DimPhysician(PhysicianKey)
);

CREATE NONCLUSTERED INDEX IX_FactTestOrder_OrderDateKey ON fact.FactTestOrder(OrderDateKey);
CREATE NONCLUSTERED INDEX IX_FactTestOrder_TestKey ON fact.FactTestOrder(TestKey);
CREATE NONCLUSTERED INDEX IX_FactTestOrder_LaboratoryKey ON fact.FactTestOrder(LaboratoryKey);
GO

-- =====================================================
-- Fact: Quality Metrics (daily snapshots)
-- =====================================================
CREATE TABLE fact.FactQualityMetrics (
    QualityMetricsKey BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    DateKey INT NOT NULL,
    LaboratoryKey INT NOT NULL,
    TestKey INT NULL,
    -- Volume metrics
    TotalTests INT NOT NULL DEFAULT 0,
    TotalOrders INT NOT NULL DEFAULT 0,
    StatOrders INT NOT NULL DEFAULT 0,
    -- Quality metrics
    CriticalValues INT NOT NULL DEFAULT 0,
    AnomalousResults INT NOT NULL DEFAULT 0,
    ValidationFailures INT NOT NULL DEFAULT 0,
    Recollections INT NOT NULL DEFAULT 0,
    -- Turnaround time metrics
    AvgTurnaroundTimeMinutes DECIMAL(10,2) NULL,
    MedianTurnaroundTimeMinutes INT NULL,
    PercentMeetingSLA DECIMAL(5,2) NULL,
    -- Completeness metrics
    MissingResults INT NOT NULL DEFAULT 0,
    IncompleteOrders INT NOT NULL DEFAULT 0,
    -- Audit columns
    SnapshotDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    -- Foreign keys
    CONSTRAINT FK_FactQualityMetrics_DimDate FOREIGN KEY (DateKey) REFERENCES dim.DimDate(DateKey),
    CONSTRAINT FK_FactQualityMetrics_DimLaboratory FOREIGN KEY (LaboratoryKey) REFERENCES dim.DimLaboratory(LaboratoryKey),
    CONSTRAINT FK_FactQualityMetrics_DimTest FOREIGN KEY (TestKey) REFERENCES dim.DimTest(TestKey)
);

CREATE NONCLUSTERED INDEX IX_FactQualityMetrics_DateKey ON fact.FactQualityMetrics(DateKey);
CREATE UNIQUE INDEX UQ_FactQualityMetrics_DateLabTest ON fact.FactQualityMetrics(DateKey, LaboratoryKey, TestKey);
GO

-- =====================================================
-- Fact: Patient Lab Summary (aggregated view)
-- =====================================================
CREATE TABLE fact.FactPatientLabSummary (
    PatientLabSummaryKey BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    PatientKey INT NOT NULL,
    TestKey INT NOT NULL,
    -- Time period
    FirstTestDateKey INT NOT NULL,
    LastTestDateKey INT NOT NULL,
    -- Aggregate measures
    TotalTests INT NOT NULL,
    TotalAbnormal INT NOT NULL,
    TotalCritical INT NOT NULL,
    TotalAnomalies INT NOT NULL,
    -- Statistical measures
    MinValue FLOAT NULL,
    MaxValue FLOAT NULL,
    AvgValue FLOAT NULL,
    StdDevValue FLOAT NULL,
    CurrentValue FLOAT NULL,
    PreviousValue FLOAT NULL,
    -- Trends
    TrendDirection VARCHAR(20) NULL, -- Increasing, Decreasing, Stable
    PercentChange DECIMAL(10,2) NULL,
    -- Audit columns
    LastUpdatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    -- Foreign keys
    CONSTRAINT FK_FactPatientLabSummary_DimPatient FOREIGN KEY (PatientKey) REFERENCES dim.DimPatient(PatientKey),
    CONSTRAINT FK_FactPatientLabSummary_DimTest FOREIGN KEY (TestKey) REFERENCES dim.DimTest(TestKey),
    CONSTRAINT FK_FactPatientLabSummary_DimDate_First FOREIGN KEY (FirstTestDateKey) REFERENCES dim.DimDate(DateKey),
    CONSTRAINT FK_FactPatientLabSummary_DimDate_Last FOREIGN KEY (LastTestDateKey) REFERENCES dim.DimDate(DateKey)
);

CREATE UNIQUE INDEX UQ_FactPatientLabSummary_PatientTest ON fact.FactPatientLabSummary(PatientKey, TestKey);
CREATE NONCLUSTERED INDEX IX_FactPatientLabSummary_TestKey ON fact.FactPatientLabSummary(TestKey);
GO

PRINT 'Fact tables created successfully';
GO
