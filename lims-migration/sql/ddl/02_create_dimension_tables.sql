/*
    LIMS Analytics Database - Dimension Tables
    Star schema dimensional model for clinical analytics
*/

-- =====================================================
-- Dimension: Patient
-- =====================================================
CREATE TABLE dim.DimPatient (
    PatientKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    PatientId BIGINT NOT NULL,
    MRN VARCHAR(50) NOT NULL,
    FirstName VARCHAR(100),
    LastName VARCHAR(100),
    DateOfBirth DATE NOT NULL,
    Age AS DATEDIFF(YEAR, DateOfBirth, GETDATE()),
    AgeGroup AS CASE
        WHEN DATEDIFF(YEAR, DateOfBirth, GETDATE()) < 18 THEN 'Pediatric'
        WHEN DATEDIFF(YEAR, DateOfBirth, GETDATE()) BETWEEN 18 AND 64 THEN 'Adult'
        ELSE 'Senior'
    END PERSISTED,
    Gender CHAR(1),
    Race VARCHAR(50),
    Ethnicity VARCHAR(50),
    -- SCD Type 2 columns
    EffectiveDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    EndDate DATETIME2 NULL,
    IsCurrent BIT NOT NULL DEFAULT 1,
    -- Audit columns
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    CONSTRAINT UQ_DimPatient_MRN_Current UNIQUE (MRN, IsCurrent)
);

CREATE NONCLUSTERED INDEX IX_DimPatient_PatientId ON dim.DimPatient(PatientId);
CREATE NONCLUSTERED INDEX IX_DimPatient_MRN ON dim.DimPatient(MRN);
CREATE NONCLUSTERED INDEX IX_DimPatient_DateOfBirth ON dim.DimPatient(DateOfBirth);
GO

-- =====================================================
-- Dimension: Test
-- =====================================================
CREATE TABLE dim.DimTest (
    TestKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    TestCode VARCHAR(50) NOT NULL UNIQUE,
    TestName VARCHAR(200) NOT NULL,
    LoincCode VARCHAR(50),
    TestCategory VARCHAR(100),
    TestCategoryHierarchyLevel1 VARCHAR(100),
    TestCategoryHierarchyLevel2 VARCHAR(100),
    TestCategoryHierarchyLevel3 VARCHAR(100),
    SpecimenType VARCHAR(50),
    ResultType VARCHAR(50),
    ResultUnit VARCHAR(50),
    TurnaroundTimeMinutes INT,
    CostUSD DECIMAL(10,2),
    IsActive BIT NOT NULL DEFAULT 1,
    -- Audit columns
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETDATE()
);

CREATE NONCLUSTERED INDEX IX_DimTest_TestCategory ON dim.DimTest(TestCategory);
CREATE NONCLUSTERED INDEX IX_DimTest_LoincCode ON dim.DimTest(LoincCode);
GO

-- =====================================================
-- Dimension: Date (for time intelligence)
-- =====================================================
CREATE TABLE dim.DimDate (
    DateKey INT NOT NULL PRIMARY KEY,
    Date DATE NOT NULL UNIQUE,
    Year INT NOT NULL,
    Quarter INT NOT NULL,
    Month INT NOT NULL,
    MonthName VARCHAR(20) NOT NULL,
    Week INT NOT NULL,
    DayOfYear INT NOT NULL,
    DayOfMonth INT NOT NULL,
    DayOfWeek INT NOT NULL,
    DayName VARCHAR(20) NOT NULL,
    IsWeekend BIT NOT NULL,
    IsHoliday BIT NOT NULL,
    HolidayName VARCHAR(100),
    FiscalYear INT NOT NULL,
    FiscalQuarter INT NOT NULL,
    FiscalMonth INT NOT NULL
);

CREATE NONCLUSTERED INDEX IX_DimDate_Date ON dim.DimDate(Date);
CREATE NONCLUSTERED INDEX IX_DimDate_YearMonth ON dim.DimDate(Year, Month);
GO

-- =====================================================
-- Dimension: Time (for intraday analysis)
-- =====================================================
CREATE TABLE dim.DimTime (
    TimeKey INT NOT NULL PRIMARY KEY,
    Time TIME NOT NULL UNIQUE,
    Hour INT NOT NULL,
    Minute INT NOT NULL,
    HourBucket VARCHAR(20) NOT NULL, -- e.g., "00:00-01:00"
    IsBusinessHours BIT NOT NULL,
    Shift VARCHAR(20) -- Morning, Afternoon, Night
);
GO

-- =====================================================
-- Dimension: Laboratory
-- =====================================================
CREATE TABLE dim.DimLaboratory (
    LaboratoryKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    LaboratoryId INT NOT NULL UNIQUE,
    LaboratoryName VARCHAR(200) NOT NULL,
    LaboratoryType VARCHAR(50), -- Reference, Hospital, Specialty
    Address VARCHAR(500),
    City VARCHAR(100),
    State VARCHAR(50),
    ZipCode VARCHAR(20),
    AccreditationBody VARCHAR(100),
    AccreditationNumber VARCHAR(100),
    IsActive BIT NOT NULL DEFAULT 1,
    -- Audit columns
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETDATE()
);
GO

-- =====================================================
-- Dimension: Physician
-- =====================================================
CREATE TABLE dim.DimPhysician (
    PhysicianKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    PhysicianId INT NOT NULL UNIQUE,
    NPI VARCHAR(20),
    FirstName VARCHAR(100),
    LastName VARCHAR(100),
    Specialty VARCHAR(100),
    Department VARCHAR(100),
    IsActive BIT NOT NULL DEFAULT 1,
    -- Audit columns
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETDATE()
);

CREATE NONCLUSTERED INDEX IX_DimPhysician_Specialty ON dim.DimPhysician(Specialty);
CREATE NONCLUSTERED INDEX IX_DimPhysician_Department ON dim.DimPhysician(Department);
GO

-- =====================================================
-- Dimension: Test Category Hierarchy
-- =====================================================
CREATE TABLE dim.DimTestCategoryHierarchy (
    TestCategoryKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    Level1 VARCHAR(100) NOT NULL, -- e.g., "Chemistry"
    Level2 VARCHAR(100), -- e.g., "Basic Metabolic Panel"
    Level3 VARCHAR(100), -- e.g., "Electrolytes"
    FullPath VARCHAR(500) NOT NULL,
    -- Audit columns
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE()
);

CREATE UNIQUE INDEX UQ_DimTestCategoryHierarchy_Path ON dim.DimTestCategoryHierarchy(FullPath);
GO

-- =====================================================
-- Insert Initial Test Category Hierarchy
-- =====================================================
INSERT INTO dim.DimTestCategoryHierarchy (Level1, Level2, Level3, FullPath) VALUES
-- Chemistry
('Chemistry', 'Basic Metabolic Panel', 'Electrolytes', 'Chemistry > Basic Metabolic Panel > Electrolytes'),
('Chemistry', 'Basic Metabolic Panel', 'Kidney Function', 'Chemistry > Basic Metabolic Panel > Kidney Function'),
('Chemistry', 'Comprehensive Metabolic Panel', 'Liver Function', 'Chemistry > Comprehensive Metabolic Panel > Liver Function'),
('Chemistry', 'Lipid Panel', 'Cholesterol', 'Chemistry > Lipid Panel > Cholesterol'),
('Chemistry', 'Cardiac Markers', 'Troponin', 'Chemistry > Cardiac Markers > Troponin'),
('Chemistry', 'Diabetes Monitoring', 'Glucose', 'Chemistry > Diabetes Monitoring > Glucose'),

-- Hematology
('Hematology', 'Complete Blood Count', 'White Blood Cells', 'Hematology > Complete Blood Count > White Blood Cells'),
('Hematology', 'Complete Blood Count', 'Red Blood Cells', 'Hematology > Complete Blood Count > Red Blood Cells'),
('Hematology', 'Coagulation', 'PT/INR', 'Hematology > Coagulation > PT/INR'),
('Hematology', 'Coagulation', 'aPTT', 'Hematology > Coagulation > aPTT'),

-- Microbiology
('Microbiology', 'Culture', 'Blood Culture', 'Microbiology > Culture > Blood Culture'),
('Microbiology', 'Culture', 'Urine Culture', 'Microbiology > Culture > Urine Culture'),
('Microbiology', 'Serology', 'Antibody Testing', 'Microbiology > Serology > Antibody Testing'),

-- Immunology
('Immunology', 'Autoimmune', 'ANA', 'Immunology > Autoimmune > ANA'),
('Immunology', 'Allergy', 'IgE', 'Immunology > Allergy > IgE'),

-- Endocrinology
('Endocrinology', 'Thyroid', 'TSH', 'Endocrinology > Thyroid > TSH'),
('Endocrinology', 'Thyroid', 'Free T4', 'Endocrinology > Thyroid > Free T4'),
('Endocrinology', 'Reproductive', 'Testosterone', 'Endocrinology > Reproductive > Testosterone'),

-- Molecular
('Molecular', 'Genetic Testing', 'DNA Sequencing', 'Molecular > Genetic Testing > DNA Sequencing'),
('Molecular', 'Oncology', 'Tumor Markers', 'Molecular > Oncology > Tumor Markers');

PRINT 'Test category hierarchy populated';
GO

PRINT 'Dimension tables created successfully';
GO
