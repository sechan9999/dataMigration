/*
    View: Unified Patient View
    Comprehensive patient clinical data repository
*/

CREATE OR ALTER VIEW dim.vw_PatientUnifiedView
AS
WITH LatestResults AS (
    SELECT
        f.PatientKey,
        t.TestCode,
        t.TestName,
        t.TestCategory,
        f.ResultValueNumeric,
        f.ResultValue,
        f.ResultUnit,
        f.AbnormalFlag,
        f.CriticalFlag,
        d.Date AS ResultDate,
        ROW_NUMBER() OVER (PARTITION BY f.PatientKey, f.TestKey ORDER BY f.ResultDateKey DESC) AS rn
    FROM fact.FactLabResult f
    JOIN dim.DimTest t ON f.TestKey = t.TestKey
    JOIN dim.DimDate d ON f.ResultDateKey = d.DateKey
    WHERE f.ResultStatus = 'FINAL'
),
PatientMetrics AS (
    SELECT
        PatientKey,
        COUNT(DISTINCT TestKey) AS TotalUniqueTests,
        COUNT(*) AS TotalLabResults,
        SUM(CASE WHEN CriticalFlag = 1 THEN 1 ELSE 0 END) AS TotalCriticalValues,
        SUM(CASE WHEN IsAnomaly = 1 THEN 1 ELSE 0 END) AS TotalAnomalies,
        MAX(ResultDateKey) AS LastTestDateKey
    FROM fact.FactLabResult
    GROUP BY PatientKey
)
SELECT
    p.PatientKey,
    p.PatientId,
    p.MRN,
    p.FirstName,
    p.LastName,
    p.DateOfBirth,
    p.Age,
    p.AgeGroup,
    p.Gender,
    p.Race,
    p.Ethnicity,
    -- Latest lab results (pivoted for common tests)
    MAX(CASE WHEN lr.TestCode = 'GLUCOSE' AND lr.rn = 1 THEN lr.ResultValueNumeric END) AS Latest_Glucose,
    MAX(CASE WHEN lr.TestCode = 'GLUCOSE' AND lr.rn = 1 THEN lr.ResultDate END) AS Latest_Glucose_Date,
    MAX(CASE WHEN lr.TestCode = 'HBA1C' AND lr.rn = 1 THEN lr.ResultValueNumeric END) AS Latest_HbA1c,
    MAX(CASE WHEN lr.TestCode = 'HBA1C' AND lr.rn = 1 THEN lr.ResultDate END) AS Latest_HbA1c_Date,
    MAX(CASE WHEN lr.TestCode = 'CREAT' AND lr.rn = 1 THEN lr.ResultValueNumeric END) AS Latest_Creatinine,
    MAX(CASE WHEN lr.TestCode = 'CREAT' AND lr.rn = 1 THEN lr.ResultDate END) AS Latest_Creatinine_Date,
    MAX(CASE WHEN lr.TestCode = 'WBC' AND lr.rn = 1 THEN lr.ResultValueNumeric END) AS Latest_WBC,
    MAX(CASE WHEN lr.TestCode = 'WBC' AND lr.rn = 1 THEN lr.ResultDate END) AS Latest_WBC_Date,
    MAX(CASE WHEN lr.TestCode = 'HGB' AND lr.rn = 1 THEN lr.ResultValueNumeric END) AS Latest_Hemoglobin,
    MAX(CASE WHEN lr.TestCode = 'HGB' AND lr.rn = 1 THEN lr.ResultDate END) AS Latest_Hemoglobin_Date,
    -- Patient metrics
    pm.TotalUniqueTests,
    pm.TotalLabResults,
    pm.TotalCriticalValues,
    pm.TotalAnomalies,
    ld.Date AS LastTestDate
FROM dim.DimPatient p
LEFT JOIN LatestResults lr ON p.PatientKey = lr.PatientKey
LEFT JOIN PatientMetrics pm ON p.PatientKey = pm.PatientKey
LEFT JOIN dim.DimDate ld ON pm.LastTestDateKey = ld.DateKey
WHERE p.IsCurrent = 1
GROUP BY
    p.PatientKey, p.PatientId, p.MRN, p.FirstName, p.LastName,
    p.DateOfBirth, p.Age, p.AgeGroup, p.Gender, p.Race, p.Ethnicity,
    pm.TotalUniqueTests, pm.TotalLabResults, pm.TotalCriticalValues,
    pm.TotalAnomalies, ld.Date;
GO
