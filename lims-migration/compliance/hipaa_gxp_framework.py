#!/usr/bin/env python3
"""
HIPAA/GxP Compliance Framework
Implements compliance controls, audit trails, and validation documentation
"""

import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any
from pathlib import Path


class ComplianceFramework:
    """
    Manages HIPAA and GxP compliance requirements
    """

    def __init__(self, config_path: str):
        """Initialize compliance framework"""
        with open(config_path, 'r') as f:
            self.config = json.load(f)

        self.compliance_config = {
            "hipaa": {
                "enabled": True,
                "phi_fields": [
                    "PatientId", "MRN", "FirstName", "LastName", "DateOfBirth",
                    "ResultValue", "ResultValueNumeric", "SpecimenId"
                ],
                "audit_retention_years": 7,
                "encryption_required": True,
                "access_logging_required": True
            },
            "gxp": {
                "enabled": True,
                "validation_level": "21 CFR Part 11",
                "electronic_signature_required": True,
                "audit_trail_required": True,
                "data_integrity_alcoa": True  # Attributable, Legible, Contemporaneous, Original, Accurate
            }
        }

    def create_audit_trail_schema(self) -> str:
        """
        Generate SQL DDL for comprehensive audit trail tables
        """
        audit_schema = """
-- =====================================================
-- HIPAA/GxP Audit Trail Schema
-- Compliance with 21 CFR Part 11 and HIPAA regulations
-- =====================================================

-- Create audit schema if not exists
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'audit')
BEGIN
    EXEC('CREATE SCHEMA audit');
    PRINT '✓ Audit schema created';
END
GO

-- =====================================================
-- Audit Trail: Data Access Log
-- =====================================================
CREATE TABLE audit.DataAccessLog (
    AuditId BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    AccessTimestamp DATETIME2 NOT NULL DEFAULT GETDATE(),
    UserId VARCHAR(100) NOT NULL,
    UserRole VARCHAR(50),
    AccessType VARCHAR(50) NOT NULL, -- SELECT, INSERT, UPDATE, DELETE
    TableName VARCHAR(100) NOT NULL,
    RecordIdentifier VARCHAR(500), -- Primary key or unique identifier
    PHIAccessed BIT NOT NULL DEFAULT 0,
    AccessReason VARCHAR(500),
    IPAddress VARCHAR(50),
    ApplicationName VARCHAR(100),
    QueryText VARCHAR(MAX),
    RowsAffected INT,
    Success BIT NOT NULL DEFAULT 1,
    ErrorMessage VARCHAR(MAX),
    INDEX IX_DataAccessLog_Timestamp (AccessTimestamp),
    INDEX IX_DataAccessLog_UserId (UserId),
    INDEX IX_DataAccessLog_PHI (PHIAccessed) WHERE PHIAccessed = 1
);
GO

-- =====================================================
-- Audit Trail: Data Modification Log
-- =====================================================
CREATE TABLE audit.DataModificationLog (
    ModificationId BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    ModificationTimestamp DATETIME2 NOT NULL DEFAULT GETDATE(),
    UserId VARCHAR(100) NOT NULL,
    ModificationType VARCHAR(50) NOT NULL, -- INSERT, UPDATE, DELETE
    TableName VARCHAR(100) NOT NULL,
    RecordIdentifier VARCHAR(500),
    ColumnName VARCHAR(100),
    OldValue VARCHAR(MAX),
    NewValue VARCHAR(MAX),
    ReasonForChange VARCHAR(1000),
    ElectronicSignature VARCHAR(500), -- Hash of user credentials + timestamp
    INDEX IX_DataModificationLog_Timestamp (ModificationTimestamp),
    INDEX IX_DataModificationLog_UserId (UserId),
    INDEX IX_DataModificationLog_Table (TableName, RecordIdentifier)
);
GO

-- =====================================================
-- Audit Trail: System Events
-- =====================================================
CREATE TABLE audit.SystemEvents (
    EventId BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    EventTimestamp DATETIME2 NOT NULL DEFAULT GETDATE(),
    EventType VARCHAR(100) NOT NULL, -- Login, Logout, ConfigChange, ValidationRun, etc.
    UserId VARCHAR(100),
    EventDescription VARCHAR(1000),
    Severity VARCHAR(20) NOT NULL, -- Info, Warning, Error, Critical
    Component VARCHAR(100), -- ADF, Databricks, SQL, PowerBI
    IPAddress VARCHAR(50),
    Success BIT NOT NULL DEFAULT 1,
    ErrorDetails VARCHAR(MAX),
    INDEX IX_SystemEvents_Timestamp (EventTimestamp),
    INDEX IX_SystemEvents_Type (EventType),
    INDEX IX_SystemEvents_Severity (Severity)
);
GO

-- =====================================================
-- Audit Trail: Validation Execution Log
-- =====================================================
CREATE TABLE audit.ValidationExecutionLog (
    ValidationId BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    ExecutionTimestamp DATETIME2 NOT NULL DEFAULT GETDATE(),
    ValidationName VARCHAR(200) NOT NULL,
    ValidationVersion VARCHAR(50),
    ExecutedBy VARCHAR(100) NOT NULL,
    DataSet VARCHAR(200), -- Which dataset was validated
    ValidationCriteria VARCHAR(MAX), -- JSON of validation rules
    PassFailStatus VARCHAR(20) NOT NULL, -- PASS, FAIL, WARNING
    TotalRecords BIGINT,
    PassedRecords BIGINT,
    FailedRecords BIGINT,
    WarningRecords BIGINT,
    ValidationResults VARCHAR(MAX), -- JSON of detailed results
    ElectronicSignature VARCHAR(500),
    ReviewedBy VARCHAR(100),
    ReviewedTimestamp DATETIME2,
    INDEX IX_ValidationLog_Timestamp (ExecutionTimestamp),
    INDEX IX_ValidationLog_Status (PassFailStatus)
);
GO

-- =====================================================
-- Compliance: Data Lineage Tracking
-- =====================================================
CREATE TABLE audit.DataLineageLog (
    LineageId BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    ProcessTimestamp DATETIME2 NOT NULL DEFAULT GETDATE(),
    SourceSystem VARCHAR(100) NOT NULL,
    SourceTable VARCHAR(100) NOT NULL,
    TargetSystem VARCHAR(100) NOT NULL,
    TargetTable VARCHAR(100) NOT NULL,
    TransformationName VARCHAR(200),
    TransformationVersion VARCHAR(50),
    RecordsProcessed BIGINT,
    ProcessDurationSeconds INT,
    DataHash VARCHAR(64), -- SHA-256 hash for data integrity
    ExecutedBy VARCHAR(100),
    PipelineRunId VARCHAR(100),
    INDEX IX_LineageLog_Timestamp (ProcessTimestamp),
    INDEX IX_LineageLog_Source (SourceSystem, SourceTable),
    INDEX IX_LineageLog_Target (TargetSystem, TargetTable)
);
GO

-- =====================================================
-- Compliance: PHI Access Monitoring
-- =====================================================
CREATE TABLE audit.PHIAccessMonitoring (
    MonitoringId BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    AccessTimestamp DATETIME2 NOT NULL DEFAULT GETDATE(),
    UserId VARCHAR(100) NOT NULL,
    PatientIdentifier VARCHAR(100), -- MRN or PatientId
    AccessPurpose VARCHAR(500) NOT NULL, -- Treatment, Payment, Operations, Research, etc.
    PHIFieldsAccessed VARCHAR(MAX), -- List of PHI fields accessed
    JustificationProvided BIT NOT NULL DEFAULT 0,
    Justification VARCHAR(1000),
    AlertGenerated BIT NOT NULL DEFAULT 0,
    ReviewRequired BIT NOT NULL DEFAULT 0,
    ReviewedBy VARCHAR(100),
    ReviewedTimestamp DATETIME2,
    ReviewNotes VARCHAR(1000),
    INDEX IX_PHIAccess_Timestamp (AccessTimestamp),
    INDEX IX_PHIAccess_UserId (UserId),
    INDEX IX_PHIAccess_Patient (PatientIdentifier),
    INDEX IX_PHIAccess_AlertReview (AlertGenerated, ReviewRequired) WHERE AlertGenerated = 1 OR ReviewRequired = 1
);
GO

-- =====================================================
-- Compliance: Electronic Signature Log
-- =====================================================
CREATE TABLE audit.ElectronicSignatureLog (
    SignatureId BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    SignatureTimestamp DATETIME2 NOT NULL DEFAULT GETDATE(),
    UserId VARCHAR(100) NOT NULL,
    UserFullName VARCHAR(200) NOT NULL,
    SignatureReason VARCHAR(500) NOT NULL,
    DocumentType VARCHAR(100), -- ValidationProtocol, ChangeControl, Review, etc.
    DocumentIdentifier VARCHAR(200),
    SignatureHash VARCHAR(500) NOT NULL, -- Hash of user credentials + document + timestamp
    SignatureMeaning VARCHAR(200) NOT NULL, -- Reviewed, Approved, Validated, etc.
    INDEX IX_ElectronicSig_Timestamp (SignatureTimestamp),
    INDEX IX_ElectronicSig_UserId (UserId),
    INDEX IX_ElectronicSig_Document (DocumentType, DocumentIdentifier)
);
GO

-- =====================================================
-- Stored Procedure: Log Data Access (HIPAA)
-- =====================================================
CREATE OR ALTER PROCEDURE audit.usp_LogDataAccess
    @UserId VARCHAR(100),
    @AccessType VARCHAR(50),
    @TableName VARCHAR(100),
    @RecordIdentifier VARCHAR(500) = NULL,
    @PHIAccessed BIT = 0,
    @AccessReason VARCHAR(500) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO audit.DataAccessLog (
        UserId,
        AccessType,
        TableName,
        RecordIdentifier,
        PHIAccessed,
        AccessReason,
        IPAddress,
        ApplicationName
    )
    VALUES (
        @UserId,
        @AccessType,
        @TableName,
        @RecordIdentifier,
        @PHIAccessed,
        @AccessReason,
        CONNECTIONPROPERTY('client_net_address'),
        APP_NAME()
    );

    -- Generate alert if unusual PHI access pattern detected
    IF @PHIAccessed = 1
    BEGIN
        -- Check for excessive access
        DECLARE @AccessCount INT;
        SELECT @AccessCount = COUNT(*)
        FROM audit.DataAccessLog
        WHERE UserId = @UserId
          AND PHIAccessed = 1
          AND AccessTimestamp >= DATEADD(HOUR, -1, GETDATE());

        IF @AccessCount > 100
        BEGIN
            INSERT INTO audit.SystemEvents (EventType, UserId, EventDescription, Severity)
            VALUES ('PHI_EXCESSIVE_ACCESS', @UserId,
                    'User accessed PHI ' + CAST(@AccessCount AS VARCHAR(10)) + ' times in the last hour',
                    'Warning');
        END
    END
END;
GO

-- =====================================================
-- Stored Procedure: Log Data Modification (GxP)
-- =====================================================
CREATE OR ALTER PROCEDURE audit.usp_LogDataModification
    @UserId VARCHAR(100),
    @ModificationType VARCHAR(50),
    @TableName VARCHAR(100),
    @RecordIdentifier VARCHAR(500),
    @ColumnName VARCHAR(100),
    @OldValue VARCHAR(MAX),
    @NewValue VARCHAR(MAX),
    @ReasonForChange VARCHAR(1000)
AS
BEGIN
    SET NOCOUNT ON;

    -- Generate electronic signature
    DECLARE @SignatureData VARCHAR(MAX) = CONCAT(@UserId, '|', GETDATE(), '|', @ReasonForChange);
    DECLARE @ElectronicSignature VARCHAR(500) = CONVERT(VARCHAR(500),
        HASHBYTES('SHA2_256', @SignatureData), 2);

    INSERT INTO audit.DataModificationLog (
        UserId,
        ModificationType,
        TableName,
        RecordIdentifier,
        ColumnName,
        OldValue,
        NewValue,
        ReasonForChange,
        ElectronicSignature
    )
    VALUES (
        @UserId,
        @ModificationType,
        @TableName,
        @RecordIdentifier,
        @ColumnName,
        @OldValue,
        @NewValue,
        @ReasonForChange,
        @ElectronicSignature
    );
END;
GO

-- =====================================================
-- View: Recent PHI Access for Monitoring
-- =====================================================
CREATE OR ALTER VIEW audit.vw_RecentPHIAccess
AS
SELECT TOP 1000
    AccessTimestamp,
    UserId,
    TableName,
    RecordIdentifier,
    AccessReason,
    IPAddress
FROM audit.DataAccessLog
WHERE PHIAccessed = 1
ORDER BY AccessTimestamp DESC;
GO

-- =====================================================
-- View: Compliance Dashboard Metrics
-- =====================================================
CREATE OR ALTER VIEW audit.vw_ComplianceDashboard
AS
SELECT
    (SELECT COUNT(*) FROM audit.DataAccessLog WHERE PHIAccessed = 1 AND AccessTimestamp >= DATEADD(DAY, -30, GETDATE())) AS PHIAccessLast30Days,
    (SELECT COUNT(*) FROM audit.DataModificationLog WHERE ModificationTimestamp >= DATEADD(DAY, -30, GETDATE())) AS DataModificationsLast30Days,
    (SELECT COUNT(*) FROM audit.ValidationExecutionLog WHERE PassFailStatus = 'FAIL' AND ExecutionTimestamp >= DATEADD(DAY, -30, GETDATE())) AS FailedValidationsLast30Days,
    (SELECT COUNT(DISTINCT UserId) FROM audit.DataAccessLog WHERE PHIAccessed = 1 AND AccessTimestamp >= DATEADD(DAY, -30, GETDATE())) AS UniqueUsersAccessingPHI,
    (SELECT COUNT(*) FROM audit.PHIAccessMonitoring WHERE AlertGenerated = 1 AND ReviewRequired = 1 AND ReviewedTimestamp IS NULL) AS PendingPHIReviews;
GO

PRINT '✓ Audit trail schema created successfully';
GO
"""
        return audit_schema

    def generate_validation_protocol(self) -> Dict[str, Any]:
        """
        Generate GxP validation protocol document
        """
        protocol = {
            "document_type": "Validation Protocol",
            "document_id": "VP-LIMS-MIGRATION-001",
            "version": "1.0",
            "effective_date": datetime.now().isoformat(),
            "title": "LIMS Migration System Validation Protocol",
            "regulatory_basis": "21 CFR Part 11, HIPAA",

            "purpose": "To establish documented evidence that the LIMS migration system "
                      "performs as intended and meets all predefined specifications",

            "scope": {
                "systems_covered": [
                    "Azure Data Factory (Orchestration)",
                    "Azure Data Lake Storage (Data Storage)",
                    "Azure Databricks (Data Transformation)",
                    "Azure SQL Database (Analytics Database)",
                    "Power BI (Reporting)"
                ],
                "data_types": [
                    "Laboratory Test Results",
                    "Patient Demographics (PHI)",
                    "Test Master Data",
                    "Quality Control Data"
                ]
            },

            "validation_approach": {
                "methodology": "Risk-based validation approach",
                "validation_type": "Prospective validation",
                "validation_phases": [
                    "Installation Qualification (IQ)",
                    "Operational Qualification (OQ)",
                    "Performance Qualification (PQ)"
                ]
            },

            "validation_activities": {
                "IQ": {
                    "description": "Verify system installation",
                    "test_cases": [
                        {
                            "id": "IQ-001",
                            "description": "Verify Azure Data Factory deployment",
                            "acceptance_criteria": "All pipelines deployed without errors"
                        },
                        {
                            "id": "IQ-002",
                            "description": "Verify Databricks cluster configuration",
                            "acceptance_criteria": "Cluster meets specifications (node type, autoscaling)"
                        },
                        {
                            "id": "IQ-003",
                            "description": "Verify SQL database schema",
                            "acceptance_criteria": "All tables, views, and stored procedures created"
                        },
                        {
                            "id": "IQ-004",
                            "description": "Verify encryption at rest",
                            "acceptance_criteria": "TDE enabled on SQL database, storage encryption enabled"
                        },
                        {
                            "id": "IQ-005",
                            "description": "Verify audit trail functionality",
                            "acceptance_criteria": "All audit tables exist and logging is functional"
                        }
                    ]
                },
                "OQ": {
                    "description": "Verify system operates as designed",
                    "test_cases": [
                        {
                            "id": "OQ-001",
                            "description": "Test CDC incremental load",
                            "acceptance_criteria": "Only changed records extracted from source"
                        },
                        {
                            "id": "OQ-002",
                            "description": "Test clinical validation rules",
                            "acceptance_criteria": "Reference range violations detected correctly"
                        },
                        {
                            "id": "OQ-003",
                            "description": "Test ML anomaly detection",
                            "acceptance_criteria": "Known anomalies flagged with >90% accuracy"
                        },
                        {
                            "id": "OQ-004",
                            "description": "Test data quality checks",
                            "acceptance_criteria": "All quality rules execute and report correctly"
                        },
                        {
                            "id": "OQ-005",
                            "description": "Test critical value alerting",
                            "acceptance_criteria": "Critical values generate alerts within 5 minutes"
                        },
                        {
                            "id": "OQ-006",
                            "description": "Test access controls",
                            "acceptance_criteria": "Unauthorized users cannot access PHI"
                        }
                    ]
                },
                "PQ": {
                    "description": "Verify system performs in production environment",
                    "test_cases": [
                        {
                            "id": "PQ-001",
                            "description": "Process full production data volume",
                            "acceptance_criteria": "Pipeline completes within SLA (4 hours)"
                        },
                        {
                            "id": "PQ-002",
                            "description": "Validate data accuracy end-to-end",
                            "acceptance_criteria": "100% row count match, 100% checksum match"
                        },
                        {
                            "id": "PQ-003",
                            "description": "Test disaster recovery",
                            "acceptance_criteria": "System recovers within RPO/RTO targets"
                        }
                    ]
                }
            },

            "acceptance_criteria": {
                "data_integrity": {
                    "row_count_match": "100%",
                    "checksum_match": "100%",
                    "key_sum_tolerance": "0.01%"
                },
                "data_quality": {
                    "completeness": ">= 95%",
                    "validity": ">= 99%",
                    "clinical_validation_pass_rate": ">= 95%"
                },
                "performance": {
                    "incremental_load_time": "<= 4 hours",
                    "critical_value_alert_time": "<= 5 minutes",
                    "query_response_time": "<= 2 seconds (95th percentile)"
                },
                "security": {
                    "encryption_at_rest": "Enabled",
                    "encryption_in_transit": "TLS 1.2+",
                    "audit_logging": "100% of PHI access logged"
                }
            },

            "roles_responsibilities": {
                "validation_lead": "Responsible for overall validation execution",
                "quality_assurance": "Review and approve validation documentation",
                "system_owner": "Sign off on validation completion",
                "subject_matter_expert": "Provide domain expertise for clinical validation"
            },

            "deviation_management": "All deviations from validation protocol must be documented, "
                                   "justified, and approved by Quality Assurance",

            "change_control": "Changes to validated system require impact assessment and "
                            "potential re-validation",

            "approval_signatures": {
                "prepared_by": {"name": "", "title": "Validation Engineer", "date": ""},
                "reviewed_by": {"name": "", "title": "Quality Assurance", "date": ""},
                "approved_by": {"name": "", "title": "System Owner", "date": ""}
            }
        }

        return protocol

    def generate_data_integrity_alcoa_checklist(self) -> Dict[str, Any]:
        """
        Generate ALCOA+ data integrity checklist
        """
        alcoa_checklist = {
            "principle": "ALCOA+",
            "full_name": "Attributable, Legible, Contemporaneous, Original, Accurate, "
                        "Complete, Consistent, Enduring, Available",

            "checklist": {
                "Attributable": {
                    "requirement": "Data must be attributable to the individual who created it",
                    "implementation": [
                        "User IDs captured in all audit logs",
                        "Electronic signatures for critical operations",
                        "Authentication via Azure AD",
                        "No shared accounts"
                    ],
                    "verification": "Review audit.DataAccessLog and audit.DataModificationLog"
                },
                "Legible": {
                    "requirement": "Data must be readable and permanent",
                    "implementation": [
                        "UTF-8 encoding throughout",
                        "Standardized date/time formats (ISO 8601)",
                        "Proper data types in all tables",
                        "No truncation of critical fields"
                    ],
                    "verification": "Review schema definitions and test data display"
                },
                "Contemporaneous": {
                    "requirement": "Data recorded at the time of activity",
                    "implementation": [
                        "Automated timestamps (GETDATE(), current_timestamp())",
                        "Real-time data capture from source systems",
                        "CDC captures changes as they occur",
                        "No manual back-dating allowed"
                    ],
                    "verification": "Compare ModifiedDate with audit timestamps"
                },
                "Original": {
                    "requirement": "Original record or true copy",
                    "implementation": [
                        "Bronze layer preserves original source data",
                        "Delta Lake maintains version history",
                        "Audit trail tracks all modifications",
                        "Source system remains system of record"
                    ],
                    "verification": "Query Bronze layer and verify against source"
                },
                "Accurate": {
                    "requirement": "Data is free from errors",
                    "implementation": [
                        "Clinical validation rules applied",
                        "Data quality checks executed",
                        "Reference range validation",
                        "Reconciliation with source (100% match required)"
                    ],
                    "verification": "Review validation execution logs and quality metrics"
                },
                "Complete": {
                    "requirement": "All data required is present",
                    "implementation": [
                        "Required field validation",
                        "Completeness checks (>95% target)",
                        "NULL value monitoring",
                        "Missing data reporting"
                    ],
                    "verification": "Review data quality reports for completeness metrics"
                },
                "Consistent": {
                    "requirement": "Data follows consistent format",
                    "implementation": [
                        "Schema enforcement in Delta Lake",
                        "Unit consistency validation",
                        "LOINC code standardization",
                        "Controlled vocabularies"
                    ],
                    "verification": "Review data quality checks for consistency violations"
                },
                "Enduring": {
                    "requirement": "Data remains available for its entire lifecycle",
                    "implementation": [
                        "7-year retention policy for audit logs",
                        "Delta Lake time travel (30 days)",
                        "Backup and disaster recovery procedures",
                        "Immutable storage tiers"
                    ],
                    "verification": "Review retention policy configuration"
                },
                "Available": {
                    "requirement": "Data available when needed",
                    "implementation": [
                        "High availability SQL database (SLA 99.99%)",
                        "Geo-redundant storage",
                        "Disaster recovery tested quarterly",
                        "RPO: 1 hour, RTO: 4 hours"
                    ],
                    "verification": "Review SLA reports and DR test results"
                }
            }
        }

        return alcoa_checklist

    def export_compliance_documentation(self, output_dir: str):
        """
        Export all compliance documentation to files
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Export audit schema
        with open(output_path / 'audit_trail_schema.sql', 'w') as f:
            f.write(self.create_audit_trail_schema())

        # Export validation protocol
        with open(output_path / 'validation_protocol.json', 'w') as f:
            json.dump(self.generate_validation_protocol(), f, indent=2)

        # Export ALCOA checklist
        with open(output_path / 'alcoa_checklist.json', 'w') as f:
            json.dump(self.generate_data_integrity_alcoa_checklist(), f, indent=2)

        print(f"✓ Compliance documentation exported to: {output_dir}")


def main():
    """Main execution"""
    from pathlib import Path

    config_path = Path(__file__).parent.parent / 'config' / 'azure_config.json'

    framework = ComplianceFramework(str(config_path))

    # Export all documentation
    output_dir = Path(__file__).parent / 'documentation'
    framework.export_compliance_documentation(str(output_dir))

    print("\n" + "="*80)
    print("✓ HIPAA/GxP Compliance Framework Generated")
    print("="*80)


if __name__ == "__main__":
    main()
