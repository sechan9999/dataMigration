# LIMS Migration Architecture

Production-grade LIMS (Laboratory Information Management System) migration using Azure Data Factory, Databricks, Azure SQL DB, and Power BI.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          LIMS Migration Pipeline                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Source LIMS                                                                │
│       │                                                                     │
│       ├──► Azure Data Factory (CDC + Orchestration)                        │
│       │           │                                                         │
│       │           ├──► Data Lake Bronze (Raw LIMS Data)                    │
│       │           │           │                                             │
│       │           │           ├──► Databricks Silver Layer                 │
│       │           │           │    - Clinical Validation Rules              │
│       │           │           │    - Data Quality Checks                    │
│       │           │           │    - Incremental Load Processing            │
│       │           │           │                                             │
│       │           │           ├──► Databricks Gold Layer                   │
│       │           │           │    - Business Aggregations                  │
│       │           │           │    - Clinical Hierarchies                   │
│       │           │           │                                             │
│       │           │           ├──► Azure SQL DB                            │
│       │           │           │    - Dimensional Model                      │
│       │           │           │    - Clinical Data Repository               │
│       │           │           │    - Unified Patient Views                  │
│       │           │           │                                             │
│       │           │           ├──► ML Anomaly Detection                    │
│       │           │           │    - Lab Result Anomalies                   │
│       │           │           │    - Critical Value Detection               │
│       │           │           │                                             │
│       │           │           └──► Power BI                                │
│       │           │                - Operational Dashboards                 │
│       │           │                - Clinical Analytics                     │
│       │           │                - Quality Monitoring                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Key Features

### 1. **Incremental CDC Pattern**
- SQL Server Change Tracking
- Watermark-based incremental loads
- Hash-based change detection
- Idempotent pipeline design

### 2. **Clinical Validation Rules**
- Reference range validation (age, gender-specific)
- Temporal consistency (result trends)
- Cross-test correlation (e.g., Glucose vs HbA1c)
- Critical value alerting
- Specimen validity checks

### 3. **Data Quality Framework**
- Clinical plausibility checks
- Completeness validation
- Conformity to standards (LOINC, SNOMED)
- Data freshness monitoring
- Duplicate detection

### 4. **ML Anomaly Detection**
- Isolation Forest for outlier detection
- Multi-variate analysis
- Real-time scoring
- Automated alerts for critical anomalies

### 5. **Security & Compliance**
- HIPAA compliant encryption
- Azure Managed Identity
- Row-level security in Power BI
- Audit logging
- PHI masking capabilities

## Quick Start

### Prerequisites
- Azure Subscription
- Azure Data Factory
- Azure Databricks (DBR 13.3+)
- Azure SQL Database
- Azure Data Lake Storage Gen2
- Power BI Premium/Pro

### Deployment

1. **Configure Azure Resources**
```bash
cd lims-migration/config
cp azure_config.template.json azure_config.json
# Edit azure_config.json with your values
```

2. **Deploy Infrastructure**
```bash
cd scripts
python deploy_infrastructure.py
```

3. **Deploy ADF Pipelines**
```bash
python deploy_adf_pipelines.py
```

4. **Deploy Databricks Notebooks**
```bash
python deploy_databricks.py
```

5. **Initialize Database**
```bash
python init_database.py
```

## Components

See individual component README files for detailed documentation:
- [ADF Pipelines](adf/README.md)
- [Databricks Notebooks](databricks/README.md)
- [SQL Schema](sql/README.md)
- [Power BI](powerbi/README.md)
