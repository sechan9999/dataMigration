# LIMS Migration - Deployment Guide

## Overview

This guide provides step-by-step instructions for deploying the LIMS migration architecture on Azure.

## Prerequisites

### Azure Requirements
- Azure subscription with contributor access
- Azure CLI installed and configured
- Databricks CLI installed
- Python 3.9 or higher
- ODBC Driver 17 for SQL Server

### Required Permissions
- Contributor role on Azure subscription
- Azure AD admin rights for SQL authentication
- Databricks workspace admin access
- Power BI Premium/Pro license

## Installation Steps

### 1. Clone Repository

```bash
git clone <repository-url>
cd dataMigration/lims-migration
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Azure Resources

Edit `config/azure_config.json` with your environment values:

```json
{
  "azure": {
    "subscription_id": "YOUR_SUBSCRIPTION_ID",
    "resource_group": "rg-lims-migration-prod",
    "location": "eastus2"
  },
  "source_lims": {
    "server": "YOUR_LIMS_SQL_SERVER",
    "database": "LIMS_Production"
  }
}
```

### 4. Deploy Infrastructure

```bash
cd scripts
python deploy_infrastructure.py
```

This will create:
- Azure Data Factory
- Data Lake Storage Gen2 (Bronze, Silver, Gold containers)
- Databricks workspace
- Azure SQL Database
- Log Analytics workspace

Expected duration: **15-20 minutes**

### 5. Deploy Databricks Components

```bash
# Configure Databricks CLI
databricks configure --token

# Deploy notebooks and clusters
python deploy_databricks.py
```

This will:
- Upload all notebooks to workspace
- Create ETL cluster
- Install required libraries
- Configure secrets

Expected duration: **10-15 minutes**

### 6. Initialize Database Schema

```bash
python init_database.py
```

This will:
- Create schemas (dim, fact, etl, audit)
- Create dimension tables with clinical hierarchies
- Create fact tables with columnstore indexes
- Create stored procedures and views
- Populate date dimension

Expected duration: **5-10 minutes**

### 7. Deploy ADF Pipelines

```bash
python deploy_adf_pipelines.py
```

This will:
- Deploy linked services
- Deploy datasets
- Deploy CDC pipelines
- Deploy orchestration pipeline
- Create triggers

Expected duration: **5 minutes**

## Validation

### Test Pipeline Execution

```bash
# Trigger test run of master orchestration pipeline
az datafactory pipeline create-run \
    --resource-group rg-lims-migration-prod \
    --factory-name adf-lims-migration-prod \
    --name pl_master_lims_orchestration
```

### Verify Data Flow

1. **Bronze Layer**: Check raw data landed from source
```python
# In Databricks
df = spark.read.format("parquet").load("abfss://bronze@<storage>.dfs.core.windows.net/lims/labresults")
display(df)
```

2. **Silver Layer**: Check validated and cleaned data
```python
df = spark.read.format("delta").load("abfss://silver@<storage>.dfs.core.windows.net/lims/labresults")
display(df)
```

3. **Gold Layer**: Check aggregated data
```python
df = spark.read.format("delta").load("abfss://gold@<storage>.dfs.core.windows.net/lims/patient_lab_summary")
display(df)
```

4. **SQL Database**: Query dimensional model
```sql
SELECT TOP 100
    p.MRN,
    t.TestName,
    f.ResultValueNumeric,
    d.Date
FROM fact.FactLabResult f
JOIN dim.DimPatient p ON f.PatientKey = p.PatientKey
JOIN dim.DimTest t ON f.TestKey = t.TestKey
JOIN dim.DimDate d ON f.ResultDateKey = d.DateKey
ORDER BY f.ResultDateKey DESC;
```

### Verify Clinical Validation

Check validation results:
```python
validation_df = spark.read.format("delta").load("abfss://gold@<storage>.dfs.core.windows.net/lims/validation_results")
validation_df.groupBy("ReferenceRangeValid", "TemporalConsistencyValid").count().show()
```

### Verify ML Anomaly Detection

Check anomaly detection results:
```python
anomaly_df = spark.read.format("delta").load("abfss://gold@<storage>.dfs.core.windows.net/lims/anomaly_detection_results")
anomaly_df.filter(col("IsAnomaly") == 1).show()
```

## Power BI Deployment

### 1. Import Semantic Model

1. Open Power BI Desktop
2. File → Import → Power BI template
3. Load `powerbi/datasets/semantic_model.json`
4. Update connection strings with your Azure SQL Database

### 2. Deploy DAX Measures

1. Open Advanced Editor in Power BI
2. Copy measures from `powerbi/measures/clinical_measures.dax`
3. Create measures in semantic model

### 3. Publish to Service

1. Publish dataset to Power BI Service
2. Configure refresh schedule (every 4 hours recommended)
3. Set up row-level security for clinical staff role

## Monitoring Setup

### Configure Alerts

```bash
# Create alert for pipeline failures
az monitor metrics alert create \
    --name "LIMS-Pipeline-Failure" \
    --resource-group rg-lims-migration-prod \
    --scopes <adf-resource-id> \
    --condition "count ActivityFailedRuns > 0" \
    --window-size 15m \
    --evaluation-frequency 5m
```

### View Logs

```bash
# Query Log Analytics
az monitor log-analytics query \
    --workspace <workspace-id> \
    --analytics-query "AzureDiagnostics | where ResourceProvider == 'MICROSOFT.DATAFACTORY' | order by TimeGenerated desc | take 100"
```

## Troubleshooting

### Common Issues

#### 1. Pipeline Fails with Authentication Error

**Solution**: Verify managed identity has required permissions:
```bash
az role assignment list --assignee <managed-identity-principal-id>
```

#### 2. Databricks Cluster Won't Start

**Solution**: Check cluster configuration and quotas:
```bash
databricks clusters list
databricks clusters get --cluster-id <cluster-id>
```

#### 3. SQL Database Connection Timeout

**Solution**: Verify firewall rules:
```bash
az sql server firewall-rule list \
    --resource-group rg-lims-migration-prod \
    --server sql-lims-analytics-prod
```

#### 4. ML Model Training Fails

**Solution**: Check MLflow tracking and increase cluster size if needed:
```python
# In Databricks
import mlflow
mlflow.list_experiments()
```

## Performance Tuning

### ADF Pipeline Optimization

- Adjust parallelism in ForEach activities
- Tune copy activity DIU (Data Integration Units)
- Enable staging for large datasets

### Databricks Optimization

```python
# Enable adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Optimize Delta tables
spark.sql("OPTIMIZE delta.`<path>` ZORDER BY (PatientId, ResultDate)")

# Vacuum old versions
spark.sql("VACUUM delta.`<path>` RETAIN 168 HOURS")
```

### SQL Database Optimization

```sql
-- Update statistics
UPDATE STATISTICS fact.FactLabResult;

-- Rebuild indexes
ALTER INDEX ALL ON fact.FactLabResult REBUILD;

-- Update columnstore index
ALTER INDEX NCCI_FactLabResult ON fact.FactLabResult REORGANIZE;
```

## Maintenance

### Daily Tasks
- Monitor pipeline execution
- Review critical value alerts
- Check data quality metrics

### Weekly Tasks
- Review anomaly detection results
- Optimize Delta tables
- Update ML models if needed

### Monthly Tasks
- Vacuum old Delta versions
- Archive old audit logs
- Review and update clinical validation rules

## Security Considerations

### PHI Data Protection

1. **Encryption at Rest**: Enabled on all storage accounts
2. **Encryption in Transit**: TLS 1.2 enforced
3. **Access Control**: RBAC with managed identities
4. **Audit Logging**: Enabled on all resources

### Compliance

- HIPAA compliance enabled on all Azure services
- Audit logs retained for 7 years
- Row-level security in Power BI
- Data masking for sensitive fields

## Support

For issues or questions:
- Check logs in Azure Monitor
- Review Databricks job outputs
- Contact: lims-support@organization.com

## Next Steps

After successful deployment:

1. Configure source LIMS connection
2. Run initial full load
3. Validate data quality
4. Enable scheduled triggers
5. Train users on Power BI dashboards
6. Set up monitoring and alerts
