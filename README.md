# Production Data Systems for Azure Databricks

A comprehensive suite of production-ready systems for:
1. **Delay Queue with Exponential Backoff** - Reliable data migration with ACID guarantees
2. **Probabilistic Forecasting System** - Macroeconomic and financial indicator forecasting

---

## 📊 Probabilistic Forecasting System (NEW)

### Overview
Production-grade probabilistic forecasting for macroeconomic and financial indicators with complete documentation and reasoning.

### Key Features
- **Probabilistic Outputs**: Full probability distributions with confidence intervals
- **Multiple Methodologies**: Bayesian, Monte Carlo, Ensemble approaches
- **Structured Reasoning**: Complete documentation of forecast rationale and assumptions
- **Expert Calibration**: Consensus building across domain experts
- **Trend Analysis**: Comprehensive feature extraction and pattern detection
- **Performance Evaluation**: Rigorous backtesting and accuracy metrics

### Quick Start
```python
from forecasting.models import BayesianForecast, ForecastHorizon
from forecasting.indicators import IndicatorDataSource

# Load data and generate forecast
data_source = IndicatorDataSource()
gdp_data = data_source.get_indicator('US_GDP')

model = BayesianForecast(prior_mean=2.5, prior_std=1.0)
model.fit(gdp_data.values, gdp_data.dates)

forecast = model.forecast(
    horizon=ForecastHorizon.MEDIUM_TERM,
    target_date=datetime(2025, 6, 1)
)

print(f"Forecast: {forecast.point_estimate:.2f}")
print(f"90% CI: {forecast.distribution.confidence_intervals[0.90]}")
```

### Documentation
- 📖 [Complete Documentation](docs/FORECASTING_SYSTEM.md)
- 📓 [Notebooks](notebooks/forecasting_01_setup.py)

---

## 🚀 Delay Queue System

A complete, production-ready delay queue system designed for reliable data migration with ACID guarantees, exponential backoff retry logic, and comprehensive data validation.

## 🎯 Key Features

### Core Capabilities
- **Delta Lake ACID Transactions**: If migration fails at 99%, Delta automatically rolls back to 0%, preventing corrupt data
- **Exponential Backoff**: Intelligent retry scheduling with jitter to prevent thundering herd
- **Network Resilience**: Automatic retry for transient network failures with configurable backoff
- **Data Reconciliation**: Never consider migration complete until reconciliation returns 'SUCCESS'
- **Dead Letter Queue**: Automatic handling of permanently failed tasks for manual review
- **Distributed Locking**: Prevents duplicate processing in multi-worker environments
- **Real-time Monitoring**: Health metrics, alerts, and performance analytics

### Data Integrity
- **Row Count Validation**: Ensures source and target have identical record counts
- **Column Checksums**: Detects data corruption at column level
- **Key Sum Validation**: Validates critical business metrics (Total Revenue, Total Patients, etc.)
- **Null Count Verification**: Ensures data completeness
- **Audit Logging**: Complete audit trail for compliance

## 📋 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Delay Queue System                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    │
│  │   Enqueue    │───▶│  Queue Mgr   │───▶│  Job Exec    │    │
│  │   Tasks      │    │  (Delta Lake)│    │  (Retry)     │    │
│  └──────────────┘    └──────────────┘    └──────────────┘    │
│         │                    │                    │            │
│         │                    ▼                    ▼            │
│         │            ┌──────────────┐    ┌──────────────┐    │
│         └───────────▶│  Monitoring  │    │ Reconcile    │    │
│                      │  & Alerts    │    │ Validator    │    │
│                      └──────────────┘    └──────────────┘    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

### Prerequisites
- Azure Databricks workspace (DBR 11.3+)
- Delta Lake enabled
- Storage mounted at `/mnt/datalake`
- Python 3.9+

### Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd dataMigration
```

2. **Upload to DBFS**
```bash
databricks fs cp -r src dbfs:/src --overwrite
databricks fs cp -r config dbfs:/config --overwrite
databricks fs cp -r notebooks dbfs:/notebooks --overwrite
```

3. **Configure your environment**

Edit `config/queue_config.yaml` with your paths:
```yaml
delta:
  queue_table_path: "/mnt/datalake/delay_queue/tasks"
  audit_log_path: "/mnt/datalake/delay_queue/audit_logs"
```

### Running the System

#### Step 1: Initialize (First Time Only)

Run notebook: `notebooks/01_setup_and_initialization.py`

This will:
- Install dependencies
- Create Delta Lake tables
- Validate configuration
- Display retry schedule

#### Step 2: Enqueue Migration Tasks

Run notebook: `notebooks/02_enqueue_migration_tasks.py`

Example:
```python
task_id = orchestrator.enqueue_migration_task(
    task_name="Migrate Customer Data",
    source_csv_path="/mnt/legacy/customers.csv",
    target_delta_path="/mnt/datalake/customers",
    table_name="customers",
    partition_columns=["region", "signup_date"],
    priority=TaskPriority.HIGH
)
```

#### Step 3: Start Processing

Run notebook: `notebooks/03_process_queue.py`

This starts a worker that:
- Polls queue for ready tasks
- Executes migrations with retry logic
- Runs reconciliation validation
- Handles failures with exponential backoff

#### Step 4: Monitor Progress

Run notebook: `notebooks/04_monitoring_dashboard.py`

View:
- Queue health status
- Task statistics
- Performance metrics
- Failed reconciliations
- System alerts

## 📊 Reconciliation Validation

The system never considers a migration complete until reconciliation returns 'SUCCESS'. This ensures data integrity through:

### Validation Checks

1. **Row Count Match**
   - Verifies source and target have identical row counts
   - Critical for detecting data loss

2. **Column Checksums**
   - MD5 hash of each column's concatenated values
   - Detects data corruption

3. **Key Sum Validation**
   - Sums critical numeric columns (e.g., Total Revenue, Total Patients)
   - Ensures business metric accuracy
   - Configurable tolerance threshold for floating-point precision

4. **Null Count Verification**
   - Validates null counts per column
   - Ensures data completeness

### Example Configuration

```yaml
reconciliation:
  enable_auto_reconciliation: true
  tolerance_threshold: 0.0001
  required_checks:
    - row_count
    - column_checksums
    - key_sums
    - null_counts
  key_sum_columns:
    - total_revenue
    - total_patients
    - transaction_amount
```

## 🔄 Exponential Backoff Strategy

### Default Configuration

```yaml
backoff:
  initial_delay_seconds: 60      # Start with 1 minute
  max_delay_seconds: 3600        # Cap at 1 hour
  exponential_base: 2            # Delay multiplier (2^retry_count)
  max_retries: 5                 # Maximum attempts
  jitter_factor: 0.1             # Add 10% randomness
```

### Retry Schedule Example

| Attempt | Delay (sec) | Delay (min) | Cumulative (min) |
|---------|-------------|-------------|------------------|
| 1       | 60          | 1.0         | 1.0              |
| 2       | 120         | 2.0         | 3.0              |
| 3       | 240         | 4.0         | 7.0              |
| 4       | 480         | 8.0         | 15.0             |
| 5       | 960         | 16.0        | 31.0             |

### Why Jitter?

Jitter adds randomness (±10%) to prevent thundering herd problem where many failed tasks retry simultaneously and overwhelm the system.

## 🛡️ Network Resilience

The system handles network failures gracefully with fast retry for transient issues:

### Network Retry Configuration

```yaml
network:
  connection_timeout_seconds: 300
  read_timeout_seconds: 600
  max_network_retries: 4
  network_backoff_seconds: [2, 4, 8, 16]  # Fast retry for network issues
```

### Network Error Detection

Automatically detects network errors:
- Connection timeouts
- Socket errors
- DNS resolution failures
- Broken pipes
- Connection resets

## 📈 Monitoring & Alerting

### Health Metrics

- **Queue Depth**: Number of pending tasks
- **Processing Rate**: Tasks completed per hour
- **Success Rate**: Percentage of successful migrations
- **Average Retry Count**: Average retries per task
- **Execution Time**: Average and max execution times
- **Dead Letter Queue**: Failed tasks requiring manual intervention

### Alert Thresholds

```yaml
monitoring:
  alert_on_consecutive_failures: 3
  max_pending_tasks: 100
  max_dlq_tasks_per_hour: 5
  max_task_age_minutes: 1440  # 24 hours
```

### Health Status

- **HEALTHY**: All systems operational
- **DEGRADED**: Non-critical issues detected
- **CRITICAL**: Immediate attention required

## 🏗️ Project Structure

```
dataMigration/
├── config/
│   └── queue_config.yaml          # System configuration
├── src/
│   ├── models/
│   │   └── task.py                # Task data models
│   ├── delay_queue/
│   │   ├── queue_manager.py       # Queue operations with Delta Lake
│   │   ├── backoff_strategy.py   # Exponential backoff logic
│   │   ├── job_executor.py        # Migration execution with retry
│   │   └── orchestrator.py        # Main orchestration logic
│   └── utils/
│       ├── reconciliation.py      # Data validation
│       └── monitoring.py          # Health monitoring
├── notebooks/
│   ├── 01_setup_and_initialization.py
│   ├── 02_enqueue_migration_tasks.py
│   ├── 03_process_queue.py
│   └── 04_monitoring_dashboard.py
├── examples/
│   └── example_usage.py           # Usage examples
├── tests/                         # Unit tests
├── docs/                          # Additional documentation
└── README.md
```

## 🎓 Usage Examples

### Simple Migration
```python
orchestrator.enqueue_migration_task(
    task_name="Migrate Customers",
    source_csv_path="/mnt/legacy/customers.csv",
    target_delta_path="/mnt/datalake/customers",
    table_name="customers"
)
```

### High-Priority with Partitioning
```python
orchestrator.enqueue_migration_task(
    task_name="Migrate Sales Data",
    source_csv_path="/mnt/legacy/sales.csv",
    target_delta_path="/mnt/datalake/sales",
    table_name="sales",
    partition_columns=["year", "month", "region"],
    priority=TaskPriority.CRITICAL
)
```

### Financial Data with Validation
```python
orchestrator.enqueue_migration_task(
    task_name="Migrate Financial Transactions",
    source_csv_path="/mnt/legacy/transactions.csv",
    target_delta_path="/mnt/datalake/transactions",
    table_name="transactions",
    custom_config={
        "validate_schema": True,
        "expected_columns": ["transaction_id", "amount", "currency"],
        "key_sum_columns": ["amount"]
    },
    tags={"compliance": "sox"}
)
```

## 🔧 Advanced Configuration

### Custom Backoff Strategy

```python
from src.delay_queue.backoff_strategy import BackoffConfig, ExponentialBackoffStrategy

config = BackoffConfig(
    initial_delay_seconds=30,
    max_delay_seconds=7200,
    exponential_base=3,
    max_retries=10,
    jitter_factor=0.2
)

strategy = ExponentialBackoffStrategy(config)
```

### Adaptive Backoff

```python
from src.delay_queue.backoff_strategy import AdaptiveBackoffStrategy

# Adjusts backoff based on error type and system load
strategy = AdaptiveBackoffStrategy(config)
backoff = strategy.calculate_backoff(
    retry_count=2,
    error_type="network_timeout",  # Faster retry
    system_load=0.8  # High load = slower retry
)
```

## 🧪 Testing

Run tests locally:
```bash
pytest tests/
```

## 📚 Documentation

- [Architecture Deep Dive](docs/ARCHITECTURE.md)
- [Configuration Guide](docs/CONFIGURATION.md)
- [API Reference](docs/API_REFERENCE.md)
- [Troubleshooting](docs/TROUBLESHOOTING.md)

## 🤝 Best Practices

1. **Always Use Reconciliation**: Never skip reconciliation validation
2. **Monitor Dead Letter Queue**: Review and fix DLQ tasks regularly
3. **Set Appropriate Priorities**: Use CRITICAL for time-sensitive data
4. **Partition Large Tables**: Improves query performance and data management
5. **Validate Critical Columns**: Configure key_sum_columns for business metrics
6. **Run Multiple Workers**: Scale horizontally for large migrations
7. **Regular Cleanup**: Archive old completed tasks to manage storage

## ⚠️ Important Notes

### Delta Lake ACID Guarantees

This system chose Delta Lake specifically because migration often fails halfway due to network issues. Delta provides ACID transactions. **If the job fails at 99%, Delta rolls back to 0%**, ensuring we don't end up with half-migrated, corrupt data. It makes the pipeline idempotent.

### Reconciliation is Mandatory

**Never consider a migration done until the Reconciliation Script returns 'SUCCESS'**. This script compares row counts and key sums (like Total Revenue or Total Patients) between the legacy CSV and the new Delta table to prove data integrity.

### Network Resilience

The system automatically retries network failures with exponential backoff. Configure `network_backoff_seconds` for your environment's network characteristics.

## 🐛 Troubleshooting

### Tasks Stuck in PROCESSING

Check visibility timeout and locked_until timestamp. Tasks auto-recover when lock expires.

### High Failure Rate

1. Check monitoring dashboard for error patterns
2. Review dead letter queue for common errors
3. Adjust backoff configuration if needed
4. Verify source data quality

### Reconciliation Failures

1. Check reconciliation results table for discrepancies
2. Verify source CSV integrity
3. Review key_sum_columns configuration
4. Check tolerance_threshold for numeric precision issues

## 📝 License

[Your License]

## 👥 Contributing

[Contributing Guidelines]

## 📧 Support

For issues and questions:
- GitHub Issues: [Repository Issues]
- Email: [Support Email]
- Documentation: [Wiki/Docs URL]

---

**Built with ❤️ for reliable, production-grade data migrations on Azure Databricks**
