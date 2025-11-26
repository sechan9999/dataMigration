# Probabilistic Forecasting System for Macroeconomic and Financial Indicators

## 🎯 Overview

A production-grade probabilistic forecasting system designed for generating high-quality predictions on macroeconomic and financial indicators with complete documentation and reasoning.

### Key Capabilities

- **Probabilistic Forecasts**: Full probability distributions, not just point estimates
- **Multiple Methodologies**: Bayesian, Monte Carlo, Ensemble approaches
- **Structured Reasoning**: Complete documentation of forecast rationale
- **Expert Calibration**: Consensus building across domain experts
- **Trend Analysis**: Comprehensive feature extraction and pattern detection
- **Performance Evaluation**: Rigorous backtesting and accuracy metrics
- **Data Source Integration**: Unified access to macro and financial indicators

## 📦 System Architecture

```
forecasting/
├── models.py              # Probabilistic forecasting models
│   ├── BayesianForecast      - Bayesian inference with priors
│   ├── MonteCarloForecast    - Monte Carlo simulation
│   ├── EnsembleForecast      - Model combination
│   └── ForecastDistribution  - Probability distributions
│
├── indicators.py          # Data source integration
│   ├── MacroIndicator        - GDP, inflation, unemployment, etc.
│   ├── FinancialIndicator    - Equities, bonds, currencies
│   └── IndicatorDataSource   - Unified data access
│
├── reasoning.py           # Forecast documentation
│   ├── ForecastRationale     - Structured reasoning
│   ├── ReasoningEngine       - Build reasoning chains
│   ├── DocumentedForecast    - Complete documentation
│   └── EvidenceSource        - Supporting evidence
│
├── trend_analysis.py      # Feature extraction
│   ├── TrendAnalyzer         - Trend detection & measurement
│   ├── SeasonalityAnalyzer   - Seasonal pattern detection
│   ├── FeatureExtractor      - Statistical features
│   └── CorrelationAnalyzer   - Cross-indicator analysis
│
├── calibration.py         # Expert input & consensus
│   ├── ExpertCalibrator      - Track expert performance
│   ├── ConsensusBuilder      - Aggregate expert forecasts
│   └── CalibrationMetrics    - Calibration quality
│
└── evaluation.py          # Performance assessment
    ├── ForecastEvaluator     - Accuracy metrics
    ├── BacktestEngine        - Historical backtesting
    └── EvaluationReport      - Performance reports
```

## 🚀 Quick Start

### 1. Setup System

```python
from forecasting.models import BayesianForecast, ForecastHorizon
from forecasting.indicators import IndicatorDataSource
from forecasting.reasoning import ReasoningEngine

# Initialize components
data_source = IndicatorDataSource()
reasoning_engine = ReasoningEngine()

# Load data
gdp_data = data_source.get_indicator('US_GDP')
```

### 2. Generate Forecast

```python
# Create model
model = BayesianForecast(
    prior_mean=2.5,  # Prior belief about GDP growth
    prior_std=1.0
)

# Fit to data
model.fit(gdp_data.values, gdp_data.dates)

# Generate forecast
forecast = model.forecast(
    horizon=ForecastHorizon.MEDIUM_TERM,
    target_date=datetime(2025, 6, 1),
    indicator_name="US GDP Growth"
)

print(f"Forecast: {forecast.point_estimate:.2f}")
print(f"90% CI: [{forecast.distribution.confidence_intervals[0.90]}]")
```

### 3. Document Reasoning

```python
# Create rationale
rationale = reasoning_engine.create_rationale(
    forecast=forecast,
    creator="Analyst Name"
)

# Add reasoning
rationale.executive_summary = "GDP growth forecast based on..."
rationale.key_assumptions = [...]
rationale.evidence_sources = [...]

# Generate report
report = reasoning_engine.generate_documentation_report(
    DocumentedForecast(forecast, rationale)
)
```

## 🔬 Forecasting Methodologies

### Bayesian Forecasting

Combines prior beliefs with observed data using Bayesian inference.

**When to use:**
- Strong domain knowledge exists
- Want to incorporate expert priors
- Need principled uncertainty quantification

**Example:**
```python
model = BayesianForecast(
    prior_mean=2.5,    # Expert belief: ~2.5% GDP growth
    prior_std=1.0      # Moderate confidence in prior
)
```

### Monte Carlo Simulation

Generates forecasts through random sampling and scenario simulation.

**When to use:**
- Complex uncertainty propagation needed
- Multiple risk factors to model
- Want empirical distributions

**Example:**
```python
model = MonteCarloForecast(
    n_simulations=10000  # 10k scenarios
)
```

### Ensemble Methods

Combines multiple models for robust predictions.

**When to use:**
- Want to reduce model risk
- Multiple good models available
- Need stable, reliable forecasts

**Example:**
```python
ensemble = EnsembleForecast(
    models=[bayesian_model, montecarlo_model],
    weights=[0.6, 0.4]  # Weight by trust/performance
)
```

## 📊 Available Indicators

### Macroeconomic Indicators

| Key | Indicator | Source | Frequency |
|-----|-----------|--------|-----------|
| `US_GDP` | US Real GDP Growth | BEA | Quarterly |
| `US_INFLATION_CPI` | US CPI Inflation | BLS | Monthly |
| `US_UNEMPLOYMENT` | US Unemployment Rate | BLS | Monthly |
| `US_FED_FUNDS` | Federal Funds Rate | Fed | Daily |
| `US_10Y_YIELD` | 10-Year Treasury Yield | Fed | Daily |

### Financial Indicators

| Key | Indicator | Source | Frequency |
|-----|-----------|--------|-----------|
| `SPX` | S&P 500 Index | Bloomberg | Daily |
| `VIX` | Volatility Index | CBOE | Daily |
| `USD_EUR` | USD/EUR Rate | Bloomberg | Daily |
| `CREDIT_SPREAD_IG` | IG Credit Spread | Bloomberg | Daily |

## 📝 Structured Reasoning Framework

Every forecast includes complete documentation:

### 1. Executive Summary
High-level summary of forecast and key findings

### 2. Key Assumptions
```python
Assumption(
    description="GDP growth continues historical patterns",
    rationale="No evidence of structural breaks",
    confidence=ConfidenceLevel.HIGH,
    sensitivity="medium",
    alternative_scenarios=["Recession", "Boom"]
)
```

### 3. Evidence Sources
```python
EvidenceSource(
    source_type=RationaleType.DATA_DRIVEN,
    description="Historical average: 2.5%",
    reliability=ConfidenceLevel.HIGH,
    quantitative_value=2.5
)
```

### 4. Reasoning Chain
Logical sequence from evidence to conclusion:
1. Historical data shows X
2. Economic theory suggests Y
3. Model output indicates Z
4. **Therefore:** Forecast is W

### 5. Scenario Analysis
- **Base Case** (60%): Most likely outcome
- **Optimistic** (20%): Upside scenario
- **Pessimistic** (20%): Downside scenario

## 👥 Expert Calibration

### Register Experts

```python
from forecasting.calibration import ExpertProfile, ExpertiseLevel

expert = ExpertProfile(
    expert_id="expert_001",
    name="Dr. Smith",
    expertise_level=ExpertiseLevel.EXPERT,
    specialization="Monetary Policy"
)

calibrator.register_expert(expert)
```

### Submit Expert Forecasts

```python
expert_forecast = calibrator.submit_forecast(
    expert_id="expert_001",
    forecast=forecast,
    rationale=rationale,
    confidence=ConfidenceLevel.HIGH
)
```

### Build Consensus

```python
consensus = consensus_builder.build_consensus(
    expert_forecasts=[ef1, ef2, ef3],
    method=AggregationMethod.WEIGHTED_AVERAGE
)
```

### Aggregation Methods

1. **Simple Average**: Equal weight to all experts
2. **Weighted Average**: Weight by expertise/track record
3. **Median**: Robust to outliers
4. **Trimmed Mean**: Remove extremes
5. **Performance-Weighted**: Weight by historical accuracy

## 📈 Trend Analysis

### Comprehensive Feature Extraction

```python
from forecasting.trend_analysis import ForecastInputAnalyzer

analyzer = ForecastInputAnalyzer()
analysis = analyzer.analyze(indicator_data)

# Access results
print(analysis['trend']['trend_strength'])
print(analysis['seasonality']['has_seasonality'])
print(analysis['features']['statistical']['mean'])
```

### Feature Categories

1. **Statistical**: Mean, median, std, skewness, kurtosis
2. **Trend**: Slope, direction, acceleration, strength
3. **Volatility**: Overall, rolling, upside/downside
4. **Momentum**: Rate of change, moving averages
5. **Correlation**: Relationships with other indicators

## 🎯 Performance Evaluation

### Track Forecast Accuracy

```python
from forecasting.evaluation import ForecastEvaluator

evaluator = ForecastEvaluator()

# Record forecasts
evaluator.record_forecast(forecast)

# Record actuals
evaluator.record_actual(
    forecast_id=forecast.forecast_id,
    indicator_name="US_GDP",
    actual_value=2.8
)

# Evaluate
metrics = evaluator.evaluate_accuracy("US_GDP")
print(f"MAE: {metrics.mae}")
print(f"RMSE: {metrics.rmse}")
print(f"Directional Accuracy: {metrics.directional_accuracy}%")
```

### Accuracy Metrics

- **MAE**: Mean Absolute Error
- **RMSE**: Root Mean Squared Error
- **MAPE**: Mean Absolute Percentage Error
- **Directional Accuracy**: % of correct direction predictions

### Probabilistic Metrics

- **Brier Score**: Probability forecast accuracy
- **Log Score**: Logarithmic scoring rule
- **CRPS**: Continuous Ranked Probability Score
- **Coverage**: Empirical coverage of confidence intervals

### Calibration Metrics

- **Coverage Ratios**: Expected vs actual CI coverage
- **Sharpness**: Average prediction interval width
- **Calibration Plot**: Reliability diagram

## 🔄 Backtesting

```python
from forecasting.evaluation import BacktestEngine

backtest = BacktestEngine(model)

errors = backtest.run_backtest(
    historical_data=data.values,
    dates=data.dates,
    train_window=60,      # 60 periods for training
    forecast_horizon=6,    # 6 periods ahead
    step_size=1           # Monthly steps
)

summary = backtest.get_backtest_summary()
print(f"Historical MAE: {summary['mae']}")
```

## 📓 Databricks Notebooks

### 1. Setup (`forecasting_01_setup.py`)
- System initialization
- Data source exploration
- Basic forecast example
- Trend analysis demo

### 2. Generate Forecasts (`forecasting_02_generate_forecasts.py`)
- Multi-indicator forecasting
- Ensemble methodology
- Complete documentation
- Visualization

### 3. Expert Calibration (`forecasting_03_expert_calibration.py`)
- Register domain experts
- Collect expert forecasts
- Build consensus
- Analyze agreement

### 4. Evaluation (Coming Soon)
- Accuracy assessment
- Calibration analysis
- Performance comparison
- Improvement recommendations

### 5. Dashboard (Coming Soon)
- Interactive visualization
- Real-time monitoring
- Historical performance
- Forecast comparison

## 💾 Data Storage (Delta Lake)

### Forecast Table
```
/mnt/datalake/forecasting/forecasts
- forecast_id, indicator_name, forecast_date, target_date
- point_estimate, mean, median, std
- ci_90_lower, ci_90_upper, ci_95_lower, ci_95_upper
- methodology, model_version
```

### Rationale Table
```
/mnt/datalake/forecasting/rationales
- forecast_id, executive_summary
- n_assumptions, n_evidence_sources
- confidence_assessment, full_rationale_json
```

### Consensus Table
```
/mnt/datalake/forecasting/consensus_forecasts
- consensus_forecast_id, n_experts
- point_estimate, confidence_intervals
- consensus_strength, agreement_score
```

### Expert Contributions
```
/mnt/datalake/forecasting/expert_contributions
- expert_id, expert_name, expertise_level
- point_estimate, confidence, weight
```

## 🎓 Best Practices

### 1. Always Document Reasoning
- Never create forecasts without rationale
- Document all key assumptions
- Provide evidence for claims
- Consider alternative scenarios

### 2. Use Appropriate Horizons
- **Short-term** (1-3 months): Use recent data heavily
- **Medium-term** (3-12 months): Balance history and trends
- **Long-term** (1-5 years): Focus on structural factors

### 3. Quantify Uncertainty
- Always provide probability distributions
- Report multiple confidence intervals (50%, 90%, 95%)
- Acknowledge unknown unknowns
- Use scenario analysis for tail risks

### 4. Validate and Calibrate
- Backtest methodologies before deployment
- Track forecast performance over time
- Adjust models based on evidence
- Monitor calibration quality

### 5. Combine Multiple Approaches
- Use ensemble methods when possible
- Incorporate expert judgment
- Consider alternative data sources
- Test methodology sensitivity

## 🐛 Troubleshooting

### Issue: Forecasts too certain (narrow intervals)
**Solution**: Increase uncertainty estimates, check for overfitting

### Issue: Poor directional accuracy
**Solution**: Add leading indicators, improve trend detection

### Issue: Forecasts biased (consistent over/under prediction)
**Solution**: Apply bias correction, review assumptions

### Issue: Low expert consensus
**Solution**: Use median or trimmed mean, perform scenario analysis

## 📚 References

### Forecasting Theory
- Hyndman & Athanasopoulos: "Forecasting: Principles and Practice"
- Diebold: "Elements of Forecasting"
- Gneiting & Katzfuss: "Probabilistic Forecasting"

### Calibration & Consensus
- Cooke: "Experts in Uncertainty"
- Tetlock: "Superforecasting"
- Hora: "Probability Judgments for Continuous Quantities"

### Economic Forecasting
- Stock & Watson: "Introduction to Econometrics"
- Hamilton: "Time Series Analysis"
- Elliott & Timmermann: "Economic Forecasting"

## 🤝 Contributing

To extend the forecasting system:

1. **Add New Indicators**: Extend `indicators.py` with new data sources
2. **Create Models**: Implement new forecasting methodologies
3. **Improve Features**: Add trend analysis techniques
4. **Enhance Evaluation**: Develop new accuracy metrics

## 📧 Support

For questions or issues:
- Review documentation in `/docs`
- Check example notebooks in `/notebooks`
- Consult system architecture diagrams

---

**Built for rigorous, professional macroeconomic and financial forecasting** 🎯
