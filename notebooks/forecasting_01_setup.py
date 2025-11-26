"""
Databricks notebook source
MAGIC %md
MAGIC # Probabilistic Forecasting System - Setup and Configuration
MAGIC
MAGIC This notebook initializes the probabilistic forecasting system for macroeconomic and financial indicators.
MAGIC
MAGIC ## Features
MAGIC - System initialization and configuration
MAGIC - Data source setup
MAGIC - Indicator catalog exploration
MAGIC - Basic forecasting demonstration
MAGIC
MAGIC ## Prerequisites
MAGIC - Python 3.9+
MAGIC - Access to DBFS for storing forecasts
MAGIC - Required libraries installed
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Dependencies

# COMMAND ----------

# MAGIC %pip install numpy scipy pandas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Import Libraries

# COMMAND ----------

import sys
sys.path.append('/dbfs/src')

from datetime import datetime, timedelta
import numpy as np
import pandas as pd

from forecasting.models import (
    BayesianForecast,
    MonteCarloForecast,
    EnsembleForecast,
    ForecastHorizon
)
from forecasting.indicators import (
    IndicatorDataSource,
    MacroIndicator,
    FinancialIndicator
)
from forecasting.reasoning import ReasoningEngine, DocumentedForecast
from forecasting.trend_analysis import ForecastInputAnalyzer
from forecasting.calibration import ExpertCalibrator, ConsensusBuilder, ExpertProfile, ExpertiseLevel
from forecasting.evaluation import ForecastEvaluator

print("✓ All imports successful!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Initialize System Components

# COMMAND ----------

# Initialize core components
data_source = IndicatorDataSource()
reasoning_engine = ReasoningEngine()
analyzer = ForecastInputAnalyzer()
calibrator = ExpertCalibrator()
consensus_builder = ConsensusBuilder(calibrator)
evaluator = ForecastEvaluator()

print("✓ System components initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Explore Available Indicators

# COMMAND ----------

# List all available macroeconomic and financial indicators
available_indicators = data_source.list_available_indicators()

print(f"Available Indicators: {len(available_indicators)}")
print("\nMacroeconomic Indicators:")
print("-" * 60)

for key, metadata in available_indicators.items():
    print(f"\n{key}:")
    print(f"  Name: {metadata.indicator_name}")
    print(f"  Type: {metadata.indicator_type.value}")
    print(f"  Source: {metadata.source}")
    print(f"  Frequency: {metadata.frequency.value}")
    print(f"  Unit: {metadata.unit}")
    print(f"  Geography: {metadata.geography}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Load Sample Data

# COMMAND ----------

# Load US GDP data
print("Loading US GDP data...")
gdp_data = data_source.get_indicator(
    'US_GDP',
    start_date=datetime(2015, 1, 1),
    end_date=datetime.now()
)

print(f"✓ Loaded {len(gdp_data.values)} observations")
print(f"  Date range: {gdp_data.dates[0].strftime('%Y-%m-%d')} to {gdp_data.dates[-1].strftime('%Y-%m-%d')}")
print(f"  Latest value: {gdp_data.values[-1]:.2f} {gdp_data.metadata.unit}")

# Convert to DataFrame for visualization
gdp_df = gdp_data.to_dataframe()
display(gdp_df.tail(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Basic Visualization

# COMMAND ----------

import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(12, 6))
ax.plot(gdp_data.dates, gdp_data.values, linewidth=2, color='#1f77b4')
ax.set_xlabel('Date', fontsize=12)
ax.set_ylabel(f'{gdp_data.metadata.indicator_name} ({gdp_data.metadata.unit})', fontsize=12)
ax.set_title(f'{gdp_data.metadata.indicator_name} - Historical Data', fontsize=14, fontweight='bold')
ax.grid(True, alpha=0.3)
plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Simple Forecast Example

# COMMAND ----------

print("Generating probabilistic forecast for US GDP...")

# Initialize Bayesian forecast model
model = BayesianForecast(
    model_name="US_GDP_Bayesian",
    prior_mean=2.5,  # Prior belief: GDP growth around 2.5%
    prior_std=1.0
)

# Fit model to historical data
model.fit(
    historical_data=gdp_data.values,
    dates=gdp_data.dates
)

# Generate forecast for 6 months ahead
target_date = datetime.now() + timedelta(days=180)
forecast = model.forecast(
    horizon=ForecastHorizon.MEDIUM_TERM,
    target_date=target_date,
    indicator_name="US Real GDP Growth"
)

print(f"\n{'='*60}")
print(f"FORECAST SUMMARY")
print(f"{'='*60}")
print(f"Indicator:      {forecast.indicator_name}")
print(f"Target Date:    {forecast.target_date.strftime('%Y-%m-%d')}")
print(f"Horizon:        {forecast.horizon.value}")
print(f"\nPoint Estimate: {forecast.point_estimate:.2f}")
print(f"Mean:           {forecast.distribution.mean:.2f}")
print(f"Median:         {forecast.distribution.median:.2f}")
print(f"Std Dev:        {forecast.distribution.std:.2f}")
print(f"\nConfidence Intervals:")
print(f"  50%: [{forecast.distribution.confidence_intervals[0.50][0]:.2f}, {forecast.distribution.confidence_intervals[0.50][1]:.2f}]")
print(f"  90%: [{forecast.distribution.confidence_intervals[0.90][0]:.2f}, {forecast.distribution.confidence_intervals[0.90][1]:.2f}]")
print(f"  95%: [{forecast.distribution.confidence_intervals[0.95][0]:.2f}, {forecast.distribution.confidence_intervals[0.95][1]:.2f}]")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Visualize Forecast Distribution

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(15, 5))

# Plot 1: Histogram of forecast distribution
ax = axes[0]
ax.hist(forecast.distribution.samples, bins=50, alpha=0.7, color='#2ca02c', edgecolor='black')
ax.axvline(forecast.point_estimate, color='red', linestyle='--', linewidth=2, label='Point Estimate')
ax.axvline(forecast.distribution.median, color='orange', linestyle='--', linewidth=2, label='Median')
ax.set_xlabel('GDP Growth (%)', fontsize=11)
ax.set_ylabel('Frequency', fontsize=11)
ax.set_title('Forecast Distribution', fontsize=12, fontweight='bold')
ax.legend()
ax.grid(True, alpha=0.3)

# Plot 2: Confidence intervals
ax = axes[1]
confidence_levels = [0.50, 0.80, 0.90, 0.95]
for i, level in enumerate(confidence_levels):
    lower, upper = forecast.distribution.confidence_intervals[level]
    ax.barh(i, upper - lower, left=lower, height=0.6,
            alpha=0.6, label=f'{level*100:.0f}% CI')

ax.axvline(forecast.point_estimate, color='red', linestyle='--', linewidth=2, label='Point Estimate')
ax.set_yticks(range(len(confidence_levels)))
ax.set_yticklabels([f'{l*100:.0f}%' for l in confidence_levels])
ax.set_xlabel('GDP Growth (%)', fontsize=11)
ax.set_ylabel('Confidence Level', fontsize=11)
ax.set_title('Confidence Intervals', fontsize=12, fontweight='bold')
ax.legend()
ax.grid(True, alpha=0.3, axis='x')

plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Perform Trend Analysis

# COMMAND ----------

print("Performing comprehensive trend analysis...")

analysis = analyzer.analyze(gdp_data)

# Display analysis report
report = analyzer.generate_analysis_report(analysis, "US Real GDP Growth")
print(report)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Create Documented Forecast with Reasoning

# COMMAND ----------

from forecasting.reasoning import (
    ForecastRationale,
    ReasoningChain,
    EvidenceSource,
    RationaleType,
    ConfidenceLevel,
    Assumption
)

# Create reasoning chain
reasoning_chain = reasoning_engine.build_data_driven_reasoning(
    historical_data=gdp_data,
    forecast=forecast
)

# Create full rationale
rationale = reasoning_engine.create_rationale(
    forecast=forecast,
    creator="Forecasting System"
)

rationale.executive_summary = f"""
Forecast for US Real GDP Growth over the next 6 months using Bayesian methodology.
The forecast incorporates historical data patterns with prior beliefs about GDP growth.

Key Finding: Expected GDP growth of {forecast.point_estimate:.2f}% with 90% confidence
interval of [{forecast.distribution.confidence_intervals[0.90][0]:.2f}%,
{forecast.distribution.confidence_intervals[0.90][1]:.2f}%].
"""

# Add key assumptions
rationale.key_assumptions = [
    Assumption(
        description="GDP growth will continue historical patterns without major structural breaks",
        rationale="No evidence of regime change in recent data",
        confidence=ConfidenceLevel.HIGH,
        sensitivity="medium"
    ),
    Assumption(
        description="Prior belief of 2.5% average GDP growth is reasonable",
        rationale="Based on long-term US GDP growth trends",
        confidence=ConfidenceLevel.HIGH,
        sensitivity="low"
    ),
    Assumption(
        description="No major economic shocks in forecast horizon",
        rationale="Base case scenario excludes tail events",
        confidence=ConfidenceLevel.MEDIUM,
        sensitivity="high",
        alternative_scenarios=["Financial crisis", "Policy shock", "Pandemic"]
    )
]

# Add evidence sources
rationale.evidence_sources = [
    EvidenceSource(
        source_type=RationaleType.DATA_DRIVEN,
        description=f"Historical average GDP growth: {analysis['features']['statistical']['mean']:.2f}%",
        reliability=ConfidenceLevel.HIGH,
        quantitative_value=analysis['features']['statistical']['mean']
    ),
    EvidenceSource(
        source_type=RationaleType.THEORY_BASED,
        description="Bayesian updating with informative prior",
        reliability=ConfidenceLevel.HIGH
    )
]

rationale.reasoning_chain = reasoning_chain
rationale.data_sources_used = ["BEA US GDP Data"]
rationale.models_used = ["Bayesian Forecast Model"]
rationale.confidence_assessment = ConfidenceLevel.HIGH

# Create documented forecast
documented_forecast = DocumentedForecast(
    forecast=forecast,
    rationale=rationale
)

# Display documentation report
doc_report = reasoning_engine.generate_documentation_report(documented_forecast)
print(doc_report)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Save Forecast to Delta Lake

# COMMAND ----------

# Convert forecast to DataFrame
forecast_dict = forecast.to_dict()
forecast_df = pd.DataFrame([{
    'forecast_id': forecast_dict['forecast_id'],
    'indicator_name': forecast_dict['indicator_name'],
    'forecast_date': forecast_dict['forecast_date'],
    'target_date': forecast_dict['target_date'],
    'horizon': forecast_dict['horizon'],
    'point_estimate': forecast_dict['point_estimate'],
    'mean': forecast_dict['distribution']['mean'],
    'median': forecast_dict['distribution']['median'],
    'std': forecast_dict['distribution']['std'],
    'ci_90_lower': forecast_dict['distribution']['confidence_intervals']['0.9'][0],
    'ci_90_upper': forecast_dict['distribution']['confidence_intervals']['0.9'][1],
    'ci_95_lower': forecast_dict['distribution']['confidence_intervals']['0.95'][0],
    'ci_95_upper': forecast_dict['distribution']['confidence_intervals']['0.95'][1],
    'methodology': forecast_dict['methodology'],
    'model_version': forecast_dict['model_version']
}])

# Save to Delta Lake
forecast_table_path = "/mnt/datalake/forecasting/forecasts"
forecast_df.write.format("delta").mode("append").save(forecast_table_path)

print(f"✓ Forecast saved to {forecast_table_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Next Steps
# MAGIC
# MAGIC Now that the system is set up, you can:
# MAGIC
# MAGIC 1. **Generate Forecasts**: Run `forecasting_02_generate_forecasts` to create forecasts for multiple indicators
# MAGIC 2. **Expert Calibration**: Use `forecasting_03_expert_calibration` to incorporate expert judgments
# MAGIC 3. **Evaluation**: Run `forecasting_04_evaluation` to assess forecast accuracy
# MAGIC 4. **Dashboard**: View `forecasting_05_dashboard` for interactive visualization
# MAGIC
# MAGIC ---
# MAGIC **System Status**: ✓ Ready for Production Forecasting
