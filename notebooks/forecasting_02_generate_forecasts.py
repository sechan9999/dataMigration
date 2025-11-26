"""
Databricks notebook source
MAGIC %md
MAGIC # Generate Probabilistic Forecasts for Multiple Indicators
MAGIC
MAGIC This notebook demonstrates generating probabilistic forecasts across multiple macroeconomic
MAGIC and financial indicators with complete documentation and reasoning.
MAGIC
MAGIC ## Workflow
MAGIC 1. Load multiple indicators
MAGIC 2. Perform trend analysis
MAGIC 3. Generate ensemble forecasts
MAGIC 4. Document reasoning
MAGIC 5. Store results
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Import and Initialize

# COMMAND ----------

import sys
sys.path.append('/dbfs/src')

from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from forecasting.models import (
    BayesianForecast,
    MonteCarloForecast,
    EnsembleForecast,
    ForecastHorizon
)
from forecasting.indicators import IndicatorDataSource
from forecasting.reasoning import (
    ReasoningEngine,
    DocumentedForecast,
    ForecastRationale,
    Assumption,
    EvidenceSource,
    RationaleType,
    ConfidenceLevel,
    ScenarioAnalysis
)
from forecasting.trend_analysis import ForecastInputAnalyzer

# Initialize
data_source = IndicatorDataSource()
reasoning_engine = ReasoningEngine()
analyzer = ForecastInputAnalyzer()

print("✓ System initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Define Indicators to Forecast

# COMMAND ----------

# Select indicators for forecasting
indicators_to_forecast = [
    'US_GDP',
    'US_INFLATION_CPI',
    'US_UNEMPLOYMENT',
    'US_FED_FUNDS',
    'US_10Y_YIELD'
]

forecast_horizon = ForecastHorizon.MEDIUM_TERM
target_date = datetime.now() + timedelta(days=180)  # 6 months ahead

print(f"Generating forecasts for {len(indicators_to_forecast)} indicators")
print(f"Target date: {target_date.strftime('%Y-%m-%d')}")
print(f"Horizon: {forecast_horizon.value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load Historical Data

# COMMAND ----------

print("Loading historical data for all indicators...")

historical_data = {}
for indicator_key in indicators_to_forecast:
    data = data_source.get_indicator(
        indicator_key,
        start_date=datetime(2010, 1, 1),
        end_date=datetime.now()
    )
    historical_data[indicator_key] = data
    print(f"  ✓ {indicator_key}: {len(data.values)} observations")

print(f"\n✓ Loaded data for {len(historical_data)} indicators")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Perform Trend Analysis for Each Indicator

# COMMAND ----------

print("Performing trend analysis...\n")

analyses = {}
for key, data in historical_data.items():
    print(f"Analyzing {key}...")
    analysis = analyzer.analyze(data)
    analyses[key] = analysis

    print(f"  Trend strength: {analysis['trend']['trend_strength']:.3f}")
    print(f"  Has seasonality: {analysis['seasonality']['has_seasonality']}")
    print(f"  Mean value: {analysis['features']['statistical']['mean']:.2f}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Generate Ensemble Forecasts

# COMMAND ----------

print("Generating ensemble forecasts combining multiple methodologies...\n")

all_forecasts = {}
all_documented_forecasts = []

for indicator_key, data in historical_data.items():
    print(f"{'='*70}")
    print(f"Forecasting: {indicator_key}")
    print(f"{'='*70}")

    # Create multiple models
    bayesian_model = BayesianForecast(
        model_name=f"{indicator_key}_Bayesian",
        prior_mean=analyses[indicator_key]['features']['statistical']['mean'],
        prior_std=analyses[indicator_key]['features']['statistical']['std']
    )

    montecarlo_model = MonteCarloForecast(
        model_name=f"{indicator_key}_MonteCarlo",
        n_simulations=10000
    )

    # Create ensemble
    ensemble = EnsembleForecast(
        models=[bayesian_model, montecarlo_model],
        weights=[0.6, 0.4]  # Weight Bayesian more heavily
    )

    # Fit ensemble
    ensemble.fit(
        historical_data=data.values,
        dates=data.dates
    )

    # Generate forecast
    forecast = ensemble.forecast(
        horizon=forecast_horizon,
        target_date=target_date,
        indicator_name=data.metadata.indicator_name
    )

    all_forecasts[indicator_key] = forecast

    print(f"\nForecast Summary:")
    print(f"  Point Estimate: {forecast.point_estimate:.2f}")
    print(f"  90% CI: [{forecast.distribution.confidence_intervals[0.90][0]:.2f}, "
          f"{forecast.distribution.confidence_intervals[0.90][1]:.2f}]")
    print(f"  Uncertainty (std): {forecast.distribution.std:.2f}")

    # Build reasoning
    rationale = reasoning_engine.create_rationale(
        forecast=forecast,
        creator="Ensemble Forecasting System"
    )

    # Executive summary
    rationale.executive_summary = f"""
Ensemble forecast for {data.metadata.indicator_name} combining Bayesian and Monte Carlo methodologies.

Historical Analysis:
- Mean: {analyses[indicator_key]['features']['statistical']['mean']:.2f}
- Trend: {'Upward' if analyses[indicator_key]['features']['trend']['trend_direction'] > 0 else 'Downward'}
- Volatility: {analyses[indicator_key]['features']['volatility']['volatility']:.2f}

Forecast: {forecast.point_estimate:.2f} with 90% confidence interval
[{forecast.distribution.confidence_intervals[0.90][0]:.2f},
{forecast.distribution.confidence_intervals[0.90][1]:.2f}]
    """

    # Add assumptions
    rationale.key_assumptions = [
        Assumption(
            description=f"Historical patterns for {indicator_key} will persist",
            rationale="No evidence of structural breaks in recent data",
            confidence=ConfidenceLevel.HIGH,
            sensitivity="medium"
        ),
        Assumption(
            description="Ensemble weighting (60% Bayesian, 40% Monte Carlo) is appropriate",
            rationale="Bayesian provides stable priors, Monte Carlo captures full uncertainty",
            confidence=ConfidenceLevel.MEDIUM,
            sensitivity="low"
        )
    ]

    # Add evidence
    reasoning_chain = reasoning_engine.build_data_driven_reasoning(data, forecast)
    rationale.reasoning_chain = reasoning_chain

    rationale.evidence_sources = [
        EvidenceSource(
            source_type=RationaleType.DATA_DRIVEN,
            description=f"Historical mean: {analyses[indicator_key]['features']['statistical']['mean']:.2f}",
            reliability=ConfidenceLevel.HIGH
        ),
        EvidenceSource(
            source_type=RationaleType.MODEL_OUTPUT,
            description="Ensemble of Bayesian and Monte Carlo models",
            reliability=ConfidenceLevel.HIGH
        )
    ]

    # Create scenario analysis
    scenario = reasoning_engine.create_scenario_analysis(
        base_forecast=forecast.point_estimate,
        indicator_name=data.metadata.indicator_name,
        optimistic_factor=1.2,
        pessimistic_factor=0.8
    )
    rationale.scenario_analysis = scenario

    rationale.data_sources_used = [data.metadata.source]
    rationale.models_used = ["BayesianForecast", "MonteCarloForecast", "EnsembleForecast"]
    rationale.confidence_assessment = ConfidenceLevel.HIGH

    # Create documented forecast
    documented = DocumentedForecast(
        forecast=forecast,
        rationale=rationale
    )
    all_documented_forecasts.append(documented)

    print(f"✓ Forecast documented\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Visualize All Forecasts

# COMMAND ----------

fig, axes = plt.subplots(len(indicators_to_forecast), 2, figsize=(15, 4*len(indicators_to_forecast)))

for idx, (indicator_key, forecast) in enumerate(all_forecasts.items()):
    data = historical_data[indicator_key]

    # Left plot: Historical + Forecast
    ax_left = axes[idx, 0] if len(indicators_to_forecast) > 1 else axes[0]

    # Plot historical
    ax_left.plot(data.dates, data.values, linewidth=2, label='Historical', color='#1f77b4')

    # Plot forecast point
    ax_left.scatter([forecast.target_date], [forecast.point_estimate],
                   s=100, color='red', zorder=5, label='Forecast')

    # Plot 90% CI
    ci_90_lower, ci_90_upper = forecast.distribution.confidence_intervals[0.90]
    ax_left.plot([data.dates[-1], forecast.target_date],
                [data.values[-1], forecast.point_estimate],
                '--', color='red', alpha=0.5)
    ax_left.fill_between([forecast.target_date], [ci_90_lower], [ci_90_upper],
                         alpha=0.3, color='red', label='90% CI')

    ax_left.set_title(f'{indicator_key}: Historical + Forecast', fontweight='bold')
    ax_left.set_xlabel('Date')
    ax_left.set_ylabel(data.metadata.unit)
    ax_left.legend()
    ax_left.grid(True, alpha=0.3)

    # Right plot: Forecast distribution
    ax_right = axes[idx, 1] if len(indicators_to_forecast) > 1 else axes[1]

    ax_right.hist(forecast.distribution.samples, bins=50, alpha=0.7,
                 color='#2ca02c', edgecolor='black')
    ax_right.axvline(forecast.point_estimate, color='red', linestyle='--',
                    linewidth=2, label='Point Estimate')
    ax_right.axvline(forecast.distribution.median, color='orange', linestyle='--',
                    linewidth=2, label='Median')

    ax_right.set_title(f'{indicator_key}: Forecast Distribution', fontweight='bold')
    ax_right.set_xlabel(data.metadata.unit)
    ax_right.set_ylabel('Frequency')
    ax_right.legend()
    ax_right.grid(True, alpha=0.3)

plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Compare Forecasts Across Indicators

# COMMAND ----------

# Create comparison table
comparison_data = []

for indicator_key, forecast in all_forecasts.items():
    data = historical_data[indicator_key]
    comparison_data.append({
        'Indicator': indicator_key,
        'Current Value': f"{data.values[-1]:.2f}",
        'Forecast': f"{forecast.point_estimate:.2f}",
        'Change': f"{((forecast.point_estimate - data.values[-1]) / data.values[-1] * 100):+.1f}%",
        '90% CI Lower': f"{forecast.distribution.confidence_intervals[0.90][0]:.2f}",
        '90% CI Upper': f"{forecast.distribution.confidence_intervals[0.90][1]:.2f}",
        'Uncertainty (Std)': f"{forecast.distribution.std:.2f}"
    })

comparison_df = pd.DataFrame(comparison_data)

print("FORECAST COMPARISON TABLE")
print("="*100)
display(comparison_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Generate Full Documentation Reports

# COMMAND ----------

print("DETAILED FORECAST DOCUMENTATION")
print("="*80)
print()

for doc_forecast in all_documented_forecasts:
    report = reasoning_engine.generate_documentation_report(doc_forecast)
    print(report)
    print("\n" + "="*80 + "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Save Forecasts to Delta Lake

# COMMAND ----------

# Prepare data for Delta Lake
forecast_records = []
rationale_records = []

for doc_forecast in all_documented_forecasts:
    forecast = doc_forecast.forecast
    rationale = doc_forecast.rationale

    # Forecast record
    forecast_dict = forecast.to_dict()
    forecast_records.append({
        'forecast_id': forecast_dict['forecast_id'],
        'indicator_name': forecast_dict['indicator_name'],
        'forecast_date': forecast_dict['forecast_date'],
        'target_date': forecast_dict['target_date'],
        'horizon': forecast_dict['horizon'],
        'point_estimate': forecast_dict['point_estimate'],
        'mean': forecast_dict['distribution']['mean'],
        'median': forecast_dict['distribution']['median'],
        'std': forecast_dict['distribution']['std'],
        'ci_50_lower': forecast_dict['distribution']['confidence_intervals']['0.5'][0],
        'ci_50_upper': forecast_dict['distribution']['confidence_intervals']['0.5'][1],
        'ci_90_lower': forecast_dict['distribution']['confidence_intervals']['0.9'][0],
        'ci_90_upper': forecast_dict['distribution']['confidence_intervals']['0.9'][1],
        'ci_95_lower': forecast_dict['distribution']['confidence_intervals']['0.95'][0],
        'ci_95_upper': forecast_dict['distribution']['confidence_intervals']['0.95'][1],
        'methodology': forecast_dict['methodology'],
        'model_version': forecast_dict['model_version'],
        'created_at': datetime.now().isoformat()
    })

    # Rationale record
    rationale_dict = rationale.to_dict()
    rationale_records.append({
        'forecast_id': rationale_dict['forecast_id'],
        'executive_summary': rationale_dict['executive_summary'],
        'n_assumptions': len(rationale_dict['key_assumptions']),
        'n_evidence_sources': len(rationale_dict['evidence_sources']),
        'confidence_assessment': rationale_dict['confidence_assessment'],
        'full_rationale_json': rationale.to_json(),
        'created_at': datetime.now().isoformat()
    })

# Convert to DataFrames
forecasts_df = pd.DataFrame(forecast_records)
rationales_df = pd.DataFrame(rationale_records)

# Save to Delta Lake
forecast_table_path = "/mnt/datalake/forecasting/forecasts"
rationale_table_path = "/mnt/datalake/forecasting/rationales"

forecasts_df.write.format("delta").mode("append").save(forecast_table_path)
rationales_df.write.format("delta").mode("append").save(rationale_table_path)

print(f"✓ Saved {len(forecast_records)} forecasts to {forecast_table_path}")
print(f"✓ Saved {len(rationale_records)} rationales to {rationale_table_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Summary Statistics

# COMMAND ----------

summary_stats = {
    'total_forecasts': len(all_forecasts),
    'forecast_date': datetime.now().strftime('%Y-%m-%d'),
    'target_date': target_date.strftime('%Y-%m-%d'),
    'horizon': forecast_horizon.value,
    'avg_uncertainty': np.mean([f.distribution.std for f in all_forecasts.values()]),
    'indicators_covered': list(all_forecasts.keys())
}

print("FORECASTING RUN SUMMARY")
print("="*60)
for key, value in summary_stats.items():
    print(f"{key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. **Expert Calibration**: Run `forecasting_03_expert_calibration` to incorporate expert adjustments
# MAGIC 2. **Evaluation**: Run `forecasting_04_evaluation` once actuals are available
# MAGIC 3. **Dashboard**: View `forecasting_05_dashboard` for interactive analysis
# MAGIC
# MAGIC ---
# MAGIC **Status**: ✓ Forecasts Generated and Documented
