"""
Databricks notebook source
MAGIC %md
MAGIC # Expert Calibration and Consensus Building
MAGIC
MAGIC This notebook demonstrates how to:
MAGIC - Collect expert forecasts
MAGIC - Build consensus across multiple experts
MAGIC - Track expert performance
MAGIC - Calibrate probabilistic forecasts
MAGIC
MAGIC ## Workflow
MAGIC 1. Register domain experts
MAGIC 2. Submit expert forecasts
MAGIC 3. Build consensus forecasts
MAGIC 4. Analyze agreement strength
MAGIC 5. Track performance over time
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Imports

# COMMAND ----------

import sys
sys.path.append('/dbfs/src')

from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from forecasting.models import ForecastHorizon, ProbabilisticForecast, ForecastDistribution, DistributionType
from forecasting.calibration import (
    ExpertCalibrator,
    ConsensusBuilder,
    ExpertProfile,
    ExpertiseLevel,
    AggregationMethod,
    ExpertForecast
)
from forecasting.reasoning import (
    ForecastRationale,
    ReasoningChain,
    EvidenceSource,
    RationaleType,
    ConfidenceLevel
)

# Initialize
calibrator = ExpertCalibrator()
consensus_builder = ConsensusBuilder(calibrator)

print("✓ Expert calibration system initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Register Domain Experts

# COMMAND ----------

# Register experts with different expertise levels
experts = [
    ExpertProfile(
        expert_id="expert_001",
        name="Dr. Sarah Chen",
        expertise_level=ExpertiseLevel.EXPERT,
        specialization="Monetary Policy & Central Banking",
        historical_accuracy={'mean_absolute_error': 0.15}
    ),
    ExpertProfile(
        expert_id="expert_002",
        name="Prof. Michael Rodriguez",
        expertise_level=ExpertiseLevel.MASTER,
        specialization="Macroeconomic Forecasting",
        historical_accuracy={'mean_absolute_error': 0.12}
    ),
    ExpertProfile(
        expert_id="expert_003",
        name="Jane Williams",
        expertise_level=ExpertiseLevel.EXPERT,
        specialization="Labor Markets & Unemployment",
        historical_accuracy={'mean_absolute_error': 0.18}
    ),
    ExpertProfile(
        expert_id="expert_004",
        name="David Kim",
        expertise_level=ExpertiseLevel.INTERMEDIATE,
        specialization="GDP & Growth Analysis",
        historical_accuracy={'mean_absolute_error': 0.22}
    ),
    ExpertProfile(
        expert_id="expert_005",
        name="Dr. Lisa Johnson",
        expertise_level=ExpertiseLevel.EXPERT,
        specialization="Inflation & Price Dynamics",
        historical_accuracy={'mean_absolute_error': 0.16}
    )
]

# Register all experts
for expert in experts:
    calibrator.register_expert(expert)

print(f"✓ Registered {len(experts)} domain experts")
print("\nExpert Profiles:")
print("="*80)

for expert in experts:
    print(f"\n{expert.name} ({expert.expert_id})")
    print(f"  Expertise: {expert.expertise_level.value}")
    print(f"  Specialization: {expert.specialization}")
    if expert.historical_accuracy:
        print(f"  Historical MAE: {expert.historical_accuracy['mean_absolute_error']:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Simulate Expert Forecasts for US Inflation
# MAGIC
# MAGIC Collect forecasts from multiple experts for CPI Inflation (6 months ahead)

# COMMAND ----------

indicator_name = "US CPI Inflation"
target_date = datetime.now() + timedelta(days=180)
horizon = ForecastHorizon.MEDIUM_TERM

print(f"Collecting expert forecasts for: {indicator_name}")
print(f"Target date: {target_date.strftime('%Y-%m-%d')}")
print(f"Horizon: {horizon.value}\n")

# Simulate expert forecasts with different views
expert_forecasts_data = [
    {
        'expert_id': 'expert_001',
        'point_estimate': 2.8,
        'uncertainty': 0.4,
        'confidence': ConfidenceLevel.HIGH,
        'key_reasoning': 'Monetary policy tightening will slow inflation. Fed commitment is credible.'
    },
    {
        'expert_id': 'expert_002',
        'point_estimate': 3.2,
        'uncertainty': 0.5,
        'confidence': ConfidenceLevel.HIGH,
        'key_reasoning': 'Supply-side pressures persist. Labor market remains tight.'
    },
    {
        'expert_id': 'expert_003',
        'point_estimate': 2.5,
        'uncertainty': 0.6,
        'confidence': ConfidenceLevel.MEDIUM,
        'key_reasoning': 'Wage growth moderating. Employment-to-population ratio normalizing.'
    },
    {
        'expert_id': 'expert_004',
        'point_estimate': 3.0,
        'uncertainty': 0.7,
        'confidence': ConfidenceLevel.MEDIUM,
        'key_reasoning': 'GDP growth slowing will reduce demand-pull inflation.'
    },
    {
        'expert_id': 'expert_005',
        'point_estimate': 2.9,
        'uncertainty': 0.4,
        'confidence': ConfidenceLevel.HIGH,
        'key_reasoning': 'Core inflation sticky but headline moderating with energy prices.'
    }
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Submit Expert Forecasts

# COMMAND ----------

expert_forecasts = []

for forecast_data in expert_forecasts_data:
    expert_id = forecast_data['expert_id']
    expert = calibrator.expert_profiles[expert_id]

    # Create forecast distribution
    samples = np.random.normal(
        forecast_data['point_estimate'],
        forecast_data['uncertainty'],
        10000
    )

    distribution = ForecastDistribution(
        mean=float(np.mean(samples)),
        median=float(np.median(samples)),
        std=float(np.std(samples)),
        quantiles={
            0.05: float(np.percentile(samples, 5)),
            0.25: float(np.percentile(samples, 25)),
            0.50: float(np.percentile(samples, 50)),
            0.75: float(np.percentile(samples, 75)),
            0.95: float(np.percentile(samples, 95))
        },
        confidence_intervals={
            0.90: (float(np.percentile(samples, 5)), float(np.percentile(samples, 95))),
            0.95: (float(np.percentile(samples, 2.5)), float(np.percentile(samples, 97.5)))
        },
        distribution_type=DistributionType.NORMAL,
        samples=samples
    )

    # Create probabilistic forecast
    forecast = ProbabilisticForecast(
        indicator_name=indicator_name,
        forecast_date=datetime.now(),
        target_date=target_date,
        horizon=horizon,
        distribution=distribution,
        point_estimate=forecast_data['point_estimate'],
        methodology=f"Expert judgment by {expert.name}",
        model_version="expert_v1.0",
        metadata={'expert_id': expert_id}
    )

    # Create rationale
    rationale = ForecastRationale(
        forecast_id=forecast.forecast_id,
        indicator_name=indicator_name,
        created_at=datetime.now(),
        created_by=expert.name,
        executive_summary=forecast_data['key_reasoning'],
        key_assumptions=[],
        evidence_sources=[
            EvidenceSource(
                source_type=RationaleType.EXPERT_JUDGMENT,
                description=forecast_data['key_reasoning'],
                reliability=forecast_data['confidence']
            )
        ],
        reasoning_chain=ReasoningChain(
            steps=[],
            conclusion=forecast_data['key_reasoning'],
            confidence=forecast_data['confidence']
        ),
        confidence_assessment=forecast_data['confidence']
    )

    # Submit forecast
    expert_forecast = calibrator.submit_forecast(
        expert_id=expert_id,
        forecast=forecast,
        rationale=rationale,
        confidence=forecast_data['confidence']
    )

    expert_forecasts.append(expert_forecast)

    print(f"✓ {expert.name}: {forecast_data['point_estimate']:.1f}% "
          f"(90% CI: [{distribution.confidence_intervals[0.90][0]:.1f}%, "
          f"{distribution.confidence_intervals[0.90][1]:.1f}%])")

print(f"\n✓ Collected {len(expert_forecasts)} expert forecasts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Visualize Expert Forecasts

# COMMAND ----------

fig, axes = plt.subplots(2, 1, figsize=(14, 10))

# Plot 1: Point estimates with confidence intervals
ax = axes[0]

expert_names = [ef.expert.name for ef in expert_forecasts]
point_estimates = [ef.forecast.point_estimate for ef in expert_forecasts]
ci_90_lower = [ef.forecast.distribution.confidence_intervals[0.90][0] for ef in expert_forecasts]
ci_90_upper = [ef.forecast.distribution.confidence_intervals[0.90][1] for ef in expert_forecasts]

y_pos = np.arange(len(expert_names))

# Plot confidence intervals
for i, (lower, upper, point) in enumerate(zip(ci_90_lower, ci_90_upper, point_estimates)):
    ax.plot([lower, upper], [i, i], 'b-', linewidth=6, alpha=0.3)
    ax.plot(point, i, 'ro', markersize=10)

ax.set_yticks(y_pos)
ax.set_yticklabels(expert_names)
ax.set_xlabel('Inflation Forecast (%)', fontsize=12)
ax.set_title('Expert Forecasts: Point Estimates and 90% Confidence Intervals',
             fontsize=14, fontweight='bold')
ax.grid(True, alpha=0.3, axis='x')

# Add vertical line at mean of point estimates
mean_estimate = np.mean(point_estimates)
ax.axvline(mean_estimate, color='green', linestyle='--', linewidth=2, label=f'Mean: {mean_estimate:.2f}%')
ax.legend()

# Plot 2: Distribution of all expert forecasts
ax = axes[1]

for ef in expert_forecasts:
    ax.hist(ef.forecast.distribution.samples, bins=50, alpha=0.3,
           label=ef.expert.name[:15])

ax.set_xlabel('Inflation (%)', fontsize=12)
ax.set_ylabel('Frequency', fontsize=12)
ax.set_title('Distribution of Expert Forecast Samples', fontsize=14, fontweight='bold')
ax.legend(loc='upper right', fontsize=9)
ax.grid(True, alpha=0.3)

plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Build Consensus Forecasts Using Different Methods

# COMMAND ----------

print("Building consensus forecasts using different aggregation methods...\n")

aggregation_methods = [
    ('Simple Average', AggregationMethod.SIMPLE_AVERAGE),
    ('Weighted Average', AggregationMethod.WEIGHTED_AVERAGE),
    ('Median', AggregationMethod.MEDIAN),
    ('Performance-Weighted', AggregationMethod.PERFORMANCE_WEIGHTED)
]

consensus_forecasts = {}

for method_name, method in aggregation_methods:
    consensus = consensus_builder.build_consensus(
        expert_forecasts=expert_forecasts,
        method=method,
        indicator_name=indicator_name
    )

    consensus_forecasts[method_name] = consensus

    print(f"{method_name}:")
    print(f"  Point Estimate: {consensus.point_estimate:.2f}%")
    print(f"  90% CI: [{consensus.distribution.confidence_intervals[0.90][0]:.2f}%, "
          f"{consensus.distribution.confidence_intervals[0.90][1]:.2f}%]")
    print(f"  Uncertainty: {consensus.distribution.std:.2f}%")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Analyze Consensus Strength

# COMMAND ----------

consensus_analysis = consensus_builder.analyze_consensus_strength(expert_forecasts)

print("CONSENSUS ANALYSIS")
print("="*60)
print(f"Number of Experts:        {consensus_analysis['n_experts']}")
print(f"Mean Estimate:            {consensus_analysis['mean_estimate']:.2f}%")
print(f"Median Estimate:          {consensus_analysis['median_estimate']:.2f}%")
print(f"Standard Deviation:       {consensus_analysis['std_dev']:.2f}%")
print(f"Range:                    {consensus_analysis['range']:.2f}%")
print(f"Coefficient of Variation: {consensus_analysis['coefficient_of_variation']:.3f}")
print(f"Agreement Score:          {consensus_analysis['agreement_score']:.3f}")
print(f"Consensus Strength:       {consensus_analysis['consensus_strength'].upper()}")
print("="*60)

# Interpret results
if consensus_analysis['consensus_strength'] == 'strong':
    print("\n✓ Strong consensus among experts. High confidence in forecast range.")
elif consensus_analysis['consensus_strength'] == 'moderate':
    print("\n⚠ Moderate consensus. Some disagreement on forecast trajectory.")
else:
    print("\n⚠ Weak consensus. Significant disagreement - consider scenario analysis.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Compare Consensus Methods Visually

# COMMAND ----------

fig, ax = plt.subplots(figsize=(14, 6))

methods = list(consensus_forecasts.keys())
point_estimates = [cf.point_estimate for cf in consensus_forecasts.values()]
ci_90_lower = [cf.distribution.confidence_intervals[0.90][0] for cf in consensus_forecasts.values()]
ci_90_upper = [cf.distribution.confidence_intervals[0.90][1] for cf in consensus_forecasts.values()]

x_pos = np.arange(len(methods))

# Plot consensus forecasts
for i, (lower, upper, point) in enumerate(zip(ci_90_lower, ci_90_upper, point_estimates)):
    ax.plot([i, i], [lower, upper], 'b-', linewidth=8, alpha=0.3)
    ax.plot(i, point, 'go', markersize=15)

# Add individual expert forecasts as reference
individual_estimates = [ef.forecast.point_estimate for ef in expert_forecasts]
for est in individual_estimates:
    ax.axhline(est, color='red', linestyle=':', alpha=0.3, linewidth=1)

ax.set_xticks(x_pos)
ax.set_xticklabels(methods, rotation=15, ha='right')
ax.set_ylabel('Inflation Forecast (%)', fontsize=12)
ax.set_title('Consensus Forecasts: Comparison of Aggregation Methods',
            fontsize=14, fontweight='bold')
ax.grid(True, alpha=0.3, axis='y')

# Add legend
from matplotlib.patches import Patch
legend_elements = [
    Patch(facecolor='blue', alpha=0.3, label='90% Confidence Interval'),
    plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='g',
              markersize=12, label='Consensus Point Estimate'),
    plt.Line2D([0], [0], color='red', linestyle=':', alpha=0.5,
              label='Individual Expert Forecasts')
]
ax.legend(handles=legend_elements, loc='upper right')

plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Select Recommended Consensus Forecast

# COMMAND ----------

# Use weighted average as recommended method (balances expertise and performance)
recommended_consensus = consensus_forecasts['Weighted Average']

print("RECOMMENDED CONSENSUS FORECAST")
print("="*80)
print(f"Method:                  Weighted Average")
print(f"Indicator:               {recommended_consensus.indicator_name}")
print(f"Target Date:             {recommended_consensus.target_date.strftime('%Y-%m-%d')}")
print(f"\nPoint Estimate:          {recommended_consensus.point_estimate:.2f}%")
print(f"Mean:                    {recommended_consensus.distribution.mean:.2f}%")
print(f"Median:                  {recommended_consensus.distribution.median:.2f}%")
print(f"Std Dev:                 {recommended_consensus.distribution.std:.2f}%")
print(f"\nConfidence Intervals:")
print(f"  50%: [{recommended_consensus.distribution.confidence_intervals[0.50][0]:.2f}%, "
      f"{recommended_consensus.distribution.confidence_intervals[0.50][1]:.2f}%]")
print(f"  90%: [{recommended_consensus.distribution.confidence_intervals[0.90][0]:.2f}%, "
      f"{recommended_consensus.distribution.confidence_intervals[0.90][1]:.2f}%]")
print(f"  95%: [{recommended_consensus.distribution.confidence_intervals[0.95][0]:.2f}%, "
      f"{recommended_consensus.distribution.confidence_intervals[0.95][1]:.2f}%]")
print("="*80)

print(f"\nConsensus Strength: {consensus_analysis['consensus_strength'].upper()}")
print(f"Agreement Score: {consensus_analysis['agreement_score']:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Save Consensus Forecast and Expert Contributions

# COMMAND ----------

# Prepare consensus forecast for saving
consensus_record = {
    'forecast_id': recommended_consensus.forecast_id,
    'indicator_name': recommended_consensus.indicator_name,
    'forecast_date': recommended_consensus.forecast_date.isoformat(),
    'target_date': recommended_consensus.target_date.isoformat(),
    'horizon': recommended_consensus.horizon.value,
    'point_estimate': recommended_consensus.point_estimate,
    'mean': recommended_consensus.distribution.mean,
    'median': recommended_consensus.distribution.median,
    'std': recommended_consensus.distribution.std,
    'ci_90_lower': recommended_consensus.distribution.confidence_intervals[0.90][0],
    'ci_90_upper': recommended_consensus.distribution.confidence_intervals[0.90][1],
    'methodology': recommended_consensus.methodology,
    'n_experts': len(expert_forecasts),
    'consensus_strength': consensus_analysis['consensus_strength'],
    'agreement_score': consensus_analysis['agreement_score'],
    'created_at': datetime.now().isoformat()
}

# Prepare expert contributions
expert_contribution_records = []
for ef in expert_forecasts:
    expert_contribution_records.append({
        'consensus_forecast_id': recommended_consensus.forecast_id,
        'expert_id': ef.expert.expert_id,
        'expert_name': ef.expert.name,
        'expertise_level': ef.expert.expertise_level.value,
        'point_estimate': ef.forecast.point_estimate,
        'confidence': ef.confidence.value,
        'weight': ef.weight,
        'submitted_at': ef.submitted_at.isoformat()
    })

# Convert to DataFrames
consensus_df = pd.DataFrame([consensus_record])
contributions_df = pd.DataFrame(expert_contribution_records)

# Save to Delta Lake
consensus_table_path = "/mnt/datalake/forecasting/consensus_forecasts"
contributions_table_path = "/mnt/datalake/forecasting/expert_contributions"

consensus_df.write.format("delta").mode("append").save(consensus_table_path)
contributions_df.write.format("delta").mode("append").save(contributions_table_path)

print(f"✓ Saved consensus forecast to {consensus_table_path}")
print(f"✓ Saved {len(expert_contribution_records)} expert contributions to {contributions_table_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Summary and Next Steps

# COMMAND ----------

summary = {
    'indicator': indicator_name,
    'n_experts': len(expert_forecasts),
    'consensus_forecast': f"{recommended_consensus.point_estimate:.2f}%",
    'consensus_90_ci': f"[{recommended_consensus.distribution.confidence_intervals[0.90][0]:.2f}%, "
                      f"{recommended_consensus.distribution.confidence_intervals[0.90][1]:.2f}%]",
    'consensus_strength': consensus_analysis['consensus_strength'],
    'expert_range': f"{min([ef.forecast.point_estimate for ef in expert_forecasts]):.2f}% to "
                   f"{max([ef.forecast.point_estimate for ef in expert_forecasts]):.2f}%",
    'methodology': 'Weighted Average of Expert Forecasts'
}

print("\n" + "="*80)
print("EXPERT CALIBRATION SUMMARY")
print("="*80)
for key, value in summary.items():
    print(f"{key}: {value}")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. **Evaluation**: Once actuals are available, run `forecasting_04_evaluation` to assess expert performance
# MAGIC 2. **Update Weights**: Adjust expert weights based on historical accuracy
# MAGIC 3. **Dashboard**: View `forecasting_05_dashboard` for interactive analysis
# MAGIC 4. **Iterate**: Collect more expert forecasts for other indicators
# MAGIC
# MAGIC ---
# MAGIC **Status**: ✓ Consensus Forecast Generated with Expert Input
