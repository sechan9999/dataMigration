"""
Forecast Reasoning and Rationale Documentation

Provides structured framework for documenting forecast logic:
- Clear reasoning chains for forecast decisions
- Evidence-based rationale with data sources
- Assumption tracking and sensitivity analysis
- Scenario analysis documentation
- Audit trail for forecast revisions
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from enum import Enum
import json

from .models import ProbabilisticForecast, ForecastHorizon
from .indicators import IndicatorData


class RationaleType(Enum):
    """Types of reasoning for forecast components"""
    DATA_DRIVEN = "data_driven"           # Based on historical data patterns
    THEORY_BASED = "theory_based"         # Economic/financial theory
    EXPERT_JUDGMENT = "expert_judgment"   # Expert opinion/judgment
    MODEL_OUTPUT = "model_output"         # Statistical/ML model result
    SCENARIO_ANALYSIS = "scenario_analysis"  # Based on scenario assumptions
    CONSENSUS = "consensus"               # Aggregated expert views
    ADJUSTMENT = "adjustment"             # Judgmental adjustment to model


class ConfidenceLevel(Enum):
    """Confidence in specific reasoning component"""
    VERY_HIGH = "very_high"  # 90%+ confidence
    HIGH = "high"            # 70-90%
    MEDIUM = "medium"        # 50-70%
    LOW = "low"              # 30-50%
    VERY_LOW = "very_low"    # <30%


@dataclass
class Assumption:
    """
    A documented assumption underlying a forecast

    Attributes:
        description: Clear statement of assumption
        rationale: Why this assumption is reasonable
        confidence: Confidence level in assumption
        sensitivity: How sensitive forecast is to this assumption
        alternative_scenarios: What if assumption doesn't hold
    """
    description: str
    rationale: str
    confidence: ConfidenceLevel
    sensitivity: str  # "high", "medium", "low"
    alternative_scenarios: Optional[List[str]] = None
    quantitative_impact: Optional[Dict[str, float]] = None  # e.g., {"optimistic": +2.0, "pessimistic": -1.5}

    def to_dict(self) -> Dict[str, Any]:
        return {
            'description': self.description,
            'rationale': self.rationale,
            'confidence': self.confidence.value,
            'sensitivity': self.sensitivity,
            'alternative_scenarios': self.alternative_scenarios,
            'quantitative_impact': self.quantitative_impact
        }


@dataclass
class EvidenceSource:
    """
    A piece of evidence supporting the forecast

    Attributes:
        source_type: Type of evidence
        description: Description of evidence
        data_reference: Reference to data used
        weight: Importance weight (0-1)
        reliability: Reliability assessment
    """
    source_type: RationaleType
    description: str
    data_reference: Optional[str] = None
    weight: float = 1.0
    reliability: ConfidenceLevel = ConfidenceLevel.MEDIUM
    quantitative_value: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'source_type': self.source_type.value,
            'description': self.description,
            'data_reference': self.data_reference,
            'weight': self.weight,
            'reliability': self.reliability.value,
            'quantitative_value': self.quantitative_value
        }


@dataclass
class ReasoningChain:
    """
    A logical chain of reasoning leading to forecast conclusion

    Example:
    1. Historical GDP growth averaged 2.5% (DATA_DRIVEN)
    2. Current PMI indicates expansion (DATA_DRIVEN)
    3. Fed signaled rate cuts (THEORY_BASED: monetary easing supports growth)
    4. Therefore, expect GDP growth of 2.8% ± 0.5% (MODEL_OUTPUT)
    """
    steps: List[EvidenceSource]
    conclusion: str
    confidence: ConfidenceLevel

    def to_dict(self) -> Dict[str, Any]:
        return {
            'steps': [step.to_dict() for step in self.steps],
            'conclusion': self.conclusion,
            'confidence': self.confidence.value
        }

    def add_step(self, evidence: EvidenceSource) -> 'ReasoningChain':
        """Add reasoning step to chain"""
        self.steps.append(evidence)
        return self


@dataclass
class ScenarioAnalysis:
    """
    Analysis of different possible scenarios

    Attributes:
        base_case: Most likely scenario and probability
        optimistic_case: Upside scenario and probability
        pessimistic_case: Downside scenario and probability
        scenario_descriptions: Detailed descriptions
    """
    base_case: Tuple[str, float, float]  # (description, forecast_value, probability)
    optimistic_case: Tuple[str, float, float]
    pessimistic_case: Tuple[str, float, float]
    scenario_descriptions: Dict[str, str]
    key_risks: List[str] = field(default_factory=list)
    upside_drivers: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'base_case': {
                'description': self.base_case[0],
                'value': self.base_case[1],
                'probability': self.base_case[2]
            },
            'optimistic_case': {
                'description': self.optimistic_case[0],
                'value': self.optimistic_case[1],
                'probability': self.optimistic_case[2]
            },
            'pessimistic_case': {
                'description': self.pessimistic_case[0],
                'value': self.pessimistic_case[1],
                'probability': self.pessimistic_case[2]
            },
            'scenario_descriptions': self.scenario_descriptions,
            'key_risks': self.key_risks,
            'upside_drivers': self.upside_drivers
        }


@dataclass
class ForecastRationale:
    """
    Complete rationale documentation for a forecast

    Provides structured reasoning including:
    - Key assumptions
    - Evidence sources
    - Reasoning chain
    - Scenario analysis
    - Sensitivity analysis
    - Comparison to consensus
    """
    forecast_id: str
    indicator_name: str
    created_at: datetime
    created_by: str

    # Core reasoning components
    executive_summary: str
    key_assumptions: List[Assumption]
    evidence_sources: List[EvidenceSource]
    reasoning_chain: ReasoningChain
    scenario_analysis: Optional[ScenarioAnalysis] = None

    # Additional context
    data_sources_used: List[str] = field(default_factory=list)
    models_used: List[str] = field(default_factory=list)
    expert_inputs: List[str] = field(default_factory=list)

    # Comparisons and context
    comparison_to_consensus: Optional[str] = None
    comparison_to_history: Optional[str] = None
    key_risks: List[str] = field(default_factory=list)

    # Metadata
    confidence_assessment: ConfidenceLevel = ConfidenceLevel.MEDIUM
    uncertainty_drivers: List[str] = field(default_factory=list)
    revision_notes: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'forecast_id': self.forecast_id,
            'indicator_name': self.indicator_name,
            'created_at': self.created_at.isoformat(),
            'created_by': self.created_by,
            'executive_summary': self.executive_summary,
            'key_assumptions': [a.to_dict() for a in self.key_assumptions],
            'evidence_sources': [e.to_dict() for e in self.evidence_sources],
            'reasoning_chain': self.reasoning_chain.to_dict(),
            'scenario_analysis': self.scenario_analysis.to_dict() if self.scenario_analysis else None,
            'data_sources_used': self.data_sources_used,
            'models_used': self.models_used,
            'expert_inputs': self.expert_inputs,
            'comparison_to_consensus': self.comparison_to_consensus,
            'comparison_to_history': self.comparison_to_history,
            'key_risks': self.key_risks,
            'confidence_assessment': self.confidence_assessment.value,
            'uncertainty_drivers': self.uncertainty_drivers,
            'revision_notes': self.revision_notes
        }

    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), indent=2)

    def add_revision(self, reason: str, changes: str, revised_by: str):
        """Document a revision to the forecast rationale"""
        self.revision_notes.append({
            'timestamp': datetime.now().isoformat(),
            'reason': reason,
            'changes': changes,
            'revised_by': revised_by
        })

    def generate_summary_report(self) -> str:
        """Generate human-readable summary report"""
        report = f"""
=== Forecast Rationale Report ===

Indicator: {self.indicator_name}
Forecast ID: {self.forecast_id}
Created: {self.created_at.strftime('%Y-%m-%d')} by {self.created_by}
Overall Confidence: {self.confidence_assessment.value.upper()}

EXECUTIVE SUMMARY
{self.executive_summary}

KEY ASSUMPTIONS ({len(self.key_assumptions)})
"""
        for i, assumption in enumerate(self.key_assumptions, 1):
            report += f"\n{i}. {assumption.description}"
            report += f"\n   Confidence: {assumption.confidence.value} | Sensitivity: {assumption.sensitivity}"

        report += f"\n\nREASONING CHAIN ({len(self.reasoning_chain.steps)} steps)"
        for i, step in enumerate(self.reasoning_chain.steps, 1):
            report += f"\n{i}. [{step.source_type.value}] {step.description}"
            if step.data_reference:
                report += f"\n   Data: {step.data_reference}"

        report += f"\n\nCONCLUSION: {self.reasoning_chain.conclusion}"

        if self.scenario_analysis:
            report += "\n\nSCENARIO ANALYSIS"
            report += f"\n• Base Case ({self.scenario_analysis.base_case[2]*100:.0f}%): "
            report += f"{self.scenario_analysis.base_case[1]:.2f} - {self.scenario_analysis.base_case[0]}"
            report += f"\n• Optimistic ({self.scenario_analysis.optimistic_case[2]*100:.0f}%): "
            report += f"{self.scenario_analysis.optimistic_case[1]:.2f} - {self.scenario_analysis.optimistic_case[0]}"
            report += f"\n• Pessimistic ({self.scenario_analysis.pessimistic_case[2]*100:.0f}%): "
            report += f"{self.scenario_analysis.pessimistic_case[1]:.2f} - {self.scenario_analysis.pessimistic_case[0]}"

        if self.key_risks:
            report += f"\n\nKEY RISKS ({len(self.key_risks)})"
            for risk in self.key_risks:
                report += f"\n• {risk}"

        if self.comparison_to_consensus:
            report += f"\n\nCOMPARISON TO CONSENSUS\n{self.comparison_to_consensus}"

        report += f"\n\n{'='*50}\n"

        return report


@dataclass
class DocumentedForecast:
    """
    A forecast with complete documentation of reasoning

    Combines:
    - The probabilistic forecast itself
    - Complete rationale documentation
    - Metadata for tracking and auditing
    """
    forecast: ProbabilisticForecast
    rationale: ForecastRationale
    version: int = 1
    status: str = "active"  # active, superseded, archived

    def to_dict(self) -> Dict[str, Any]:
        return {
            'forecast': self.forecast.to_dict(),
            'rationale': self.rationale.to_dict(),
            'version': self.version,
            'status': self.status
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


class ReasoningEngine:
    """
    Engine for constructing and validating forecast reasoning

    Provides tools for:
    - Building structured reasoning chains
    - Validating logical consistency
    - Generating documentation
    - Comparing reasoning across forecasts
    """

    def __init__(self):
        self.reasoning_templates: Dict[str, Any] = {}

    def create_rationale(self,
                        forecast: ProbabilisticForecast,
                        creator: str = "System") -> ForecastRationale:
        """
        Create initial rationale structure for a forecast

        Args:
            forecast: The probabilistic forecast
            creator: Name of person/system creating rationale
        """
        return ForecastRationale(
            forecast_id=forecast.forecast_id,
            indicator_name=forecast.indicator_name,
            created_at=datetime.now(),
            created_by=creator,
            executive_summary="",
            key_assumptions=[],
            evidence_sources=[],
            reasoning_chain=ReasoningChain(steps=[], conclusion="", confidence=ConfidenceLevel.MEDIUM)
        )

    def build_data_driven_reasoning(self,
                                   historical_data: IndicatorData,
                                   forecast: ProbabilisticForecast) -> ReasoningChain:
        """
        Build reasoning chain from historical data analysis

        Args:
            historical_data: Historical indicator data
            forecast: The forecast being made
        """
        reasoning = ReasoningChain(steps=[], conclusion="", confidence=ConfidenceLevel.HIGH)

        # Analyze historical average
        mean_value = float(historical_data.values.mean())
        reasoning.add_step(EvidenceSource(
            source_type=RationaleType.DATA_DRIVEN,
            description=f"Historical average: {mean_value:.2f}",
            data_reference=f"{historical_data.metadata.indicator_name} (n={len(historical_data.values)})",
            quantitative_value=mean_value,
            reliability=ConfidenceLevel.HIGH
        ))

        # Analyze trend
        if len(historical_data.values) > 1:
            recent_values = historical_data.values[-12:]  # Last 12 observations
            trend = float(recent_values.mean() - historical_data.values[:-12].mean())
            trend_direction = "upward" if trend > 0 else "downward"

            reasoning.add_step(EvidenceSource(
                source_type=RationaleType.DATA_DRIVEN,
                description=f"Recent {trend_direction} trend of {abs(trend):.2f}",
                data_reference="Last 12 observations vs. prior period",
                quantitative_value=trend,
                reliability=ConfidenceLevel.MEDIUM
            ))

        # Analyze volatility
        volatility = float(historical_data.values.std())
        reasoning.add_step(EvidenceSource(
            source_type=RationaleType.DATA_DRIVEN,
            description=f"Historical volatility: {volatility:.2f}",
            data_reference="Standard deviation of historical values",
            quantitative_value=volatility,
            reliability=ConfidenceLevel.HIGH
        ))

        # Form conclusion
        reasoning.conclusion = (
            f"Based on historical analysis, forecast {forecast.indicator_name} "
            f"at {forecast.point_estimate:.2f} with uncertainty of ±{forecast.distribution.std:.2f}"
        )

        return reasoning

    def create_scenario_analysis(self,
                                 base_forecast: float,
                                 indicator_name: str,
                                 optimistic_factor: float = 1.5,
                                 pessimistic_factor: float = 0.5) -> ScenarioAnalysis:
        """
        Create scenario analysis around base forecast

        Args:
            base_forecast: Base case forecast value
            indicator_name: Name of indicator
            optimistic_factor: Multiplier for optimistic case
            pessimistic_factor: Multiplier for pessimistic case
        """
        return ScenarioAnalysis(
            base_case=(
                "Most likely scenario based on current conditions",
                base_forecast,
                0.60  # 60% probability
            ),
            optimistic_case=(
                "Favorable conditions with positive surprises",
                base_forecast * optimistic_factor,
                0.20  # 20% probability
            ),
            pessimistic_case=(
                "Adverse conditions with negative shocks",
                base_forecast * pessimistic_factor,
                0.20  # 20% probability
            ),
            scenario_descriptions={
                'base': f"Current trends continue for {indicator_name}",
                'optimistic': f"Stronger than expected growth/improvement in {indicator_name}",
                'pessimistic': f"Weaker than expected performance in {indicator_name}"
            }
        )

    def validate_reasoning_consistency(self, rationale: ForecastRationale) -> Dict[str, Any]:
        """
        Validate logical consistency of forecast rationale

        Returns:
            Dictionary with validation results
        """
        validation = {
            'is_valid': True,
            'issues': [],
            'warnings': []
        }

        # Check completeness
        if not rationale.executive_summary:
            validation['warnings'].append("Missing executive summary")

        if len(rationale.key_assumptions) == 0:
            validation['warnings'].append("No key assumptions documented")

        if len(rationale.evidence_sources) == 0:
            validation['issues'].append("No evidence sources provided")
            validation['is_valid'] = False

        if len(rationale.reasoning_chain.steps) == 0:
            validation['issues'].append("Empty reasoning chain")
            validation['is_valid'] = False

        # Check for high-sensitivity assumptions without alternatives
        for assumption in rationale.key_assumptions:
            if assumption.sensitivity == "high" and not assumption.alternative_scenarios:
                validation['warnings'].append(
                    f"High-sensitivity assumption without alternatives: {assumption.description}"
                )

        # Check confidence consistency
        low_confidence_sources = sum(
            1 for e in rationale.evidence_sources
            if e.reliability in [ConfidenceLevel.LOW, ConfidenceLevel.VERY_LOW]
        )

        if low_confidence_sources > len(rationale.evidence_sources) / 2:
            validation['warnings'].append(
                "More than 50% of evidence sources have low confidence"
            )

        return validation

    def compare_forecasts(self,
                         forecast1: DocumentedForecast,
                         forecast2: DocumentedForecast) -> Dict[str, Any]:
        """
        Compare two forecasts and their reasoning

        Args:
            forecast1: First documented forecast
            forecast2: Second documented forecast

        Returns:
            Comparison analysis
        """
        comparison = {
            'point_estimate_difference': (
                forecast1.forecast.point_estimate - forecast2.forecast.point_estimate
            ),
            'uncertainty_comparison': {
                'forecast1_std': forecast1.forecast.distribution.std,
                'forecast2_std': forecast2.forecast.distribution.std
            },
            'methodology_comparison': {
                'forecast1': forecast1.forecast.methodology,
                'forecast2': forecast2.forecast.methodology
            },
            'reasoning_overlap': self._analyze_reasoning_overlap(
                forecast1.rationale,
                forecast2.rationale
            )
        }

        return comparison

    def _analyze_reasoning_overlap(self,
                                   rationale1: ForecastRationale,
                                   rationale2: ForecastRationale) -> Dict[str, Any]:
        """Analyze overlap in reasoning between two forecasts"""
        # Compare evidence sources
        sources1 = set(e.description for e in rationale1.evidence_sources)
        sources2 = set(e.description for e in rationale2.evidence_sources)

        common_sources = sources1.intersection(sources2)
        unique_to_1 = sources1 - sources2
        unique_to_2 = sources2 - sources1

        return {
            'common_evidence_sources': list(common_sources),
            'unique_to_forecast1': list(unique_to_1),
            'unique_to_forecast2': list(unique_to_2),
            'overlap_percentage': (
                len(common_sources) / max(len(sources1), len(sources2)) * 100
                if max(len(sources1), len(sources2)) > 0 else 0
            )
        }

    def generate_documentation_report(self, documented_forecast: DocumentedForecast) -> str:
        """
        Generate comprehensive documentation report

        Args:
            documented_forecast: Forecast with rationale

        Returns:
            Formatted report string
        """
        forecast = documented_forecast.forecast
        rationale = documented_forecast.rationale

        report = f"""
{'='*80}
PROBABILISTIC FORECAST DOCUMENTATION
{'='*80}

FORECAST SUMMARY
---------------
Indicator:        {forecast.indicator_name}
Forecast Date:    {forecast.forecast_date.strftime('%Y-%m-%d')}
Target Date:      {forecast.target_date.strftime('%Y-%m-%d')}
Horizon:          {forecast.horizon.value}
Point Estimate:   {forecast.point_estimate:.4f}

PROBABILITY DISTRIBUTION
-----------------------
Mean:             {forecast.distribution.mean:.4f}
Median:           {forecast.distribution.median:.4f}
Std Deviation:    {forecast.distribution.std:.4f}

Confidence Intervals:
  50%: [{forecast.distribution.confidence_intervals[0.50][0]:.4f}, {forecast.distribution.confidence_intervals[0.50][1]:.4f}]
  90%: [{forecast.distribution.confidence_intervals[0.90][0]:.4f}, {forecast.distribution.confidence_intervals[0.90][1]:.4f}]
  95%: [{forecast.distribution.confidence_intervals[0.95][0]:.4f}, {forecast.distribution.confidence_intervals[0.95][1]:.4f}]

METHODOLOGY
-----------
{forecast.methodology}
Model Version: {forecast.model_version}

{rationale.generate_summary_report()}

VALIDATION STATUS: Version {documented_forecast.version} - {documented_forecast.status.upper()}

{'='*80}
"""
        return report
