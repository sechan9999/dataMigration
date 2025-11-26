"""
Expert Calibration and Consensus Building

Provides mechanisms for:
- Expert forecast elicitation and aggregation
- Forecast calibration and adjustment
- Consensus building across multiple experts
- Performance tracking and expert weighting
- Forecast combination methodologies
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from enum import Enum
import numpy as np
from scipy import stats

from .models import ProbabilisticForecast, ForecastDistribution, DistributionType
from .reasoning import ForecastRationale, ConfidenceLevel


class ExpertiseLevel(Enum):
    """Level of domain expertise"""
    NOVICE = "novice"
    INTERMEDIATE = "intermediate"
    EXPERT = "expert"
    MASTER = "master"


class AggregationMethod(Enum):
    """Methods for aggregating expert forecasts"""
    SIMPLE_AVERAGE = "simple_average"
    WEIGHTED_AVERAGE = "weighted_average"
    MEDIAN = "median"
    TRIMMED_MEAN = "trimmed_mean"
    BAYESIAN_AGGREGATION = "bayesian_aggregation"
    PERFORMANCE_WEIGHTED = "performance_weighted"


@dataclass
class ExpertProfile:
    """
    Profile of a forecast expert

    Attributes:
        expert_id: Unique identifier
        name: Expert name
        expertise_level: Level of domain expertise
        specialization: Area of specialization
        historical_accuracy: Track record metrics
        confidence_calibration: How well-calibrated their confidence is
    """
    expert_id: str
    name: str
    expertise_level: ExpertiseLevel
    specialization: str
    historical_accuracy: Optional[Dict[str, float]] = None
    confidence_calibration: Optional[float] = None
    forecast_count: int = 0
    last_active: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'expert_id': self.expert_id,
            'name': self.name,
            'expertise_level': self.expertise_level.value,
            'specialization': self.specialization,
            'historical_accuracy': self.historical_accuracy,
            'confidence_calibration': self.confidence_calibration,
            'forecast_count': self.forecast_count,
            'last_active': self.last_active.isoformat() if self.last_active else None
        }


@dataclass
class ExpertForecast:
    """
    A forecast provided by an individual expert

    Attributes:
        expert: Expert profile
        forecast: Probabilistic forecast
        rationale: Reasoning behind forecast
        confidence: Expert's confidence in this forecast
        submitted_at: Submission timestamp
    """
    expert: ExpertProfile
    forecast: ProbabilisticForecast
    rationale: ForecastRationale
    confidence: ConfidenceLevel
    submitted_at: datetime
    weight: float = 1.0  # Weight for aggregation

    def to_dict(self) -> Dict[str, Any]:
        return {
            'expert': self.expert.to_dict(),
            'forecast': self.forecast.to_dict(),
            'confidence': self.confidence.value,
            'submitted_at': self.submitted_at.isoformat(),
            'weight': self.weight
        }


@dataclass
class CalibrationMetrics:
    """
    Metrics for evaluating forecast calibration

    Attributes:
        brier_score: Brier score (lower is better)
        log_score: Logarithmic scoring rule
        calibration_error: Expected calibration error
        sharpness: Average forecast precision
        coverage: Empirical coverage of confidence intervals
    """
    brier_score: Optional[float] = None
    log_score: Optional[float] = None
    calibration_error: Optional[float] = None
    sharpness: Optional[float] = None
    coverage: Optional[Dict[str, float]] = None  # {0.50: 0.48, 0.90: 0.88, ...}

    def to_dict(self) -> Dict[str, Any]:
        return {
            'brier_score': self.brier_score,
            'log_score': self.log_score,
            'calibration_error': self.calibration_error,
            'sharpness': self.sharpness,
            'coverage': self.coverage
        }


class ExpertCalibrator:
    """
    Calibrates expert forecasts and tracks performance

    Provides:
    - Calibration of probability forecasts
    - Performance tracking
    - Weight adjustment based on track record
    """

    def __init__(self):
        self.expert_profiles: Dict[str, ExpertProfile] = {}
        self.forecast_history: List[ExpertForecast] = []
        self.actual_outcomes: Dict[str, float] = {}  # forecast_id -> actual_value

    def register_expert(self, expert: ExpertProfile):
        """Register an expert in the system"""
        self.expert_profiles[expert.expert_id] = expert

    def submit_forecast(self,
                       expert_id: str,
                       forecast: ProbabilisticForecast,
                       rationale: ForecastRationale,
                       confidence: ConfidenceLevel) -> ExpertForecast:
        """
        Submit an expert forecast

        Args:
            expert_id: ID of submitting expert
            forecast: Probabilistic forecast
            rationale: Reasoning behind forecast
            confidence: Expert's confidence level

        Returns:
            ExpertForecast object
        """
        if expert_id not in self.expert_profiles:
            raise ValueError(f"Unknown expert: {expert_id}")

        expert = self.expert_profiles[expert_id]

        # Calculate weight based on expertise and track record
        weight = self._calculate_expert_weight(expert)

        expert_forecast = ExpertForecast(
            expert=expert,
            forecast=forecast,
            rationale=rationale,
            confidence=confidence,
            submitted_at=datetime.now(),
            weight=weight
        )

        # Update expert profile
        expert.forecast_count += 1
        expert.last_active = datetime.now()

        self.forecast_history.append(expert_forecast)

        return expert_forecast

    def _calculate_expert_weight(self, expert: ExpertProfile) -> float:
        """
        Calculate weight for expert based on track record

        Args:
            expert: Expert profile

        Returns:
            Weight (higher = more influence)
        """
        base_weight = {
            ExpertiseLevel.NOVICE: 0.5,
            ExpertiseLevel.INTERMEDIATE: 1.0,
            ExpertiseLevel.EXPERT: 1.5,
            ExpertiseLevel.MASTER: 2.0
        }.get(expert.expertise_level, 1.0)

        # Adjust based on historical accuracy
        if expert.historical_accuracy:
            accuracy_multiplier = expert.historical_accuracy.get('mean_absolute_error', 1.0)
            # Lower error = higher weight
            accuracy_multiplier = 1.0 / max(accuracy_multiplier, 0.1)
            base_weight *= accuracy_multiplier

        # Adjust based on confidence calibration
        if expert.confidence_calibration:
            # Penalize over/under-confident experts
            calibration_penalty = 1.0 - abs(1.0 - expert.confidence_calibration)
            base_weight *= calibration_penalty

        return base_weight

    def record_actual_outcome(self, forecast_id: str, actual_value: float):
        """
        Record actual outcome for a forecast

        Args:
            forecast_id: Forecast identifier
            actual_value: Realized value
        """
        self.actual_outcomes[forecast_id] = actual_value

        # Update expert performance metrics
        self._update_expert_performance()

    def _update_expert_performance(self):
        """Update performance metrics for all experts"""
        for expert_id, expert in self.expert_profiles.items():
            # Collect forecasts and outcomes for this expert
            forecasts = [
                ef for ef in self.forecast_history
                if ef.expert.expert_id == expert_id
            ]

            errors = []
            for ef in forecasts:
                if ef.forecast.forecast_id in self.actual_outcomes:
                    actual = self.actual_outcomes[ef.forecast.forecast_id]
                    predicted = ef.forecast.point_estimate
                    errors.append(abs(actual - predicted))

            if errors:
                expert.historical_accuracy = {
                    'mean_absolute_error': float(np.mean(errors)),
                    'median_absolute_error': float(np.median(errors)),
                    'rmse': float(np.sqrt(np.mean([e**2 for e in errors]))),
                    'n_forecasts': len(errors)
                }

    def evaluate_calibration(self,
                            expert_id: str,
                            forecasts: Optional[List[ExpertForecast]] = None) -> CalibrationMetrics:
        """
        Evaluate calibration of expert's probabilistic forecasts

        Args:
            expert_id: Expert to evaluate
            forecasts: Optional specific forecasts to evaluate

        Returns:
            CalibrationMetrics
        """
        if forecasts is None:
            forecasts = [
                ef for ef in self.forecast_history
                if ef.expert.expert_id == expert_id
            ]

        # Filter to forecasts with known outcomes
        evaluated_forecasts = [
            (ef, self.actual_outcomes[ef.forecast.forecast_id])
            for ef in forecasts
            if ef.forecast.forecast_id in self.actual_outcomes
        ]

        if not evaluated_forecasts:
            return CalibrationMetrics()

        # Calculate metrics
        metrics = CalibrationMetrics()

        # Coverage: check if actuals fall within predicted intervals
        coverage = {}
        for conf_level in [0.50, 0.80, 0.90, 0.95]:
            if conf_level in evaluated_forecasts[0][0].forecast.distribution.confidence_intervals:
                within_interval = 0
                for ef, actual in evaluated_forecasts:
                    lower, upper = ef.forecast.distribution.confidence_intervals[conf_level]
                    if lower <= actual <= upper:
                        within_interval += 1

                coverage[conf_level] = within_interval / len(evaluated_forecasts)

        metrics.coverage = coverage

        # Sharpness: average width of prediction intervals
        if 0.90 in evaluated_forecasts[0][0].forecast.distribution.confidence_intervals:
            widths = []
            for ef, _ in evaluated_forecasts:
                lower, upper = ef.forecast.distribution.confidence_intervals[0.90]
                widths.append(upper - lower)
            metrics.sharpness = float(np.mean(widths))

        return metrics


class ConsensusBuilder:
    """
    Builds consensus forecasts from multiple expert inputs

    Implements various aggregation methods:
    - Simple averaging
    - Weighted averaging
    - Bayesian aggregation
    - Performance-weighted combinations
    """

    def __init__(self, calibrator: ExpertCalibrator):
        self.calibrator = calibrator

    def build_consensus(self,
                       expert_forecasts: List[ExpertForecast],
                       method: AggregationMethod = AggregationMethod.WEIGHTED_AVERAGE,
                       indicator_name: str = "Unknown") -> ProbabilisticForecast:
        """
        Build consensus forecast from expert inputs

        Args:
            expert_forecasts: List of expert forecasts
            method: Aggregation method
            indicator_name: Name of indicator

        Returns:
            Consensus probabilistic forecast
        """
        if not expert_forecasts:
            raise ValueError("No expert forecasts provided")

        if method == AggregationMethod.SIMPLE_AVERAGE:
            return self._simple_average(expert_forecasts, indicator_name)

        elif method == AggregationMethod.WEIGHTED_AVERAGE:
            return self._weighted_average(expert_forecasts, indicator_name)

        elif method == AggregationMethod.MEDIAN:
            return self._median_forecast(expert_forecasts, indicator_name)

        elif method == AggregationMethod.TRIMMED_MEAN:
            return self._trimmed_mean(expert_forecasts, indicator_name, trim_pct=0.1)

        elif method == AggregationMethod.PERFORMANCE_WEIGHTED:
            return self._performance_weighted(expert_forecasts, indicator_name)

        else:
            raise ValueError(f"Unknown aggregation method: {method}")

    def _simple_average(self,
                       forecasts: List[ExpertForecast],
                       indicator_name: str) -> ProbabilisticForecast:
        """Simple average of point estimates"""
        point_estimates = [f.forecast.point_estimate for f in forecasts]
        consensus_estimate = float(np.mean(point_estimates))

        # Average uncertainty
        stds = [f.forecast.distribution.std for f in forecasts]
        consensus_std = float(np.sqrt(np.mean([s**2 for s in stds])))

        # Generate distribution
        distribution = self._generate_consensus_distribution(
            consensus_estimate,
            consensus_std
        )

        return ProbabilisticForecast(
            indicator_name=indicator_name,
            forecast_date=datetime.now(),
            target_date=forecasts[0].forecast.target_date,
            horizon=forecasts[0].forecast.horizon,
            distribution=distribution,
            point_estimate=consensus_estimate,
            methodology=f"Simple average of {len(forecasts)} expert forecasts",
            model_version="consensus_v1.0",
            metadata={
                'n_experts': len(forecasts),
                'expert_estimates': point_estimates,
                'aggregation_method': 'simple_average'
            }
        )

    def _weighted_average(self,
                         forecasts: List[ExpertForecast],
                         indicator_name: str) -> ProbabilisticForecast:
        """Weighted average based on expert weights"""
        weights = np.array([f.weight for f in forecasts])
        weights = weights / weights.sum()  # Normalize

        # Weighted point estimate
        point_estimates = np.array([f.forecast.point_estimate for f in forecasts])
        consensus_estimate = float(np.sum(weights * point_estimates))

        # Weighted uncertainty
        stds = np.array([f.forecast.distribution.std for f in forecasts])
        consensus_std = float(np.sqrt(np.sum(weights * stds**2)))

        distribution = self._generate_consensus_distribution(
            consensus_estimate,
            consensus_std
        )

        return ProbabilisticForecast(
            indicator_name=indicator_name,
            forecast_date=datetime.now(),
            target_date=forecasts[0].forecast.target_date,
            horizon=forecasts[0].forecast.horizon,
            distribution=distribution,
            point_estimate=consensus_estimate,
            methodology=f"Weighted average of {len(forecasts)} expert forecasts",
            model_version="consensus_v1.0",
            metadata={
                'n_experts': len(forecasts),
                'expert_weights': weights.tolist(),
                'expert_estimates': point_estimates.tolist(),
                'aggregation_method': 'weighted_average'
            }
        )

    def _median_forecast(self,
                        forecasts: List[ExpertForecast],
                        indicator_name: str) -> ProbabilisticForecast:
        """Median of expert forecasts (robust to outliers)"""
        point_estimates = np.array([f.forecast.point_estimate for f in forecasts])
        consensus_estimate = float(np.median(point_estimates))

        # Use MAD (Median Absolute Deviation) for uncertainty
        mad = float(np.median(np.abs(point_estimates - consensus_estimate)))
        consensus_std = mad * 1.4826  # Scale factor for normal distribution

        distribution = self._generate_consensus_distribution(
            consensus_estimate,
            consensus_std
        )

        return ProbabilisticForecast(
            indicator_name=indicator_name,
            forecast_date=datetime.now(),
            target_date=forecasts[0].forecast.target_date,
            horizon=forecasts[0].forecast.horizon,
            distribution=distribution,
            point_estimate=consensus_estimate,
            methodology=f"Median of {len(forecasts)} expert forecasts",
            model_version="consensus_v1.0",
            metadata={
                'n_experts': len(forecasts),
                'expert_estimates': point_estimates.tolist(),
                'aggregation_method': 'median'
            }
        )

    def _trimmed_mean(self,
                     forecasts: List[ExpertForecast],
                     indicator_name: str,
                     trim_pct: float = 0.1) -> ProbabilisticForecast:
        """Trimmed mean (removes extreme outliers)"""
        point_estimates = np.array([f.forecast.point_estimate for f in forecasts])

        # Calculate trimmed mean
        consensus_estimate = float(stats.trim_mean(point_estimates, trim_pct))

        # Uncertainty from trimmed standard deviation
        trimmed_std = float(np.std(point_estimates[
            int(len(point_estimates) * trim_pct):
            int(len(point_estimates) * (1 - trim_pct))
        ]))

        distribution = self._generate_consensus_distribution(
            consensus_estimate,
            trimmed_std
        )

        return ProbabilisticForecast(
            indicator_name=indicator_name,
            forecast_date=datetime.now(),
            target_date=forecasts[0].forecast.target_date,
            horizon=forecasts[0].forecast.horizon,
            distribution=distribution,
            point_estimate=consensus_estimate,
            methodology=f"Trimmed mean ({trim_pct*100}%) of {len(forecasts)} expert forecasts",
            model_version="consensus_v1.0",
            metadata={
                'n_experts': len(forecasts),
                'trim_percentage': trim_pct,
                'aggregation_method': 'trimmed_mean'
            }
        )

    def _performance_weighted(self,
                             forecasts: List[ExpertForecast],
                             indicator_name: str) -> ProbabilisticForecast:
        """Weight by historical performance"""
        # Use expert weights based on performance
        weights = np.array([f.expert.historical_accuracy.get('mean_absolute_error', 1.0)
                           if f.expert.historical_accuracy else 1.0
                           for f in forecasts])

        # Invert (lower error = higher weight) and normalize
        weights = 1.0 / (weights + 0.1)  # Add small constant to avoid division by zero
        weights = weights / weights.sum()

        point_estimates = np.array([f.forecast.point_estimate for f in forecasts])
        consensus_estimate = float(np.sum(weights * point_estimates))

        stds = np.array([f.forecast.distribution.std for f in forecasts])
        consensus_std = float(np.sqrt(np.sum(weights * stds**2)))

        distribution = self._generate_consensus_distribution(
            consensus_estimate,
            consensus_std
        )

        return ProbabilisticForecast(
            indicator_name=indicator_name,
            forecast_date=datetime.now(),
            target_date=forecasts[0].forecast.target_date,
            horizon=forecasts[0].forecast.horizon,
            distribution=distribution,
            point_estimate=consensus_estimate,
            methodology=f"Performance-weighted average of {len(forecasts)} expert forecasts",
            model_version="consensus_v1.0",
            metadata={
                'n_experts': len(forecasts),
                'performance_weights': weights.tolist(),
                'aggregation_method': 'performance_weighted'
            }
        )

    def _generate_consensus_distribution(self,
                                        mean: float,
                                        std: float) -> ForecastDistribution:
        """Generate distribution from consensus statistics"""
        samples = np.random.normal(mean, std, 10000)

        quantiles = {
            0.05: float(np.percentile(samples, 5)),
            0.25: float(np.percentile(samples, 25)),
            0.50: float(np.percentile(samples, 50)),
            0.75: float(np.percentile(samples, 75)),
            0.95: float(np.percentile(samples, 95))
        }

        confidence_intervals = {
            0.90: (float(np.percentile(samples, 5)), float(np.percentile(samples, 95))),
            0.95: (float(np.percentile(samples, 2.5)), float(np.percentile(samples, 97.5)))
        }

        return ForecastDistribution(
            mean=mean,
            median=float(np.median(samples)),
            std=std,
            quantiles=quantiles,
            confidence_intervals=confidence_intervals,
            distribution_type=DistributionType.NORMAL,
            samples=samples
        )

    def analyze_consensus_strength(self, expert_forecasts: List[ExpertForecast]) -> Dict[str, Any]:
        """
        Analyze strength and agreement of consensus

        Args:
            expert_forecasts: List of expert forecasts

        Returns:
            Dictionary with consensus analysis
        """
        point_estimates = [f.forecast.point_estimate for f in expert_forecasts]

        # Dispersion metrics
        std_dev = float(np.std(point_estimates))
        cv = std_dev / abs(np.mean(point_estimates)) if np.mean(point_estimates) != 0 else 0
        range_span = float(max(point_estimates) - min(point_estimates))

        # Agreement strength (inverse of dispersion)
        agreement_score = 1.0 / (1.0 + cv)

        return {
            'n_experts': len(expert_forecasts),
            'mean_estimate': float(np.mean(point_estimates)),
            'median_estimate': float(np.median(point_estimates)),
            'std_dev': std_dev,
            'coefficient_of_variation': cv,
            'range': range_span,
            'agreement_score': agreement_score,  # 0-1, higher = more agreement
            'consensus_strength': 'strong' if agreement_score > 0.8 else
                                'moderate' if agreement_score > 0.6 else 'weak'
        }
