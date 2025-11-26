"""
Core Probabilistic Forecasting Models

Implements various forecasting methodologies with probabilistic outputs:
- Monte Carlo simulation for uncertainty quantification
- Bayesian forecasting with prior incorporation
- Ensemble methods for robust predictions
- Quantile regression for distribution estimation
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from scipy import stats
import json


class ForecastHorizon(Enum):
    """Forecast time horizons"""
    SHORT_TERM = "1-3_months"
    MEDIUM_TERM = "3-12_months"
    LONG_TERM = "1-5_years"
    VERY_LONG_TERM = "5+_years"


class DistributionType(Enum):
    """Probability distribution types for forecasts"""
    NORMAL = "normal"
    LOGNORMAL = "lognormal"
    STUDENT_T = "student_t"
    MIXTURE = "mixture"
    EMPIRICAL = "empirical"


@dataclass
class ForecastDistribution:
    """
    Represents a probability distribution for a forecast

    Attributes:
        mean: Expected value
        median: Median prediction
        std: Standard deviation
        quantiles: Dictionary of quantile predictions (e.g., {0.05: value, 0.95: value})
        confidence_intervals: Confidence intervals at various levels
        distribution_type: Type of probability distribution
        samples: Optional Monte Carlo samples from the distribution
    """
    mean: float
    median: float
    std: float
    quantiles: Dict[float, float]  # e.g., {0.05: lower_bound, 0.50: median, 0.95: upper_bound}
    confidence_intervals: Dict[float, Tuple[float, float]]  # e.g., {0.90: (lower, upper)}
    distribution_type: DistributionType
    samples: Optional[np.ndarray] = None
    skewness: Optional[float] = None
    kurtosis: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'mean': float(self.mean),
            'median': float(self.median),
            'std': float(self.std),
            'quantiles': {float(k): float(v) for k, v in self.quantiles.items()},
            'confidence_intervals': {
                float(k): (float(v[0]), float(v[1]))
                for k, v in self.confidence_intervals.items()
            },
            'distribution_type': self.distribution_type.value,
            'skewness': float(self.skewness) if self.skewness else None,
            'kurtosis': float(self.kurtosis) if self.kurtosis else None
        }

    def probability_above(self, threshold: float) -> float:
        """Calculate probability that forecast exceeds threshold"""
        if self.samples is not None:
            return float(np.mean(self.samples > threshold))
        elif self.distribution_type == DistributionType.NORMAL:
            return float(1 - stats.norm.cdf(threshold, self.mean, self.std))
        else:
            # Approximate using quantiles
            for q, val in sorted(self.quantiles.items()):
                if val >= threshold:
                    return 1 - q
            return 0.0

    def probability_below(self, threshold: float) -> float:
        """Calculate probability that forecast is below threshold"""
        return 1 - self.probability_above(threshold)


@dataclass
class ProbabilisticForecast:
    """
    A complete probabilistic forecast for a specific indicator and time horizon

    Attributes:
        indicator_name: Name of the indicator (e.g., "GDP Growth", "Inflation")
        forecast_date: Date when forecast was made
        target_date: Date for which prediction is made
        horizon: Forecast time horizon
        distribution: Probability distribution of the forecast
        point_estimate: Single best estimate (typically mean or median)
        methodology: Description of forecasting methodology used
        model_version: Version identifier for the model
        metadata: Additional metadata
    """
    indicator_name: str
    forecast_date: datetime
    target_date: datetime
    horizon: ForecastHorizon
    distribution: ForecastDistribution
    point_estimate: float
    methodology: str
    model_version: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def forecast_id(self) -> str:
        """Unique identifier for this forecast"""
        return f"{self.indicator_name}_{self.forecast_date.strftime('%Y%m%d')}_{self.target_date.strftime('%Y%m%d')}"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'forecast_id': self.forecast_id,
            'indicator_name': self.indicator_name,
            'forecast_date': self.forecast_date.isoformat(),
            'target_date': self.target_date.isoformat(),
            'horizon': self.horizon.value,
            'distribution': self.distribution.to_dict(),
            'point_estimate': float(self.point_estimate),
            'methodology': self.methodology,
            'model_version': self.model_version,
            'metadata': self.metadata
        }


class ForecastModel:
    """
    Base class for probabilistic forecasting models
    """

    def __init__(self, model_name: str, model_version: str = "1.0"):
        self.model_name = model_name
        self.model_version = model_version
        self.is_fitted = False
        self.training_history: List[Dict[str, Any]] = []

    def fit(self,
            historical_data: np.ndarray,
            dates: List[datetime],
            **kwargs) -> 'ForecastModel':
        """
        Fit the model to historical data

        Args:
            historical_data: Historical values
            dates: Corresponding dates
            **kwargs: Additional model-specific parameters
        """
        raise NotImplementedError("Subclasses must implement fit()")

    def forecast(self,
                 horizon: ForecastHorizon,
                 target_date: datetime,
                 **kwargs) -> ProbabilisticForecast:
        """
        Generate a probabilistic forecast

        Args:
            horizon: Forecast time horizon
            target_date: Date for prediction
            **kwargs: Additional parameters
        """
        raise NotImplementedError("Subclasses must implement forecast()")

    def generate_distribution(self,
                             point_estimate: float,
                             uncertainty: float,
                             n_samples: int = 10000) -> ForecastDistribution:
        """
        Generate forecast distribution from point estimate and uncertainty

        Args:
            point_estimate: Central forecast value
            uncertainty: Measure of forecast uncertainty (e.g., std deviation)
            n_samples: Number of Monte Carlo samples
        """
        # Generate Monte Carlo samples
        samples = np.random.normal(point_estimate, uncertainty, n_samples)

        # Calculate quantiles
        quantiles = {
            0.01: float(np.percentile(samples, 1)),
            0.05: float(np.percentile(samples, 5)),
            0.10: float(np.percentile(samples, 10)),
            0.25: float(np.percentile(samples, 25)),
            0.50: float(np.percentile(samples, 50)),
            0.75: float(np.percentile(samples, 75)),
            0.90: float(np.percentile(samples, 90)),
            0.95: float(np.percentile(samples, 95)),
            0.99: float(np.percentile(samples, 99))
        }

        # Calculate confidence intervals
        confidence_intervals = {
            0.50: (float(np.percentile(samples, 25)), float(np.percentile(samples, 75))),
            0.80: (float(np.percentile(samples, 10)), float(np.percentile(samples, 90))),
            0.90: (float(np.percentile(samples, 5)), float(np.percentile(samples, 95))),
            0.95: (float(np.percentile(samples, 2.5)), float(np.percentile(samples, 97.5))),
            0.99: (float(np.percentile(samples, 0.5)), float(np.percentile(samples, 99.5)))
        }

        return ForecastDistribution(
            mean=float(np.mean(samples)),
            median=float(np.median(samples)),
            std=float(np.std(samples)),
            quantiles=quantiles,
            confidence_intervals=confidence_intervals,
            distribution_type=DistributionType.NORMAL,
            samples=samples,
            skewness=float(stats.skew(samples)),
            kurtosis=float(stats.kurtosis(samples))
        )


class BayesianForecast(ForecastModel):
    """
    Bayesian forecasting model that incorporates prior beliefs

    Uses Bayesian inference to combine:
    - Prior beliefs (from expert judgment or historical patterns)
    - Observed data likelihood
    - Posterior predictions with proper uncertainty quantification
    """

    def __init__(self,
                 model_name: str = "BayesianForecast",
                 prior_mean: Optional[float] = None,
                 prior_std: Optional[float] = None,
                 model_version: str = "1.0"):
        super().__init__(model_name, model_version)
        self.prior_mean = prior_mean
        self.prior_std = prior_std
        self.posterior_mean = None
        self.posterior_std = None
        self.data_mean = None
        self.data_std = None

    def fit(self,
            historical_data: np.ndarray,
            dates: List[datetime],
            prior_mean: Optional[float] = None,
            prior_std: Optional[float] = None,
            **kwargs) -> 'BayesianForecast':
        """
        Fit Bayesian model by combining prior with data likelihood
        """
        # Use provided priors or defaults
        if prior_mean is not None:
            self.prior_mean = prior_mean
        if prior_std is not None:
            self.prior_std = prior_std

        # Calculate data statistics
        self.data_mean = float(np.mean(historical_data))
        self.data_std = float(np.std(historical_data))
        n = len(historical_data)

        # If no prior specified, use non-informative prior
        if self.prior_mean is None:
            self.prior_mean = self.data_mean
            self.prior_std = self.data_std * 10  # Wide prior

        # Bayesian update: combine prior with likelihood
        # Posterior precision = prior precision + data precision
        prior_precision = 1 / (self.prior_std ** 2)
        data_precision = n / (self.data_std ** 2)
        posterior_precision = prior_precision + data_precision

        # Posterior mean is weighted average
        self.posterior_mean = (
            (prior_precision * self.prior_mean + data_precision * self.data_mean)
            / posterior_precision
        )
        self.posterior_std = np.sqrt(1 / posterior_precision)

        self.is_fitted = True

        # Record training history
        self.training_history.append({
            'fit_date': datetime.now().isoformat(),
            'n_observations': n,
            'data_mean': self.data_mean,
            'data_std': self.data_std,
            'prior_mean': self.prior_mean,
            'prior_std': self.prior_std,
            'posterior_mean': self.posterior_mean,
            'posterior_std': self.posterior_std
        })

        return self

    def forecast(self,
                 horizon: ForecastHorizon,
                 target_date: datetime,
                 indicator_name: str = "Unknown",
                 **kwargs) -> ProbabilisticForecast:
        """
        Generate Bayesian probabilistic forecast
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted before forecasting")

        # Adjust uncertainty based on horizon
        horizon_factors = {
            ForecastHorizon.SHORT_TERM: 1.0,
            ForecastHorizon.MEDIUM_TERM: 1.5,
            ForecastHorizon.LONG_TERM: 2.5,
            ForecastHorizon.VERY_LONG_TERM: 4.0
        }

        adjusted_std = self.posterior_std * horizon_factors.get(horizon, 2.0)

        # Generate distribution
        distribution = self.generate_distribution(
            point_estimate=self.posterior_mean,
            uncertainty=adjusted_std,
            n_samples=kwargs.get('n_samples', 10000)
        )

        return ProbabilisticForecast(
            indicator_name=indicator_name,
            forecast_date=datetime.now(),
            target_date=target_date,
            horizon=horizon,
            distribution=distribution,
            point_estimate=self.posterior_mean,
            methodology="Bayesian inference with conjugate prior",
            model_version=self.model_version,
            metadata={
                'prior_mean': self.prior_mean,
                'prior_std': self.prior_std,
                'posterior_mean': self.posterior_mean,
                'posterior_std': self.posterior_std,
                'horizon_adjustment': horizon_factors.get(horizon, 2.0)
            }
        )


class EnsembleForecast(ForecastModel):
    """
    Ensemble forecasting combining multiple models for robust predictions

    Combines forecasts from multiple models using:
    - Simple averaging
    - Weighted averaging based on historical performance
    - Bayesian model averaging
    """

    def __init__(self,
                 models: List[ForecastModel],
                 model_name: str = "EnsembleForecast",
                 weights: Optional[List[float]] = None,
                 model_version: str = "1.0"):
        super().__init__(model_name, model_version)
        self.models = models
        self.weights = weights if weights else [1.0 / len(models)] * len(models)

        # Normalize weights
        total_weight = sum(self.weights)
        self.weights = [w / total_weight for w in self.weights]

    def fit(self,
            historical_data: np.ndarray,
            dates: List[datetime],
            **kwargs) -> 'EnsembleForecast':
        """
        Fit all component models
        """
        for model in self.models:
            model.fit(historical_data, dates, **kwargs)

        self.is_fitted = True
        return self

    def forecast(self,
                 horizon: ForecastHorizon,
                 target_date: datetime,
                 indicator_name: str = "Unknown",
                 **kwargs) -> ProbabilisticForecast:
        """
        Generate ensemble forecast by combining component forecasts
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted before forecasting")

        # Get forecasts from all models
        component_forecasts = []
        for model in self.models:
            forecast = model.forecast(horizon, target_date, indicator_name, **kwargs)
            component_forecasts.append(forecast)

        # Combine point estimates
        ensemble_point = sum(
            w * f.point_estimate
            for w, f in zip(self.weights, component_forecasts)
        )

        # Combine distributions by mixing samples
        all_samples = []
        for weight, forecast in zip(self.weights, component_forecasts):
            if forecast.distribution.samples is not None:
                n_samples = int(weight * 10000)
                samples = np.random.choice(
                    forecast.distribution.samples,
                    size=n_samples,
                    replace=True
                )
                all_samples.append(samples)

        combined_samples = np.concatenate(all_samples) if all_samples else None

        # Generate ensemble distribution
        if combined_samples is not None:
            ensemble_mean = float(np.mean(combined_samples))
            ensemble_std = float(np.std(combined_samples))
        else:
            # Fallback: combine means and stds
            ensemble_mean = ensemble_point
            ensemble_std = float(np.sqrt(sum(
                w * (f.distribution.std ** 2 + (f.point_estimate - ensemble_point) ** 2)
                for w, f in zip(self.weights, component_forecasts)
            )))

        distribution = self.generate_distribution(
            point_estimate=ensemble_mean,
            uncertainty=ensemble_std,
            n_samples=10000
        )

        return ProbabilisticForecast(
            indicator_name=indicator_name,
            forecast_date=datetime.now(),
            target_date=target_date,
            horizon=horizon,
            distribution=distribution,
            point_estimate=ensemble_point,
            methodology=f"Ensemble of {len(self.models)} models with weighted averaging",
            model_version=self.model_version,
            metadata={
                'component_models': [m.model_name for m in self.models],
                'model_weights': self.weights,
                'component_forecasts': [
                    {'model': f.methodology, 'point_estimate': f.point_estimate}
                    for f in component_forecasts
                ]
            }
        )


class MonteCarloForecast(ForecastModel):
    """
    Monte Carlo simulation-based forecasting

    Generates probabilistic forecasts through:
    - Random sampling from historical distributions
    - Scenario simulation with different parameter assumptions
    - Full uncertainty propagation
    """

    def __init__(self,
                 model_name: str = "MonteCarloForecast",
                 n_simulations: int = 10000,
                 model_version: str = "1.0"):
        super().__init__(model_name, model_version)
        self.n_simulations = n_simulations
        self.historical_mean = None
        self.historical_std = None
        self.trend = None

    def fit(self,
            historical_data: np.ndarray,
            dates: List[datetime],
            **kwargs) -> 'MonteCarloForecast':
        """
        Fit model by estimating historical parameters
        """
        self.historical_mean = float(np.mean(historical_data))
        self.historical_std = float(np.std(historical_data))

        # Estimate linear trend
        x = np.arange(len(historical_data))
        self.trend = float(np.polyfit(x, historical_data, 1)[0])

        self.is_fitted = True
        return self

    def forecast(self,
                 horizon: ForecastHorizon,
                 target_date: datetime,
                 indicator_name: str = "Unknown",
                 **kwargs) -> ProbabilisticForecast:
        """
        Generate Monte Carlo probabilistic forecast
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted before forecasting")

        # Estimate time steps to target
        forecast_date = datetime.now()
        days_ahead = (target_date - forecast_date).days
        periods_ahead = days_ahead / 30  # Convert to months

        # Run Monte Carlo simulations
        samples = []
        for _ in range(self.n_simulations):
            # Base forecast with trend
            forecast_value = self.historical_mean + self.trend * periods_ahead

            # Add uncertainty that grows with horizon
            uncertainty = self.historical_std * np.sqrt(periods_ahead)
            simulated_value = np.random.normal(forecast_value, uncertainty)

            samples.append(simulated_value)

        samples = np.array(samples)

        # Generate distribution from samples
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
            distribution_type=DistributionType.EMPIRICAL,
            samples=samples
        )

        return ProbabilisticForecast(
            indicator_name=indicator_name,
            forecast_date=forecast_date,
            target_date=target_date,
            horizon=horizon,
            distribution=distribution,
            point_estimate=float(np.mean(samples)),
            methodology=f"Monte Carlo simulation with {self.n_simulations} scenarios",
            model_version=self.model_version,
            metadata={
                'n_simulations': self.n_simulations,
                'periods_ahead': periods_ahead,
                'trend': self.trend
            }
        )
