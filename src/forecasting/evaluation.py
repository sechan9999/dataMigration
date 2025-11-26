"""
Forecast Feedback and Methodology Evaluation

Provides comprehensive evaluation framework:
- Backtest forecasts against historical data
- Track forecast accuracy over time
- Evaluate methodology performance
- Provide actionable feedback for improvement
- A/B testing of forecasting methodologies
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from scipy import stats

from .models import ProbabilisticForecast, ForecastModel
from .reasoning import DocumentedForecast, ForecastRationale


class MetricType(Enum):
    """Types of evaluation metrics"""
    POINT_ACCURACY = "point_accuracy"           # MAE, RMSE, MAPE
    PROBABILISTIC = "probabilistic"             # Brier, log score
    CALIBRATION = "calibration"                 # Coverage, sharpness
    DIRECTIONAL = "directional"                 # Direction accuracy
    BUSINESS_VALUE = "business_value"           # Business-specific metrics


@dataclass
class ForecastError:
    """
    Detailed forecast error analysis

    Attributes:
        forecast_id: Identifier for forecast
        indicator_name: Name of indicator
        forecast_value: Predicted value
        actual_value: Realized value
        error: Raw error (actual - forecast)
        absolute_error: Absolute error
        percentage_error: Percentage error
        squared_error: Squared error
        forecast_date: When forecast was made
        target_date: Date of prediction
        horizon_days: Days ahead forecast
    """
    forecast_id: str
    indicator_name: str
    forecast_value: float
    actual_value: float
    error: float
    absolute_error: float
    percentage_error: float
    squared_error: float
    forecast_date: datetime
    target_date: datetime
    horizon_days: int

    @classmethod
    def from_forecast(cls,
                     forecast: ProbabilisticForecast,
                     actual_value: float) -> 'ForecastError':
        """Create ForecastError from forecast and actual"""
        error = actual_value - forecast.point_estimate
        abs_error = abs(error)
        pct_error = (error / actual_value * 100) if actual_value != 0 else 0
        sq_error = error ** 2

        horizon_days = (forecast.target_date - forecast.forecast_date).days

        return cls(
            forecast_id=forecast.forecast_id,
            indicator_name=forecast.indicator_name,
            forecast_value=forecast.point_estimate,
            actual_value=actual_value,
            error=error,
            absolute_error=abs_error,
            percentage_error=pct_error,
            squared_error=sq_error,
            forecast_date=forecast.forecast_date,
            target_date=forecast.target_date,
            horizon_days=horizon_days
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'forecast_id': self.forecast_id,
            'indicator_name': self.indicator_name,
            'forecast_value': self.forecast_value,
            'actual_value': self.actual_value,
            'error': self.error,
            'absolute_error': self.absolute_error,
            'percentage_error': self.percentage_error,
            'squared_error': self.squared_error,
            'forecast_date': self.forecast_date.isoformat(),
            'target_date': self.target_date.isoformat(),
            'horizon_days': self.horizon_days
        }


@dataclass
class AccuracyMetrics:
    """
    Comprehensive accuracy metrics

    Attributes:
        mae: Mean Absolute Error
        rmse: Root Mean Squared Error
        mape: Mean Absolute Percentage Error
        median_absolute_error: Median Absolute Error
        directional_accuracy: Percentage of correct direction predictions
        n_forecasts: Number of forecasts evaluated
    """
    mae: float
    rmse: float
    mape: float
    median_absolute_error: float
    directional_accuracy: float
    n_forecasts: int
    mean_bias: float = 0.0  # Average signed error

    def to_dict(self) -> Dict[str, Any]:
        return {
            'mae': self.mae,
            'rmse': self.rmse,
            'mape': self.mape,
            'median_absolute_error': self.median_absolute_error,
            'directional_accuracy': self.directional_accuracy,
            'n_forecasts': self.n_forecasts,
            'mean_bias': self.mean_bias
        }


@dataclass
class ProbabilisticMetrics:
    """
    Metrics for probabilistic forecasts

    Attributes:
        brier_score: Brier score
        log_score: Logarithmic score
        crps: Continuous Ranked Probability Score
        pit_uniformity: Probability Integral Transform uniformity test
    """
    brier_score: Optional[float] = None
    log_score: Optional[float] = None
    crps: Optional[float] = None
    pit_uniformity: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'brier_score': self.brier_score,
            'log_score': self.log_score,
            'crps': self.crps,
            'pit_uniformity': self.pit_uniformity
        }


@dataclass
class CalibrationMetrics:
    """
    Calibration quality metrics

    Attributes:
        coverage_ratios: Actual vs expected coverage for confidence intervals
        sharpness: Average interval width
        calibration_slope: Slope of calibration plot
        calibration_intercept: Intercept of calibration plot
    """
    coverage_ratios: Dict[float, Tuple[float, float]]  # level -> (expected, actual)
    sharpness: float
    calibration_slope: Optional[float] = None
    calibration_intercept: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'coverage_ratios': {
                str(k): {'expected': v[0], 'actual': v[1]}
                for k, v in self.coverage_ratios.items()
            },
            'sharpness': self.sharpness,
            'calibration_slope': self.calibration_slope,
            'calibration_intercept': self.calibration_intercept
        }


@dataclass
class EvaluationReport:
    """
    Comprehensive evaluation report

    Attributes:
        indicator_name: Name of indicator evaluated
        evaluation_period: Period covered
        accuracy_metrics: Point accuracy metrics
        probabilistic_metrics: Probabilistic forecast metrics
        calibration_metrics: Calibration metrics
        methodology: Forecasting methodology evaluated
        recommendations: List of improvement recommendations
    """
    indicator_name: str
    evaluation_period: Tuple[datetime, datetime]
    accuracy_metrics: AccuracyMetrics
    probabilistic_metrics: Optional[ProbabilisticMetrics] = None
    calibration_metrics: Optional[CalibrationMetrics] = None
    methodology: str = "Unknown"
    recommendations: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'indicator_name': self.indicator_name,
            'evaluation_period': {
                'start': self.evaluation_period[0].isoformat(),
                'end': self.evaluation_period[1].isoformat()
            },
            'accuracy_metrics': self.accuracy_metrics.to_dict(),
            'probabilistic_metrics': self.probabilistic_metrics.to_dict() if self.probabilistic_metrics else None,
            'calibration_metrics': self.calibration_metrics.to_dict() if self.calibration_metrics else None,
            'methodology': self.methodology,
            'recommendations': self.recommendations,
            'created_at': self.created_at.isoformat()
        }

    def generate_report(self) -> str:
        """Generate human-readable evaluation report"""
        report = f"""
{'='*80}
FORECAST EVALUATION REPORT
{'='*80}

Indicator: {self.indicator_name}
Methodology: {self.methodology}
Evaluation Period: {self.evaluation_period[0].strftime('%Y-%m-%d')} to {self.evaluation_period[1].strftime('%Y-%m-%d')}
Number of Forecasts: {self.accuracy_metrics.n_forecasts}

POINT ACCURACY METRICS
---------------------
Mean Absolute Error (MAE):        {self.accuracy_metrics.mae:.4f}
Root Mean Squared Error (RMSE):   {self.accuracy_metrics.rmse:.4f}
Mean Absolute Percentage Error:   {self.accuracy_metrics.mape:.2f}%
Median Absolute Error:            {self.accuracy_metrics.median_absolute_error:.4f}
Mean Bias:                        {self.accuracy_metrics.mean_bias:+.4f}
Directional Accuracy:             {self.accuracy_metrics.directional_accuracy:.1f}%

"""
        if self.probabilistic_metrics:
            report += "PROBABILISTIC METRICS\n"
            report += "---------------------\n"
            if self.probabilistic_metrics.brier_score:
                report += f"Brier Score:                      {self.probabilistic_metrics.brier_score:.4f}\n"
            if self.probabilistic_metrics.log_score:
                report += f"Log Score:                        {self.probabilistic_metrics.log_score:.4f}\n"
            if self.probabilistic_metrics.crps:
                report += f"CRPS:                             {self.probabilistic_metrics.crps:.4f}\n"
            report += "\n"

        if self.calibration_metrics:
            report += "CALIBRATION METRICS\n"
            report += "-------------------\n"
            report += "Coverage Ratios (Expected vs Actual):\n"
            for level, (expected, actual) in sorted(self.calibration_metrics.coverage_ratios.items()):
                status = "✓" if abs(expected - actual) < 0.05 else "✗"
                report += f"  {level*100:.0f}% CI: {expected:.2f} vs {actual:.2f} {status}\n"
            report += f"\nSharpness (avg interval width):   {self.calibration_metrics.sharpness:.4f}\n\n"

        if self.recommendations:
            report += "RECOMMENDATIONS FOR IMPROVEMENT\n"
            report += "-------------------------------\n"
            for i, rec in enumerate(self.recommendations, 1):
                report += f"{i}. {rec}\n"
            report += "\n"

        report += f"{'='*80}\n"

        return report


class ForecastEvaluator:
    """
    Evaluates forecast performance and provides feedback

    Tracks forecasts, compares to actuals, computes metrics,
    and generates actionable feedback
    """

    def __init__(self):
        self.forecast_errors: List[ForecastError] = []
        self.forecasts_by_indicator: Dict[str, List[ProbabilisticForecast]] = {}
        self.actuals_by_indicator: Dict[str, Dict[str, float]] = {}  # indicator -> {forecast_id: actual}

    def record_forecast(self, forecast: ProbabilisticForecast):
        """Record a forecast for later evaluation"""
        if forecast.indicator_name not in self.forecasts_by_indicator:
            self.forecasts_by_indicator[forecast.indicator_name] = []

        self.forecasts_by_indicator[forecast.indicator_name].append(forecast)

    def record_actual(self, forecast_id: str, indicator_name: str, actual_value: float):
        """Record actual outcome for a forecast"""
        if indicator_name not in self.actuals_by_indicator:
            self.actuals_by_indicator[indicator_name] = {}

        self.actuals_by_indicator[indicator_name][forecast_id] = actual_value

        # Create error record if forecast exists
        forecasts = self.forecasts_by_indicator.get(indicator_name, [])
        for forecast in forecasts:
            if forecast.forecast_id == forecast_id:
                error = ForecastError.from_forecast(forecast, actual_value)
                self.forecast_errors.append(error)
                break

    def evaluate_accuracy(self,
                         indicator_name: Optional[str] = None,
                         min_forecasts: int = 5) -> Optional[AccuracyMetrics]:
        """
        Evaluate point forecast accuracy

        Args:
            indicator_name: Specific indicator to evaluate (None for all)
            min_forecasts: Minimum forecasts required for evaluation

        Returns:
            AccuracyMetrics or None if insufficient data
        """
        # Filter errors
        if indicator_name:
            errors = [e for e in self.forecast_errors if e.indicator_name == indicator_name]
        else:
            errors = self.forecast_errors

        if len(errors) < min_forecasts:
            return None

        # Calculate metrics
        abs_errors = [e.absolute_error for e in errors]
        pct_errors = [abs(e.percentage_error) for e in errors]
        sq_errors = [e.squared_error for e in errors]
        signed_errors = [e.error for e in errors]

        mae = float(np.mean(abs_errors))
        rmse = float(np.sqrt(np.mean(sq_errors)))
        mape = float(np.mean(pct_errors))
        median_ae = float(np.median(abs_errors))
        mean_bias = float(np.mean(signed_errors))

        # Directional accuracy
        correct_direction = 0
        for e in errors:
            # Compare forecast change direction to actual change
            # (simplified: assumes we have previous value to compare)
            if e.error * e.forecast_value >= 0:  # Same sign
                correct_direction += 1

        directional_acc = (correct_direction / len(errors)) * 100

        return AccuracyMetrics(
            mae=mae,
            rmse=rmse,
            mape=mape,
            median_absolute_error=median_ae,
            directional_accuracy=directional_acc,
            n_forecasts=len(errors),
            mean_bias=mean_bias
        )

    def evaluate_probabilistic(self,
                              indicator_name: str,
                              min_forecasts: int = 10) -> Optional[ProbabilisticMetrics]:
        """
        Evaluate probabilistic forecast quality

        Args:
            indicator_name: Indicator to evaluate
            min_forecasts: Minimum forecasts required

        Returns:
            ProbabilisticMetrics or None
        """
        forecasts = self.forecasts_by_indicator.get(indicator_name, [])
        actuals = self.actuals_by_indicator.get(indicator_name, {})

        # Match forecasts with actuals
        matched = [
            (f, actuals[f.forecast_id])
            for f in forecasts
            if f.forecast_id in actuals
        ]

        if len(matched) < min_forecasts:
            return None

        metrics = ProbabilisticMetrics()

        # Calculate CRPS (Continuous Ranked Probability Score)
        crps_values = []
        for forecast, actual in matched:
            if forecast.distribution.samples is not None:
                # CRPS using empirical distribution
                samples = forecast.distribution.samples
                crps = np.mean(np.abs(samples - actual)) - 0.5 * np.mean(
                    np.abs(samples[:, None] - samples[None, :])
                )
                crps_values.append(crps)

        if crps_values:
            metrics.crps = float(np.mean(crps_values))

        return metrics

    def evaluate_calibration(self,
                            indicator_name: str,
                            min_forecasts: int = 20) -> Optional[CalibrationMetrics]:
        """
        Evaluate forecast calibration

        Args:
            indicator_name: Indicator to evaluate
            min_forecasts: Minimum forecasts required

        Returns:
            CalibrationMetrics or None
        """
        forecasts = self.forecasts_by_indicator.get(indicator_name, [])
        actuals = self.actuals_by_indicator.get(indicator_name, {})

        matched = [
            (f, actuals[f.forecast_id])
            for f in forecasts
            if f.forecast_id in actuals
        ]

        if len(matched) < min_forecasts:
            return None

        # Check coverage for each confidence level
        coverage_ratios = {}
        interval_widths = []

        confidence_levels = [0.50, 0.80, 0.90, 0.95]

        for level in confidence_levels:
            within_interval = 0
            widths = []

            for forecast, actual in matched:
                if level in forecast.distribution.confidence_intervals:
                    lower, upper = forecast.distribution.confidence_intervals[level]
                    widths.append(upper - lower)

                    if lower <= actual <= upper:
                        within_interval += 1

            if widths:
                actual_coverage = within_interval / len(matched)
                coverage_ratios[level] = (level, actual_coverage)
                interval_widths.extend(widths)

        sharpness = float(np.mean(interval_widths)) if interval_widths else 0.0

        return CalibrationMetrics(
            coverage_ratios=coverage_ratios,
            sharpness=sharpness
        )

    def generate_evaluation_report(self,
                                  indicator_name: str,
                                  methodology: str = "Unknown") -> EvaluationReport:
        """
        Generate comprehensive evaluation report

        Args:
            indicator_name: Indicator to evaluate
            methodology: Forecasting methodology used

        Returns:
            EvaluationReport
        """
        # Get errors for this indicator
        errors = [e for e in self.forecast_errors if e.indicator_name == indicator_name]

        if not errors:
            raise ValueError(f"No forecast errors for indicator: {indicator_name}")

        # Determine evaluation period
        dates = [e.target_date for e in errors]
        eval_period = (min(dates), max(dates))

        # Calculate metrics
        accuracy = self.evaluate_accuracy(indicator_name)
        probabilistic = self.evaluate_probabilistic(indicator_name)
        calibration = self.evaluate_calibration(indicator_name)

        # Generate recommendations
        recommendations = self._generate_recommendations(
            accuracy, probabilistic, calibration
        )

        report = EvaluationReport(
            indicator_name=indicator_name,
            evaluation_period=eval_period,
            accuracy_metrics=accuracy,
            probabilistic_metrics=probabilistic,
            calibration_metrics=calibration,
            methodology=methodology,
            recommendations=recommendations
        )

        return report

    def _generate_recommendations(self,
                                 accuracy: Optional[AccuracyMetrics],
                                 probabilistic: Optional[ProbabilisticMetrics],
                                 calibration: Optional[CalibrationMetrics]) -> List[str]:
        """Generate actionable recommendations based on metrics"""
        recommendations = []

        if accuracy:
            # Check bias
            if abs(accuracy.mean_bias) > 0.1 * accuracy.mae:
                if accuracy.mean_bias > 0:
                    recommendations.append(
                        "Forecasts show positive bias (over-prediction). "
                        "Consider adjusting model parameters or adding downward correction."
                    )
                else:
                    recommendations.append(
                        "Forecasts show negative bias (under-prediction). "
                        "Consider adjusting model parameters or adding upward correction."
                    )

            # Check directional accuracy
            if accuracy.directional_accuracy < 60:
                recommendations.append(
                    f"Directional accuracy is low ({accuracy.directional_accuracy:.1f}%). "
                    "Consider incorporating leading indicators or improving trend detection."
                )

        if calibration:
            # Check coverage
            for level, (expected, actual) in calibration.coverage_ratios.items():
                if abs(expected - actual) > 0.10:
                    if actual < expected:
                        recommendations.append(
                            f"{level*100:.0f}% confidence intervals are too narrow "
                            f"(actual coverage {actual*100:.1f}% vs expected {expected*100:.1f}%). "
                            "Increase uncertainty estimates."
                        )
                    else:
                        recommendations.append(
                            f"{level*100:.0f}% confidence intervals are too wide "
                            f"(actual coverage {actual*100:.1f}% vs expected {expected*100:.1f}%). "
                            "Forecasts can be sharpened."
                        )

        if not recommendations:
            recommendations.append(
                "Forecast performance is within acceptable ranges. "
                "Continue monitoring and consider incremental improvements."
            )

        return recommendations

    def compare_methodologies(self,
                             indicator_name: str,
                             methodology_forecasts: Dict[str, List[ProbabilisticForecast]]) -> Dict[str, Any]:
        """
        Compare performance of different forecasting methodologies

        Args:
            indicator_name: Indicator being forecasted
            methodology_forecasts: Dict mapping methodology name to forecasts

        Returns:
            Comparison results
        """
        results = {}

        for method_name, forecasts in methodology_forecasts.items():
            # Temporarily store forecasts
            temp_eval = ForecastEvaluator()
            for f in forecasts:
                temp_eval.record_forecast(f)

            # Copy actuals
            if indicator_name in self.actuals_by_indicator:
                temp_eval.actuals_by_indicator[indicator_name] = (
                    self.actuals_by_indicator[indicator_name]
                )

                # Record errors
                for f in forecasts:
                    if f.forecast_id in temp_eval.actuals_by_indicator[indicator_name]:
                        actual = temp_eval.actuals_by_indicator[indicator_name][f.forecast_id]
                        error = ForecastError.from_forecast(f, actual)
                        temp_eval.forecast_errors.append(error)

            # Evaluate
            accuracy = temp_eval.evaluate_accuracy(indicator_name)

            if accuracy:
                results[method_name] = {
                    'mae': accuracy.mae,
                    'rmse': accuracy.rmse,
                    'mape': accuracy.mape,
                    'directional_accuracy': accuracy.directional_accuracy,
                    'n_forecasts': accuracy.n_forecasts
                }

        # Rank methodologies
        if results:
            best_mae = min(r['mae'] for r in results.values())
            best_method = [m for m, r in results.items() if r['mae'] == best_mae][0]

            return {
                'methodology_results': results,
                'best_methodology': best_method,
                'best_mae': best_mae
            }

        return {'methodology_results': results}


class BacktestEngine:
    """
    Backtesting engine for forecast methodologies

    Simulates historical forecasting to evaluate methodology performance
    """

    def __init__(self, model: ForecastModel):
        self.model = model
        self.backtest_results: List[ForecastError] = []

    def run_backtest(self,
                    historical_data: np.ndarray,
                    dates: List[datetime],
                    train_window: int,
                    forecast_horizon: int,
                    step_size: int = 1) -> List[ForecastError]:
        """
        Run rolling window backtest

        Args:
            historical_data: Full historical dataset
            dates: Corresponding dates
            train_window: Size of training window
            forecast_horizon: Forecast horizon in periods
            step_size: Step between forecasts

        Returns:
            List of forecast errors
        """
        errors = []

        for i in range(train_window, len(historical_data) - forecast_horizon, step_size):
            # Split data
            train_data = historical_data[:i]
            train_dates = dates[:i]
            actual_value = historical_data[i + forecast_horizon]
            target_date = dates[i + forecast_horizon]

            # Fit model on training data
            self.model.fit(train_data, train_dates)

            # Generate forecast
            # (Simplified: assuming model has proper forecast method)
            # In real implementation, would use actual model forecast

            # For demonstration, create mock forecast
            forecast_value = float(np.mean(train_data[-12:]))  # Simple moving average

            # Calculate error
            error = ForecastError(
                forecast_id=f"backtest_{i}",
                indicator_name="Backtest",
                forecast_value=forecast_value,
                actual_value=actual_value,
                error=actual_value - forecast_value,
                absolute_error=abs(actual_value - forecast_value),
                percentage_error=((actual_value - forecast_value) / actual_value * 100)
                if actual_value != 0 else 0,
                squared_error=(actual_value - forecast_value) ** 2,
                forecast_date=dates[i],
                target_date=target_date,
                horizon_days=forecast_horizon
            )

            errors.append(error)

        self.backtest_results = errors
        return errors

    def get_backtest_summary(self) -> Dict[str, float]:
        """Get summary statistics from backtest"""
        if not self.backtest_results:
            return {}

        abs_errors = [e.absolute_error for e in self.backtest_results]
        pct_errors = [abs(e.percentage_error) for e in self.backtest_results]

        return {
            'mae': float(np.mean(abs_errors)),
            'rmse': float(np.sqrt(np.mean([e.squared_error for e in self.backtest_results]))),
            'mape': float(np.mean(pct_errors)),
            'median_ae': float(np.median(abs_errors)),
            'n_forecasts': len(self.backtest_results)
        }
