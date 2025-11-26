"""
Trend Analysis and Feature Extraction for Forecasting

Provides sophisticated analysis of time series patterns:
- Trend detection and extraction
- Seasonality analysis
- Cycle identification
- Feature engineering for forecast models
- Change point detection
- Correlation analysis across indicators
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import numpy as np
from scipy import stats, signal
from scipy.fft import fft, fftfreq

from .indicators import IndicatorData


@dataclass
class TrendComponents:
    """
    Decomposition of time series into components

    Attributes:
        trend: Long-term trend component
        seasonal: Seasonal component
        cycle: Cyclical component
        residual: Residual/irregular component
        dates: Corresponding dates
    """
    trend: np.ndarray
    seasonal: Optional[np.ndarray]
    cycle: Optional[np.ndarray]
    residual: np.ndarray
    dates: List[datetime]

    def to_dict(self) -> Dict[str, Any]:
        return {
            'trend': self.trend.tolist(),
            'seasonal': self.seasonal.tolist() if self.seasonal is not None else None,
            'cycle': self.cycle.tolist() if self.cycle is not None else None,
            'residual': self.residual.tolist(),
            'dates': [d.isoformat() for d in self.dates]
        }


@dataclass
class FeatureSet:
    """
    Extracted features for forecasting

    Attributes:
        statistical_features: Basic statistical measures
        trend_features: Trend-related features
        volatility_features: Volatility and dispersion measures
        momentum_features: Momentum indicators
        correlation_features: Correlations with other indicators
    """
    statistical_features: Dict[str, float]
    trend_features: Dict[str, float]
    volatility_features: Dict[str, float]
    momentum_features: Dict[str, float]
    correlation_features: Optional[Dict[str, float]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'statistical': self.statistical_features,
            'trend': self.trend_features,
            'volatility': self.volatility_features,
            'momentum': self.momentum_features,
            'correlation': self.correlation_features or {}
        }

    def get_all_features(self) -> Dict[str, float]:
        """Get all features as flat dictionary"""
        all_features = {}
        all_features.update(self.statistical_features)
        all_features.update(self.trend_features)
        all_features.update(self.volatility_features)
        all_features.update(self.momentum_features)
        if self.correlation_features:
            all_features.update(self.correlation_features)
        return all_features


class TrendAnalyzer:
    """
    Analyzes trends in time series data

    Provides methods for:
    - Trend extraction
    - Trend strength measurement
    - Trend direction and acceleration
    - Change point detection
    """

    @staticmethod
    def extract_linear_trend(data: IndicatorData) -> Tuple[float, float, float]:
        """
        Extract linear trend from data

        Args:
            data: Time series data

        Returns:
            Tuple of (slope, intercept, r_squared)
        """
        x = np.arange(len(data.values))
        slope, intercept, r_value, _, _ = stats.linregress(x, data.values)

        return float(slope), float(intercept), float(r_value ** 2)

    @staticmethod
    def extract_polynomial_trend(data: IndicatorData, degree: int = 2) -> np.ndarray:
        """
        Extract polynomial trend

        Args:
            data: Time series data
            degree: Polynomial degree

        Returns:
            Trend component
        """
        x = np.arange(len(data.values))
        coeffs = np.polyfit(x, data.values, degree)
        trend = np.polyval(coeffs, x)

        return trend

    @staticmethod
    def extract_moving_average_trend(data: IndicatorData,
                                     window: int = 12) -> np.ndarray:
        """
        Extract trend using moving average

        Args:
            data: Time series data
            window: Moving average window size

        Returns:
            Smoothed trend
        """
        # Simple moving average
        trend = np.convolve(data.values, np.ones(window)/window, mode='same')

        # Handle edges
        for i in range(window // 2):
            trend[i] = np.mean(data.values[:i+1])
            trend[-(i+1)] = np.mean(data.values[-(i+1):])

        return trend

    @staticmethod
    def detect_change_points(data: IndicatorData,
                            min_segment_length: int = 10) -> List[int]:
        """
        Detect structural breaks/change points in time series

        Args:
            data: Time series data
            min_segment_length: Minimum length of segments

        Returns:
            List of change point indices
        """
        values = data.values
        n = len(values)

        if n < 2 * min_segment_length:
            return []

        # Simple method: look for significant changes in mean
        change_points = []

        for i in range(min_segment_length, n - min_segment_length):
            before = values[:i]
            after = values[i:]

            # T-test for difference in means
            t_stat, p_value = stats.ttest_ind(before, after)

            if p_value < 0.01:  # Significant at 1% level
                change_points.append(i)

        # Remove nearby change points (keep only most significant)
        if len(change_points) > 1:
            filtered = [change_points[0]]
            for cp in change_points[1:]:
                if cp - filtered[-1] >= min_segment_length:
                    filtered.append(cp)
            change_points = filtered

        return change_points

    @staticmethod
    def measure_trend_strength(data: IndicatorData) -> float:
        """
        Measure strength of trend (0 = no trend, 1 = perfect trend)

        Args:
            data: Time series data

        Returns:
            Trend strength metric
        """
        _, _, r_squared = TrendAnalyzer.extract_linear_trend(data)
        return r_squared


class SeasonalityAnalyzer:
    """
    Analyzes seasonal patterns in data
    """

    @staticmethod
    def detect_seasonality(data: IndicatorData,
                          max_lag: int = 50) -> Dict[str, Any]:
        """
        Detect seasonal patterns using autocorrelation

        Args:
            data: Time series data
            max_lag: Maximum lag to check

        Returns:
            Dictionary with seasonality information
        """
        values = data.values
        n = len(values)

        if n < max_lag:
            max_lag = n - 1

        # Calculate autocorrelation function
        acf = np.correlate(values - values.mean(), values - values.mean(), mode='full')
        acf = acf[n-1:n+max_lag]
        acf = acf / acf[0]

        # Find peaks in ACF (excluding lag 0)
        peaks, properties = signal.find_peaks(acf[1:], height=0.3)
        peaks = peaks + 1  # Adjust for excluding lag 0

        seasonal_periods = []
        for peak in peaks[:3]:  # Top 3 peaks
            seasonal_periods.append({
                'period': int(peak),
                'strength': float(acf[peak])
            })

        return {
            'has_seasonality': len(peaks) > 0,
            'seasonal_periods': seasonal_periods,
            'acf': acf.tolist()
        }

    @staticmethod
    def extract_seasonal_component(data: IndicatorData,
                                   period: int) -> np.ndarray:
        """
        Extract seasonal component with given period

        Args:
            data: Time series data
            period: Seasonal period (e.g., 12 for monthly data)

        Returns:
            Seasonal component
        """
        values = data.values
        n = len(values)

        # Calculate seasonal indices
        seasonal_indices = np.zeros(period)
        counts = np.zeros(period)

        for i, value in enumerate(values):
            season_idx = i % period
            seasonal_indices[season_idx] += value
            counts[season_idx] += 1

        seasonal_indices = seasonal_indices / np.maximum(counts, 1)
        seasonal_indices = seasonal_indices - seasonal_indices.mean()

        # Extend to full length
        seasonal = np.tile(seasonal_indices, n // period + 1)[:n]

        return seasonal

    @staticmethod
    def decompose(data: IndicatorData,
                 seasonal_period: Optional[int] = None) -> TrendComponents:
        """
        Decompose time series into trend, seasonal, and residual

        Args:
            data: Time series data
            seasonal_period: Seasonal period (auto-detected if None)

        Returns:
            TrendComponents with decomposition
        """
        values = data.values

        # Extract trend
        trend = TrendAnalyzer.extract_moving_average_trend(data, window=12)

        # Detect/extract seasonal component
        if seasonal_period is None:
            seasonality_info = SeasonalityAnalyzer.detect_seasonality(data)
            if seasonality_info['has_seasonality']:
                seasonal_period = seasonality_info['seasonal_periods'][0]['period']

        if seasonal_period:
            seasonal = SeasonalityAnalyzer.extract_seasonal_component(data, seasonal_period)
        else:
            seasonal = None

        # Calculate residual
        detrended = values - trend
        if seasonal is not None:
            residual = detrended - seasonal
        else:
            residual = detrended

        return TrendComponents(
            trend=trend,
            seasonal=seasonal,
            cycle=None,  # Could add cycle detection
            residual=residual,
            dates=data.dates
        )


class FeatureExtractor:
    """
    Extracts features from time series for forecasting
    """

    @staticmethod
    def extract_statistical_features(data: IndicatorData) -> Dict[str, float]:
        """Extract basic statistical features"""
        values = data.values

        return {
            'mean': float(np.mean(values)),
            'median': float(np.median(values)),
            'std': float(np.std(values)),
            'min': float(np.min(values)),
            'max': float(np.max(values)),
            'range': float(np.max(values) - np.min(values)),
            'skewness': float(stats.skew(values)),
            'kurtosis': float(stats.kurtosis(values)),
            'cv': float(np.std(values) / np.mean(values)) if np.mean(values) != 0 else 0
        }

    @staticmethod
    def extract_trend_features(data: IndicatorData) -> Dict[str, float]:
        """Extract trend-related features"""
        slope, intercept, r_squared = TrendAnalyzer.extract_linear_trend(data)
        trend_strength = TrendAnalyzer.measure_trend_strength(data)

        # Calculate recent trend (last 25% of data)
        split_point = int(len(data.values) * 0.75)
        recent_data = IndicatorData(
            metadata=data.metadata,
            dates=data.dates[split_point:],
            values=data.values[split_point:]
        )
        recent_slope, _, _ = TrendAnalyzer.extract_linear_trend(recent_data)

        return {
            'trend_slope': float(slope),
            'trend_intercept': float(intercept),
            'trend_r_squared': float(r_squared),
            'trend_strength': float(trend_strength),
            'recent_trend_slope': float(recent_slope),
            'trend_direction': 1.0 if slope > 0 else -1.0,
            'trend_acceleration': float(recent_slope - slope)
        }

    @staticmethod
    def extract_volatility_features(data: IndicatorData) -> Dict[str, float]:
        """Extract volatility and dispersion features"""
        values = data.values

        # Calculate returns/changes
        returns = np.diff(values) / values[:-1]

        # Rolling volatility (last 12 periods)
        window = min(12, len(returns) - 1)
        if window > 0:
            rolling_vol = np.std(returns[-window:])
        else:
            rolling_vol = 0

        return {
            'volatility': float(np.std(returns)),
            'rolling_volatility': float(rolling_vol),
            'volatility_of_volatility': float(np.std([
                np.std(returns[max(0, i-12):i])
                for i in range(12, len(returns))
            ])) if len(returns) > 24 else 0,
            'max_drawdown': float(FeatureExtractor._calculate_max_drawdown(values)),
            'upside_volatility': float(np.std(returns[returns > 0])) if np.any(returns > 0) else 0,
            'downside_volatility': float(np.std(returns[returns < 0])) if np.any(returns < 0) else 0
        }

    @staticmethod
    def extract_momentum_features(data: IndicatorData) -> Dict[str, float]:
        """Extract momentum indicators"""
        values = data.values

        # Rate of change over different periods
        roc_1 = (values[-1] - values[-2]) / values[-2] if len(values) >= 2 else 0
        roc_3 = (values[-1] - values[-4]) / values[-4] if len(values) >= 4 else 0
        roc_12 = (values[-1] - values[-13]) / values[-13] if len(values) >= 13 else 0

        # Moving average crossovers
        ma_short = np.mean(values[-3:]) if len(values) >= 3 else values[-1]
        ma_long = np.mean(values[-12:]) if len(values) >= 12 else np.mean(values)
        ma_crossover = float(ma_short - ma_long)

        return {
            'roc_1_period': float(roc_1),
            'roc_3_period': float(roc_3),
            'roc_12_period': float(roc_12),
            'ma_crossover': float(ma_crossover),
            'momentum_score': float(roc_1 + roc_3 + roc_12) / 3,
            'relative_position': float((values[-1] - np.min(values)) / (np.max(values) - np.min(values)))
            if np.max(values) != np.min(values) else 0.5
        }

    @staticmethod
    def _calculate_max_drawdown(values: np.ndarray) -> float:
        """Calculate maximum drawdown"""
        cummax = np.maximum.accumulate(values)
        drawdown = (values - cummax) / cummax
        return float(np.min(drawdown))

    @staticmethod
    def extract_all_features(data: IndicatorData) -> FeatureSet:
        """Extract complete feature set"""
        return FeatureSet(
            statistical_features=FeatureExtractor.extract_statistical_features(data),
            trend_features=FeatureExtractor.extract_trend_features(data),
            volatility_features=FeatureExtractor.extract_volatility_features(data),
            momentum_features=FeatureExtractor.extract_momentum_features(data)
        )


class CorrelationAnalyzer:
    """
    Analyzes correlations between indicators
    """

    @staticmethod
    def calculate_correlation(data1: IndicatorData,
                            data2: IndicatorData,
                            method: str = 'pearson') -> float:
        """
        Calculate correlation between two indicators

        Args:
            data1: First indicator data
            data2: Second indicator data
            method: 'pearson', 'spearman', or 'kendall'

        Returns:
            Correlation coefficient
        """
        # Align data by dates (simple version: assume same dates)
        if len(data1.values) != len(data2.values):
            raise ValueError("Data series must have same length")

        if method == 'pearson':
            corr, _ = stats.pearsonr(data1.values, data2.values)
        elif method == 'spearman':
            corr, _ = stats.spearmanr(data1.values, data2.values)
        elif method == 'kendall':
            corr, _ = stats.kendalltau(data1.values, data2.values)
        else:
            raise ValueError(f"Unknown correlation method: {method}")

        return float(corr)

    @staticmethod
    def calculate_lagged_correlation(data1: IndicatorData,
                                    data2: IndicatorData,
                                    max_lag: int = 12) -> Dict[int, float]:
        """
        Calculate correlation at different lags

        Args:
            data1: First indicator data
            data2: Second indicator data (lagged relative to data1)
            max_lag: Maximum lag to consider

        Returns:
            Dictionary mapping lag to correlation
        """
        lagged_corrs = {}

        for lag in range(max_lag + 1):
            if lag >= len(data2.values):
                break

            # data1[t] vs data2[t-lag]
            values1 = data1.values[lag:]
            values2 = data2.values[:-lag] if lag > 0 else data2.values

            if len(values1) > 10:  # Minimum data for meaningful correlation
                corr, _ = stats.pearsonr(values1, values2)
                lagged_corrs[lag] = float(corr)

        return lagged_corrs

    @staticmethod
    def find_leading_indicators(target_data: IndicatorData,
                               candidate_indicators: Dict[str, IndicatorData],
                               threshold: float = 0.5) -> List[Tuple[str, int, float]]:
        """
        Find indicators that lead the target indicator

        Args:
            target_data: Target indicator to forecast
            candidate_indicators: Dictionary of potential leading indicators
            threshold: Minimum correlation threshold

        Returns:
            List of (indicator_name, optimal_lag, correlation)
        """
        leading_indicators = []

        for name, indicator_data in candidate_indicators.items():
            lagged_corrs = CorrelationAnalyzer.calculate_lagged_correlation(
                target_data,
                indicator_data,
                max_lag=12
            )

            if lagged_corrs:
                # Find lag with highest absolute correlation
                best_lag = max(lagged_corrs.items(), key=lambda x: abs(x[1]))

                if abs(best_lag[1]) >= threshold:
                    leading_indicators.append((name, best_lag[0], best_lag[1]))

        # Sort by correlation strength
        leading_indicators.sort(key=lambda x: abs(x[2]), reverse=True)

        return leading_indicators


class ForecastInputAnalyzer:
    """
    Comprehensive analyzer for preparing forecast inputs

    Combines all analysis components to provide complete
    input preparation for forecasting models
    """

    def __init__(self):
        self.trend_analyzer = TrendAnalyzer()
        self.seasonality_analyzer = SeasonalityAnalyzer()
        self.feature_extractor = FeatureExtractor()
        self.correlation_analyzer = CorrelationAnalyzer()

    def analyze(self,
               data: IndicatorData,
               related_indicators: Optional[Dict[str, IndicatorData]] = None) -> Dict[str, Any]:
        """
        Perform comprehensive analysis

        Args:
            data: Primary indicator data
            related_indicators: Optional related indicators for correlation analysis

        Returns:
            Complete analysis results
        """
        analysis = {}

        # Trend analysis
        analysis['trend'] = {
            'linear_trend': TrendAnalyzer.extract_linear_trend(data),
            'trend_strength': TrendAnalyzer.measure_trend_strength(data),
            'change_points': TrendAnalyzer.detect_change_points(data)
        }

        # Seasonality analysis
        analysis['seasonality'] = SeasonalityAnalyzer.detect_seasonality(data)

        # Decomposition
        decomposition = SeasonalityAnalyzer.decompose(data)
        analysis['decomposition'] = decomposition.to_dict()

        # Feature extraction
        features = FeatureExtractor.extract_all_features(data)
        analysis['features'] = features.to_dict()

        # Correlation analysis
        if related_indicators:
            correlations = {}
            for name, related_data in related_indicators.items():
                try:
                    corr = CorrelationAnalyzer.calculate_correlation(data, related_data)
                    correlations[name] = corr
                except:
                    pass

            analysis['correlations'] = correlations

            # Leading indicators
            leading = CorrelationAnalyzer.find_leading_indicators(
                data,
                related_indicators
            )
            analysis['leading_indicators'] = [
                {'name': name, 'lag': lag, 'correlation': corr}
                for name, lag, corr in leading
            ]

        return analysis

    def generate_analysis_report(self, analysis: Dict[str, Any], indicator_name: str) -> str:
        """Generate human-readable analysis report"""
        report = f"""
{'='*80}
TREND & FEATURE ANALYSIS: {indicator_name}
{'='*80}

TREND ANALYSIS
--------------
Linear Trend:
  Slope: {analysis['trend']['linear_trend'][0]:.6f}
  R-squared: {analysis['trend']['linear_trend'][2]:.4f}
  Trend Strength: {analysis['trend']['trend_strength']:.4f}

Change Points: {len(analysis['trend']['change_points'])} detected
"""

        if analysis['seasonality']['has_seasonality']:
            report += "\nSEASONALITY DETECTED\n"
            report += "--------------------\n"
            for sp in analysis['seasonality']['seasonal_periods']:
                report += f"  Period: {sp['period']} (strength: {sp['strength']:.4f})\n"

        report += "\nKEY FEATURES\n"
        report += "------------\n"
        features = analysis['features']
        report += f"Statistical:\n"
        report += f"  Mean: {features['statistical']['mean']:.4f}\n"
        report += f"  Std Dev: {features['statistical']['std']:.4f}\n"
        report += f"  Skewness: {features['statistical']['skewness']:.4f}\n"

        report += f"\nTrend:\n"
        report += f"  Direction: {'Upward' if features['trend']['trend_direction'] > 0 else 'Downward'}\n"
        report += f"  Acceleration: {features['trend']['trend_acceleration']:.6f}\n"

        report += f"\nVolatility:\n"
        report += f"  Overall: {features['volatility']['volatility']:.4f}\n"
        report += f"  Rolling (12-period): {features['volatility']['rolling_volatility']:.4f}\n"

        if 'correlations' in analysis:
            report += "\nCORRELATIONS WITH OTHER INDICATORS\n"
            report += "-----------------------------------\n"
            for name, corr in sorted(analysis['correlations'].items(),
                                   key=lambda x: abs(x[1]),
                                   reverse=True)[:5]:
                report += f"  {name}: {corr:+.4f}\n"

        if 'leading_indicators' in analysis and analysis['leading_indicators']:
            report += "\nLEADING INDICATORS\n"
            report += "------------------\n"
            for li in analysis['leading_indicators'][:3]:
                report += f"  {li['name']}: lag={li['lag']}, corr={li['correlation']:+.4f}\n"

        report += f"\n{'='*80}\n"

        return report
