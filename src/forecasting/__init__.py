"""
Probabilistic Forecasting System for Macroeconomic and Financial Indicators

This module provides production-grade probabilistic forecasting capabilities with:
- Monte Carlo simulation for uncertainty quantification
- Bayesian methods for prior incorporation
- Ensemble modeling for robust predictions
- Expert calibration and consensus mechanisms
- Structured reasoning and documentation
"""

from .models import (
    ForecastModel,
    ProbabilisticForecast,
    ForecastDistribution,
    EnsembleForecast,
    BayesianForecast
)

from .indicators import (
    IndicatorDataSource,
    MacroIndicator,
    FinancialIndicator,
    IndicatorType
)

from .reasoning import (
    ForecastRationale,
    ReasoningEngine,
    DocumentedForecast
)

from .calibration import (
    ExpertCalibrator,
    ConsensusBuilder,
    CalibrationMetrics
)

__all__ = [
    'ForecastModel',
    'ProbabilisticForecast',
    'ForecastDistribution',
    'EnsembleForecast',
    'BayesianForecast',
    'IndicatorDataSource',
    'MacroIndicator',
    'FinancialIndicator',
    'IndicatorType',
    'ForecastRationale',
    'ReasoningEngine',
    'DocumentedForecast',
    'ExpertCalibrator',
    'ConsensusBuilder',
    'CalibrationMetrics'
]

__version__ = '1.0.0'
