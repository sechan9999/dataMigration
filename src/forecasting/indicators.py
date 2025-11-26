"""
Macroeconomic and Financial Indicator Data Sources

Provides structured access to various economic and financial data sources:
- Macroeconomic indicators (GDP, inflation, unemployment, etc.)
- Financial market indicators (equity indices, bond yields, currencies)
- Alternative data sources (sentiment, nowcasting indicators)
- Data quality validation and preprocessing
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
import pandas as pd


class IndicatorType(Enum):
    """Categories of economic and financial indicators"""
    # Macroeconomic
    GDP = "gdp"
    INFLATION = "inflation"
    UNEMPLOYMENT = "unemployment"
    INTEREST_RATE = "interest_rate"
    TRADE_BALANCE = "trade_balance"
    BUDGET_DEFICIT = "budget_deficit"
    CONSUMER_CONFIDENCE = "consumer_confidence"
    BUSINESS_SENTIMENT = "business_sentiment"

    # Financial Markets
    EQUITY_INDEX = "equity_index"
    BOND_YIELD = "bond_yield"
    CURRENCY_RATE = "currency_rate"
    COMMODITY_PRICE = "commodity_price"
    VOLATILITY_INDEX = "volatility_index"
    CREDIT_SPREAD = "credit_spread"

    # Leading Indicators
    PMI = "pmi"  # Purchasing Managers Index
    YIELD_CURVE = "yield_curve"
    HOUSING_STARTS = "housing_starts"
    BUILDING_PERMITS = "building_permits"

    # Alternative Data
    SENTIMENT_INDEX = "sentiment_index"
    NOWCAST = "nowcast"
    SEARCH_TRENDS = "search_trends"


class DataFrequency(Enum):
    """Data release frequencies"""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"


class DataQuality(Enum):
    """Data quality assessment levels"""
    HIGH = "high"          # Official, timely, complete data
    MEDIUM = "medium"      # Some revisions or gaps expected
    LOW = "low"            # Preliminary or incomplete
    ESTIMATED = "estimated"  # Model-based estimates


@dataclass
class IndicatorMetadata:
    """
    Metadata about an economic or financial indicator

    Attributes:
        indicator_name: Full name of indicator
        indicator_type: Category of indicator
        source: Data provider (e.g., "Federal Reserve", "BLS", "Bloomberg")
        frequency: How often data is released
        unit: Unit of measurement (e.g., "percent", "billions USD", "index")
        seasonal_adjustment: Whether data is seasonally adjusted
        geography: Geographic coverage (e.g., "US", "Eurozone", "Global")
        vintage: Data vintage for real-time analysis
    """
    indicator_name: str
    indicator_type: IndicatorType
    source: str
    frequency: DataFrequency
    unit: str
    seasonal_adjustment: bool = True
    geography: str = "US"
    vintage: Optional[datetime] = None
    description: str = ""
    data_quality: DataQuality = DataQuality.MEDIUM

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'indicator_name': self.indicator_name,
            'indicator_type': self.indicator_type.value,
            'source': self.source,
            'frequency': self.frequency.value,
            'unit': self.unit,
            'seasonal_adjustment': self.seasonal_adjustment,
            'geography': self.geography,
            'vintage': self.vintage.isoformat() if self.vintage else None,
            'description': self.description,
            'data_quality': self.data_quality.value
        }


@dataclass
class IndicatorData:
    """
    Time series data for an indicator

    Attributes:
        metadata: Indicator metadata
        dates: List of observation dates
        values: Corresponding values
        revisions: Historical revisions (for real-time analysis)
        quality_flags: Data quality indicators per observation
    """
    metadata: IndicatorMetadata
    dates: List[datetime]
    values: np.ndarray
    revisions: Optional[Dict[datetime, List[Tuple[datetime, float]]]] = None
    quality_flags: Optional[List[DataQuality]] = None

    def __post_init__(self):
        """Validate data consistency"""
        if len(self.dates) != len(self.values):
            raise ValueError("Dates and values must have same length")

        if self.quality_flags and len(self.quality_flags) != len(self.values):
            raise ValueError("Quality flags must match data length")

    def to_dataframe(self) -> pd.DataFrame:
        """Convert to pandas DataFrame"""
        df = pd.DataFrame({
            'date': self.dates,
            'value': self.values
        })

        if self.quality_flags:
            df['quality'] = [q.value for q in self.quality_flags]

        return df

    def get_latest_value(self) -> Tuple[datetime, float]:
        """Get most recent observation"""
        if len(self.dates) == 0:
            raise ValueError("No data available")
        return self.dates[-1], float(self.values[-1])

    def get_value_at_date(self, target_date: datetime) -> Optional[float]:
        """Get value at specific date (or nearest prior date)"""
        for i in range(len(self.dates) - 1, -1, -1):
            if self.dates[i] <= target_date:
                return float(self.values[i])
        return None

    def calculate_growth_rate(self, periods: int = 1) -> np.ndarray:
        """Calculate period-over-period growth rates"""
        if len(self.values) < periods + 1:
            raise ValueError(f"Insufficient data for {periods}-period growth")

        growth_rates = np.zeros(len(self.values) - periods)
        for i in range(len(growth_rates)):
            growth_rates[i] = (
                (self.values[i + periods] - self.values[i]) / self.values[i] * 100
            )

        return growth_rates

    def calculate_year_over_year(self, periods_per_year: int = 12) -> np.ndarray:
        """Calculate year-over-year growth rates"""
        return self.calculate_growth_rate(periods=periods_per_year)


class MacroIndicator:
    """
    Macroeconomic indicator with data access and preprocessing
    """

    # Common macroeconomic indicators
    INDICATORS_CATALOG = {
        'US_GDP': IndicatorMetadata(
            indicator_name="US Real GDP Growth",
            indicator_type=IndicatorType.GDP,
            source="BEA",
            frequency=DataFrequency.QUARTERLY,
            unit="percent",
            seasonal_adjustment=True,
            geography="US",
            description="Quarterly real GDP growth rate, annualized"
        ),
        'US_INFLATION_CPI': IndicatorMetadata(
            indicator_name="US CPI Inflation",
            indicator_type=IndicatorType.INFLATION,
            source="BLS",
            frequency=DataFrequency.MONTHLY,
            unit="percent_yoy",
            seasonal_adjustment=False,
            geography="US",
            description="Year-over-year change in Consumer Price Index"
        ),
        'US_UNEMPLOYMENT': IndicatorMetadata(
            indicator_name="US Unemployment Rate",
            indicator_type=IndicatorType.UNEMPLOYMENT,
            source="BLS",
            frequency=DataFrequency.MONTHLY,
            unit="percent",
            seasonal_adjustment=True,
            geography="US",
            description="Civilian unemployment rate"
        ),
        'US_FED_FUNDS': IndicatorMetadata(
            indicator_name="Federal Funds Rate",
            indicator_type=IndicatorType.INTEREST_RATE,
            source="Federal Reserve",
            frequency=DataFrequency.DAILY,
            unit="percent",
            seasonal_adjustment=False,
            geography="US",
            description="Effective federal funds rate"
        ),
        'US_10Y_YIELD': IndicatorMetadata(
            indicator_name="US 10-Year Treasury Yield",
            indicator_type=IndicatorType.BOND_YIELD,
            source="Federal Reserve",
            frequency=DataFrequency.DAILY,
            unit="percent",
            seasonal_adjustment=False,
            geography="US",
            description="10-year constant maturity Treasury yield"
        )
    }

    def __init__(self, indicator_key: str):
        """
        Initialize macro indicator

        Args:
            indicator_key: Key from INDICATORS_CATALOG
        """
        if indicator_key not in self.INDICATORS_CATALOG:
            raise ValueError(f"Unknown indicator: {indicator_key}")

        self.metadata = self.INDICATORS_CATALOG[indicator_key]
        self.indicator_key = indicator_key
        self.data: Optional[IndicatorData] = None

    def load_data(self,
                  start_date: Optional[datetime] = None,
                  end_date: Optional[datetime] = None,
                  data_source: str = "default") -> IndicatorData:
        """
        Load historical data for this indicator

        In production, this would connect to actual data APIs (FRED, Bloomberg, etc.)
        For now, generates synthetic data for demonstration

        Args:
            start_date: Start of data range
            end_date: End of data range
            data_source: Specific data source to use
        """
        # Default date range
        if end_date is None:
            end_date = datetime.now()
        if start_date is None:
            start_date = end_date - timedelta(days=365 * 10)  # 10 years

        # Generate dates based on frequency
        dates = self._generate_date_range(start_date, end_date, self.metadata.frequency)

        # Generate synthetic data (in production, fetch real data)
        values = self._generate_synthetic_data(len(dates))

        self.data = IndicatorData(
            metadata=self.metadata,
            dates=dates,
            values=values,
            quality_flags=[DataQuality.HIGH] * len(dates)
        )

        return self.data

    def _generate_date_range(self,
                            start_date: datetime,
                            end_date: datetime,
                            frequency: DataFrequency) -> List[datetime]:
        """Generate dates based on frequency"""
        dates = []
        current = start_date

        if frequency == DataFrequency.DAILY:
            while current <= end_date:
                dates.append(current)
                current += timedelta(days=1)

        elif frequency == DataFrequency.MONTHLY:
            while current <= end_date:
                dates.append(current)
                # Move to next month
                if current.month == 12:
                    current = current.replace(year=current.year + 1, month=1)
                else:
                    current = current.replace(month=current.month + 1)

        elif frequency == DataFrequency.QUARTERLY:
            while current <= end_date:
                dates.append(current)
                # Move to next quarter
                new_month = current.month + 3
                new_year = current.year
                if new_month > 12:
                    new_month -= 12
                    new_year += 1
                current = current.replace(year=new_year, month=new_month)

        return dates

    def _generate_synthetic_data(self, n_points: int) -> np.ndarray:
        """
        Generate synthetic data for demonstration

        In production, this would be replaced with actual data fetching
        """
        # Generate realistic-looking data based on indicator type
        if self.metadata.indicator_type == IndicatorType.GDP:
            # GDP growth around 2-3% with some volatility
            trend = 2.5
            values = trend + np.random.normal(0, 1.5, n_points)

        elif self.metadata.indicator_type == IndicatorType.INFLATION:
            # Inflation around 2% target with occasional spikes
            trend = 2.0
            values = trend + np.random.normal(0, 1.0, n_points)
            # Add some persistence
            for i in range(1, n_points):
                values[i] = 0.7 * values[i-1] + 0.3 * values[i]

        elif self.metadata.indicator_type == IndicatorType.UNEMPLOYMENT:
            # Unemployment with cyclical pattern
            cycle = np.sin(np.linspace(0, 4 * np.pi, n_points)) * 2
            values = 5.0 + cycle + np.random.normal(0, 0.5, n_points)

        elif self.metadata.indicator_type == IndicatorType.INTEREST_RATE:
            # Interest rates with trend and noise
            trend_component = np.linspace(2.0, 4.0, n_points)
            values = trend_component + np.random.normal(0, 0.3, n_points)

        elif self.metadata.indicator_type == IndicatorType.BOND_YIELD:
            # Bond yields similar to interest rates
            trend_component = np.linspace(2.5, 3.5, n_points)
            values = trend_component + np.random.normal(0, 0.4, n_points)

        else:
            # Default: random walk
            values = np.cumsum(np.random.normal(0, 1, n_points))

        return values


class FinancialIndicator:
    """
    Financial market indicator (equity indices, yields, currencies, etc.)
    """

    INDICATORS_CATALOG = {
        'SPX': IndicatorMetadata(
            indicator_name="S&P 500 Index",
            indicator_type=IndicatorType.EQUITY_INDEX,
            source="Bloomberg",
            frequency=DataFrequency.DAILY,
            unit="index_level",
            seasonal_adjustment=False,
            geography="US",
            description="S&P 500 equity index"
        ),
        'VIX': IndicatorMetadata(
            indicator_name="CBOE Volatility Index",
            indicator_type=IndicatorType.VOLATILITY_INDEX,
            source="CBOE",
            frequency=DataFrequency.DAILY,
            unit="percent",
            seasonal_adjustment=False,
            geography="US",
            description="Implied volatility of S&P 500 options"
        ),
        'USD_EUR': IndicatorMetadata(
            indicator_name="USD/EUR Exchange Rate",
            indicator_type=IndicatorType.CURRENCY_RATE,
            source="Bloomberg",
            frequency=DataFrequency.DAILY,
            unit="rate",
            seasonal_adjustment=False,
            geography="Global",
            description="US Dollar to Euro exchange rate"
        ),
        'CREDIT_SPREAD_IG': IndicatorMetadata(
            indicator_name="Investment Grade Credit Spread",
            indicator_type=IndicatorType.CREDIT_SPREAD,
            source="Bloomberg",
            frequency=DataFrequency.DAILY,
            unit="basis_points",
            seasonal_adjustment=False,
            geography="US",
            description="IG corporate bond spread over Treasuries"
        )
    }

    def __init__(self, indicator_key: str):
        if indicator_key not in self.INDICATORS_CATALOG:
            raise ValueError(f"Unknown financial indicator: {indicator_key}")

        self.metadata = self.INDICATORS_CATALOG[indicator_key]
        self.indicator_key = indicator_key
        self.data: Optional[IndicatorData] = None

    def load_data(self,
                  start_date: Optional[datetime] = None,
                  end_date: Optional[datetime] = None) -> IndicatorData:
        """Load financial data (similar to MacroIndicator)"""
        # Reuse macro indicator logic
        macro = MacroIndicator.__new__(MacroIndicator)
        macro.metadata = self.metadata
        macro.indicator_key = self.indicator_key

        self.data = macro.load_data(start_date, end_date)
        return self.data


class IndicatorDataSource:
    """
    Centralized data source manager for all indicators

    Provides:
    - Unified API for accessing macro and financial data
    - Data caching and refresh management
    - Data quality validation
    - Multiple source fallback
    """

    def __init__(self):
        self.macro_indicators: Dict[str, MacroIndicator] = {}
        self.financial_indicators: Dict[str, FinancialIndicator] = {}
        self.data_cache: Dict[str, IndicatorData] = {}

    def get_indicator(self,
                      indicator_key: str,
                      start_date: Optional[datetime] = None,
                      end_date: Optional[datetime] = None,
                      force_refresh: bool = False) -> IndicatorData:
        """
        Get indicator data with caching

        Args:
            indicator_key: Indicator identifier
            start_date: Start of data range
            end_date: End of data range
            force_refresh: Bypass cache and fetch fresh data
        """
        cache_key = f"{indicator_key}_{start_date}_{end_date}"

        # Check cache
        if not force_refresh and cache_key in self.data_cache:
            return self.data_cache[cache_key]

        # Load data
        if indicator_key in MacroIndicator.INDICATORS_CATALOG:
            if indicator_key not in self.macro_indicators:
                self.macro_indicators[indicator_key] = MacroIndicator(indicator_key)
            indicator = self.macro_indicators[indicator_key]
            data = indicator.load_data(start_date, end_date)

        elif indicator_key in FinancialIndicator.INDICATORS_CATALOG:
            if indicator_key not in self.financial_indicators:
                self.financial_indicators[indicator_key] = FinancialIndicator(indicator_key)
            indicator = self.financial_indicators[indicator_key]
            data = indicator.load_data(start_date, end_date)

        else:
            raise ValueError(f"Unknown indicator: {indicator_key}")

        # Cache data
        self.data_cache[cache_key] = data

        return data

    def get_multiple_indicators(self,
                               indicator_keys: List[str],
                               start_date: Optional[datetime] = None,
                               end_date: Optional[datetime] = None) -> Dict[str, IndicatorData]:
        """
        Get data for multiple indicators

        Args:
            indicator_keys: List of indicator identifiers
            start_date: Start of data range
            end_date: End of data range

        Returns:
            Dictionary mapping indicator keys to data
        """
        return {
            key: self.get_indicator(key, start_date, end_date)
            for key in indicator_keys
        }

    def list_available_indicators(self) -> Dict[str, IndicatorMetadata]:
        """List all available indicators"""
        all_indicators = {}

        for key, metadata in MacroIndicator.INDICATORS_CATALOG.items():
            all_indicators[key] = metadata

        for key, metadata in FinancialIndicator.INDICATORS_CATALOG.items():
            all_indicators[key] = metadata

        return all_indicators

    def validate_data_quality(self, data: IndicatorData) -> Dict[str, Any]:
        """
        Validate data quality

        Returns:
            Dictionary with quality assessment
        """
        quality_report = {
            'indicator': data.metadata.indicator_name,
            'n_observations': len(data.values),
            'completeness': 1.0,  # Fraction of non-missing data
            'issues': []
        }

        # Check for missing values
        n_missing = np.sum(np.isnan(data.values))
        if n_missing > 0:
            quality_report['completeness'] = 1 - (n_missing / len(data.values))
            quality_report['issues'].append(f"{n_missing} missing observations")

        # Check for outliers (simple z-score method)
        z_scores = np.abs((data.values - np.nanmean(data.values)) / np.nanstd(data.values))
        n_outliers = np.sum(z_scores > 3)
        if n_outliers > 0:
            quality_report['issues'].append(f"{n_outliers} potential outliers detected")

        # Check data recency
        if data.dates:
            latest_date = data.dates[-1]
            days_old = (datetime.now() - latest_date).days
            quality_report['days_since_update'] = days_old

            if days_old > 60:
                quality_report['issues'].append(f"Data is {days_old} days old")

        # Overall quality score
        if len(quality_report['issues']) == 0:
            quality_report['overall_quality'] = DataQuality.HIGH
        elif len(quality_report['issues']) <= 2:
            quality_report['overall_quality'] = DataQuality.MEDIUM
        else:
            quality_report['overall_quality'] = DataQuality.LOW

        return quality_report
