"""
Exponential Backoff Strategy Implementation
Provides sophisticated retry logic with jitter to prevent thundering herd
"""

import random
import math
from typing import Optional, Dict, Any
from dataclasses import dataclass


@dataclass
class BackoffConfig:
    """Configuration for exponential backoff"""
    initial_delay_seconds: float = 60.0
    max_delay_seconds: float = 3600.0
    exponential_base: float = 2.0
    max_retries: int = 5
    jitter_factor: float = 0.1  # 10% randomness


class ExponentialBackoffStrategy:
    """
    Implements exponential backoff with jitter for retry scheduling.

    Formula: delay = min(initial_delay * (base ^ retry_count), max_delay) * (1 ± jitter)

    This prevents thundering herd problem where many failed tasks
    retry simultaneously and overwhelm the system.
    """

    def __init__(self, config: BackoffConfig):
        self.config = config

    def calculate_backoff(self, retry_count: int) -> float:
        """
        Calculate backoff delay for given retry attempt.

        Args:
            retry_count: Number of previous retry attempts (0-indexed)

        Returns:
            Backoff delay in seconds with jitter applied
        """
        if retry_count < 0:
            retry_count = 0

        # Calculate base exponential delay
        base_delay = self.config.initial_delay_seconds * (
            self.config.exponential_base ** retry_count
        )

        # Cap at maximum delay
        capped_delay = min(base_delay, self.config.max_delay_seconds)

        # Add jitter to prevent thundering herd
        jittered_delay = self._apply_jitter(capped_delay)

        return jittered_delay

    def _apply_jitter(self, delay: float) -> float:
        """
        Apply jitter to delay to prevent synchronized retries.

        Uses full jitter approach: random value between 0 and calculated delay.
        This is more aggressive than decorrelated jitter but prevents clustering.
        """
        if self.config.jitter_factor <= 0:
            return delay

        # Full jitter: random value in range [delay * (1-jitter), delay * (1+jitter)]
        min_delay = delay * (1 - self.config.jitter_factor)
        max_delay = delay * (1 + self.config.jitter_factor)

        return random.uniform(min_delay, max_delay)

    def should_retry(self, retry_count: int) -> bool:
        """
        Determine if task should be retried based on retry count.

        Args:
            retry_count: Number of previous retry attempts

        Returns:
            True if should retry, False if max retries exceeded
        """
        return retry_count < self.config.max_retries

    def get_retry_schedule(self) -> list[Dict[str, Any]]:
        """
        Get complete retry schedule for visualization/debugging.

        Returns:
            List of retry attempts with delays
        """
        schedule = []
        for attempt in range(self.config.max_retries):
            delay = self.calculate_backoff(attempt)
            schedule.append({
                "attempt": attempt + 1,
                "delay_seconds": round(delay, 2),
                "delay_minutes": round(delay / 60, 2),
                "cumulative_minutes": round(sum(
                    self.calculate_backoff(i) for i in range(attempt + 1)
                ) / 60, 2)
            })
        return schedule

    def estimate_total_retry_time(self) -> float:
        """
        Estimate total time for all retries (worst case).

        Returns:
            Total seconds for all retry attempts
        """
        return sum(
            self.calculate_backoff(i) for i in range(self.config.max_retries)
        )


class NetworkBackoffStrategy:
    """
    Specialized backoff for network failures with more aggressive retries.
    Network issues are often transient, so we retry faster initially.
    """

    def __init__(self, backoff_seconds: list[float] = None):
        """
        Args:
            backoff_seconds: Custom backoff schedule (default: [2, 4, 8, 16])
        """
        self.backoff_schedule = backoff_seconds or [2, 4, 8, 16]
        self.max_retries = len(self.backoff_schedule)

    def calculate_backoff(self, retry_count: int) -> float:
        """Get backoff delay for network retry attempt"""
        if retry_count >= len(self.backoff_schedule):
            return self.backoff_schedule[-1]
        return self.backoff_schedule[retry_count]

    def should_retry(self, retry_count: int) -> bool:
        """Check if should retry network operation"""
        return retry_count < self.max_retries


class AdaptiveBackoffStrategy(ExponentialBackoffStrategy):
    """
    Adaptive backoff that adjusts based on error type and system load.
    Provides more sophisticated retry logic for production systems.
    """

    # Error type multipliers
    ERROR_TYPE_MULTIPLIERS = {
        "network_timeout": 0.5,      # Faster retry for network issues
        "rate_limit": 2.0,           # Slower retry for rate limits
        "resource_exhausted": 3.0,   # Much slower for resource issues
        "data_corruption": 5.0,      # Very slow for data issues
        "unknown": 1.0               # Standard retry
    }

    def __init__(self, config: BackoffConfig):
        super().__init__(config)
        self.recent_failures = []  # Track recent system failures
        self.max_recent_tracking = 100

    def calculate_backoff(
        self,
        retry_count: int,
        error_type: Optional[str] = None,
        system_load: Optional[float] = None
    ) -> float:
        """
        Calculate adaptive backoff based on error type and system load.

        Args:
            retry_count: Number of retry attempts
            error_type: Type of error (for custom multiplier)
            system_load: Current system load factor (0.0 to 1.0)

        Returns:
            Adjusted backoff delay in seconds
        """
        # Get base backoff
        base_backoff = super().calculate_backoff(retry_count)

        # Apply error type multiplier
        if error_type:
            multiplier = self.ERROR_TYPE_MULTIPLIERS.get(
                error_type,
                self.ERROR_TYPE_MULTIPLIERS["unknown"]
            )
            base_backoff *= multiplier

        # Apply system load factor (increase delay during high load)
        if system_load is not None:
            load_multiplier = 1.0 + system_load
            base_backoff *= load_multiplier

        # Cap at max delay
        return min(base_backoff, self.config.max_delay_seconds)

    def record_failure(self, error_type: str):
        """Record a failure for adaptive adjustment"""
        self.recent_failures.append({
            "error_type": error_type,
            "timestamp": math.floor(random.random() * 1000)  # Simplified timestamp
        })

        # Keep only recent failures
        if len(self.recent_failures) > self.max_recent_tracking:
            self.recent_failures = self.recent_failures[-self.max_recent_tracking:]

    def get_failure_rate(self) -> float:
        """Calculate recent failure rate (simplified)"""
        if not self.recent_failures:
            return 0.0
        return len(self.recent_failures) / self.max_recent_tracking


# Factory function for creating appropriate strategy
def create_backoff_strategy(
    strategy_type: str = "exponential",
    config: Optional[BackoffConfig] = None
) -> ExponentialBackoffStrategy:
    """
    Factory function to create backoff strategy.

    Args:
        strategy_type: Type of strategy ("exponential", "adaptive", "network")
        config: Configuration for backoff behavior

    Returns:
        Appropriate backoff strategy instance
    """
    if config is None:
        config = BackoffConfig()

    if strategy_type == "adaptive":
        return AdaptiveBackoffStrategy(config)
    elif strategy_type == "network":
        return NetworkBackoffStrategy()
    else:
        return ExponentialBackoffStrategy(config)
