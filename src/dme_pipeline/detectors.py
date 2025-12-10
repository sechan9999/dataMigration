"""
Fraud Detection Algorithms for DME Medicare Claims

Implements detection methodologies based on:
- CMS Program Integrity Manual (PIM) Chapter 2: Data Analysis
- UPIC investigation patterns
- OIG audit findings

Reference: https://www.cms.gov/regulations-and-guidance/guidance/manuals/downloads/pim83c02.pdf
"""

from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import statistics
import math

from .models import (
    DMEClaim,
    Provider,
    FraudAlert,
    FraudType,
    RiskLevel,
    HIGH_RISK_HCPCS,
    HCPCS_DME_CATEGORIES,
)


class BaseFraudDetector(ABC):
    """
    Base class for all fraud detection algorithms

    Following CMS PIM Chapter 2 methodology:
    1. Identify billing patterns/trends
    2. Compare against expected norms
    3. Flag statistical outliers
    4. Generate investigation leads
    """

    def __init__(self, name: str, version: str = "1.0.0"):
        self.name = name
        self.version = version

    @abstractmethod
    def detect(
        self,
        claims: List[DMEClaim],
        providers: Dict[str, Provider],
        **kwargs
    ) -> List[FraudAlert]:
        """Run detection algorithm and return fraud alerts"""
        pass

    def calculate_zscore(self, value: float, mean: float, std: float) -> float:
        """Calculate z-score for outlier detection"""
        if std == 0:
            return 0.0
        return (value - mean) / std

    def calculate_percentile(self, value: float, values: List[float]) -> float:
        """Calculate percentile rank of a value"""
        if not values:
            return 0.0
        sorted_values = sorted(values)
        count_below = sum(1 for v in sorted_values if v < value)
        return (count_below / len(sorted_values)) * 100

    def create_alert(
        self,
        fraud_type: FraudType,
        risk_level: RiskLevel,
        confidence: float,
        provider_npi: Optional[str] = None,
        claim_ids: List[str] = None,
        description: str = "",
        evidence: str = "",
        rules: List[str] = None,
        observed: float = None,
        expected: float = None,
        std_devs: float = None,
        action: str = "",
    ) -> FraudAlert:
        """Helper to create consistent fraud alerts"""
        return FraudAlert(
            fraud_type=fraud_type,
            risk_level=risk_level,
            confidence_score=confidence,
            provider_npi=provider_npi,
            claim_ids=claim_ids or [],
            description=description,
            evidence_summary=evidence,
            detection_rules_triggered=rules or [],
            observed_value=observed,
            expected_value=expected,
            standard_deviations=std_devs,
            detector_name=self.name,
            detector_version=self.version,
            recommended_action=action,
            priority_score=self._calculate_priority(risk_level, confidence),
        )

    def _calculate_priority(self, risk_level: RiskLevel, confidence: float) -> int:
        """Calculate priority score 1-100"""
        base_scores = {
            RiskLevel.LOW: 10,
            RiskLevel.MEDIUM: 40,
            RiskLevel.HIGH: 70,
            RiskLevel.CRITICAL: 90,
        }
        base = base_scores.get(risk_level, 10)
        return min(100, int(base + (confidence * 10)))


class UpcodingDetector(BaseFraudDetector):
    """
    Detect upcoding patterns - billing for more expensive equipment than provided

    Key indicators:
    - Provider bills higher-priced HCPCS codes at rates significantly above peers
    - Sudden shift from lower to higher-priced codes
    - High percentage of claims at top tier of equipment categories
    """

    def __init__(self):
        super().__init__("UpcodingDetector", "1.0.0")
        # Define code tiers within categories (lower index = lower cost)
        self.wheelchair_tiers = {
            "K0001": 1, "K0002": 1, "K0003": 2, "K0004": 2,
            "K0005": 3, "K0006": 3, "K0007": 4, "K0008": 4,
            "K0823": 5, "K0824": 5, "K0825": 5,  # Power wheelchairs
        }
        self.oxygen_tiers = {
            "E0424": 1, "E0431": 2, "E0433": 2,
            "E0434": 3, "E0439": 3, "E0441": 4, "E0442": 4,
        }

    def detect(
        self,
        claims: List[DMEClaim],
        providers: Dict[str, Provider],
        **kwargs
    ) -> List[FraudAlert]:
        alerts = []

        # Group claims by provider
        provider_claims = defaultdict(list)
        for claim in claims:
            provider_claims[claim.provider_npi].append(claim)

        # Calculate peer averages for tier distribution
        all_tier_ratios = []
        provider_tier_data = {}

        for npi, pclaims in provider_claims.items():
            wheelchair_claims = [c for c in pclaims if c.hcpcs_code in self.wheelchair_tiers]
            if len(wheelchair_claims) >= 10:  # Minimum sample size
                high_tier = sum(1 for c in wheelchair_claims
                              if self.wheelchair_tiers.get(c.hcpcs_code, 0) >= 4)
                ratio = high_tier / len(wheelchair_claims)
                all_tier_ratios.append(ratio)
                provider_tier_data[npi] = (ratio, wheelchair_claims)

        if not all_tier_ratios:
            return alerts

        mean_ratio = statistics.mean(all_tier_ratios)
        std_ratio = statistics.stdev(all_tier_ratios) if len(all_tier_ratios) > 1 else 0.1

        # Flag outliers
        for npi, (ratio, pclaims) in provider_tier_data.items():
            zscore = self.calculate_zscore(ratio, mean_ratio, std_ratio)

            if zscore > 2.0:  # More than 2 standard deviations above mean
                risk_level = RiskLevel.HIGH if zscore > 3.0 else RiskLevel.MEDIUM
                confidence = min(0.95, 0.5 + (zscore - 2.0) * 0.15)

                alerts.append(self.create_alert(
                    fraud_type=FraudType.UPCODING,
                    risk_level=risk_level,
                    confidence=confidence,
                    provider_npi=npi,
                    claim_ids=[c.claim_id for c in pclaims[:20]],
                    description=f"Provider shows {ratio*100:.1f}% high-tier wheelchair claims vs peer average of {mean_ratio*100:.1f}%",
                    evidence=f"Z-score: {zscore:.2f}, Sample size: {len(pclaims)} claims",
                    rules=["UPCODE-WHEELCHAIR-TIER", "PEER-COMPARISON"],
                    observed=ratio * 100,
                    expected=mean_ratio * 100,
                    std_devs=zscore,
                    action="Review medical necessity documentation for high-tier equipment claims",
                ))

        return alerts


class PhantomBillingDetector(BaseFraudDetector):
    """
    Detect phantom billing - billing for equipment never delivered

    Key indicators:
    - Missing or invalid delivery documentation
    - Beneficiary at multiple distant locations same day
    - Equipment delivered to deceased beneficiaries
    - Unusually high delivery volumes per day
    """

    def __init__(self, max_daily_deliveries: int = 50):
        super().__init__("PhantomBillingDetector", "1.0.0")
        self.max_daily_deliveries = max_daily_deliveries

    def detect(
        self,
        claims: List[DMEClaim],
        providers: Dict[str, Provider],
        **kwargs
    ) -> List[FraudAlert]:
        alerts = []

        # Group claims by provider and date
        provider_date_claims = defaultdict(lambda: defaultdict(list))
        for claim in claims:
            if claim.service_date:
                provider_date_claims[claim.provider_npi][claim.service_date].append(claim)

        for npi, date_claims in provider_date_claims.items():
            # Check for abnormally high daily volumes
            daily_volumes = [len(claims_list) for claims_list in date_claims.values()]
            if not daily_volumes:
                continue

            mean_volume = statistics.mean(daily_volumes)
            max_volume = max(daily_volumes)

            # Flag days with suspiciously high volume
            for service_date, claims_list in date_claims.items():
                volume = len(claims_list)

                if volume > self.max_daily_deliveries:
                    # Calculate unique beneficiaries - phantom billing often reuses beneficiary IDs
                    unique_beneficiaries = len(set(c.beneficiary_id for c in claims_list))
                    beneficiary_ratio = unique_beneficiaries / volume

                    risk_level = RiskLevel.CRITICAL if volume > 100 else RiskLevel.HIGH
                    confidence = min(0.95, 0.6 + (volume / 200))

                    alerts.append(self.create_alert(
                        fraud_type=FraudType.PHANTOM_BILLING,
                        risk_level=risk_level,
                        confidence=confidence,
                        provider_npi=npi,
                        claim_ids=[c.claim_id for c in claims_list],
                        description=f"{volume} claims on {service_date} exceeds physical delivery capacity",
                        evidence=f"Unique beneficiaries: {unique_beneficiaries}, Ratio: {beneficiary_ratio:.2f}",
                        rules=["PHANTOM-VOLUME-LIMIT", "DAILY-DELIVERY-CAP"],
                        observed=volume,
                        expected=self.max_daily_deliveries,
                        action="Request proof of delivery documentation for all claims on this date",
                    ))

        return alerts


class VolumeAnomalyDetector(BaseFraudDetector):
    """
    Detect unusual billing volume patterns

    Key indicators (per CMS PIM Chapter 2):
    - Sudden spikes in claim volume
    - Volume exceeds peer benchmarks by significant margin
    - High concentration in specific HCPCS codes
    - Weekend/holiday billing anomalies
    """

    def __init__(self, spike_threshold: float = 3.0):
        super().__init__("VolumeAnomalyDetector", "1.0.0")
        self.spike_threshold = spike_threshold

    def detect(
        self,
        claims: List[DMEClaim],
        providers: Dict[str, Provider],
        **kwargs
    ) -> List[FraudAlert]:
        alerts = []

        # Group claims by provider and month
        provider_monthly = defaultdict(lambda: defaultdict(list))
        for claim in claims:
            if claim.service_date:
                month_key = claim.service_date.strftime("%Y-%m")
                provider_monthly[claim.provider_npi][month_key].append(claim)

        for npi, monthly_claims in provider_monthly.items():
            if len(monthly_claims) < 3:  # Need history for comparison
                continue

            # Calculate monthly volumes
            sorted_months = sorted(monthly_claims.keys())
            volumes = [len(monthly_claims[m]) for m in sorted_months]

            # Check for volume spikes
            for i, month in enumerate(sorted_months):
                if i < 2:  # Need baseline
                    continue

                baseline = statistics.mean(volumes[:i])
                baseline_std = statistics.stdev(volumes[:i]) if i > 1 else baseline * 0.2
                current = volumes[i]

                zscore = self.calculate_zscore(current, baseline, baseline_std)

                if zscore > self.spike_threshold:
                    # Additional check: concentration in high-risk codes
                    month_claims = monthly_claims[month]
                    high_risk_count = sum(1 for c in month_claims
                                        if c.hcpcs_code in HIGH_RISK_HCPCS)
                    high_risk_ratio = high_risk_count / len(month_claims) if month_claims else 0

                    risk_level = RiskLevel.HIGH if zscore > 4.0 else RiskLevel.MEDIUM
                    if high_risk_ratio > 0.5:
                        risk_level = RiskLevel.CRITICAL

                    confidence = min(0.95, 0.5 + (zscore - self.spike_threshold) * 0.1)

                    alerts.append(self.create_alert(
                        fraud_type=FraudType.HIT_AND_RUN if zscore > 5.0 else FraudType.UPCODING,
                        risk_level=risk_level,
                        confidence=confidence,
                        provider_npi=npi,
                        claim_ids=[c.claim_id for c in month_claims[:30]],
                        description=f"Volume spike in {month}: {current} claims vs baseline {baseline:.0f}",
                        evidence=f"Z-score: {zscore:.2f}, High-risk HCPCS ratio: {high_risk_ratio*100:.1f}%",
                        rules=["VOLUME-SPIKE", "MONTHLY-TREND", "HIGH-RISK-CONCENTRATION"],
                        observed=current,
                        expected=baseline,
                        std_devs=zscore,
                        action="Immediate review - potential hit-and-run scheme if new provider",
                    ))

        return alerts


class KickbackPatternDetector(BaseFraudDetector):
    """
    Detect patterns suggestive of kickback arrangements

    Key indicators:
    - Single provider receiving disproportionate referrals from one physician
    - Geographic mismatch between referring physician and supplier
    - Suspicious beneficiary acquisition patterns
    """

    def __init__(self, referral_concentration_threshold: float = 0.6):
        super().__init__("KickbackPatternDetector", "1.0.0")
        self.threshold = referral_concentration_threshold

    def detect(
        self,
        claims: List[DMEClaim],
        providers: Dict[str, Provider],
        **kwargs
    ) -> List[FraudAlert]:
        alerts = []

        # Analyze referral patterns
        provider_referrers = defaultdict(lambda: defaultdict(int))
        for claim in claims:
            if claim.ordering_physician_npi:
                provider_referrers[claim.provider_npi][claim.ordering_physician_npi] += 1

        for npi, referrer_counts in provider_referrers.items():
            total_claims = sum(referrer_counts.values())
            if total_claims < 20:  # Minimum sample
                continue

            # Check for concentration
            for physician_npi, count in referrer_counts.items():
                concentration = count / total_claims

                if concentration > self.threshold:
                    risk_level = RiskLevel.HIGH if concentration > 0.8 else RiskLevel.MEDIUM
                    confidence = min(0.9, concentration)

                    alerts.append(self.create_alert(
                        fraud_type=FraudType.KICKBACK,
                        risk_level=risk_level,
                        confidence=confidence,
                        provider_npi=npi,
                        description=f"{concentration*100:.1f}% of claims from single ordering physician {physician_npi}",
                        evidence=f"Total claims: {total_claims}, From this physician: {count}",
                        rules=["REFERRAL-CONCENTRATION", "KICKBACK-INDICATOR"],
                        observed=concentration * 100,
                        expected=self.threshold * 100,
                        action="Investigate relationship between supplier and ordering physician",
                    ))

        return alerts


class HitAndRunDetector(BaseFraudDetector):
    """
    Detect hit-and-run schemes - intensive billing followed by abandonment

    Key indicators (per CMS PIM):
    - New provider with immediate high volume
    - Short operational period with maximum billing
    - Ownership/address changes before billing spike
    - Sudden cessation of activity
    """

    def __init__(self, new_provider_months: int = 6, volume_percentile: float = 95.0):
        super().__init__("HitAndRunDetector", "1.0.0")
        self.new_provider_months = new_provider_months
        self.volume_percentile = volume_percentile

    def detect(
        self,
        claims: List[DMEClaim],
        providers: Dict[str, Provider],
        **kwargs
    ) -> List[FraudAlert]:
        alerts = []
        analysis_date = kwargs.get("analysis_date", datetime.now().date())

        # Calculate peer volume distribution
        provider_volumes = defaultdict(float)
        provider_first_claim = {}
        provider_last_claim = {}

        for claim in claims:
            npi = claim.provider_npi
            provider_volumes[npi] += claim.billed_amount

            if claim.service_date:
                if npi not in provider_first_claim or claim.service_date < provider_first_claim[npi]:
                    provider_first_claim[npi] = claim.service_date
                if npi not in provider_last_claim or claim.service_date > provider_last_claim[npi]:
                    provider_last_claim[npi] = claim.service_date

        all_volumes = list(provider_volumes.values())
        if not all_volumes:
            return alerts

        volume_threshold = sorted(all_volumes)[int(len(all_volumes) * self.volume_percentile / 100)]

        for npi, total_volume in provider_volumes.items():
            provider = providers.get(npi)
            first_claim = provider_first_claim.get(npi)
            last_claim = provider_last_claim.get(npi)

            if not first_claim or not last_claim:
                continue

            # Check if relatively new provider
            operational_days = (last_claim - first_claim).days + 1
            months_active = operational_days / 30

            is_new = months_active <= self.new_provider_months
            is_high_volume = total_volume >= volume_threshold
            daily_rate = total_volume / operational_days if operational_days > 0 else 0

            # Check for recent cessation (might indicate hit-and-run)
            days_since_last = (analysis_date - last_claim).days if last_claim else 0
            appears_inactive = days_since_last > 60

            if is_new and is_high_volume:
                risk_level = RiskLevel.CRITICAL if appears_inactive else RiskLevel.HIGH
                confidence = min(0.95, 0.7 + (total_volume / volume_threshold - 1) * 0.1)

                description = f"New provider ({months_active:.1f} months) with top {100-self.volume_percentile:.0f}% billing volume"
                if appears_inactive:
                    description += f" - No claims for {days_since_last} days (possible abandonment)"

                alerts.append(self.create_alert(
                    fraud_type=FraudType.HIT_AND_RUN,
                    risk_level=risk_level,
                    confidence=confidence,
                    provider_npi=npi,
                    description=description,
                    evidence=f"Total billed: ${total_volume:,.2f}, Daily rate: ${daily_rate:,.2f}",
                    rules=["HIT-RUN-NEW-PROVIDER", "HIGH-VOLUME-SHORT-TENURE"],
                    observed=total_volume,
                    expected=volume_threshold,
                    action="Prioritize for immediate investigation - high flight risk",
                ))

        return alerts


class CompositeFraudDetector(BaseFraudDetector):
    """
    Combines multiple detectors and aggregates risk scores
    """

    def __init__(self):
        super().__init__("CompositeFraudDetector", "1.0.0")
        self.detectors = [
            UpcodingDetector(),
            PhantomBillingDetector(),
            VolumeAnomalyDetector(),
            KickbackPatternDetector(),
            HitAndRunDetector(),
        ]

    def detect(
        self,
        claims: List[DMEClaim],
        providers: Dict[str, Provider],
        **kwargs
    ) -> List[FraudAlert]:
        all_alerts = []

        for detector in self.detectors:
            try:
                alerts = detector.detect(claims, providers, **kwargs)
                all_alerts.extend(alerts)
            except Exception as e:
                print(f"Warning: {detector.name} failed: {e}")

        # Sort by priority
        all_alerts.sort(key=lambda a: a.priority_score, reverse=True)

        return all_alerts
