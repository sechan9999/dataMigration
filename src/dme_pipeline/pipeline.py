"""
DME Medicare Fraud Detection Pipeline

Main orchestration module that processes claims data through multiple
fraud detection algorithms following CMS UPIC methodology.

Usage:
    pipeline = DMEFraudDetectionPipeline()
    results = pipeline.run(claims, providers)
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Optional, Any
from collections import defaultdict
import json

from .models import DMEClaim, Provider, FraudAlert, RiskLevel, FraudType
from .detectors import (
    BaseFraudDetector,
    UpcodingDetector,
    PhantomBillingDetector,
    VolumeAnomalyDetector,
    KickbackPatternDetector,
    HitAndRunDetector,
    CompositeFraudDetector,
)


@dataclass
class PipelineConfig:
    """Configuration for the fraud detection pipeline"""
    # Detection thresholds
    min_claims_for_analysis: int = 10
    confidence_threshold: float = 0.5
    max_alerts_per_provider: int = 10

    # Detector settings
    enable_upcoding_detection: bool = True
    enable_phantom_billing_detection: bool = True
    enable_volume_anomaly_detection: bool = True
    enable_kickback_detection: bool = True
    enable_hit_and_run_detection: bool = True

    # Output settings
    include_low_risk: bool = False
    generate_summary: bool = True


@dataclass
class PipelineResult:
    """Results from fraud detection pipeline run"""
    alerts: List[FraudAlert] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    providers_analyzed: int = 0
    claims_processed: int = 0
    execution_time_seconds: float = 0.0
    errors: List[str] = field(default_factory=list)

    def get_high_priority_alerts(self, min_priority: int = 70) -> List[FraudAlert]:
        """Get alerts above priority threshold"""
        return [a for a in self.alerts if a.priority_score >= min_priority]

    def get_alerts_by_type(self, fraud_type: FraudType) -> List[FraudAlert]:
        """Filter alerts by fraud type"""
        return [a for a in self.alerts if a.fraud_type == fraud_type]

    def get_alerts_by_provider(self, npi: str) -> List[FraudAlert]:
        """Get all alerts for a specific provider"""
        return [a for a in self.alerts if a.provider_npi == npi]

    def to_json(self) -> str:
        """Export results to JSON"""
        return json.dumps({
            "summary": self.summary,
            "providers_analyzed": self.providers_analyzed,
            "claims_processed": self.claims_processed,
            "execution_time_seconds": self.execution_time_seconds,
            "total_alerts": len(self.alerts),
            "alerts": [a.to_dict() for a in self.alerts],
            "errors": self.errors,
        }, indent=2, default=str)


class DMEFraudDetectionPipeline:
    """
    Main pipeline for Medicare DME fraud detection

    Implements multi-stage detection process:
    1. Data validation and preprocessing
    2. Provider profiling
    3. Multi-detector analysis
    4. Alert aggregation and prioritization
    5. Report generation

    Example:
        >>> from dme_pipeline import DMEFraudDetectionPipeline, DMEClaim
        >>> pipeline = DMEFraudDetectionPipeline()
        >>> claims = [DMEClaim(...), ...]
        >>> results = pipeline.run(claims)
        >>> print(f"Found {len(results.alerts)} potential fraud cases")
    """

    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or PipelineConfig()
        self.detectors: List[BaseFraudDetector] = []
        self._initialize_detectors()

    def _initialize_detectors(self):
        """Initialize enabled fraud detectors"""
        if self.config.enable_upcoding_detection:
            self.detectors.append(UpcodingDetector())
        if self.config.enable_phantom_billing_detection:
            self.detectors.append(PhantomBillingDetector())
        if self.config.enable_volume_anomaly_detection:
            self.detectors.append(VolumeAnomalyDetector())
        if self.config.enable_kickback_detection:
            self.detectors.append(KickbackPatternDetector())
        if self.config.enable_hit_and_run_detection:
            self.detectors.append(HitAndRunDetector())

    def run(
        self,
        claims: List[DMEClaim],
        providers: Optional[Dict[str, Provider]] = None,
        analysis_date: Optional[datetime] = None,
    ) -> PipelineResult:
        """
        Execute the fraud detection pipeline

        Args:
            claims: List of DME claims to analyze
            providers: Optional dict of provider info keyed by NPI
            analysis_date: Date for analysis context (defaults to now)

        Returns:
            PipelineResult with all detected alerts and summary
        """
        start_time = datetime.now()
        result = PipelineResult()

        try:
            # Stage 1: Validate and preprocess
            valid_claims = self._preprocess_claims(claims)
            result.claims_processed = len(valid_claims)

            if len(valid_claims) < self.config.min_claims_for_analysis:
                result.errors.append(
                    f"Insufficient claims for analysis: {len(valid_claims)} < {self.config.min_claims_for_analysis}"
                )
                return result

            # Stage 2: Build provider profiles if not provided
            if providers is None:
                providers = self._build_provider_profiles(valid_claims)
            result.providers_analyzed = len(providers)

            # Stage 3: Run all detectors
            all_alerts = []
            for detector in self.detectors:
                try:
                    detector_alerts = detector.detect(
                        valid_claims,
                        providers,
                        analysis_date=analysis_date or datetime.now().date(),
                    )
                    all_alerts.extend(detector_alerts)
                except Exception as e:
                    result.errors.append(f"{detector.name} error: {str(e)}")

            # Stage 4: Filter and deduplicate alerts
            filtered_alerts = self._filter_alerts(all_alerts)

            # Stage 5: Limit alerts per provider
            result.alerts = self._limit_alerts_per_provider(filtered_alerts)

            # Stage 6: Generate summary
            if self.config.generate_summary:
                result.summary = self._generate_summary(result.alerts, valid_claims, providers)

        except Exception as e:
            result.errors.append(f"Pipeline error: {str(e)}")

        result.execution_time_seconds = (datetime.now() - start_time).total_seconds()
        return result

    def _preprocess_claims(self, claims: List[DMEClaim]) -> List[DMEClaim]:
        """Validate and clean claims data"""
        valid_claims = []
        for claim in claims:
            # Basic validation
            if not claim.provider_npi:
                continue
            if claim.billed_amount <= 0:
                continue
            valid_claims.append(claim)
        return valid_claims

    def _build_provider_profiles(self, claims: List[DMEClaim]) -> Dict[str, Provider]:
        """Build provider profiles from claims data"""
        profiles = {}
        provider_claims = defaultdict(list)

        for claim in claims:
            provider_claims[claim.provider_npi].append(claim)

        for npi, pclaims in provider_claims.items():
            total_amount = sum(c.billed_amount for c in pclaims)
            unique_beneficiaries = len(set(c.beneficiary_id for c in pclaims))

            profiles[npi] = Provider(
                npi=npi,
                total_claims_ytd=len(pclaims),
                total_amount_ytd=total_amount,
                avg_claim_amount=total_amount / len(pclaims) if pclaims else 0,
                unique_beneficiaries=unique_beneficiaries,
            )

        return profiles

    def _filter_alerts(self, alerts: List[FraudAlert]) -> List[FraudAlert]:
        """Filter alerts based on configuration"""
        filtered = []
        for alert in alerts:
            # Apply confidence threshold
            if alert.confidence_score < self.config.confidence_threshold:
                continue

            # Optionally exclude low risk
            if not self.config.include_low_risk and alert.risk_level == RiskLevel.LOW:
                continue

            filtered.append(alert)

        # Sort by priority
        filtered.sort(key=lambda a: a.priority_score, reverse=True)
        return filtered

    def _limit_alerts_per_provider(self, alerts: List[FraudAlert]) -> List[FraudAlert]:
        """Limit number of alerts per provider to avoid flooding"""
        provider_counts = defaultdict(int)
        limited = []

        for alert in alerts:
            if alert.provider_npi:
                if provider_counts[alert.provider_npi] >= self.config.max_alerts_per_provider:
                    continue
                provider_counts[alert.provider_npi] += 1
            limited.append(alert)

        return limited

    def _generate_summary(
        self,
        alerts: List[FraudAlert],
        claims: List[DMEClaim],
        providers: Dict[str, Provider],
    ) -> Dict[str, Any]:
        """Generate summary statistics"""
        # Count by risk level
        risk_counts = defaultdict(int)
        for alert in alerts:
            risk_counts[alert.risk_level.value] += 1

        # Count by fraud type
        type_counts = defaultdict(int)
        for alert in alerts:
            type_counts[alert.fraud_type.value] += 1

        # Top flagged providers
        provider_alert_counts = defaultdict(int)
        for alert in alerts:
            if alert.provider_npi:
                provider_alert_counts[alert.provider_npi] += 1

        top_providers = sorted(
            provider_alert_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )[:10]

        # Financial exposure estimate
        flagged_claim_ids = set()
        for alert in alerts:
            flagged_claim_ids.update(alert.claim_ids)

        flagged_amount = sum(
            c.billed_amount for c in claims
            if c.claim_id in flagged_claim_ids
        )

        return {
            "total_alerts": len(alerts),
            "by_risk_level": dict(risk_counts),
            "by_fraud_type": dict(type_counts),
            "top_flagged_providers": [
                {"npi": npi, "alert_count": count}
                for npi, count in top_providers
            ],
            "flagged_claims_count": len(flagged_claim_ids),
            "flagged_amount_total": flagged_amount,
            "claims_analyzed": len(claims),
            "providers_analyzed": len(providers),
        }

    def add_detector(self, detector: BaseFraudDetector):
        """Add a custom detector to the pipeline"""
        self.detectors.append(detector)

    def remove_detector(self, detector_name: str):
        """Remove a detector by name"""
        self.detectors = [d for d in self.detectors if d.name != detector_name]


class StreamingPipeline(DMEFraudDetectionPipeline):
    """
    Streaming version of the pipeline for real-time fraud detection

    Processes claims in batches and maintains state across batches.
    Suitable for integration with Kafka, Spark Streaming, etc.
    """

    def __init__(self, config: Optional[PipelineConfig] = None, batch_size: int = 1000):
        super().__init__(config)
        self.batch_size = batch_size
        self.claims_buffer: List[DMEClaim] = []
        self.provider_profiles: Dict[str, Provider] = {}
        self.historical_alerts: List[FraudAlert] = []

    def process_claim(self, claim: DMEClaim) -> Optional[PipelineResult]:
        """
        Process a single claim, triggering analysis when batch is full

        Returns PipelineResult when batch is processed, None otherwise
        """
        self.claims_buffer.append(claim)

        if len(self.claims_buffer) >= self.batch_size:
            return self.flush()

        return None

    def flush(self) -> PipelineResult:
        """Process all buffered claims"""
        if not self.claims_buffer:
            return PipelineResult()

        result = self.run(self.claims_buffer, self.provider_profiles)

        # Update historical state
        self.historical_alerts.extend(result.alerts)

        # Update provider profiles
        for claim in self.claims_buffer:
            if claim.provider_npi not in self.provider_profiles:
                self.provider_profiles[claim.provider_npi] = Provider(npi=claim.provider_npi)
            profile = self.provider_profiles[claim.provider_npi]
            profile.total_claims_ytd += 1
            profile.total_amount_ytd += claim.billed_amount

        # Clear buffer
        self.claims_buffer = []

        return result
