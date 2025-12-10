#!/usr/bin/env python3
"""
DME Medicare Fraud Detection - Local Demo Script

이 스크립트는 DME (Durable Medical Equipment) Medicare 사기 탐지 파이프라인을
로컬에서 테스트하고 시연하는 방법을 보여줍니다.

실행 방법:
    python examples/run_dme_fraud_detection.py

Author: Medicare Fraud Detection Team
"""

import sys
import json
from pathlib import Path
from datetime import date

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.dme_pipeline import (
    DMEFraudDetectionPipeline,
    DMEClaim,
    Provider,
    FraudAlert,
)
from src.dme_pipeline.pipeline import PipelineConfig, PipelineResult
from src.dme_pipeline.data_loader import SyntheticDataGenerator, CMSDataLoader
from src.dme_pipeline.models import FraudType, RiskLevel


def print_header(title: str):
    """Print formatted section header"""
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def print_alert(alert: FraudAlert, index: int):
    """Print formatted fraud alert"""
    risk_colors = {
        RiskLevel.LOW: "🟢",
        RiskLevel.MEDIUM: "🟡",
        RiskLevel.HIGH: "🟠",
        RiskLevel.CRITICAL: "🔴",
    }
    icon = risk_colors.get(alert.risk_level, "⚪")

    print(f"\n  [{index}] {icon} {alert.fraud_type.value.upper()}")
    print(f"      Risk: {alert.risk_level.value} | Confidence: {alert.confidence_score:.0%}")
    print(f"      Provider NPI: {alert.provider_npi}")
    print(f"      Description: {alert.description}")
    if alert.observed_value and alert.expected_value:
        print(f"      Observed: {alert.observed_value:.1f} | Expected: {alert.expected_value:.1f}")
    print(f"      Action: {alert.recommended_action}")


def demo_basic_usage():
    """Demo 1: Basic pipeline usage with synthetic data"""
    print_header("Demo 1: Basic Pipeline Usage")

    print("\n📊 Generating synthetic Medicare DME claims data...")
    generator = SyntheticDataGenerator(seed=42)

    # Generate 1000 claims with 10% fraud rate
    claims = generator.generate_claims(
        count=1000,
        fraud_rate=0.10,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 6, 30),
    )
    print(f"   Generated {len(claims)} claims")

    # Run the fraud detection pipeline
    print("\n🔍 Running fraud detection pipeline...")
    pipeline = DMEFraudDetectionPipeline()
    result = pipeline.run(claims)

    # Print results
    print(f"\n📈 Results:")
    print(f"   Claims processed: {result.claims_processed}")
    print(f"   Providers analyzed: {result.providers_analyzed}")
    print(f"   Execution time: {result.execution_time_seconds:.2f}s")
    print(f"   Total alerts: {len(result.alerts)}")

    if result.summary:
        print(f"\n   By Risk Level:")
        for level, count in result.summary.get("by_risk_level", {}).items():
            print(f"      - {level}: {count}")

        print(f"\n   By Fraud Type:")
        for ftype, count in result.summary.get("by_fraud_type", {}).items():
            print(f"      - {ftype}: {count}")

    # Show top 3 alerts
    if result.alerts:
        print("\n🚨 Top Priority Alerts:")
        for i, alert in enumerate(result.alerts[:3], 1):
            print_alert(alert, i)


def demo_fraud_scenarios():
    """Demo 2: Specific fraud scenario detection"""
    print_header("Demo 2: Fraud Scenario Detection")

    generator = SyntheticDataGenerator(seed=123)
    pipeline = DMEFraudDetectionPipeline()

    scenarios = [
        ("upcoding", "Upcoding - 고가 장비로 청구"),
        ("phantom", "Phantom Billing - 미배송 장비 청구"),
        ("kickback", "Kickback - 불법 환자 추천 대가"),
        ("hit_and_run", "Hit & Run - 단기 대량 청구 후 도주"),
    ]

    for scenario_key, scenario_name in scenarios:
        print(f"\n📋 Scenario: {scenario_name}")
        print("-" * 40)

        claims, providers = generator.generate_fraud_scenario(scenario_key)
        result = pipeline.run(claims, providers)

        print(f"   Claims: {len(claims)} | Providers: {len(providers)}")
        print(f"   Alerts detected: {len(result.alerts)}")

        # Show relevant alerts
        relevant_alerts = [
            a for a in result.alerts
            if a.risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL]
        ][:2]

        for i, alert in enumerate(relevant_alerts, 1):
            print(f"\n   Alert {i}:")
            print(f"      Type: {alert.fraud_type.value}")
            print(f"      Risk: {alert.risk_level.value}")
            print(f"      {alert.description[:80]}...")


def demo_custom_configuration():
    """Demo 3: Custom pipeline configuration"""
    print_header("Demo 3: Custom Configuration")

    print("\n⚙️  Creating pipeline with custom settings...")

    config = PipelineConfig(
        min_claims_for_analysis=50,
        confidence_threshold=0.6,  # Higher threshold
        max_alerts_per_provider=5,
        enable_upcoding_detection=True,
        enable_phantom_billing_detection=True,
        enable_volume_anomaly_detection=True,
        enable_kickback_detection=True,
        enable_hit_and_run_detection=True,
        include_low_risk=False,  # Exclude low risk alerts
    )

    print(f"   - Min claims: {config.min_claims_for_analysis}")
    print(f"   - Confidence threshold: {config.confidence_threshold:.0%}")
    print(f"   - Max alerts per provider: {config.max_alerts_per_provider}")

    pipeline = DMEFraudDetectionPipeline(config)
    print(f"   - Active detectors: {len(pipeline.detectors)}")

    # Generate and process data
    generator = SyntheticDataGenerator(seed=456)
    claims = generator.generate_claims(count=500, fraud_rate=0.15)

    result = pipeline.run(claims)

    print(f"\n📊 Results with custom config:")
    print(f"   Alerts (high confidence only): {len(result.alerts)}")


def demo_data_export():
    """Demo 4: Export results to JSON"""
    print_header("Demo 4: Data Export")

    generator = SyntheticDataGenerator(seed=789)
    claims = generator.generate_claims(count=300, fraud_rate=0.1)

    pipeline = DMEFraudDetectionPipeline()
    result = pipeline.run(claims)

    # Export to JSON
    output_path = project_root / "examples" / "fraud_detection_results.json"

    print(f"\n💾 Exporting results to JSON...")
    json_data = result.to_json()

    with open(output_path, "w") as f:
        f.write(json_data)

    print(f"   Saved to: {output_path}")
    print(f"   File size: {len(json_data):,} bytes")

    # Show sample of JSON structure
    parsed = json.loads(json_data)
    print(f"\n   JSON structure:")
    print(f"   - summary: {list(parsed['summary'].keys())}")
    print(f"   - alerts: {len(parsed['alerts'])} items")
    print(f"   - claims_processed: {parsed['claims_processed']}")


def demo_streaming_pipeline():
    """Demo 5: Streaming/batch processing"""
    print_header("Demo 5: Streaming Pipeline")

    from src.dme_pipeline.pipeline import StreamingPipeline

    print("\n🌊 Creating streaming pipeline...")
    streaming = StreamingPipeline(batch_size=100)

    generator = SyntheticDataGenerator(seed=101)
    claims = generator.generate_claims(count=350, fraud_rate=0.1)

    print(f"   Processing {len(claims)} claims in batches of 100...")

    batch_results = []
    for claim in claims:
        result = streaming.process_claim(claim)
        if result:  # Batch completed
            batch_results.append(result)
            print(f"   Batch {len(batch_results)} processed: {len(result.alerts)} alerts")

    # Flush remaining
    final_result = streaming.flush()
    if final_result.claims_processed > 0:
        batch_results.append(final_result)
        print(f"   Final batch: {final_result.claims_processed} claims, {len(final_result.alerts)} alerts")

    total_alerts = sum(len(r.alerts) for r in batch_results)
    print(f"\n   Total batches: {len(batch_results)}")
    print(f"   Total alerts across batches: {total_alerts}")


def demo_single_provider_analysis():
    """Demo 6: Analyze a single provider"""
    print_header("Demo 6: Single Provider Analysis")

    generator = SyntheticDataGenerator(seed=202)

    # Create a suspect provider with high-risk patterns
    suspect_npi = "9876543210"
    providers = {
        suspect_npi: Provider(
            npi=suspect_npi,
            legal_business_name="Suspect DME Supplier LLC",
            state="FL",
            enrollment_date=date(2024, 1, 1),
        )
    }

    # Generate claims with suspicious patterns
    claims = []

    # Pattern 1: High volume of high-risk HCPCS codes
    for _ in range(100):
        claims.append(DMEClaim(
            provider_npi=suspect_npi,
            beneficiary_id=generator.generate_beneficiary_id(),
            service_date=date(2024, 3, 15),  # Many on same day
            hcpcs_code="A4352",  # High-risk catheter code
            billed_amount=450.00,
            units=10,
        ))

    # Pattern 2: Single referring physician
    single_doc = "1111111111"
    for _ in range(50):
        claims.append(DMEClaim(
            provider_npi=suspect_npi,
            beneficiary_id=generator.generate_beneficiary_id(),
            service_date=date(2024, 4, 15),
            hcpcs_code="E0601",
            billed_amount=800.00,
            ordering_physician_npi=single_doc,
        ))

    print(f"\n🔎 Analyzing Provider: {suspect_npi}")
    print(f"   Total claims: {len(claims)}")

    pipeline = DMEFraudDetectionPipeline()
    result = pipeline.run(claims, providers)

    # Get alerts for this provider
    provider_alerts = result.get_alerts_by_provider(suspect_npi)

    print(f"\n🚨 Alerts for this provider: {len(provider_alerts)}")
    for i, alert in enumerate(provider_alerts, 1):
        print_alert(alert, i)


def main():
    """Run all demos"""
    print("\n" + "🏥" * 30)
    print("\n  DME Medicare Fraud Detection Pipeline - Demo Suite")
    print("  Based on CMS Program Integrity Manual & UPIC Methodology")
    print("\n" + "🏥" * 30)

    try:
        demo_basic_usage()
        demo_fraud_scenarios()
        demo_custom_configuration()
        demo_data_export()
        demo_streaming_pipeline()
        demo_single_provider_analysis()

        print_header("All Demos Complete!")
        print("\n✅ The DME Fraud Detection Pipeline is working correctly.")
        print("   You can now use these patterns in your own analysis.\n")

    except Exception as e:
        print(f"\n❌ Error during demo: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
