"""
Data models for DME Medicare Fraud Detection

Based on CMS Provider Specific File (PSF) structure and claims data formats.
Reference: https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/ProspMedicareFeeSvcPmtGen/psf_SAS
"""

from dataclasses import dataclass, field
from datetime import datetime, date
from enum import Enum
from typing import Optional, List
import uuid


class ClaimStatus(Enum):
    """Medicare claim processing status"""
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    UNDER_REVIEW = "under_review"
    FLAGGED = "flagged"


class FraudType(Enum):
    """Types of DME fraud patterns based on CMS PIM Chapter 2"""
    UPCODING = "upcoding"
    PHANTOM_BILLING = "phantom_billing"
    UNNECESSARY_EQUIPMENT = "unnecessary_equipment"
    KICKBACK = "kickback"
    HIT_AND_RUN = "hit_and_run"
    UNBUNDLING = "unbundling"
    DUPLICATE_BILLING = "duplicate_billing"
    IDENTITY_THEFT = "identity_theft"
    FALSE_CERTIFICATION = "false_certification"


class RiskLevel(Enum):
    """Risk scoring levels for fraud alerts"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class Provider:
    """
    DME Supplier/Provider information

    Based on CMS Provider Enrollment data structure.
    Key fields from Provider Specific File (PSF).
    """
    npi: str  # National Provider Identifier (10-digit)
    ptan: Optional[str] = None  # Provider Transaction Access Number
    legal_business_name: str = ""
    doing_business_as: Optional[str] = None
    organization_type: str = ""

    # Location
    address_line1: str = ""
    address_line2: Optional[str] = None
    city: str = ""
    state: str = ""
    zip_code: str = ""

    # Enrollment info
    enrollment_date: Optional[date] = None
    revalidation_date: Optional[date] = None
    accreditation_org: Optional[str] = None

    # Computed metrics for fraud detection
    total_claims_ytd: int = 0
    total_amount_ytd: float = 0.0
    avg_claim_amount: float = 0.0
    unique_beneficiaries: int = 0

    # Risk indicators
    ownership_changes: int = 0
    address_changes: int = 0

    def __post_init__(self):
        # Validate NPI format
        if self.npi and len(self.npi) != 10:
            raise ValueError(f"NPI must be 10 digits, got: {self.npi}")


@dataclass
class Beneficiary:
    """
    Medicare beneficiary information

    Anonymized representation for fraud detection purposes.
    """
    beneficiary_id: str  # Hashed/anonymized ID
    hic_number: Optional[str] = None  # Health Insurance Claim Number

    # Demographics (anonymized)
    birth_year: Optional[int] = None
    gender: Optional[str] = None
    state: Optional[str] = None
    zip_code_prefix: Optional[str] = None  # First 3 digits only

    # Medicare eligibility
    part_a_enrollment: bool = False
    part_b_enrollment: bool = False

    # Computed metrics
    total_claims: int = 0
    unique_providers: int = 0
    distinct_equipment_types: int = 0


@dataclass
class DMEClaim:
    """
    DME Medicare Claim Record

    Structure based on CMS Basic Stand Alone (BSA) Medicare Claims format
    and DME-specific HCPCS coding requirements.

    Key HCPCS codes for DME fraud detection:
    - E0601-E0604: Breathing equipment
    - E0710-E0770: Patient lifts
    - E1390-E1392: Oxygen equipment
    - A4352-A4353: Intermittent catheters
    - L0643: Lumbar-sacral orthosis
    - K0001-K0108: Wheelchairs
    """
    claim_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # Identifiers
    provider_npi: str = ""
    beneficiary_id: str = ""

    # Service details
    service_date: Optional[date] = None
    claim_date: Optional[date] = None

    # HCPCS coding
    hcpcs_code: str = ""  # Healthcare Common Procedure Coding System
    hcpcs_modifier: Optional[str] = None
    icd10_diagnosis: Optional[str] = None  # ICD-10 diagnosis code

    # Financial
    billed_amount: float = 0.0
    allowed_amount: float = 0.0
    paid_amount: float = 0.0
    beneficiary_liability: float = 0.0

    # Quantity
    units: int = 1
    days_supply: Optional[int] = None

    # Place of service
    place_of_service: str = ""  # POS code

    # Ordering/Referring physician
    ordering_physician_npi: Optional[str] = None

    # Documentation
    prior_authorization: bool = False
    certificate_of_medical_necessity: bool = False

    # Processing
    status: ClaimStatus = ClaimStatus.PENDING
    denial_reason: Optional[str] = None

    # Fraud detection flags (populated by detectors)
    fraud_flags: List[str] = field(default_factory=list)
    risk_score: float = 0.0

    def calculate_unit_price(self) -> float:
        """Calculate price per unit"""
        if self.units > 0:
            return self.billed_amount / self.units
        return 0.0

    def is_high_value(self, threshold: float = 1000.0) -> bool:
        """Check if claim exceeds high-value threshold"""
        return self.billed_amount >= threshold


@dataclass
class FraudAlert:
    """
    Fraud detection alert generated by analysis pipeline

    Following UPIC investigation workflow and CMS PIM Chapter 2 guidelines.
    """
    alert_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # Alert details
    fraud_type: FraudType = FraudType.UPCODING
    risk_level: RiskLevel = RiskLevel.LOW
    confidence_score: float = 0.0  # 0.0 to 1.0

    # Subject of alert
    provider_npi: Optional[str] = None
    beneficiary_id: Optional[str] = None
    claim_ids: List[str] = field(default_factory=list)

    # Evidence
    description: str = ""
    evidence_summary: str = ""
    detection_rules_triggered: List[str] = field(default_factory=list)

    # Statistical support
    observed_value: Optional[float] = None
    expected_value: Optional[float] = None
    standard_deviations: Optional[float] = None
    percentile_rank: Optional[float] = None

    # Metadata
    created_at: datetime = field(default_factory=datetime.now)
    detector_name: str = ""
    detector_version: str = ""

    # Actions
    recommended_action: str = ""
    priority_score: int = 0  # 1-100

    def to_dict(self) -> dict:
        """Convert alert to dictionary for reporting"""
        return {
            "alert_id": self.alert_id,
            "fraud_type": self.fraud_type.value,
            "risk_level": self.risk_level.value,
            "confidence_score": self.confidence_score,
            "provider_npi": self.provider_npi,
            "claim_count": len(self.claim_ids),
            "description": self.description,
            "observed_value": self.observed_value,
            "expected_value": self.expected_value,
            "recommended_action": self.recommended_action,
            "created_at": self.created_at.isoformat(),
        }


# HCPCS Code reference for common DME items
HCPCS_DME_CATEGORIES = {
    "oxygen_equipment": ["E0424", "E0431", "E0433", "E0434", "E0439", "E0441", "E0442", "E0443", "E0444", "E1390", "E1391", "E1392"],
    "wheelchairs": ["K0001", "K0002", "K0003", "K0004", "K0005", "K0006", "K0007", "K0008", "K0009", "K0010"],
    "hospital_beds": ["E0250", "E0251", "E0255", "E0256", "E0260", "E0261", "E0265", "E0266", "E0270", "E0271"],
    "cpap_bipap": ["E0601", "E0470", "E0471", "E0472"],
    "catheters": ["A4351", "A4352", "A4353", "A4354", "A4355", "A4356", "A4357", "A4358"],
    "orthotic_devices": ["L0643", "L2630", "L3000", "L3001", "L3002"],
    "breast_pumps": ["E0602", "E0603", "E0604"],
    "walkers": ["E0130", "E0135", "E0140", "E0141", "E0143", "E0144", "E0147", "E0148", "E0149"],
    "nebulizers": ["E0570", "E0575", "E0580", "E0585"],
    "glucose_monitors": ["E0607", "E2100", "E2101"],
}

# High-risk HCPCS codes frequently associated with fraud (based on OIG reports)
HIGH_RISK_HCPCS = [
    "A4352",  # Intermittent urinary catheter - major fraud target
    "A4353",  # Intermittent urinary catheter
    "E0603",  # Electric breast pump
    "L0643",  # Lumbar-sacral orthosis
    "L2630",  # Pelvic control band
    "E0601",  # CPAP
    "K0823",  # Power wheelchair
    "E0431",  # Portable oxygen
]
