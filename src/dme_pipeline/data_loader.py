"""
CMS Data Loader for DME Medicare Claims

Handles loading data from various CMS file formats:
- Provider Specific File (PSF) in SAS format
- Basic Stand Alone (BSA) Medicare Claims PUFs
- Provider of Services (POS) files

Reference: https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/ProspMedicareFeeSvcPmtGen/psf_SAS
"""

import csv
import json
from dataclasses import asdict
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Optional, Iterator, Any, Union
import random
import string

from .models import DMEClaim, Provider, Beneficiary, ClaimStatus, HIGH_RISK_HCPCS, HCPCS_DME_CATEGORIES


class CMSDataLoader:
    """
    Load and parse CMS Medicare data files

    Supports multiple file formats commonly used by CMS:
    - CSV (Basic Stand Alone files)
    - JSON (API responses)
    - SAS7BDAT (Provider Specific Files) - requires sas7bdat package

    Example:
        >>> loader = CMSDataLoader()
        >>> claims = loader.load_claims_csv("medicare_claims.csv")
        >>> providers = loader.load_providers_csv("provider_data.csv")
    """

    # Standard CMS column mappings
    CLAIM_COLUMN_MAPPINGS = {
        "CLM_ID": "claim_id",
        "PRVDR_NPI": "provider_npi",
        "BENE_ID": "beneficiary_id",
        "CLM_FROM_DT": "service_date",
        "CLM_THRU_DT": "claim_date",
        "HCPCS_CD": "hcpcs_code",
        "HCPCS_1ST_MDFR_CD": "hcpcs_modifier",
        "ICD_DGNS_CD1": "icd10_diagnosis",
        "CLM_PMT_AMT": "paid_amount",
        "NCH_CLM_PRVDR_PMT_AMT": "allowed_amount",
        "CLM_TOT_CHRG_AMT": "billed_amount",
        "LINE_SRVC_CNT": "units",
        "PRVDR_STATE_CD": "place_of_service",
        "ORD_PHYSN_NPI": "ordering_physician_npi",
    }

    PROVIDER_COLUMN_MAPPINGS = {
        "NPI": "npi",
        "PRVDR_LGL_NM": "legal_business_name",
        "PRVDR_DBA_NM": "doing_business_as",
        "PRVDR_1ST_LN_ADR": "address_line1",
        "PRVDR_2ND_LN_ADR": "address_line2",
        "PRVDR_CITY": "city",
        "PRVDR_STATE": "state",
        "PRVDR_ZIP": "zip_code",
        "ENRLMT_DT": "enrollment_date",
    }

    def __init__(self, date_format: str = "%Y-%m-%d"):
        self.date_format = date_format

    def load_claims_csv(
        self,
        file_path: Union[str, Path],
        column_mappings: Optional[Dict[str, str]] = None,
        limit: Optional[int] = None,
    ) -> List[DMEClaim]:
        """
        Load claims from CSV file

        Args:
            file_path: Path to CSV file
            column_mappings: Optional custom column name mappings
            limit: Maximum number of records to load

        Returns:
            List of DMEClaim objects
        """
        mappings = column_mappings or self.CLAIM_COLUMN_MAPPINGS
        claims = []

        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if limit and i >= limit:
                    break
                claim = self._parse_claim_row(row, mappings)
                if claim:
                    claims.append(claim)

        return claims

    def load_claims_json(
        self,
        file_path: Union[str, Path],
        limit: Optional[int] = None,
    ) -> List[DMEClaim]:
        """Load claims from JSON file"""
        with open(file_path, "r") as f:
            data = json.load(f)

        claims = []
        records = data if isinstance(data, list) else data.get("claims", [])

        for i, record in enumerate(records):
            if limit and i >= limit:
                break
            claim = self._dict_to_claim(record)
            claims.append(claim)

        return claims

    def load_providers_csv(
        self,
        file_path: Union[str, Path],
        column_mappings: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Provider]:
        """Load providers from CSV file, keyed by NPI"""
        mappings = column_mappings or self.PROVIDER_COLUMN_MAPPINGS
        providers = {}

        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                provider = self._parse_provider_row(row, mappings)
                if provider:
                    providers[provider.npi] = provider

        return providers

    def stream_claims_csv(
        self,
        file_path: Union[str, Path],
        batch_size: int = 1000,
    ) -> Iterator[List[DMEClaim]]:
        """
        Stream claims from large CSV file in batches

        Yields batches of claims for memory-efficient processing
        """
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            batch = []

            for row in reader:
                claim = self._parse_claim_row(row, self.CLAIM_COLUMN_MAPPINGS)
                if claim:
                    batch.append(claim)

                if len(batch) >= batch_size:
                    yield batch
                    batch = []

            if batch:  # Yield remaining
                yield batch

    def _parse_claim_row(
        self,
        row: Dict[str, str],
        mappings: Dict[str, str],
    ) -> Optional[DMEClaim]:
        """Parse a CSV row into a DMEClaim object"""
        try:
            kwargs = {}
            for csv_col, field_name in mappings.items():
                if csv_col in row:
                    value = row[csv_col]
                    kwargs[field_name] = self._convert_value(field_name, value)

            return DMEClaim(**kwargs)
        except Exception as e:
            return None

    def _parse_provider_row(
        self,
        row: Dict[str, str],
        mappings: Dict[str, str],
    ) -> Optional[Provider]:
        """Parse a CSV row into a Provider object"""
        try:
            kwargs = {}
            for csv_col, field_name in mappings.items():
                if csv_col in row:
                    value = row[csv_col]
                    kwargs[field_name] = self._convert_value(field_name, value)

            if "npi" not in kwargs:
                return None

            return Provider(**kwargs)
        except Exception as e:
            return None

    def _convert_value(self, field_name: str, value: str) -> Any:
        """Convert string value to appropriate type"""
        if not value or value.strip() == "":
            return None

        # Date fields
        if field_name in ["service_date", "claim_date", "enrollment_date", "revalidation_date"]:
            try:
                return datetime.strptime(value, self.date_format).date()
            except ValueError:
                return None

        # Numeric fields
        if field_name in ["billed_amount", "allowed_amount", "paid_amount", "beneficiary_liability"]:
            try:
                return float(value.replace(",", "").replace("$", ""))
            except ValueError:
                return 0.0

        if field_name in ["units", "days_supply"]:
            try:
                return int(float(value))
            except ValueError:
                return 1

        return value.strip()

    def _dict_to_claim(self, data: Dict[str, Any]) -> DMEClaim:
        """Convert dictionary to DMEClaim"""
        # Handle date conversion
        for date_field in ["service_date", "claim_date"]:
            if date_field in data and isinstance(data[date_field], str):
                try:
                    data[date_field] = datetime.strptime(
                        data[date_field], self.date_format
                    ).date()
                except ValueError:
                    data[date_field] = None

        return DMEClaim(**data)

    def save_claims_csv(
        self,
        claims: List[DMEClaim],
        file_path: Union[str, Path],
    ):
        """Save claims to CSV file"""
        if not claims:
            return

        with open(file_path, "w", newline="", encoding="utf-8") as f:
            # Get field names from first claim
            fieldnames = list(asdict(claims[0]).keys())
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for claim in claims:
                row = asdict(claim)
                # Convert dates to strings
                for key, value in row.items():
                    if isinstance(value, (date, datetime)):
                        row[key] = value.strftime(self.date_format)
                    elif isinstance(value, list):
                        row[key] = json.dumps(value)
                    elif hasattr(value, "value"):  # Enum
                        row[key] = value.value
                writer.writerow(row)

    def save_claims_json(
        self,
        claims: List[DMEClaim],
        file_path: Union[str, Path],
    ):
        """Save claims to JSON file"""
        def serialize(obj):
            if isinstance(obj, (date, datetime)):
                return obj.isoformat()
            if hasattr(obj, "value"):  # Enum
                return obj.value
            return str(obj)

        with open(file_path, "w") as f:
            json.dump(
                [asdict(c) for c in claims],
                f,
                indent=2,
                default=serialize,
            )


class SyntheticDataGenerator:
    """
    Generate synthetic Medicare DME claims data for testing

    Creates realistic-looking test data with configurable fraud patterns
    for local development and testing of fraud detection algorithms.
    """

    def __init__(self, seed: Optional[int] = None):
        if seed:
            random.seed(seed)

        # Reference data for generation
        self.states = ["CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI"]
        self.all_hcpcs = []
        for codes in HCPCS_DME_CATEGORIES.values():
            self.all_hcpcs.extend(codes)

    def generate_npi(self) -> str:
        """Generate a valid-format NPI (10 digits)"""
        return "".join(random.choices(string.digits, k=10))

    def generate_beneficiary_id(self) -> str:
        """Generate anonymized beneficiary ID"""
        return "BENE" + "".join(random.choices(string.ascii_uppercase + string.digits, k=8))

    def generate_providers(self, count: int = 100) -> Dict[str, Provider]:
        """Generate synthetic provider data"""
        providers = {}

        for _ in range(count):
            npi = self.generate_npi()
            state = random.choice(self.states)

            providers[npi] = Provider(
                npi=npi,
                legal_business_name=f"DME Supplier {npi[-4:]}",
                state=state,
                zip_code=f"{random.randint(10000, 99999)}",
                enrollment_date=date(
                    random.randint(2018, 2024),
                    random.randint(1, 12),
                    random.randint(1, 28)
                ),
            )

        return providers

    def generate_claims(
        self,
        count: int = 1000,
        providers: Optional[Dict[str, Provider]] = None,
        fraud_rate: float = 0.05,
        start_date: date = None,
        end_date: date = None,
    ) -> List[DMEClaim]:
        """
        Generate synthetic claims with optional fraud patterns

        Args:
            count: Number of claims to generate
            providers: Provider dict (generated if not provided)
            fraud_rate: Proportion of claims with fraud indicators (0.0-1.0)
            start_date: Earliest service date
            end_date: Latest service date

        Returns:
            List of synthetic DMEClaim objects
        """
        if providers is None:
            providers = self.generate_providers(max(10, count // 10))

        if start_date is None:
            start_date = date(2024, 1, 1)
        if end_date is None:
            end_date = date(2024, 12, 31)

        provider_npis = list(providers.keys())
        claims = []

        # Generate pool of beneficiaries
        beneficiary_pool = [self.generate_beneficiary_id() for _ in range(count // 5)]

        for i in range(count):
            is_fraud = random.random() < fraud_rate

            # Select provider (fraud claims concentrate on fewer providers)
            if is_fraud and random.random() < 0.7:
                # Concentrate fraud on specific providers
                npi = random.choice(provider_npis[:max(1, len(provider_npis) // 10)])
            else:
                npi = random.choice(provider_npis)

            # Generate service date
            days_range = (end_date - start_date).days
            service_date = start_date + __import__("datetime").timedelta(
                days=random.randint(0, days_range)
            )

            # Select HCPCS code (fraud more likely to use high-risk codes)
            if is_fraud and random.random() < 0.8:
                hcpcs = random.choice(HIGH_RISK_HCPCS)
            else:
                hcpcs = random.choice(self.all_hcpcs)

            # Generate amounts
            base_amount = random.uniform(50, 500)
            if is_fraud:
                # Fraud claims tend to have higher amounts
                base_amount *= random.uniform(1.5, 3.0)

            billed_amount = round(base_amount, 2)
            allowed_amount = round(billed_amount * random.uniform(0.6, 0.9), 2)
            paid_amount = round(allowed_amount * 0.8, 2)

            # Generate units
            units = random.randint(1, 5)
            if is_fraud and random.random() < 0.5:
                units = random.randint(10, 50)  # Unusually high units

            claim = DMEClaim(
                provider_npi=npi,
                beneficiary_id=random.choice(beneficiary_pool),
                service_date=service_date,
                claim_date=service_date + __import__("datetime").timedelta(
                    days=random.randint(1, 14)
                ),
                hcpcs_code=hcpcs,
                billed_amount=billed_amount,
                allowed_amount=allowed_amount,
                paid_amount=paid_amount,
                units=units,
                ordering_physician_npi=self.generate_npi() if random.random() < 0.8 else None,
                status=ClaimStatus.APPROVED,
            )

            claims.append(claim)

        return claims

    def generate_fraud_scenario(
        self,
        scenario: str,
        base_claims: int = 500,
    ) -> tuple[List[DMEClaim], Dict[str, Provider]]:
        """
        Generate specific fraud scenario for testing

        Scenarios:
        - "upcoding": Provider consistently bills higher-tier equipment
        - "phantom": High volume with delivery impossibilities
        - "kickback": Concentrated referrals from single physician
        - "hit_and_run": New provider with explosive growth
        """
        providers = self.generate_providers(20)
        provider_npis = list(providers.keys())

        # Generate baseline legitimate claims
        claims = self.generate_claims(base_claims, providers, fraud_rate=0)

        fraud_provider = provider_npis[0]  # Use first provider for fraud

        if scenario == "upcoding":
            # Add high-tier wheelchair claims
            high_tier_codes = ["K0823", "K0824", "K0825"]
            for _ in range(100):
                claims.append(DMEClaim(
                    provider_npi=fraud_provider,
                    beneficiary_id=self.generate_beneficiary_id(),
                    service_date=date(2024, random.randint(1, 12), random.randint(1, 28)),
                    hcpcs_code=random.choice(high_tier_codes),
                    billed_amount=random.uniform(2000, 5000),
                    units=1,
                    status=ClaimStatus.APPROVED,
                ))

        elif scenario == "phantom":
            # Add impossible delivery volumes on single days
            for month in range(1, 7):
                target_date = date(2024, month, 15)
                for _ in range(150):  # Impossible 150 deliveries/day
                    claims.append(DMEClaim(
                        provider_npi=fraud_provider,
                        beneficiary_id=self.generate_beneficiary_id(),
                        service_date=target_date,
                        hcpcs_code=random.choice(self.all_hcpcs),
                        billed_amount=random.uniform(100, 300),
                        units=1,
                        status=ClaimStatus.APPROVED,
                    ))

        elif scenario == "kickback":
            # All claims from single ordering physician
            kickback_physician = self.generate_npi()
            for _ in range(200):
                claims.append(DMEClaim(
                    provider_npi=fraud_provider,
                    beneficiary_id=self.generate_beneficiary_id(),
                    service_date=date(2024, random.randint(1, 12), random.randint(1, 28)),
                    hcpcs_code=random.choice(self.all_hcpcs),
                    billed_amount=random.uniform(200, 800),
                    ordering_physician_npi=kickback_physician,
                    units=1,
                    status=ClaimStatus.APPROVED,
                ))

        elif scenario == "hit_and_run":
            # New provider with massive growth in short period
            new_provider = self.generate_npi()
            providers[new_provider] = Provider(
                npi=new_provider,
                enrollment_date=date(2024, 9, 1),  # Very recent
            )

            # Explosive billing in 3 months
            for month in [9, 10, 11]:
                volume = 50 * (month - 8)  # 50, 100, 150 claims
                for _ in range(volume):
                    claims.append(DMEClaim(
                        provider_npi=new_provider,
                        beneficiary_id=self.generate_beneficiary_id(),
                        service_date=date(2024, month, random.randint(1, 28)),
                        hcpcs_code=random.choice(HIGH_RISK_HCPCS),
                        billed_amount=random.uniform(500, 2000),
                        units=random.randint(1, 10),
                        status=ClaimStatus.APPROVED,
                    ))

        return claims, providers
