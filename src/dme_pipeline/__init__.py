"""
DME (Durable Medical Equipment) Medicare Fraud Detection Pipeline

This module provides tools for detecting Medicare fraud patterns in DME claims,
following CMS Program Integrity Manual guidelines and UPIC methodologies.
"""

from .models import DMEClaim, Provider, Beneficiary, FraudAlert
from .detectors import (
    UpcodingDetector,
    PhantomBillingDetector,
    VolumeAnomalyDetector,
    KickbackPatternDetector,
    HitAndRunDetector,
)
from .pipeline import DMEFraudDetectionPipeline
from .data_loader import CMSDataLoader

__version__ = "0.1.0"
__all__ = [
    "DMEClaim",
    "Provider",
    "Beneficiary",
    "FraudAlert",
    "UpcodingDetector",
    "PhantomBillingDetector",
    "VolumeAnomalyDetector",
    "KickbackPatternDetector",
    "HitAndRunDetector",
    "DMEFraudDetectionPipeline",
    "CMSDataLoader",
]
