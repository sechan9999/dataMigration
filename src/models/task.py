"""
Task Models for Delay Queue System
Defines the structure of migration tasks with full lifecycle tracking
"""

from enum import Enum
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field, asdict
import json


class TaskStatus(Enum):
    """Task lifecycle states"""
    PENDING = "PENDING"           # Newly created, waiting for first execution
    READY = "READY"               # Ready to be picked up by worker
    PROCESSING = "PROCESSING"     # Currently being processed
    RETRY_SCHEDULED = "RETRY_SCHEDULED"  # Failed, scheduled for retry with backoff
    COMPLETED = "COMPLETED"       # Successfully completed and reconciled
    FAILED = "FAILED"             # Permanently failed (exceeded max retries)
    DEAD_LETTER = "DEAD_LETTER"   # Moved to dead letter queue for manual review


class TaskPriority(Enum):
    """Task priority levels"""
    CRITICAL = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4


@dataclass
class MigrationMetadata:
    """Metadata about the migration job"""
    source_csv_path: str
    target_delta_path: str
    table_name: str
    source_row_count: Optional[int] = None
    estimated_size_mb: Optional[float] = None
    partition_columns: List[str] = field(default_factory=list)
    schema_version: str = "v1"
    business_date: Optional[str] = None
    data_owner: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MigrationMetadata':
        return cls(**data)


@dataclass
class ReconciliationResult:
    """Results from reconciliation validation"""
    status: str  # SUCCESS, FAILED, PARTIAL
    row_count_match: bool
    source_row_count: int
    target_row_count: int
    checksum_match: bool
    key_sums_match: bool
    key_sum_results: Dict[str, Dict[str, float]] = field(default_factory=dict)
    discrepancies: List[str] = field(default_factory=list)
    validated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ReconciliationResult':
        return cls(**data)


@dataclass
class RetryMetadata:
    """Tracks retry attempts and exponential backoff"""
    retry_count: int = 0
    max_retries: int = 5
    last_error: Optional[str] = None
    last_error_timestamp: Optional[str] = None
    retry_history: List[Dict[str, Any]] = field(default_factory=list)
    next_retry_at: Optional[str] = None
    backoff_seconds: Optional[float] = None

    def add_retry_attempt(self, error: str, backoff_seconds: float):
        """Record a retry attempt"""
        self.retry_count += 1
        self.last_error = error
        self.last_error_timestamp = datetime.utcnow().isoformat()
        self.backoff_seconds = backoff_seconds
        self.next_retry_at = (datetime.utcnow() + timedelta(seconds=backoff_seconds)).isoformat()

        self.retry_history.append({
            "attempt": self.retry_count,
            "error": error,
            "timestamp": self.last_error_timestamp,
            "backoff_seconds": backoff_seconds,
            "next_retry_at": self.next_retry_at
        })

    def should_retry(self) -> bool:
        """Check if task should be retried"""
        return self.retry_count < self.max_retries

    def is_ready_for_retry(self) -> bool:
        """Check if enough time has passed for retry"""
        if not self.next_retry_at:
            return True
        return datetime.utcnow() >= datetime.fromisoformat(self.next_retry_at)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RetryMetadata':
        return cls(**data)


@dataclass
class MigrationTask:
    """Complete migration task with full lifecycle tracking"""
    task_id: str
    task_name: str
    migration_metadata: MigrationMetadata
    status: TaskStatus = TaskStatus.PENDING
    priority: TaskPriority = TaskPriority.NORMAL

    # Timestamps
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    started_at: Optional[str] = None
    completed_at: Optional[str] = None

    # Retry tracking
    retry_metadata: RetryMetadata = field(default_factory=RetryMetadata)

    # Results
    reconciliation_result: Optional[ReconciliationResult] = None
    execution_time_seconds: Optional[float] = None

    # Processing lock (for distributed systems)
    locked_until: Optional[str] = None
    locked_by: Optional[str] = None

    # Additional context
    tags: Dict[str, str] = field(default_factory=dict)
    custom_config: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for Delta Lake storage"""
        data = {
            "task_id": self.task_id,
            "task_name": self.task_name,
            "status": self.status.value,
            "priority": self.priority.value,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "execution_time_seconds": self.execution_time_seconds,
            "locked_until": self.locked_until,
            "locked_by": self.locked_by,

            # Nested objects as JSON strings for Delta Lake
            "migration_metadata_json": json.dumps(self.migration_metadata.to_dict()),
            "retry_metadata_json": json.dumps(self.retry_metadata.to_dict()),
            "reconciliation_result_json": json.dumps(self.reconciliation_result.to_dict()) if self.reconciliation_result else None,
            "tags_json": json.dumps(self.tags),
            "custom_config_json": json.dumps(self.custom_config)
        }
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MigrationTask':
        """Reconstruct from Delta Lake row"""
        return cls(
            task_id=data["task_id"],
            task_name=data["task_name"],
            status=TaskStatus(data["status"]),
            priority=TaskPriority(data["priority"]),
            created_at=data["created_at"],
            updated_at=data["updated_at"],
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            execution_time_seconds=data.get("execution_time_seconds"),
            locked_until=data.get("locked_until"),
            locked_by=data.get("locked_by"),
            migration_metadata=MigrationMetadata.from_dict(json.loads(data["migration_metadata_json"])),
            retry_metadata=RetryMetadata.from_dict(json.loads(data["retry_metadata_json"])),
            reconciliation_result=ReconciliationResult.from_dict(json.loads(data["reconciliation_result_json"])) if data.get("reconciliation_result_json") else None,
            tags=json.loads(data.get("tags_json", "{}")),
            custom_config=json.loads(data.get("custom_config_json", "{}"))
        )

    def mark_processing(self, worker_id: str, visibility_timeout_seconds: int = 900):
        """Mark task as being processed with a lock"""
        self.status = TaskStatus.PROCESSING
        self.started_at = datetime.utcnow().isoformat()
        self.locked_by = worker_id
        self.locked_until = (datetime.utcnow() + timedelta(seconds=visibility_timeout_seconds)).isoformat()
        self.updated_at = datetime.utcnow().isoformat()

    def mark_completed(self, reconciliation_result: ReconciliationResult, execution_time_seconds: float):
        """Mark task as successfully completed"""
        self.status = TaskStatus.COMPLETED
        self.completed_at = datetime.utcnow().isoformat()
        self.reconciliation_result = reconciliation_result
        self.execution_time_seconds = execution_time_seconds
        self.locked_by = None
        self.locked_until = None
        self.updated_at = datetime.utcnow().isoformat()

    def mark_failed(self, error: str):
        """Mark task as permanently failed"""
        self.status = TaskStatus.FAILED
        self.retry_metadata.last_error = error
        self.retry_metadata.last_error_timestamp = datetime.utcnow().isoformat()
        self.locked_by = None
        self.locked_until = None
        self.updated_at = datetime.utcnow().isoformat()

    def schedule_retry(self, error: str, backoff_seconds: float):
        """Schedule task for retry with exponential backoff"""
        self.retry_metadata.add_retry_attempt(error, backoff_seconds)
        self.status = TaskStatus.RETRY_SCHEDULED
        self.locked_by = None
        self.locked_until = None
        self.updated_at = datetime.utcnow().isoformat()

    def is_lock_expired(self) -> bool:
        """Check if processing lock has expired"""
        if not self.locked_until:
            return True
        return datetime.utcnow() >= datetime.fromisoformat(self.locked_until)
