from __future__ import annotations

from enum import StrEnum


class JobState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    DONE = "done"
    ERROR = "error"
    CANCELLED = "cancelled"


class ChunkState(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    RETRYING = "retrying"
    DONE = "done"
    ERROR = "error"


class JobPhase(StrEnum):
    VALIDATE = "validate"
    PROCESS = "process"
    MERGE = "merge"
    DONE = "done"
