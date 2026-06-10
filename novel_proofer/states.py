from __future__ import annotations

from enum import StrEnum


class JobState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    DONE = "done"
    ERROR = "error"
    CANCELLED = "cancelled"


class ExecutionState(StrEnum):
    IDLE = "idle"
    QUEUED = "queued"
    RUNNING = "running"


class WaitReason(StrEnum):
    READY_TO_PROCESS = "ready_to_process"
    USER_PAUSED = "user_paused"
    READY_TO_MERGE = "ready_to_merge"
    SERVER_RECOVERED = "server_recovered"


class TerminalState(StrEnum):
    DONE = "done"
    ERROR = "error"
    CANCELLED = "cancelled"


class JobCommand(StrEnum):
    VALIDATE = "validate"
    PROCESS = "process"
    PAUSE = "pause"
    RETRY_FAILED = "retry_failed"
    MERGE = "merge"
    DETACH = "detach"
    RESET = "reset"
    DOWNLOAD = "download"


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
