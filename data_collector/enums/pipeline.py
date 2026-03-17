"""Pipeline-related enums for task processing state tracking."""

from enum import IntEnum


class PipelineStatus(IntEnum):
    """Execution state of a pipeline task."""

    PENDING = 0
    IN_PROGRESS = 1
    COMPLETED = 2
    FAILED = 3
    RETRY = 4


class PipelineStage(IntEnum):
    """Processing stage within a pipeline."""

    PREPARE = 0
    EXTRACT = 1
    PROCESS = 2
    VALIDATE = 3
    LOAD = 4


class EventType(IntEnum):
    """Type of file system event detected by WatchService."""

    CREATED = 1
    MODIFIED = 2
    DELETED = 3
