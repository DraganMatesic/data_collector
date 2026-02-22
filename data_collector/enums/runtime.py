from enum import IntEnum


class RunStatus(IntEnum):
    NOT_RUNNING = 0
    RUNNING = 1
    STOPPED = 2


class FatalFlag(IntEnum):
    FAILED_TO_START = 1
    APP_STOPPED_ALERT_SENT = 2
    UNEXPECTED_BEHAVIOUR = 3


class RuntimeExitCode(IntEnum):
    FINISHED = 0
    MANAGER_EXIT = 1
    ORPHAN_PID = 2
    CMD_DISABLE = 3
    CMD_RESET = 4
    CMD_STOP = 5
    CMD_START = 6
