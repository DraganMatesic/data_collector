from enum import IntEnum


class CmdFlag(IntEnum):
    PENDING = 0
    EXECUTED = 1
    NOT_EXECUTED = 2


class CmdName(IntEnum):
    START = 1
    STOP = 2
    RESTART = 3
    ENABLE = 4
    DISABLE = 5
