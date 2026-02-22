from enum import IntEnum, StrEnum

from data_collector.enums import (
    AlertSeverity,
    CmdFlag,
    CmdName,
    DbObjectType,
    FatalFlag,
    LogLevel,
    RunStatus,
    RuntimeExitCode,
    UnicodeForm,
)


# ---------------------------------------------------------------------------
# CmdFlag
# ---------------------------------------------------------------------------

def test_cmd_flag_is_int_enum() -> None:
    assert issubclass(CmdFlag, IntEnum)


def test_cmd_flag_values() -> None:
    assert CmdFlag.PENDING == 0
    assert CmdFlag.EXECUTED == 1
    assert CmdFlag.NOT_EXECUTED == 2


def test_cmd_flag_member_count() -> None:
    assert len(CmdFlag) == 3


# ---------------------------------------------------------------------------
# CmdName
# ---------------------------------------------------------------------------

def test_cmd_name_is_int_enum() -> None:
    assert issubclass(CmdName, IntEnum)


def test_cmd_name_values() -> None:
    assert CmdName.START == 1
    assert CmdName.STOP == 2
    assert CmdName.RESTART == 3
    assert CmdName.ENABLE == 4
    assert CmdName.DISABLE == 5


def test_cmd_name_member_count() -> None:
    assert len(CmdName) == 5


# ---------------------------------------------------------------------------
# RunStatus
# ---------------------------------------------------------------------------

def test_run_status_is_int_enum() -> None:
    assert issubclass(RunStatus, IntEnum)


def test_run_status_values() -> None:
    assert RunStatus.NOT_RUNNING == 0
    assert RunStatus.RUNNING == 1
    assert RunStatus.STOPPED == 2


def test_run_status_member_count() -> None:
    assert len(RunStatus) == 3


# ---------------------------------------------------------------------------
# FatalFlag
# ---------------------------------------------------------------------------

def test_fatal_flag_is_int_enum() -> None:
    assert issubclass(FatalFlag, IntEnum)


def test_fatal_flag_values() -> None:
    assert FatalFlag.FAILED_TO_START == 1
    assert FatalFlag.APP_STOPPED_ALERT_SENT == 2
    assert FatalFlag.UNEXPECTED_BEHAVIOUR == 3


def test_fatal_flag_member_count() -> None:
    assert len(FatalFlag) == 3


# ---------------------------------------------------------------------------
# RuntimeExitCode
# ---------------------------------------------------------------------------

def test_runtime_exit_code_is_int_enum() -> None:
    assert issubclass(RuntimeExitCode, IntEnum)


def test_runtime_exit_code_values() -> None:
    assert RuntimeExitCode.FINISHED == 0
    assert RuntimeExitCode.MANAGER_EXIT == 1
    assert RuntimeExitCode.ORPHAN_PID == 2
    assert RuntimeExitCode.CMD_DISABLE == 3
    assert RuntimeExitCode.CMD_RESET == 4
    assert RuntimeExitCode.CMD_STOP == 5
    assert RuntimeExitCode.CMD_START == 6


def test_runtime_exit_code_member_count() -> None:
    assert len(RuntimeExitCode) == 7


# ---------------------------------------------------------------------------
# LogLevel
# ---------------------------------------------------------------------------

def test_log_level_is_int_enum() -> None:
    assert issubclass(LogLevel, IntEnum)


def test_log_level_values() -> None:
    assert LogLevel.NOTSET == 0
    assert LogLevel.DEBUG == 10
    assert LogLevel.INFO == 20
    assert LogLevel.WARNING == 30
    assert LogLevel.ERROR == 40
    assert LogLevel.CRITICAL == 50


def test_log_level_member_count() -> None:
    assert len(LogLevel) == 6


# ---------------------------------------------------------------------------
# DbObjectType
# ---------------------------------------------------------------------------

def test_db_object_type_is_int_enum() -> None:
    assert issubclass(DbObjectType, IntEnum)


def test_db_object_type_values() -> None:
    assert DbObjectType.PROCEDURE == 1
    assert DbObjectType.FUNCTION == 2


def test_db_object_type_member_count() -> None:
    assert len(DbObjectType) == 2


# ---------------------------------------------------------------------------
# UnicodeForm
# ---------------------------------------------------------------------------

def test_unicode_form_is_str_enum() -> None:
    assert issubclass(UnicodeForm, StrEnum)


def test_unicode_form_values() -> None:
    assert UnicodeForm.NFC == "NFC"
    assert UnicodeForm.NFD == "NFD"
    assert UnicodeForm.NFKC == "NFKC"
    assert UnicodeForm.NFKD == "NFKD"


def test_unicode_form_member_count() -> None:
    assert len(UnicodeForm) == 4


# ---------------------------------------------------------------------------
# AlertSeverity
# ---------------------------------------------------------------------------

def test_alert_severity_is_int_enum() -> None:
    assert issubclass(AlertSeverity, IntEnum)


def test_alert_severity_values() -> None:
    assert AlertSeverity.INFO == 1
    assert AlertSeverity.WARNING == 2
    assert AlertSeverity.ERROR == 3
    assert AlertSeverity.CRITICAL == 4


def test_alert_severity_member_count() -> None:
    assert len(AlertSeverity) == 4


# ---------------------------------------------------------------------------
# Cross-cutting: no duplicate values within any enum
# ---------------------------------------------------------------------------

def test_no_duplicate_values_in_any_enum() -> None:
    for enum_cls in (CmdFlag, CmdName, RunStatus, FatalFlag, RuntimeExitCode,
                     LogLevel, DbObjectType, UnicodeForm, AlertSeverity):
        values = [m.value for m in enum_cls]
        assert len(values) == len(set(values)), f"Duplicate values in {enum_cls.__name__}"
