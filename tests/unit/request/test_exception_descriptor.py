from datetime import datetime, timedelta

from data_collector.utilities.request import ExceptionDescriptor


# ---------------------------------------------------------------------------
# add_error
# ---------------------------------------------------------------------------

def test_add_error_stores_with_timestamp() -> None:
    ed = ExceptionDescriptor()
    ed.add_error("timeout", "Connection timed out", "https://example.com")
    assert len(ed.errors) == 1
    entry = next(iter(ed.errors.values()))
    assert entry["type"] == "timeout"
    assert entry["message"] == "Connection timed out"
    assert entry["url"] == "https://example.com"


def test_add_error_url_defaults_to_empty() -> None:
    ed = ExceptionDescriptor()
    ed.add_error("timeout", "Connection timed out")
    entry = next(iter(ed.errors.values()))
    assert entry["url"] == ""


def test_add_error_key_is_datetime() -> None:
    ed = ExceptionDescriptor()
    ed.add_error("timeout", "test")
    key = next(iter(ed.errors.keys()))
    assert isinstance(key, datetime)


# ---------------------------------------------------------------------------
# get_last_error
# ---------------------------------------------------------------------------

def test_get_last_error_returns_most_recent() -> None:
    ed = ExceptionDescriptor()
    ed.add_error("timeout", "first")
    ed.add_error("proxy", "second")
    ed.add_error("other", "third")
    last = ed.get_last_error()
    assert last is not None
    assert last["type"] == "other"
    assert last["message"] == "third"


def test_get_last_error_empty() -> None:
    ed = ExceptionDescriptor()
    assert ed.get_last_error() is None


# ---------------------------------------------------------------------------
# get_errors_by_type
# ---------------------------------------------------------------------------

def test_get_errors_by_type() -> None:
    ed = ExceptionDescriptor()
    ed.add_error("timeout", "first")
    ed.add_error("proxy", "second")
    ed.add_error("timeout", "third")
    results = ed.get_errors_by_type("timeout")
    assert len(results) == 2
    assert all(r["type"] == "timeout" for r in results)


def test_get_errors_by_type_no_match() -> None:
    ed = ExceptionDescriptor()
    ed.add_error("timeout", "test")
    assert ed.get_errors_by_type("proxy") == []


# ---------------------------------------------------------------------------
# has_errors_after
# ---------------------------------------------------------------------------

def test_has_errors_after_true() -> None:
    before = datetime.now() - timedelta(seconds=1)
    ed = ExceptionDescriptor()
    ed.add_error("timeout", "test")
    assert ed.has_errors_after(before) is True


def test_has_errors_after_false() -> None:
    ed = ExceptionDescriptor()
    ed.add_error("timeout", "test")
    after = datetime.now() + timedelta(seconds=1)
    assert ed.has_errors_after(after) is False


# ---------------------------------------------------------------------------
# clear
# ---------------------------------------------------------------------------

def test_clear() -> None:
    ed = ExceptionDescriptor()
    ed.add_error("timeout", "test")
    ed.add_error("proxy", "test2")
    ed.clear()
    assert len(ed.errors) == 0
    assert ed.get_last_error() is None
