"""Data conversion helpers used across runtime utilities."""

from collections.abc import Mapping
from dataclasses import is_dataclass
from typing import Any, cast

_SENTINEL = object()


def ns_to_sec(nanoseconds: int | float) -> float:
    """Converts nanoseconds to seconds."""
    return nanoseconds / 1_000_000_000


def sec_to_min(seconds: int | float, round_to: int = 0) -> int | float:
    """Converts seconds to minutes"""
    minutes = seconds / 60
    return round(minutes, round_to) if round_to > 0 else int(minutes)


def min_to_h(minutes: int | float, round_to: int = 0) -> int | float:
    """Converts minutes to hours"""
    hours = minutes / 60
    return round(hours, round_to) if round_to > 0 else int(hours)


def sec_to_h(seconds: int | float, round_to: int = 0) -> int | float:
    """Converts seconds to hours"""
    return min_to_h(sec_to_min(seconds), round_to=round_to)


def to_none(value: Any) -> Any | None:
    """Converts None NaN NaT to None"""
    if value is None:
        return None
    if str(value).lower() in ['none', 'nan', 'nat']:
        return None
    return value


def object_to_dict(obj: Any) -> dict[str, Any] | Any:
    """
    Best-effort conversion of obj to a plain `dict` suitable for deterministic hashing.
    """

    # If it is a mapping, return dict version without callables or private keys
    if isinstance(obj, Mapping):
        obj_map = dict(cast(Mapping[str, Any], obj))
        return {
            k: v for k, v in obj_map.items()
            if not callable(v) and not k.startswith("_")
        }

    # Dataclass instances are converted through vars(...)
    if is_dataclass(obj) and not isinstance(obj, type):
        obj_map = vars(obj)
        return {
            k: v for k, v in obj_map.items()
            if not callable(v) and not k.startswith("_")
        }

    # If it is generic object with attribute __dict__
    raw = getattr(obj, "__dict__", _SENTINEL)
    if isinstance(raw, dict):
        clean: dict[str, Any] = {}
        for key, val in cast(dict[str, Any], raw).items():
            if key.startswith("_") or callable(val):
                continue
            clean[key] = val
        return clean

    # If class contains __slots__ only
    if hasattr(obj, "__slots__"):
        clean = {}
        slots = getattr(obj, "__slots__", ())
        if isinstance(slots, str):
            slots = (slots,)

        for slot in slots:
            if slot.startswith("_"):
                continue
            val = getattr(obj, slot, _SENTINEL)
            if val is not _SENTINEL and not callable(val):
                clean[slot] = val
        return clean

    # primitives, lists, tuples, enums, other types
    return obj
