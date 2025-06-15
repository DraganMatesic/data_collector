import inspect
from typing import Dict, Any, Union
from collections.abc import Mapping
from dataclasses import is_dataclass, asdict

_SENTINEL = object()

def ns_to_sec(nanoseconds):
    """Converts nanoseconds to seconds"""
    return nanoseconds / 1000000


def sec_to_min(seconds, round_to=0):
    """Converts seconds to minutes"""
    return int(seconds/60)


def min_to_h(minutes):
    """Converts minutes to hours"""
    return int(minutes/60)


def sec_to_h(seconds):
    """Converts seconds to hours"""
    return min_to_h(sec_to_min(seconds))

def to_none(value):
    """Converts None NaN NaT to None"""
    if value is None:
        return None
    if str(value).lower() in ['none', 'nan', 'nat']:
        return None
    return value



def object_to_dict(obj: Any) -> Union[Dict[str, Any], Any]:
    """
    Best-effort conversion of obj to a plain `dict` suitable for deterministic hashing.
    """

    # If it is mapping or dataclass return dict version, without callables, or non str keys
    if isinstance(obj, Mapping) or is_dataclass(obj):
        return {k: v for k, v in obj.items()
                if not callable(v) and (isinstance(k, str) and not k.startswith("_"))}

    # If it is generic object with attribute __dict__
    obj_dict: Dict[str, Any] | object = getattr(obj, "__dict__", _SENTINEL)
    if obj_dict is not _SENTINEL:
        clean: Dict[str, Any] = {}
        for key, val in obj_dict.items():
            if key.startswith("_") or callable(val):
                continue
            clean[key] = val
        return clean

    # If class contains __slots__ only
    if hasattr(obj, "__slots__"):
        clean = {}
        slots = obj.__slots__
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

