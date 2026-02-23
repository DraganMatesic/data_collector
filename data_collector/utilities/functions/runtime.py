"""Runtime utility functions used by hashing and synchronization workflows."""

from __future__ import annotations

import hashlib
import importlib
import json
import logging
import re
from collections.abc import Sequence
from enum import Enum
from typing import Any

from data_collector.utilities.functions import converters


def is_module_available(module_name: str) -> bool:
    """Return True when a Python module is importable."""
    try:
        importlib.import_module(module_name)
        return True
    except ImportError:
        return False


def list_enum_values(enum_cls: type[Enum]) -> list[Any]:
    """Return declared enum values in order."""
    return [member.value for member in enum_cls]


def obj_diff(
    new_objs: Sequence[Any],
    existing_objs: Sequence[Any],
    compare_key: str | list[str] | tuple[str, ...] = "sha",
    logger: logging.Logger | None = None,
) -> tuple[list[Any], list[Any]]:
    """Compare two object collections and return (to_insert, to_remove)."""

    def get_key(obj: Any) -> Any:
        if isinstance(compare_key, (list, tuple)):
            return tuple(getattr(obj, key) for key in compare_key)
        return getattr(obj, compare_key)

    new_map: dict[Any, Any] = {}
    for obj in new_objs:
        key = get_key(obj)
        if key in new_map and logger:
            logger.warning("Duplicate key in new_objs: %s", key)
        new_map[key] = obj

    existing_map: dict[Any, Any] = {}
    for obj in existing_objs:
        key = get_key(obj)
        if key in existing_map and logger:
            logger.warning("Duplicate key in existing_objs: %s", key)
        existing_map[key] = obj

    to_insert = [obj for key, obj in new_map.items() if key not in existing_map]
    to_remove = [obj for key, obj in existing_map.items() if key not in new_map]
    return to_insert, to_remove


def make_hash(
    data: dict[str, Any] | str | Any,
    constructor: str = "sha3_256",
    on_keys: list[str] | None = None,
    exclude_keys: list[str] | None = None,
    sort_keys: bool = True,
    inplace: bool = False,
    hash_col_name: str = "sha",
    normalize_case: bool = True,
    no_spacing: bool = True,
) -> str | dict[str, Any] | Any:
    """Create a deterministic hash from input data."""
    try:
        hash_func = hashlib.new(constructor)
    except ValueError as exc:
        raise ValueError(f"Hash constructor '{constructor}' not available.") from exc

    hash_data: dict[str, Any] | str | Any = converters.object_to_dict(data)

    if isinstance(hash_data, dict):
        hash_data.pop(hash_col_name, None)

        if exclude_keys:
            hash_data = {k: v for k, v in hash_data.items() if k not in exclude_keys}

        if on_keys:
            hash_data = {k: hash_data[k] for k in on_keys if k in hash_data}

        if sort_keys:
            hash_data = dict(sorted(hash_data.items(), key=lambda item: item[0]))

    if normalize_case:
        if isinstance(hash_data, dict):
            hash_data = {
                k: (v.lower() if isinstance(v, str) else v) for k, v in hash_data.items()
            }
        if isinstance(hash_data, str):
            hash_data = hash_data.lower()

    if no_spacing:
        if isinstance(hash_data, dict):
            hash_data = {
                k: (re.sub(r"\s+", "", v) if isinstance(v, str) else v) for k, v in hash_data.items()
            }
        if isinstance(hash_data, str):
            hash_data = re.sub(r"\s+", "", hash_data)

    payload = (
        json.dumps(hash_data, sort_keys=sort_keys, ensure_ascii=False, default=str).encode()
        if not isinstance(hash_data, str)
        else hash_data.encode()
    )
    hash_func.update(payload)
    hash_value = hash_func.hexdigest()

    if inplace:
        if isinstance(data, dict):
            data.update({hash_col_name: hash_value})
            return data

        if hasattr(data, hash_col_name):
            setattr(data, hash_col_name, hash_value)
            return data

    return hash_value


def bulk_hash(data: list[Any], **kwargs: Any) -> list[Any]:
    """Hash every element in *data* by delegating to :func:`make_hash`."""
    hashed_list: list[Any] = []

    if "inplace" not in kwargs:
        kwargs.update({"inplace": True})

    for item in data:
        hashed_list.append(make_hash(item, **kwargs))

    return hashed_list
