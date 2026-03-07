"""Runtime utility functions used by hashing and synchronization workflows."""

from __future__ import annotations

import hashlib
import importlib
import json
import logging
import re
from collections.abc import Sequence
from enum import Enum
from pathlib import PurePath
from typing import Any, TypedDict, cast

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
        hash_data = cast(dict[str, Any], hash_data)
        hash_data.pop(hash_col_name, None)

        if exclude_keys:
            hash_data = {k: v for k, v in hash_data.items() if k not in exclude_keys}

        if on_keys:
            hash_data = {k: hash_data[k] for k in on_keys if k in hash_data}

        if sort_keys:
            hash_data = dict(sorted(hash_data.items(), key=lambda item: item[0]))

    if normalize_case:
        if isinstance(hash_data, dict):
            hash_data = cast(dict[str, Any], hash_data)
            hash_data = {
                k: (v.lower() if isinstance(v, str) else v) for k, v in hash_data.items()
            }
        if isinstance(hash_data, str):
            hash_data = hash_data.lower()

    if no_spacing:
        if isinstance(hash_data, dict):
            hash_data = cast(dict[str, Any], hash_data)
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
            data = cast(dict[str, Any], data)
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


class AppInfo(TypedDict):
    """Typed dictionary returned by :func:`get_app_info`."""

    app_id: str
    app_group: str
    app_parent: str
    app_name: str
    parent_id: str
    module_name: str
    filepath: str


def _resolve_app_group(app_group: str, app_parent: str, app_name: str) -> str:
    """Resolve the effective app group for framework-internal modules.

    When both ``app_parent`` and ``app_name`` equal ``"data_collector"``, the module
    belongs to the framework itself rather than a user application.  In that case the
    group is normalised to ``"data_collector"`` regardless of the directory above it.
    """
    if app_parent == "data_collector" and app_name == "data_collector":
        return "data_collector"
    return app_group


def _split_app_path(filepath: str, depth: int = -4) -> list[str]:
    """Split a file path and return the last *abs(depth)* components.

    Uses :class:`pathlib.PurePath` for cross-platform path handling.

    Args:
        filepath: Absolute or relative path to a Python module.
        depth: Negative index indicating how many trailing path components
            to return.  Default ``-4`` yields
            ``[app_group, app_parent, app_name, module_name]``.

    Raises:
        ValueError: If the path contains fewer components than ``abs(depth)``.
    """
    parts = PurePath(filepath).parts
    required = abs(depth)
    if len(parts) < required:
        raise ValueError(
            f"Path '{filepath}' has {len(parts)} components, need at least {required}"
        )
    return list(parts[depth:])


def get_parent_id(app_group: str, app_parent: str) -> str:
    """Compute a deterministic parent identifier from the application hierarchy.

    Concatenates ``app_group|app_parent`` with a pipe separator and
    returns the SHA-256 hex digest (64 characters).

    Args:
        app_group: Top-level grouping (typically country code or ``"examples"``).
        app_parent: Parent application or domain (e.g. ``"financials"``).
    """
    return hashlib.sha256(f"{app_group}|{app_parent}".encode()).hexdigest()


def get_app_id(app_group: str, app_parent: str, app_name: str) -> str:
    """Compute a deterministic app identifier from the application hierarchy.

    Concatenates ``app_group|app_parent|app_name`` with pipe separators and
    returns the SHA-256 hex digest (64 characters).

    Args:
        app_group: Top-level grouping (typically country code or ``"examples"``).
        app_parent: Parent application or domain (e.g. ``"financials"``).
        app_name: Application directory name (e.g. ``"company_data"``).
    """
    app_group = _resolve_app_group(app_group, app_parent, app_name)
    return hashlib.sha256(f"{app_group}|{app_parent}|{app_name}".encode()).hexdigest()


def get_app_info(filepath: str, only_id: bool = False, depth: int = -4) -> str | AppInfo:
    """Derive application identity from a module's file path.

    Extracts trailing path components and computes a SHA-256 ``app_id``
    from ``app_group``, ``app_parent``, and ``app_name``.

    With the default ``depth=-4`` (production apps), the path is split as
    ``(app_group, app_parent, app_name, module_name)`` -- e.g.
    ``data_collector/cro/financials/company_data/main.py``.

    With ``depth=-3`` (flat examples/tests), the path is split as
    ``(app_group, app_parent, module_name)`` and ``app_name`` is derived
    from the module filename stem -- e.g.
    ``data_collector/examples/fun_watch/01_basic_decorator.py``.

    Typical usage::

        # Production app (depth=-4, default)
        app_id = get_app_info(__file__, only_id=True)

        # Flat example/test (depth=-3)
        app_info = get_app_info(__file__, depth=-3)

    Args:
        filepath: Path to the Python module (usually ``__file__``).
        only_id: When ``True``, return only the ``app_id`` string instead
            of the full :class:`AppInfo` dictionary.
        depth: Number of trailing path components to extract.  Use ``-4``
            (default) for production apps and ``-3`` for flat-structured
            examples or tests.

    Returns:
        The 64-character hex ``app_id`` when *only_id* is ``True``, otherwise
        an :class:`AppInfo` dictionary with keys ``app_id``, ``app_group``,
        ``app_parent``, ``app_name``, ``module_name``, and ``filepath``.

    Raises:
        ValueError: If the path has fewer components than ``abs(depth)``
            or *depth* resolves to fewer than 3 components.
    """
    parts = _split_app_path(filepath, depth=depth)
    if len(parts) == 3:
        app_group, app_parent, module_name = parts
        app_name = PurePath(module_name).stem
    elif len(parts) >= 4:
        app_group, app_parent, app_name, module_name = parts[0], parts[1], parts[2], parts[3]
    else:
        raise ValueError(f"Expected 3 or 4 path components, got {len(parts)}")
    app_group = _resolve_app_group(app_group, app_parent, app_name)
    app_id = get_app_id(app_group, app_parent, app_name)
    if only_id:
        return app_id
    return AppInfo(
        app_id=app_id,
        app_group=app_group,
        app_parent=app_parent,
        app_name=app_name,
        parent_id=get_parent_id(app_group, app_parent),
        module_name=module_name,
        filepath=filepath,
    )
