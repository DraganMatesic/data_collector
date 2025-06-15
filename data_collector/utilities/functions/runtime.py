"""
Contains utility functions for runtime logging and app/function identification,
primarily used by manager.py.
"""
import re
import sys
import json
import logging
import hashlib
import importlib
from datetime import datetime
from typing import Optional, List, Union, Tuple, Dict

from yaml import serialize

from data_collector.utilities.functions import math
from data_collector.utilities.functions import converters


def app_group_check(app_group, app_parent, app_name):
    """
    Normalizes the app_group value for special cases.
    """
    if app_parent == 'data_collector' and app_name == 'data_collector':
        return 'data_collector'
    return app_group


def get_app_id(app_group, app_parent, app_name):
    """
    Generates a unique app ID using SHA-256 hash based on app identifiers.
    """
    app_group = app_group_check(app_group, app_parent, app_name)
    return hashlib.sha256(f"{app_group}|{app_parent}|{app_name}".encode()).hexdigest()


def get_function_info(app_group, app_parent, app_name, function_name):
    """
    Returns function information including a unique function ID.
    """
    app_group = app_group_check(app_group, app_parent, app_name)
    function_id = hashlib.sha256(f"{app_group}|{app_parent}|{app_name}|{function_name}".encode()).hexdigest()
    return {'function_id': function_id, 'function_name': function_name}


def app_split_path(filepath, no=-4):
    """
    Splits the filepath into components starting from the specified index `no`.
    Automatically handles both Unix (`/`) and Windows (`\\`) path separators.

    :param filepath: Full file path as a string.
    :param no: Index to start slicing the split path from (default is -4).
    :return: List of path components from the specified position.
    """
    return filepath.split('/')[no:] if '/' in filepath else filepath.split('\\')[no:]


def get_app_info(filepath, only_id=False):
    """
    Extracts and returns app-related metadata from a file path.
    Can return only the app ID if `only_id=True`.
    """
    app_group, app_parent, app_name, module_name = app_split_path(filepath)
    app_group = app_group_check(app_group, app_parent, app_name)
    app_id = get_app_id(app_group, app_parent, app_name)

    if only_id:
        return app_id

    return {
        'app_id': app_id,
        'app_group': app_group,
        'app_parent': app_parent,
        'app_name': app_name,
        'module_name': module_name,
        'filepath': filepath
    }


def get_runtime(filepath):
    """
    Generates a unique runtime string based on the current timestamp and app ID.
    """
    return hashlib.sha256(f"{datetime.now()}{get_app_info(filepath).get('app_id')}".encode()).hexdigest()


def runtime_start(filepath, function_name, idx, runtime):
    """
    Captures and returns metadata at the start of a function runtime execution.
    """
    start_time = datetime.now()
    app_group, app_parent, app_name, _ = app_split_path(filepath)
    app_group = app_group_check(app_group, app_parent, app_name)
    app_id = get_app_id(app_group, app_parent, app_name)

    extra = {
        'runtime': runtime,
        'start_time': start_time,
        'rlevel': True,
        'function_no': idx,
        'main_app': app_id
    }
    extra.update(get_app_info(filepath))
    extra.update(get_function_info(app_group, app_parent, app_name, function_name))
    return extra


def runtime_end(runtime, runtime_start, filepath, function_name):
    """
    Captures and returns metadata at the end of a function runtime execution,
    including duration in seconds, minutes, and hours.
    """
    end_time = datetime.now()
    totals = math.get_totals(start=runtime_start, end=end_time)
    extra = {
        'runtime': runtime,
        'end_time': end_time,
        'totals': totals,
        'totalm': converters.sec_to_min(totals),
        'totalh': converters.sec_to_h(totals),
        'rlevel': True,
        'start_time': runtime_start
    }

    app_group, app_parent, app_name, _ = app_split_path(filepath)
    app_group = app_group_check(app_group, app_parent, app_name)
    app_id = get_app_id(app_group, app_parent, app_name)

    extra.update({'main_app': app_id})
    extra.update(get_app_info(filepath))
    extra.update(get_function_info(app_group, app_parent, app_name, function_name))
    return extra


def function_end(runtime_start):
    """
    Returns a lightweight runtime summary without app/function metadata.
    """
    end_time = datetime.now()
    totals = math.get_totals(start=runtime_start, end=end_time)
    return {
        'end_time': end_time,
        'totals': totals,
        'totalm': converters.sec_to_min(totals),
        'totalh': converters.sec_to_h(totals)
    }


def pop_rlevel(extra):
    """
    Removes the 'rlevel' key from the metadata if present.
    Useful for sanitizing logs.
    """
    if extra:
        extra = extra.copy()
        extra.pop('rlevel', None)
    return extra


def make_extra(filepath, extra=None, function_name=None, function_no=None):
    """
    Generates and combines extra metadata related to file, app, and function.
    """
    app_group, app_parent, app_name, module_name = app_split_path(filepath)
    app_group = app_group_check(app_group, app_parent, app_name)
    app_id = get_app_id(app_group, app_parent, app_name)

    func_extra = {
        'filepath': filepath,
        'app_group': app_group,
        'app_parent': app_parent,
        'app_name': app_name,
        'module_name': module_name,
        'app_id': app_id
    }

    if function_name:
        func_extra['function_name'] = function_name
        func_extra['function_id'] = hashlib.sha256(
            f"{app_group}|{app_parent}|{app_name}|{function_name}".encode()).hexdigest()

    if function_no is not None:
        func_extra['function_no'] = function_no

    # Merge or initialize metadata
    extra_c = func_extra if extra is None else {**extra.copy(), **func_extra}
    return extra_c


def is_module_available(module_name: str) -> bool:
    """
    Checks if a Python module is installed and importable.

    :param module_name: name of the module (e.g. 'pyodbc', 'pymssql', 'cx_oracle')
    :return: True if module can be imported, False otherwise
    """
    try:
        importlib.import_module(module_name)
        return True
    except ImportError:
        return False

def list_enum_values(enum_cls) -> list[str]:
    """
    Returns values of class that inherited Enum class
    """
    return [member.value for member in enum_cls]

def setup_logger(name: str = 'my_logger', level=logging.INFO) -> logging.Logger:
    """
    Creates and returns a logger that writes to sys.stdout using a stream handler.

    :param name: Name of the logger
    :param level: Logging level (e.g., logging.INFO)
    :return: Configured Logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding duplicate handlers
    if not logger.handlers:
        stream_handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger


def obj_diff(
    new_objs,
    existing_objs,
    compare_key: Union[str, List[str], Tuple[str, ...]] = 'sha',
    logger=None
):
    """
    Compares new and existing SQLAlchemy objects using a single or composite key.

    Args:
        new_objs: List of new ORM objects (e.g., from web).
        existing_objs: List of current DB ORM objects.
        compare_key: One or more attribute names (str or list/tuple of str) used to uniquely identify each object.
        logger: Optional logger to warn about duplicates.

    Returns:
        to_insert: objects in new_objs but not in existing_objs
        to_remove: objects in existing_objs but not in new_objs
    """

    def get_key(obj):
        if isinstance(compare_key, (list, tuple)):
            return tuple(getattr(obj, key) for key in compare_key)
        return getattr(obj, compare_key)

    new_map = {}
    for obj in new_objs:
        key = get_key(obj)
        if key in new_map and logger:
            logger.warning(f"Duplicate key in new_objs: {key}")
        new_map[key] = obj

    existing_map = {}
    for obj in existing_objs:
        key = get_key(obj)
        if key in existing_map and logger:
            logger.warning(f"Duplicate key in existing_objs: {key}")
        existing_map[key] = obj

    to_insert = [obj for k, obj in new_map.items() if k not in existing_map]
    to_remove = [obj for k, obj in existing_map.items() if k not in new_map]

    return to_insert, to_remove


def make_hash(
        data: Union[dict, str, object],
        constructor: Optional[str] = 'sha3_256',
        on_keys: Optional[List[str]] = None,
        exclude_keys: Optional[List[str]] = None,
        sort_keys: bool = True,
        inplace: bool = False,
        hash_col_name: str = 'sha',
        normalize_case: bool = True,
        no_spacing: bool = True
) -> Union[str, dict, object]:
    """
    Creates a hash value from provided data.

    Args:
        data: Any input — string, object, or dictionary.
        constructor: Name of the hash function to use (default: 'sha3_256').
        on_keys: hash only these keys or attributes (optional).
        exclude_keys: exclude keys or attributes from hash (optional).
        sort_keys: Whether to sort dictionary keys before hashing (default: True).
        inplace: It will update dict or object if hash key or attribute is available
        hash_col_name: If user has different name for hash column that represents uniqueness of row
        normalize_case: if True all values will be converted in lower case when calculating hash (default: True)
        no_spacing: if True in all string values spaces will be removed (default: True)
    Returns:
        Hexadecimal string representing the hash.

    Raises:
        ValueError: If the constructor is invalid or not callable.
    """
    try:
        hash_func = hashlib.new(constructor)
    except ValueError as exc:
        raise ValueError(f"Hash constructor '{constructor}' not available.") from exc

    # Convert object to dictionary for hashing
    hash_data = converters.object_to_dict(data)

    # Options if type is dictionary
    if isinstance(hash_data, dict):
        # Never hash the existing digest - remove if exists
        hash_data.pop(hash_col_name, None)

        # excluding keys specific keys from hash
        if exclude_keys:
            hash_data = {k: v for k,v in hash_data.items() if k not in exclude_keys}

        # isolating specific keys for hash
        if on_keys:
            hash_data = {k: hash_data[k] for k in on_keys if k in hash_data}

        # sorting keys from hash
        if sort_keys:
            # Sorting both keys and nested values for deep consistency
            hash_data = dict(sorted(hash_data.items(), key=lambda item: item[0]))

    # normalize case
    if normalize_case is True:
        if isinstance(hash_data, dict):
            hash_data = {k: (v.lower() if isinstance(v, str) else v)
                         for k, v in hash_data.items()}
        if isinstance(hash_data, str):
            hash_data = hash_data.lower()

    # removing spacing
    if no_spacing is True:
        if isinstance(hash_data, dict):
            hash_data = {k: (re.sub(r'\s+', '', v) if isinstance(v, str) else v)
                         for k, v in hash_data.items()}
        if isinstance(hash_data, str):
            hash_data = re.sub(r'\s+', '', hash_data)

    # Calculating hash value
    payload = (
        json.dumps(hash_data, sort_keys=sort_keys,
                   ensure_ascii=False, default=str).encode()
        if not isinstance(hash_data, str)
        else hash_data.encode()
    )
    hash_func.update(payload)
    hash_value = hash_func.hexdigest()

    if inplace is True:
        # Update dictionary hash value if inplace True
        if isinstance(data, dict):
            data.update({hash_col_name: hash_value})
            return data

        # Update object if attribute sha is present
        if isinstance(data, object) and hasattr(data, hash_col_name):
            setattr(data, hash_col_name, hash_value)
            return data

    return hash_value


def bulk_hash(
        data: Union[List[Dict], List[object], List[str]],
        **kwargs
) ->  Union[List[Dict], List[object], List[str]]:
    """
        Hash every element in *data* by delegating to :func:`make_hash`.
    Args:
        data: An iterable (list, tuple, generator) whose items are
            strings, dictionaries, or arbitrary Python objects.

        kwargs: Any keyword accepted by :func:`make_hash`

    Returns:
        List[Union[str, dict, object]]:
            - If ``inplace`` is -- True -- (the default when not supplied),
              each list element is the same object it came in with,
              but now contains/has its ``sha`` field or attribute set.
            - If ``inplace`` is -- False --, the list contains the hexadecimal
              hash strings returned by :func:`make_hash`.
    """

    hashed_list = list()

    # Enforcing inplace to True if not specified
    if 'inplace' in kwargs:
        kwargs.get('inplace')
    else:
        kwargs.update({"inplace": True})

    # Iteration over list
    for item in data:
         hashed_list.append(make_hash(item, **kwargs))

    return hashed_list


