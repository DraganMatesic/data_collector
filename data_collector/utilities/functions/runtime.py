"""
Contains utility functions for runtime logging and app/function identification,
primarily used by manager.py.
"""
import sys
import logging
import hashlib
import importlib
from datetime import datetime
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
