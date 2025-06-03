import inspect
import logging
import sys
import os

def create_logger(module_name: str = None):
    logger_name = module_name or get_caller_module_name()
    logger = logging.getLogger(logger_name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)
        stream_handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger

def get_caller_module_name():
    frame = inspect.stack()[2]
    filepath = frame.filename
    project_root = find_project_root(filepath)
    rel_path = os.path.relpath(filepath, start=project_root)
    module_name = os.path.splitext(rel_path.replace(os.sep, "."))[0]
    return module_name


def find_project_root(start_path):
    """
    Walks up the file tree until it finds a directory that doesn't contain __init__.py
    Assumes project root is above your package
    """
    path = os.path.abspath(start_path)
    while True:
        parent = os.path.dirname(path)
        if not os.path.exists(os.path.join(parent, '__init__.py')):
            return parent
        if parent == path:  # root reached
            break
        path = parent
    return os.path.dirname(start_path)