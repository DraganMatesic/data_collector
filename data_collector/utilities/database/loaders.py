import os
import importlib
from data_collector.utilities.functions import runtime

def initialize_oracle_client(logger=None) -> bool:
    """
    Initializes the Oracle Instant Client if the environment variable `oracle_client` is set.

    :param logger: Optional logger for error or debug output
    :return: True if initialization was attempted (and succeeded or already initialized), False otherwise
    """
    try:
        cx_oracle = importlib.import_module('cx_Oracle')
    except ImportError:
        if logger:
            logger.error("cx_Oracle module not installed.")
        return False

    oracle_path = os.getenv('oracle_client')

    if not oracle_path:
        if logger:
            logger.warning("Environment variable 'oracle_client' is not set.")
        return False

    try:
        cx_oracle.init_oracle_client(lib_dir=oracle_path)
        if logger:
            logger.info(f"Oracle Instant Client initialized from: {oracle_path}")
        return True
    except cx_oracle.ProgrammingError as e:
        # Already initialized is a known safe case
        if "already initialized" in str(e):
            if logger:
                logger.info("Oracle client was already initialized.")
            return True
        if logger:
            logger.error(f"Oracle client initialization failed: {e}")
        return False


def check_oracle(logger=None):
    # checks if oracle lib is installed
    if not runtime.is_module_available('cx_Oracle'):
        raise ImportError("cx_Oracle is required but not installed. Install it with `pip install cx_Oracle`.")

    # checks if oracle client is initialized
    if not initialize_oracle_client(logger=logger):
            raise RuntimeError("Oracle initialization failed or 'oracle_client' env variable missing.")


def check_pyodbc():
    # checks if pyodbc lib is installed
    if not runtime.is_module_available('pyodbc'):
        raise ImportError("pyodbc is required but not installed. Install it with `pip install pyodbc`.")

