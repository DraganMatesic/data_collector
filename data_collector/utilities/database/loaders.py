import os
import importlib
from data_collector.utilities.functions import runtime

def initialize_oracle_client(logger=None) -> bool:
    """
    Initializes the Oracle Instant Client if the environment variable `oracle_client` is set.

    oracledb runs in thin mode by default (no Oracle Client needed).
    If `oracle_client` env var is set, it switches to thick mode for advanced features.

    :param logger: Optional logger for error or debug output
    :return: True if initialization succeeded, False otherwise
    """
    try:
        oracledb = importlib.import_module('oracledb')
    except ImportError:
        if logger:
            logger.error("oracledb module not installed.")
        return False

    oracle_path = os.getenv('oracle_client')

    if not oracle_path:
        if logger:
            logger.info("No 'oracle_client' env var set. Using oracledb thin mode (no Oracle Client required).")
        return True

    try:
        oracledb.init_oracle_client(lib_dir=oracle_path)
        if logger:
            logger.info(f"Oracle thick mode initialized from: {oracle_path}")
        return True
    except oracledb.ProgrammingError as e:
        # Already initialized is a known safe case
        if "already initialized" in str(e):
            if logger:
                logger.info("Oracle client was already initialized.")
            return True
        if logger:
            logger.error(f"Oracle client initialization failed: {e}")
        return False


def check_oracle(logger=None):
    # checks if oracledb lib is installed
    if not runtime.is_module_available('oracledb'):
        raise ImportError("oracledb is required but not installed. Install it with `pip install oracledb`.")

    # checks if oracle client is initialized
    if not initialize_oracle_client(logger=logger):
            raise RuntimeError("Oracle initialization failed or 'oracle_client' env variable missing.")


def check_pyodbc():
    # checks if pyodbc lib is installed
    if not runtime.is_module_available('pyodbc'):
        raise ImportError("pyodbc is required but not installed. Install it with `pip install pyodbc`.")

