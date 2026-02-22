import os


# Default values allow imports of settings modules in local/unit contexts.
# Integration tests still skip when database connectivity is unavailable.
TEST_ENV_DEFAULTS = {
    "DC_DB_MAIN_USERNAME": "postgres",
    "DC_DB_MAIN_PASSWORD": "postgres",
    "DC_DB_MAIN_DATABASENAME": "postgres",
    "DC_DB_MAIN_IP": "127.0.0.1",
    "DC_DB_MAIN_PORT": "5432",
}

for env_key, env_value in TEST_ENV_DEFAULTS.items():
    os.environ.setdefault(env_key, env_value)
