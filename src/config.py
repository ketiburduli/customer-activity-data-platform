"""
config.py

Purpose
-------
Centralised project configuration.

Responsibilities
----------------
- Define project paths.
- Store ClickHouse connection settings.
- Keep environment-specific values in one place.
"""
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_DATA_DIR = PROJECT_ROOT / "sample-data"

PAM_CUSTOMERS_PATH = SAMPLE_DATA_DIR / "pam_customers.json"
APP_CUSTOMERS_PATH = SAMPLE_DATA_DIR / "app_customers.json"
ACTIVITY_EVENTS_PATH = SAMPLE_DATA_DIR / "activity_events.json"
CUSTOMER_SESSIONS_PATH = SAMPLE_DATA_DIR / "customer_sessions.json"

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "dpe"
CLICKHOUSE_PASSWORD = "dpe"
CLICKHOUSE_DATABASE = "warehouse"