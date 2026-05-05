"""Centralized path and environment configuration for the API."""

import os
from pathlib import Path

ROOT = Path(__file__).parent.parent

DEMO_MODE: bool = os.getenv("DEMO_MODE", "false").lower() in ("1", "true", "yes")

# SQLite pipeline database
from pipeline.database import DB_PATH as _PROD_DB_PATH

_DEMO_DB = ROOT / "data" / "demo" / "weather_demo.db"
APP_DB_PATH: Path = _DEMO_DB if DEMO_MODE else _PROD_DB_PATH

# DuckDB analytics database — overridable via DUCKDB_PATH env var
_default_duckdb = (
    ROOT / "data" / "demo" / "analytics_demo.duckdb"
    if DEMO_MODE
    else ROOT / "data" / "analytics.duckdb"
)
DUCKDB_PATH: Path = Path(os.getenv("DUCKDB_PATH", str(_default_duckdb)))
ANALYTICS_DB_PATH: Path = DUCKDB_PATH

# Models
MODELS_PATH: Path = ROOT / "models"
MLFLOW_JSON_PATH: Path = MODELS_PATH / "mlflow_latest.json"

# Data exports
DATA_EXPORTS_PATH: Path = ROOT / "data" / "output"
OUTPUT_CSV: Path = DATA_EXPORTS_PATH / "weather_final.csv"
