"""Sync weather_raw and weather_predictions from local SQLite to Neon PostgreSQL.

Called by the ingestion DAG after each daily cycle.
Skips silently when NEON_DATABASE_URL is not set.
"""

import logging
import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

# Load .env when running as a standalone script or in local Airflow
try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

logger = logging.getLogger(__name__)

DAYS_RAW = 180
DAYS_PREDICTIONS = 90

_CREATE_RAW = """
CREATE TABLE IF NOT EXISTS weather_raw (
    date                    TEXT NOT NULL,
    city                    TEXT NOT NULL,
    state                   TEXT,
    latitude                REAL,
    longitude               REAL,
    min_temp                REAL,
    max_temp                REAL,
    rainfall                REAL,
    rain_sum                REAL,
    precipitation_hours     REAL,
    evaporation             REAL,
    sunshine_hours          REAL,
    wind_gust_dir           TEXT,
    wind_gust_speed         REAL,
    wind_dir_9am            TEXT,
    wind_dir_3pm            TEXT,
    wind_speed_9am          REAL,
    wind_speed_3pm          REAL,
    humidity_9am            REAL,
    humidity_3pm            REAL,
    dew_point_9am           REAL,
    dew_point_3pm           REAL,
    pressure_9am            REAL,
    pressure_3pm            REAL,
    surface_pressure_9am    REAL,
    surface_pressure_3pm    REAL,
    cloud_9am               REAL,
    cloud_3pm               REAL,
    temp_9am                REAL,
    temp_3pm                REAL,
    rain_today              INTEGER,
    weather_code            INTEGER,
    shortwave_radiation_sum REAL,
    vpd_9am                 REAL,
    vpd_3pm                 REAL,
    wind_speed_100m_9am     REAL,
    wind_speed_100m_3pm     REAL,
    PRIMARY KEY (date, city)
)
"""

_CREATE_PRED = """
CREATE TABLE IF NOT EXISTS weather_predictions (
    date                    TEXT NOT NULL,
    city                    TEXT NOT NULL,
    rain_tomorrow           INTEGER,
    rain_tomorrow_proba     REAL,
    max_temp_tomorrow       REAL,
    weather_type_tomorrow   TEXT,
    comfort_score           REAL,
    heatwave_risk           REAL,
    frost_risk              REAL,
    storm_probability       REAL,
    predicted_at            TEXT,
    PRIMARY KEY (date, city)
)
"""

_CREATE_VIEW = """
CREATE OR REPLACE VIEW v_weather_full AS
SELECT
    r.date, r.city, r.state, r.latitude, r.longitude,
    r.min_temp, r.max_temp, r.rainfall, r.rain_sum, r.precipitation_hours,
    r.evaporation, r.sunshine_hours,
    r.wind_gust_dir, r.wind_gust_speed,
    r.wind_dir_9am, r.wind_dir_3pm,
    r.wind_speed_9am, r.wind_speed_3pm,
    r.humidity_9am, r.humidity_3pm,
    r.dew_point_9am, r.dew_point_3pm,
    r.pressure_9am, r.pressure_3pm,
    r.surface_pressure_9am, r.surface_pressure_3pm,
    r.cloud_9am, r.cloud_3pm,
    r.temp_9am, r.temp_3pm,
    r.rain_today, r.weather_code,
    r.shortwave_radiation_sum,
    r.vpd_9am, r.vpd_3pm,
    r.wind_speed_100m_9am, r.wind_speed_100m_3pm,
    p.rain_tomorrow, p.rain_tomorrow_proba,
    p.max_temp_tomorrow, p.weather_type_tomorrow,
    p.comfort_score, p.heatwave_risk, p.frost_risk,
    p.storm_probability, p.predicted_at
FROM weather_raw r
LEFT JOIN weather_predictions p ON r.date = p.date AND r.city = p.city
"""


def sync_to_postgres(days_raw: int = DAYS_RAW, days_predictions: int = DAYS_PREDICTIONS) -> None:
    db_url = os.getenv("NEON_DATABASE_URL", "")
    if not db_url:
        logger.info("NEON_DATABASE_URL not set — skipping PostgreSQL sync")
        return

    import psycopg2
    from psycopg2.extras import execute_values

    from pipeline.database import (
        DB_PATH,
        RAW_WEATHER_COLUMNS,
        WEATHER_PREDICTION_COLUMN_DEFS,
        get_connection,
    )

    pred_cols = [c for c, _ in WEATHER_PREDICTION_COLUMN_DEFS]
    cutoff_raw = (date.today() - timedelta(days=days_raw)).isoformat()
    cutoff_pred = (date.today() - timedelta(days=days_predictions)).isoformat()

    logger.info("Reading from SQLite (raw >= %s, pred >= %s)...", cutoff_raw, cutoff_pred)
    with get_connection(DB_PATH) as conn:
        raw_cols_sql = ", ".join(RAW_WEATHER_COLUMNS)
        df_raw = pd.read_sql(
            f"SELECT {raw_cols_sql} FROM weather_raw WHERE date >= ?", conn, params=(cutoff_raw,)
        )
        pred_cols_sql = ", ".join(pred_cols)
        df_pred = pd.read_sql(
            f"SELECT {pred_cols_sql} FROM weather_predictions WHERE date >= ?",
            conn,
            params=(cutoff_pred,),
        )

    logger.info("Syncing %d raw rows, %d prediction rows...", len(df_raw), len(df_pred))

    pg = psycopg2.connect(db_url)
    try:
        with pg.cursor() as cur:
            cur.execute(_CREATE_RAW)
            cur.execute(_CREATE_PRED)
            cur.execute(_CREATE_VIEW)
        pg.commit()

        _upsert(pg, df_raw, "weather_raw", ["date", "city"], execute_values)
        _upsert(pg, df_pred, "weather_predictions", ["date", "city"], execute_values)
        pg.commit()
        logger.info("PostgreSQL sync complete.")
    finally:
        pg.close()


def _to_python(v):
    """Convert numpy scalar to Python native; NaN/inf → None."""
    import math

    if v is None:
        return None
    if hasattr(v, "item"):  # numpy scalar → Python native int/float/str
        v = v.item()
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def _upsert(conn, df: pd.DataFrame, table: str, pk: list, execute_values_fn) -> None:
    if df.empty:
        return

    cols = list(df.columns)
    update_cols = [c for c in cols if c not in pk]
    col_list = ", ".join(cols)
    conflict_cols = ", ".join(pk)

    if update_cols:
        update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        sql = (
            f"INSERT INTO {table} ({col_list}) VALUES %s "
            f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_clause}"
        )
    else:
        sql = (
            f"INSERT INTO {table} ({col_list}) VALUES %s "
            f"ON CONFLICT ({conflict_cols}) DO NOTHING"
        )

    rows = [tuple(_to_python(v) for v in row) for row in df.itertuples(index=False)]
    with conn.cursor() as cur:
        execute_values_fn(cur, sql, rows, page_size=500)
