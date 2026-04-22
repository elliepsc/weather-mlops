import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "weather.db"


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(db_path)


def init_db(db_path: Path = DB_PATH):
    with get_connection(db_path) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS weather_raw (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                date            TEXT    NOT NULL,
                city            TEXT    NOT NULL,
                state           TEXT,
                latitude        REAL,
                longitude       REAL,
                min_temp        REAL,
                max_temp        REAL,
                rainfall        REAL,
                evaporation     REAL,
                sunshine_hours  REAL,
                wind_gust_dir   TEXT,
                wind_gust_speed REAL,
                wind_dir_9am    TEXT,
                wind_dir_3pm    TEXT,
                wind_speed_9am  REAL,
                wind_speed_3pm  REAL,
                humidity_9am    REAL,
                humidity_3pm    REAL,
                pressure_9am    REAL,
                pressure_3pm    REAL,
                cloud_9am       REAL,
                cloud_3pm       REAL,
                temp_9am        REAL,
                temp_3pm        REAL,
                rain_today      INTEGER,
                weather_code    INTEGER,
                UNIQUE(date, city)
            );

            CREATE TABLE IF NOT EXISTS weather_predictions (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                date                 TEXT    NOT NULL,
                city                 TEXT    NOT NULL,
                rain_tomorrow        INTEGER,
                rain_tomorrow_proba  REAL,
                max_temp_tomorrow    REAL,
                weather_type_tomorrow TEXT,
                comfort_score        REAL,
                heatwave_risk        REAL,
                frost_risk           REAL,
                storm_probability    REAL,
                predicted_at         TEXT,
                UNIQUE(date, city)
            );

            CREATE VIEW IF NOT EXISTS v_weather_full AS
            SELECT
                r.date,
                r.city,
                r.state,
                r.latitude,
                r.longitude,
                r.min_temp,
                r.max_temp,
                r.rainfall,
                r.evaporation,
                r.sunshine_hours,
                r.wind_gust_dir,
                r.wind_gust_speed,
                r.wind_dir_9am,
                r.wind_dir_3pm,
                r.wind_speed_9am,
                r.wind_speed_3pm,
                r.humidity_9am,
                r.humidity_3pm,
                r.pressure_9am,
                r.pressure_3pm,
                r.cloud_9am,
                r.cloud_3pm,
                r.temp_9am,
                r.temp_3pm,
                r.rain_today,
                r.weather_code,
                p.rain_tomorrow,
                p.rain_tomorrow_proba,
                p.max_temp_tomorrow,
                p.weather_type_tomorrow,
                p.comfort_score,
                p.heatwave_risk,
                p.frost_risk,
                p.storm_probability,
                p.predicted_at
            FROM weather_raw r
            LEFT JOIN weather_predictions p ON r.date = p.date AND r.city = p.city;
        """)


def upsert_weather_raw(df: pd.DataFrame, db_path: Path = DB_PATH):
    """Insert or replace raw weather rows (unique on date+city)."""
    with get_connection(db_path) as conn:
        df.to_sql("weather_raw_staging", conn, if_exists="replace", index=False)
        conn.execute("""
            INSERT OR REPLACE INTO weather_raw
                (date, city, state, latitude, longitude,
                 min_temp, max_temp, rainfall, evaporation, sunshine_hours,
                 wind_gust_dir, wind_gust_speed, wind_dir_9am, wind_dir_3pm,
                 wind_speed_9am, wind_speed_3pm, humidity_9am, humidity_3pm,
                 pressure_9am, pressure_3pm, cloud_9am, cloud_3pm,
                 temp_9am, temp_3pm, rain_today, weather_code)
            SELECT date, city, state, latitude, longitude,
                   min_temp, max_temp, rainfall, evaporation, sunshine_hours,
                   wind_gust_dir, wind_gust_speed, wind_dir_9am, wind_dir_3pm,
                   wind_speed_9am, wind_speed_3pm, humidity_9am, humidity_3pm,
                   pressure_9am, pressure_3pm, cloud_9am, cloud_3pm,
                   temp_9am, temp_3pm, rain_today, weather_code
            FROM weather_raw_staging
        """)
        conn.execute("DROP TABLE IF EXISTS weather_raw_staging")


def upsert_predictions(df: pd.DataFrame, db_path: Path = DB_PATH):
    """Insert or replace prediction rows (unique on date+city)."""
    with get_connection(db_path) as conn:
        df.to_sql("pred_staging", conn, if_exists="replace", index=False)
        conn.execute("""
            INSERT OR REPLACE INTO weather_predictions
                (date, city, rain_tomorrow, rain_tomorrow_proba, max_temp_tomorrow,
                 weather_type_tomorrow, comfort_score, heatwave_risk,
                 frost_risk, storm_probability, predicted_at)
            SELECT date, city, rain_tomorrow, rain_tomorrow_proba, max_temp_tomorrow,
                   weather_type_tomorrow, comfort_score, heatwave_risk,
                   frost_risk, storm_probability, predicted_at
            FROM pred_staging
        """)
        conn.execute("DROP TABLE IF EXISTS pred_staging")


def read_all(table: str = "v_weather_full", db_path: Path = DB_PATH) -> pd.DataFrame:
    with get_connection(db_path) as conn:
        return pd.read_sql(f"SELECT * FROM {table} ORDER BY date, city", conn)


def read_raw(db_path: Path = DB_PATH) -> pd.DataFrame:
    return read_all("weather_raw", db_path)


def get_latest_date(city: str, db_path: Path = DB_PATH) -> str | None:
    with get_connection(db_path) as conn:
        row = conn.execute(
            "SELECT MAX(date) FROM weather_raw WHERE city = ?", (city,)
        ).fetchone()
    return row[0] if row else None
