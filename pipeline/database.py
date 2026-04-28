import sqlite3
from pathlib import Path

import pandas as pd

from config.settings import settings

ROOT = Path(__file__).parent.parent


def _resolve_db_path() -> Path:
    configured = Path(settings.db_path).expanduser()
    return configured if configured.is_absolute() else ROOT / configured


DB_PATH = _resolve_db_path()
RAW_WEATHER_COLUMN_DEFS = [
    ("date", "TEXT NOT NULL"),
    ("city", "TEXT NOT NULL"),
    ("state", "TEXT"),
    ("latitude", "REAL"),
    ("longitude", "REAL"),
    ("min_temp", "REAL"),
    ("max_temp", "REAL"),
    ("rainfall", "REAL"),
    ("rain_sum", "REAL"),
    ("precipitation_hours", "REAL"),
    ("evaporation", "REAL"),
    ("sunshine_hours", "REAL"),
    ("wind_gust_dir", "TEXT"),
    ("wind_gust_speed", "REAL"),
    ("wind_dir_9am", "TEXT"),
    ("wind_dir_3pm", "TEXT"),
    ("wind_speed_9am", "REAL"),
    ("wind_speed_3pm", "REAL"),
    ("humidity_9am", "REAL"),
    ("humidity_3pm", "REAL"),
    ("dew_point_9am", "REAL"),
    ("dew_point_3pm", "REAL"),
    ("pressure_9am", "REAL"),
    ("pressure_3pm", "REAL"),
    ("surface_pressure_9am", "REAL"),
    ("surface_pressure_3pm", "REAL"),
    ("cloud_9am", "REAL"),
    ("cloud_3pm", "REAL"),
    ("temp_9am", "REAL"),
    ("temp_3pm", "REAL"),
    ("rain_today", "INTEGER"),
    ("weather_code", "INTEGER"),
    ("shortwave_radiation_sum", "REAL"),
    ("vpd_9am", "REAL"),
    ("vpd_3pm", "REAL"),
    ("wind_speed_100m_9am", "REAL"),
    ("wind_speed_100m_3pm", "REAL"),
]
RAW_WEATHER_COLUMNS = [column for column, _ in RAW_WEATHER_COLUMN_DEFS]
RAW_WEATHER_VALUE_COLUMNS = [
    column for column in RAW_WEATHER_COLUMNS if column not in {"date", "city"}
]
WEATHER_PREDICTION_COLUMN_DEFS = [
    ("date", "TEXT NOT NULL"),
    ("city", "TEXT NOT NULL"),
    ("rain_tomorrow", "INTEGER"),
    ("rain_tomorrow_proba", "REAL"),
    ("max_temp_tomorrow", "REAL"),
    ("weather_type_tomorrow", "TEXT"),
    ("comfort_score", "REAL"),
    ("heatwave_risk", "REAL"),
    ("frost_risk", "REAL"),
    ("storm_probability", "REAL"),
    ("predicted_at", "TEXT"),
]
RAW_VIEW_COLUMNS = [
    "date",
    "city",
    "state",
    "latitude",
    "longitude",
    "min_temp",
    "max_temp",
    "rainfall",
    "rain_sum",
    "precipitation_hours",
    "evaporation",
    "sunshine_hours",
    "wind_gust_dir",
    "wind_gust_speed",
    "wind_dir_9am",
    "wind_dir_3pm",
    "wind_speed_9am",
    "wind_speed_3pm",
    "humidity_9am",
    "humidity_3pm",
    "dew_point_9am",
    "dew_point_3pm",
    "pressure_9am",
    "pressure_3pm",
    "surface_pressure_9am",
    "surface_pressure_3pm",
    "cloud_9am",
    "cloud_3pm",
    "temp_9am",
    "temp_3pm",
    "rain_today",
    "weather_code",
    "shortwave_radiation_sum",
    "vpd_9am",
    "vpd_3pm",
    "wind_speed_100m_9am",
    "wind_speed_100m_3pm",
]


def _validate_raw_columns(columns: list[str] | None):
    if not columns:
        return
    invalid = sorted(set(columns) - set(RAW_WEATHER_COLUMNS))
    if invalid:
        raise ValueError(f"Unknown weather_raw columns: {invalid}")


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(db_path)


def _ensure_weather_raw_columns(conn: sqlite3.Connection):
    existing_columns = {row[1] for row in conn.execute("PRAGMA table_info(weather_raw)").fetchall()}
    for column, sql_type in RAW_WEATHER_COLUMN_DEFS:
        if column in existing_columns or column in {"date", "city"}:
            continue
        conn.execute(f"ALTER TABLE weather_raw ADD COLUMN {column} {sql_type}")


def _ensure_weather_prediction_columns(conn: sqlite3.Connection):
    existing_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(weather_predictions)").fetchall()
    }
    for column, sql_type in WEATHER_PREDICTION_COLUMN_DEFS:
        if column in existing_columns or column in {"date", "city"}:
            continue
        conn.execute(f"ALTER TABLE weather_predictions ADD COLUMN {column} {sql_type}")


def _refresh_weather_full_view(conn: sqlite3.Connection):
    raw_columns_sql = ",\n                ".join(f"r.{column}" for column in RAW_VIEW_COLUMNS)
    conn.execute("DROP VIEW IF EXISTS v_weather_full")
    conn.execute(
        f"""
        CREATE VIEW v_weather_full AS
        SELECT
                {raw_columns_sql},
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
        LEFT JOIN weather_predictions p ON r.date = p.date AND r.city = p.city
        """
    )


def init_db(db_path: Path = DB_PATH):
    with get_connection(db_path) as conn:
        conn.executescript(
            """
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
                rain_sum        REAL,
                precipitation_hours REAL,
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
                dew_point_9am   REAL,
                dew_point_3pm   REAL,
                pressure_9am    REAL,
                pressure_3pm    REAL,
                surface_pressure_9am REAL,
                surface_pressure_3pm REAL,
                cloud_9am       REAL,
                cloud_3pm       REAL,
                temp_9am        REAL,
                temp_3pm        REAL,
                rain_today      INTEGER,
                weather_code    INTEGER,
                shortwave_radiation_sum REAL,
                vpd_9am         REAL,
                vpd_3pm         REAL,
                wind_speed_100m_9am REAL,
                wind_speed_100m_3pm REAL,
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
        """
        )
        _ensure_weather_raw_columns(conn)
        _ensure_weather_prediction_columns(conn)
        _refresh_weather_full_view(conn)


def upsert_weather_raw(df: pd.DataFrame, db_path: Path = DB_PATH):
    """Upsert raw weather rows, preserving existing non-null values on partial re-fetches."""
    df = df.copy()
    for column in RAW_WEATHER_COLUMNS:
        if column not in df.columns:
            df[column] = None
    df = df[RAW_WEATHER_COLUMNS].copy()
    records = df.where(pd.notna(df), None).to_dict("records")
    column_list = ", ".join(RAW_WEATHER_COLUMNS)
    value_list = ", ".join(f":{column}" for column in RAW_WEATHER_COLUMNS)
    update_list = ",\n                 ".join(
        f"{column} = COALESCE(excluded.{column}, weather_raw.{column})"
        for column in RAW_WEATHER_VALUE_COLUMNS
    )

    with get_connection(db_path) as conn:
        conn.executemany(
            f"""
            INSERT INTO weather_raw
                ({column_list})
            VALUES ({value_list})
            ON CONFLICT(date, city) DO UPDATE SET
                 {update_list}
        """,
            records,
        )


def upsert_predictions(df: pd.DataFrame, db_path: Path = DB_PATH):
    """Insert or replace prediction rows (unique on date+city)."""
    with get_connection(db_path) as conn:
        df.to_sql("pred_staging", conn, if_exists="replace", index=False)
        conn.execute(
            """
            INSERT OR REPLACE INTO weather_predictions
                (date, city, rain_tomorrow, rain_tomorrow_proba, max_temp_tomorrow,
                 weather_type_tomorrow, comfort_score, heatwave_risk,
                 frost_risk, storm_probability, predicted_at)
            SELECT date, city, rain_tomorrow, rain_tomorrow_proba, max_temp_tomorrow,
                   weather_type_tomorrow, comfort_score, heatwave_risk,
                   frost_risk, storm_probability, predicted_at
            FROM pred_staging
        """
        )
        conn.execute("DROP TABLE IF EXISTS pred_staging")


def get_existing_cities_for_date(
    target_date: str,
    table: str = "weather_raw",
    required_columns: list[str] | None = None,
    db_path: Path = DB_PATH,
) -> set[str]:
    """Return distinct cities already stored for a given date in a given table."""
    if table not in {"weather_raw", "weather_predictions"}:
        raise ValueError(f"Unsupported table for city lookup: {table}")
    if table != "weather_raw" and required_columns:
        raise ValueError("required_columns is only supported for weather_raw")
    _validate_raw_columns(required_columns)

    query = f"SELECT DISTINCT city FROM {table} WHERE date = ?"
    params: list[str] = [target_date]
    if required_columns:
        query += " AND " + " AND ".join(f"{column} IS NOT NULL" for column in required_columns)

    with get_connection(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
    return {row[0] for row in rows}


def read_all(table: str = "v_weather_full", db_path: Path = DB_PATH) -> pd.DataFrame:
    with get_connection(db_path) as conn:
        return pd.read_sql(f"SELECT * FROM {table} ORDER BY date, city", conn)


def read_raw(db_path: Path = DB_PATH) -> pd.DataFrame:
    return read_all("weather_raw", db_path)


def get_latest_date(
    city: str,
    db_path: Path = DB_PATH,
    required_columns: list[str] | None = None,
) -> str | None:
    _validate_raw_columns(required_columns)
    query = "SELECT MAX(date) FROM weather_raw WHERE city = ?"
    params: list[str] = [city]
    if required_columns:
        query += " AND " + " AND ".join(f"{column} IS NOT NULL" for column in required_columns)

    with get_connection(db_path) as conn:
        row = conn.execute(query, params).fetchone()
    return row[0] if row else None


def find_weather_raw_gaps(
    start_date: str,
    end_date: str,
    *,
    expected_cities: list[str],
    required_columns: list[str] | None = None,
    db_path: Path = DB_PATH,
) -> dict[str, list[str]]:
    """
    Return missing or incomplete dates by city for weather_raw.

    A date is considered complete only if all required_columns are non-null.
    """
    _validate_raw_columns(required_columns)
    query_columns = ["date", "city"] + list(required_columns or [])

    with get_connection(db_path) as conn:
        df = pd.read_sql(
            f"""
            SELECT {", ".join(query_columns)}
            FROM weather_raw
            WHERE date BETWEEN ? AND ?
            """,
            conn,
            params=(start_date, end_date),
        )

    expected_dates = pd.date_range(start_date, end_date, freq="D").strftime("%Y-%m-%d").tolist()
    complete_dates_by_city = {city: set() for city in expected_cities}

    if not df.empty:
        if required_columns:
            complete_mask = df[required_columns].notna().all(axis=1)
            df = df[complete_mask]

        for city, city_rows in df.groupby("city"):
            if city in complete_dates_by_city:
                complete_dates_by_city[city] = set(city_rows["date"].tolist())

    gaps = {}
    for city in expected_cities:
        complete_dates = complete_dates_by_city[city]
        missing_or_incomplete = [day for day in expected_dates if day not in complete_dates]
        if missing_or_incomplete:
            gaps[city] = missing_or_incomplete

    return gaps
