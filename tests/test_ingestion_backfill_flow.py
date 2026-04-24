from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock

import pandas as pd
import pytest

from dags import backfill_dag as backfill
from pipeline import run_pipeline as rp
from pipeline.database import init_db, read_raw, upsert_weather_raw


def test_step_ingest_daily_skips_when_target_day_is_complete(monkeypatch):
    monkeypatch.setattr(rp, "LOCATIONS", {"A": {}, "B": {}})
    monkeypatch.setattr(
        rp,
        "get_existing_cities_for_date",
        lambda target_date, table="weather_raw", required_columns=None: {"A", "B"},
    )

    fetch_city = MagicMock()
    upsert_weather_raw = MagicMock()
    monkeypatch.setattr(rp, "fetch_city", fetch_city)
    monkeypatch.setattr(rp, "upsert_weather_raw", upsert_weather_raw)

    summary = rp.step_ingest_daily(target_date="2026-04-23", delay_seconds=0)

    assert summary["status"] == "skipped"
    assert summary["stored_rows"] == 0
    fetch_city.assert_not_called()
    upsert_weather_raw.assert_not_called()


def test_step_ingest_daily_fetches_only_missing_cities(monkeypatch):
    monkeypatch.setattr(rp, "LOCATIONS", {"A": {}, "B": {}})
    monkeypatch.setattr(
        rp,
        "get_existing_cities_for_date",
        lambda target_date, table="weather_raw", required_columns=None: {"A"} if required_columns else {"A"},
    )

    fetch_calls = []

    def fake_fetch_city(city: str, start_date: str, end_date: str):
        fetch_calls.append((city, start_date, end_date))
        return pd.DataFrame({"date": [start_date], "city": [city]})

    upsert_weather_raw = MagicMock()
    monkeypatch.setattr(rp, "fetch_city", fake_fetch_city)
    monkeypatch.setattr(rp, "upsert_weather_raw", upsert_weather_raw)

    summary = rp.step_ingest_daily(target_date="2026-04-23", delay_seconds=0)

    assert summary["status"] == "fetched"
    assert summary["fetched_cities"] == ["B"]
    assert fetch_calls == [("B", "2026-04-23", "2026-04-23")]
    upsert_weather_raw.assert_called_once()


def test_step_ingest_daily_refetches_partial_cities(monkeypatch):
    monkeypatch.setattr(rp, "LOCATIONS", {"A": {}, "B": {}})

    def fake_get_existing(target_date, table="weather_raw", required_columns=None):
        return {"A", "B"} if required_columns is None else {"A"}

    fetch_calls = []

    def fake_fetch_city(city: str, start_date: str, end_date: str):
        fetch_calls.append((city, start_date, end_date))
        return pd.DataFrame({"date": [start_date], "city": [city]})

    upsert_raw = MagicMock()
    monkeypatch.setattr(rp, "get_existing_cities_for_date", fake_get_existing)
    monkeypatch.setattr(rp, "fetch_city", fake_fetch_city)
    monkeypatch.setattr(rp, "upsert_weather_raw", upsert_raw)

    summary = rp.step_ingest_daily(target_date="2026-04-23", delay_seconds=0)

    assert summary["fetched_cities"] == ["B"]
    assert summary["incomplete_cities"] == ["B"]
    assert fetch_calls == [("B", "2026-04-23", "2026-04-23")]
    upsert_raw.assert_called_once()


def test_backfill_date_range_resumes_from_latest_date(monkeypatch):
    monkeypatch.setattr(rp, "LOCATIONS", {"A": {}, "B": {}})
    monkeypatch.setattr(
        rp,
        "get_latest_date",
        lambda city, db_path=None, required_columns=None: {"A": "2026-04-22", "B": None}[city],
    )

    fetch_calls = []

    def fake_fetch_city(city: str, start_date: str, end_date: str):
        fetch_calls.append((city, start_date, end_date))
        return pd.DataFrame({"date": [start_date], "city": [city]})

    upsert_weather_raw = MagicMock()
    monkeypatch.setattr(rp, "fetch_city", fake_fetch_city)
    monkeypatch.setattr(rp, "upsert_weather_raw", upsert_weather_raw)

    summary = rp.backfill_date_range("2026-04-20", "2026-04-23", delay_seconds=0)

    assert summary["failed_cities"] == {}
    assert fetch_calls == [
        ("A", "2026-04-23", "2026-04-23"),
        ("B", "2026-04-20", "2026-04-23"),
    ]
    assert upsert_weather_raw.call_count == 2


def test_backfill_date_range_retries_from_latest_complete_row(monkeypatch):
    monkeypatch.setattr(rp, "LOCATIONS", {"A": {}, "B": {}})
    monkeypatch.setattr(
        rp,
        "get_latest_date",
        lambda city, db_path=None, required_columns=None: {"A": "2026-04-22", "B": "2026-04-23"}[city],
    )

    fetch_calls = []

    def fake_fetch_city(city: str, start_date: str, end_date: str):
        fetch_calls.append((city, start_date, end_date))
        return pd.DataFrame({"date": [start_date], "city": [city]})

    upsert_raw = MagicMock()
    monkeypatch.setattr(rp, "fetch_city", fake_fetch_city)
    monkeypatch.setattr(rp, "upsert_weather_raw", upsert_raw)

    summary = rp.backfill_date_range("2026-04-20", "2026-04-23", delay_seconds=0)

    assert summary["skipped_cities"] == ["B"]
    assert fetch_calls == [("A", "2026-04-23", "2026-04-23")]
    upsert_raw.assert_called_once()


def test_backfill_date_range_keeps_partial_progress_and_reports_failures(monkeypatch):
    monkeypatch.setattr(rp, "LOCATIONS", {"A": {}, "B": {}})
    monkeypatch.setattr(
        rp,
        "get_latest_date",
        lambda city, db_path=None, required_columns=None: None,
    )

    def fake_fetch_city(city: str, start_date: str, end_date: str):
        if city == "B":
            raise RuntimeError("429")
        return pd.DataFrame({"date": [start_date], "city": [city]})

    upsert_weather_raw = MagicMock()
    monkeypatch.setattr(rp, "fetch_city", fake_fetch_city)
    monkeypatch.setattr(rp, "upsert_weather_raw", upsert_weather_raw)

    summary = rp.backfill_date_range("2026-04-20", "2026-04-23", delay_seconds=0)

    assert summary["stored_rows"] == 1
    assert summary["fetched_cities"] == ["A"]
    assert summary["failed_cities"] == {"B": "429"}
    upsert_weather_raw.assert_called_once()


def test_repair_gaps_in_range_groups_contiguous_dates(monkeypatch):
    gap_snapshots = [
        {"A": ["2026-04-20", "2026-04-21", "2026-04-23"]},
        {},
    ]
    fetch_calls = []
    upsert_raw = MagicMock()

    def fake_find_gaps(*args, **kwargs):
        return gap_snapshots.pop(0)

    def fake_fetch_city(city: str, start_date: str, end_date: str):
        fetch_calls.append((city, start_date, end_date))
        return pd.DataFrame({"date": [start_date], "city": [city]})

    monkeypatch.setattr(rp, "LOCATIONS", {"A": {}})
    monkeypatch.setattr(rp, "find_weather_raw_gaps", fake_find_gaps)
    monkeypatch.setattr(rp, "fetch_city", fake_fetch_city)
    monkeypatch.setattr(rp, "upsert_weather_raw", upsert_raw)

    summary = rp.repair_gaps_in_range("2026-04-20", "2026-04-23", delay_seconds=0)

    assert fetch_calls == [
        ("A", "2026-04-20", "2026-04-21"),
        ("A", "2026-04-23", "2026-04-23"),
    ]
    assert summary["initial_gap_counts"] == {"A": 3}
    assert summary["remaining_gap_counts"] == {}
    assert len(summary["repaired_ranges"]) == 2
    assert upsert_raw.call_count == 2


def test_backfill_and_repair_combines_summaries(monkeypatch):
    monkeypatch.setattr(
        rp,
        "backfill_date_range",
        lambda *args, **kwargs: {
            "start_date": "2026-04-20",
            "end_date": "2026-04-23",
            "stored_rows": 10,
            "fetched_cities": ["A"],
            "skipped_cities": ["B"],
            "failed_cities": {},
        },
    )
    monkeypatch.setattr(
        rp,
        "repair_gaps_in_range",
        lambda *args, **kwargs: {
            "start_date": "2026-04-20",
            "end_date": "2026-04-23",
            "status": "repaired",
            "stored_rows": 2,
            "initial_gap_counts": {"A": 2},
            "repaired_ranges": [{"city": "A", "start_date": "2026-04-21", "end_date": "2026-04-22", "gap_days": 2}],
            "failed_ranges": {},
            "remaining_gap_counts": {},
        },
    )

    summary = rp.backfill_and_repair_date_range("2026-04-20", "2026-04-23", delay_seconds=0)

    assert summary["stored_rows"] == 12
    assert summary["failed_cities"] == {}
    assert summary["failed_ranges"] == {}
    assert summary["remaining_gap_counts"] == {}


def test_upsert_weather_raw_keeps_existing_values_when_refetch_has_nulls(tmp_path):
    db_path = tmp_path / "weather.db"
    init_db(db_path)

    full_row = pd.DataFrame(
        [
            {
                "date": "2026-04-23",
                "city": "Sydney",
                "state": "NSW",
                "latitude": -33.86,
                "longitude": 151.21,
                "min_temp": 14.0,
                "max_temp": 23.0,
                "rainfall": 2.0,
                "evaporation": 4.2,
                "sunshine_hours": 8.5,
                "wind_gust_dir": "NE",
                "wind_gust_speed": 32.0,
                "wind_dir_9am": "N",
                "wind_dir_3pm": "NE",
                "wind_speed_9am": 12.0,
                "wind_speed_3pm": 18.0,
                "humidity_9am": 65.0,
                "humidity_3pm": 48.0,
                "pressure_9am": 1014.0,
                "pressure_3pm": 1011.0,
                "cloud_9am": 3.0,
                "cloud_3pm": 4.0,
                "temp_9am": 18.0,
                "temp_3pm": 22.0,
                "rain_today": 1,
                "weather_code": 61,
            }
        ]
    )
    partial_refetch = full_row.copy()
    partial_refetch["humidity_9am"] = None
    partial_refetch["humidity_3pm"] = None
    partial_refetch["pressure_9am"] = None
    partial_refetch["pressure_3pm"] = None

    upsert_weather_raw(full_row, db_path=db_path)
    upsert_weather_raw(partial_refetch, db_path=db_path)

    stored = read_raw(db_path)
    row = stored.iloc[0]

    assert len(stored) == 1
    assert row["humidity_9am"] == 65.0
    assert row["humidity_3pm"] == 48.0
    assert row["pressure_9am"] == 1014.0
    assert row["pressure_3pm"] == 1011.0


def test_init_db_migrates_new_open_meteo_columns_and_refreshes_view(tmp_path):
    db_path = tmp_path / "weather.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE weather_raw (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                city TEXT NOT NULL,
                state TEXT,
                latitude REAL,
                longitude REAL,
                min_temp REAL,
                max_temp REAL,
                rainfall REAL,
                evaporation REAL,
                sunshine_hours REAL,
                wind_gust_dir TEXT,
                wind_gust_speed REAL,
                wind_dir_9am TEXT,
                wind_dir_3pm TEXT,
                wind_speed_9am REAL,
                wind_speed_3pm REAL,
                humidity_9am REAL,
                humidity_3pm REAL,
                pressure_9am REAL,
                pressure_3pm REAL,
                cloud_9am REAL,
                cloud_3pm REAL,
                temp_9am REAL,
                temp_3pm REAL,
                rain_today INTEGER,
                weather_code INTEGER,
                UNIQUE(date, city)
            );
            CREATE TABLE weather_predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                city TEXT NOT NULL,
                predicted_at TEXT,
                UNIQUE(date, city)
            );
            CREATE VIEW v_weather_full AS
            SELECT r.date, r.city, r.rainfall, p.predicted_at
            FROM weather_raw r
            LEFT JOIN weather_predictions p ON r.date = p.date AND r.city = p.city;
            """
        )

    init_db(db_path)

    with sqlite3.connect(db_path) as conn:
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(weather_raw)").fetchall()
        }
        view_columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(v_weather_full)").fetchall()
        }

    assert {"rain_sum", "precipitation_hours", "dew_point_9am", "dew_point_3pm"} <= columns
    assert {"surface_pressure_9am", "surface_pressure_3pm"} <= columns
    assert {"rain_sum", "precipitation_hours", "dew_point_9am", "surface_pressure_9am"} <= view_columns


def test_backfill_dag_raises_when_fetch_is_incomplete(monkeypatch):
    monkeypatch.setattr("pipeline.database.init_db", lambda: None)
    monkeypatch.setattr(
        "pipeline.run_pipeline.backfill_and_repair_date_range",
        lambda *args, **kwargs: {
            "start_date": "2008-01-01",
            "end_date": "2026-04-23",
            "stored_rows": 100,
            "failed_cities": {"B": "429"},
            "failed_ranges": {"A:2026-04-20->2026-04-21": "still incomplete"},
            "remaining_gap_counts": {"A": 2},
            "backfill": {},
            "repair": {},
        },
    )

    ti = MagicMock()

    def xcom_pull(task_ids: str, key: str):
        mapping = {
            ("validate_backfill_config", "start_date"): "2008-01-01",
            ("validate_backfill_config", "end_date"): "2026-04-23",
        }
        return mapping[(task_ids, key)]

    ti.xcom_pull.side_effect = xcom_pull

    with pytest.raises(RuntimeError, match="Backfill incomplete"):
        backfill.run_backfill_with_config(ti=ti)
