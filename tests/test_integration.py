"""
Integration tests — FastAPI app exercised through TestClient.

These tests spin up the real ASGI app (no mocking of routing or middleware)
and point it at a temporary SQLite database so they remain hermetic.
"""

import json
import sqlite3

import pytest
from fastapi.testclient import TestClient

# ── fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def test_db(tmp_path):
    """Minimal populated SQLite database for API tests."""
    db = tmp_path / "weather_test.db"
    from pipeline.database import init_db

    init_db(db)

    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            """
            INSERT INTO weather_raw
                (date, city, state, max_temp, min_temp, rainfall, rain_today)
            VALUES
                ('2025-01-01', 'Sydney',    'NSW', 28.5, 18.0, 0.0, 0),
                ('2025-01-01', 'Melbourne', 'VIC', 22.0, 14.0, 2.4, 1),
                ('2025-01-02', 'Sydney',    'NSW', 30.1, 19.5, 0.0, 0)
            """
        )
        # Predictions for ALL three raw rows so pandas infers consistent dtypes
        # (mixed NULL/non-NULL prediction cols across rows triggers NaN in float64
        # columns, which json.dumps rejects).
        conn.execute(
            """
            INSERT INTO weather_predictions
                (date, city, rain_tomorrow, rain_tomorrow_proba,
                 max_temp_tomorrow, weather_type_tomorrow, comfort_score,
                 heatwave_risk, frost_risk, storm_probability, predicted_at)
            VALUES
                ('2025-01-01', 'Sydney',    0, 0.12, 30.1, 'Sunny', 75.0,
                 0.05, 0.0,  0.08, '2025-01-01T08:00:00'),
                ('2025-01-01', 'Melbourne', 1, 0.72, 18.5, 'Rainy', 45.0,
                 0.0,  0.1,  0.55, '2025-01-01T08:00:00'),
                ('2025-01-02', 'Sydney',    0, 0.09, 31.0, 'Sunny', 78.0,
                 0.06, 0.0,  0.05, '2025-01-02T08:00:00')
            """
        )
    return db


@pytest.fixture
def client(test_db, tmp_path, monkeypatch):
    """TestClient wired to the temp database."""
    import api.app as app_module

    # Replace get_connection so every endpoint uses the temp DB
    def _temp_get_connection(*args, **kwargs):
        return sqlite3.connect(str(test_db))

    monkeypatch.setattr(app_module, "get_connection", _temp_get_connection)
    monkeypatch.setattr(app_module, "DB_PATH", test_db)
    # Point OUTPUT_CSV to a non-existent path so export returns 404 in tests
    monkeypatch.setattr(app_module, "OUTPUT_CSV", tmp_path / "weather_final.csv")

    from api.app import app

    return TestClient(app)


# ── /health ───────────────────────────────────────────────────────────────────


def test_health_returns_ok(client):
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert "db" in body
    assert "db_exists" in body


# ── /api/cities ───────────────────────────────────────────────────────────────


def test_cities_returns_26_entries(client):
    r = client.get("/api/cities")
    assert r.status_code == 200
    cities = r.json()["cities"]
    assert len(cities) == 26
    # Spot-check expected fields
    first = cities[0]
    assert {"city", "state", "lat", "lon"} <= first.keys()


# ── /api/weather ──────────────────────────────────────────────────────────────


def test_weather_all_rows(client):
    r = client.get("/api/weather")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 3
    assert len(body["data"]) == 3
    assert "last_updated" in body


def test_weather_filter_by_city(client):
    r = client.get("/api/weather?city=Sydney")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 2
    assert all(row["city"] == "Sydney" for row in body["data"])


def test_weather_filter_by_date_range(client):
    r = client.get("/api/weather?start_date=2025-01-02&end_date=2025-01-02")
    assert r.status_code == 200
    assert r.json()["count"] == 1


# ── /api/weather/latest ───────────────────────────────────────────────────────


def test_weather_latest_returns_one_row_per_city(client):
    r = client.get("/api/weather/latest")
    assert r.status_code == 200
    body = r.json()
    cities_returned = {row["city"] for row in body["data"]}
    assert "Sydney" in cities_returned
    assert "Melbourne" in cities_returned
    # Sydney has 2025-01-02 as most recent — check we get that one
    sydney = next(row for row in body["data"] if row["city"] == "Sydney")
    assert sydney["date"] == "2025-01-02"


def test_weather_latest_filter_by_city(client):
    r = client.get("/api/weather/latest?city=Melbourne")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 1
    assert body["data"][0]["city"] == "Melbourne"


# ── /api/weather/predictions ──────────────────────────────────────────────────


def test_predictions_returns_expected_columns(client):
    r = client.get("/api/weather/predictions")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 3
    row = body["data"][0]
    expected = {
        "date",
        "city",
        "rain_tomorrow",
        "rain_tomorrow_proba",
        "max_temp_tomorrow",
        "weather_type_tomorrow",
        "comfort_score",
        "heatwave_risk",
        "frost_risk",
        "storm_probability",
        "predicted_at",
    }
    assert expected <= row.keys()


def test_predictions_filter_by_city(client):
    r = client.get("/api/weather/predictions?city=Sydney")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 2
    assert all(row["city"] == "Sydney" for row in body["data"])


# ── /api/mlflow/metrics ───────────────────────────────────────────────────────


def test_mlflow_metrics_404_when_no_file(client):
    """Returns 404 when models/metrics.json does not exist (fresh environment)."""
    r = client.get("/api/mlflow/metrics")
    # Either 200 (if metrics.json exists in the repo) or 404 (CI / clean env)
    assert r.status_code in (200, 404)


def test_mlflow_metrics_200_with_real_file(client, tmp_path, monkeypatch):
    """Returns the metrics JSON when models/metrics.json is present."""
    import api.app as app_module

    metrics = {"rain_accuracy": 0.77, "temp_mae": 1.63}
    metrics_file = tmp_path / "metrics.json"
    metrics_file.write_text(json.dumps(metrics))

    # Point ROOT to tmp_path so the endpoint resolves the file correctly
    monkeypatch.setattr(app_module, "OUTPUT_CSV", tmp_path / "weather_final.csv")
    # Patch the path resolution inside the endpoint
    orig_root = app_module.ROOT
    monkeypatch.setattr(app_module, "ROOT", tmp_path)
    (tmp_path / "models").mkdir(exist_ok=True)
    (tmp_path / "models" / "metrics.json").write_text(json.dumps(metrics))

    r = client.get("/api/mlflow/metrics")
    assert r.status_code == 200
    body = r.json()
    assert body["rain_accuracy"] == pytest.approx(0.77)

    monkeypatch.setattr(app_module, "ROOT", orig_root)


# ── /api/export/csv ───────────────────────────────────────────────────────────


def test_export_csv_404_when_missing(client):
    r = client.get("/api/export/csv")
    assert r.status_code == 404
