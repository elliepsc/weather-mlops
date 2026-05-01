#!/usr/bin/env python3
"""
Generate demo databases for DEMO_MODE deployment.

Creates:
  data/demo/weather_demo.db       — SQLite: weather_raw + weather_predictions (90d × 5 cities)
  data/demo/analytics_demo.duckdb — DuckDB: 5 analytics marts

Run:
  python pipeline/generate_demo_dataset.py

Target size: < 8 MB total (safe to commit).
"""

import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent.parent
DEMO_DIR = ROOT / "data" / "demo"
SQLITE_PATH = DEMO_DIR / "weather_demo.db"
DUCKDB_PATH = DEMO_DIR / "analytics_demo.duckdb"

RNG = np.random.default_rng(42)

CITIES: dict[str, dict] = {
    "Sydney":    {"state": "NSW", "lat": -33.87, "lon": 151.21,
                  "base_max": 24.0, "base_min": 16.0, "rain_prob": 0.38},
    "Melbourne": {"state": "VIC", "lat": -37.81, "lon": 144.96,
                  "base_max": 20.0, "base_min": 11.0, "rain_prob": 0.34},
    "Brisbane":  {"state": "QLD", "lat": -27.47, "lon": 153.02,
                  "base_max": 29.0, "base_min": 19.0, "rain_prob": 0.43},
    "Perth":     {"state": "WA",  "lat": -31.95, "lon": 115.86,
                  "base_max": 27.0, "base_min": 16.0, "rain_prob": 0.19},
    "Adelaide":  {"state": "SA",  "lat": -34.93, "lon": 138.60,
                  "base_max": 23.0, "base_min": 13.0, "rain_prob": 0.24},
}

WIND_DIRS = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
             "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
WEATHER_TYPES = ["Sunny", "Cloudy", "Rainy", "Stormy"]

END_DATE = date(2026, 5, 1)
N_DAYS = 90
START_DATE = END_DATE - timedelta(days=N_DAYS - 1)

# σ chosen so MAE = σ√(2/π) ≈ 1.63°C (matches models/metrics.json)
_TEMP_ERROR_SD = 2.04
_RAIN_ACCURACY_TARGET = 0.82


# ─── helpers ──────────────────────────────────────────────────────────────────


def _season_offset(d: date) -> float:
    """Southern-hemisphere temperature offset: +4°C Jan peak, −4°C Jul trough."""
    return np.cos((d.month - 1) * 2 * np.pi / 12) * 4.0


# ─── raw data ─────────────────────────────────────────────────────────────────


def generate_raw() -> pd.DataFrame:
    """Generate 90 × 5 rows of realistic raw weather (same schema as weather_raw)."""
    dates = [START_DATE + timedelta(days=i) for i in range(N_DAYS)]
    rows: list[dict] = []

    for city, c in CITIES.items():
        for d in dates:
            sf = _season_offset(d)

            max_t = round(c["base_max"] + sf + float(RNG.normal(0, 1.5)), 1)
            min_t = round(c["base_min"] + sf * 0.7 + float(RNG.normal(0, 1.2)), 1)
            temp_9am = round(min_t + (max_t - min_t) * 0.35 + float(RNG.normal(0, 0.5)), 1)
            temp_3pm = round(min_t + (max_t - min_t) * 0.80 + float(RNG.normal(0, 0.5)), 1)

            is_rainy = bool(RNG.random() < c["rain_prob"])
            rainfall = round(float(RNG.exponential(5.0) if is_rainy else 0.0), 1)

            h9 = float(np.clip(65 + float(RNG.normal(0, 10)) + (12 if is_rainy else 0), 20, 99))
            h3 = float(np.clip(h9 - float(RNG.uniform(5, 15)), 15, 95))
            p = 1015.0 + float(RNG.normal(0, 5))
            p9 = round(p + float(RNG.normal(0, 1)), 1)
            p3 = round(p - float(RNG.uniform(0, 3)), 1)

            ws9 = round(float(RNG.uniform(5, 25)), 1)
            ws3 = round(float(RNG.uniform(8, 35)), 1)
            sunshine = round(float(RNG.uniform(6, 12) if not is_rainy else RNG.uniform(0, 4)), 1)

            rows.append({
                "date": d.isoformat(),
                "city": city,
                "state": c["state"],
                "latitude": c["lat"],
                "longitude": c["lon"],
                "min_temp": min_t,
                "max_temp": max_t,
                "rainfall": rainfall,
                "rain_sum": round(rainfall * float(RNG.uniform(0.9, 1.1)), 1),
                "precipitation_hours": round(float(RNG.uniform(1, 6) if is_rainy else 0.0), 1),
                "evaporation": round(float(RNG.uniform(3, 9) if not is_rainy else RNG.uniform(1, 4)), 1),
                "sunshine_hours": sunshine,
                "wind_gust_dir": str(RNG.choice(WIND_DIRS)),
                "wind_gust_speed": round(ws3 * float(RNG.uniform(1.1, 1.5)), 1),
                "wind_dir_9am": str(RNG.choice(WIND_DIRS)),
                "wind_dir_3pm": str(RNG.choice(WIND_DIRS)),
                "wind_speed_9am": ws9,
                "wind_speed_3pm": ws3,
                "humidity_9am": round(h9, 1),
                "humidity_3pm": round(h3, 1),
                "dew_point_9am": round(temp_9am - float(RNG.uniform(3, 12)), 1),
                "dew_point_3pm": round(temp_3pm - float(RNG.uniform(5, 15)), 1),
                "pressure_9am": p9,
                "pressure_3pm": p3,
                "surface_pressure_9am": round(p - abs(c["lat"]) * 0.1 + float(RNG.normal(0, 0.5)), 1),
                "surface_pressure_3pm": round(p - abs(c["lat"]) * 0.1 - float(RNG.uniform(0, 2)), 1),
                "cloud_9am": round(float(RNG.uniform(0, 4) if not is_rainy else RNG.uniform(4, 8)), 1),
                "cloud_3pm": round(float(RNG.uniform(0, 5) if not is_rainy else RNG.uniform(5, 8)), 1),
                "temp_9am": temp_9am,
                "temp_3pm": temp_3pm,
                "rain_today": int(is_rainy),
                "weather_code": int(RNG.choice([61, 63, 80, 95])) if is_rainy else 0,
                "shortwave_radiation_sum": round(sunshine * float(RNG.uniform(180, 220)), 1),
                "vpd_9am": round(float(RNG.uniform(0.3, 1.5) if not is_rainy else RNG.uniform(0.1, 0.5)), 2),
                "vpd_3pm": round(float(RNG.uniform(0.8, 2.5) if not is_rainy else RNG.uniform(0.2, 0.8)), 2),
                "wind_speed_100m_9am": round(ws9 * float(RNG.uniform(1.3, 1.8)), 1),
                "wind_speed_100m_3pm": round(ws3 * float(RNG.uniform(1.3, 1.8)), 1),
            })

    return pd.DataFrame(rows)


# ─── predictions ──────────────────────────────────────────────────────────────


def generate_predictions(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Generate predictions with calibrated accuracy:
      - Rain: ~82% correct (TARGET set, not inferred from noise)
      - Temp MAE: ~1.6°C (Gaussian error σ=2.04 around the true next-day value)
    """
    predicted_at = datetime(2026, 5, 1, 6, 0, 0).isoformat()
    raw_idx = raw.set_index(["date", "city"])
    date_list = sorted(raw["date"].unique())
    next_date = {d: date_list[i + 1] for i, d in enumerate(date_list[:-1])}

    rows: list[dict] = []
    for _, row in raw.iterrows():
        d, city = row["date"], row["city"]
        c = CITIES[city]
        max_t = float(row["max_temp"])
        is_rainy_today = bool(row["rain_today"])

        nd = next_date.get(d)
        if nd and (nd, city) in raw_idx.index:
            nxt = raw_idx.loc[(nd, city)]
            actual_rain_tm = int(nxt["rain_today"])
            actual_max_tm = float(nxt["max_temp"])
            # Force accuracy to _RAIN_ACCURACY_TARGET (avoids luck-of-the-draw variance)
            rain_tomorrow = actual_rain_tm if RNG.random() < _RAIN_ACCURACY_TARGET else 1 - actual_rain_tm
            max_temp_tomorrow = round(actual_max_tm + float(RNG.normal(0, _TEMP_ERROR_SD)), 1)
        else:
            # Last day: no next-day actuals yet
            rain_tomorrow = int(RNG.random() < c["rain_prob"])
            max_temp_tomorrow = round(max_t + float(RNG.normal(0, 2.0)), 1)

        # Probability coherent with prediction (above/below 0.5 threshold)
        base_p = c["rain_prob"] + (0.35 if is_rainy_today else -0.12) + float(RNG.normal(0, 0.08))
        rain_proba = float(np.clip(base_p, 0.03, 0.97))
        if rain_tomorrow == 1 and rain_proba < 0.5:
            rain_proba = float(np.clip(0.5 + abs(rain_proba - 0.5) + 0.05, 0.5, 0.97))
        elif rain_tomorrow == 0 and rain_proba >= 0.5:
            rain_proba = float(np.clip(0.5 - abs(rain_proba - 0.5) - 0.05, 0.03, 0.49))

        if rain_proba > 0.7:
            wt_probs = [0.05, 0.15, 0.55, 0.25]
        elif rain_proba > 0.4:
            wt_probs = [0.15, 0.45, 0.30, 0.10]
        else:
            wt_probs = [0.55, 0.30, 0.12, 0.03]

        heatwave = float(np.clip(float(RNG.beta(1, 6) if max_t < 35 else RNG.beta(4, 2)), 0, 1))
        frost = float(np.clip(float(RNG.beta(1, 9) if max_t > 10 else RNG.beta(4, 2)), 0, 1))
        storm = float(np.clip(rain_proba * float(RNG.uniform(0.2, 0.55)), 0, 1))

        h3 = float(row["humidity_3pm"])
        temp_ok = max(0.0, 1.0 - abs(max_t - 22.0) / 20.0)
        hum_ok = max(0.0, 1.0 - abs(h3 - 55.0) / 55.0)
        comfort = float(np.clip((temp_ok * 0.5 + hum_ok * 0.3 + (1 - storm) * 0.2) * 100, 0, 100))

        rows.append({
            "date": d,
            "city": city,
            "rain_tomorrow": rain_tomorrow,
            "rain_tomorrow_proba": round(rain_proba, 3),
            "max_temp_tomorrow": max_temp_tomorrow,
            "weather_type_tomorrow": str(RNG.choice(WEATHER_TYPES, p=wt_probs)),
            "comfort_score": round(comfort, 1),
            "heatwave_risk": round(heatwave, 3),
            "frost_risk": round(frost, 3),
            "storm_probability": round(storm, 3),
            "predicted_at": predicted_at,
        })

    return pd.DataFrame(rows)


# ─── SQLite ───────────────────────────────────────────────────────────────────

_DDL_SQLITE = """
CREATE TABLE weather_raw (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    date                    TEXT    NOT NULL,
    city                    TEXT    NOT NULL,
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
    UNIQUE(date, city)
);
CREATE TABLE weather_predictions (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    date                  TEXT    NOT NULL,
    city                  TEXT    NOT NULL,
    rain_tomorrow         INTEGER,
    rain_tomorrow_proba   REAL,
    max_temp_tomorrow     REAL,
    weather_type_tomorrow TEXT,
    comfort_score         REAL,
    heatwave_risk         REAL,
    frost_risk            REAL,
    storm_probability     REAL,
    predicted_at          TEXT,
    UNIQUE(date, city)
);
"""

_DDL_VIEW = """
CREATE VIEW v_weather_full AS
SELECT
    r.date, r.city, r.state, r.latitude, r.longitude,
    r.min_temp, r.max_temp, r.rainfall, r.rain_sum, r.precipitation_hours,
    r.evaporation, r.sunshine_hours,
    r.wind_gust_dir, r.wind_gust_speed, r.wind_dir_9am, r.wind_dir_3pm,
    r.wind_speed_9am, r.wind_speed_3pm,
    r.humidity_9am, r.humidity_3pm,
    r.dew_point_9am, r.dew_point_3pm,
    r.pressure_9am, r.pressure_3pm,
    r.surface_pressure_9am, r.surface_pressure_3pm,
    r.cloud_9am, r.cloud_3pm, r.temp_9am, r.temp_3pm,
    r.rain_today, r.weather_code, r.shortwave_radiation_sum,
    r.vpd_9am, r.vpd_3pm, r.wind_speed_100m_9am, r.wind_speed_100m_3pm,
    p.rain_tomorrow, p.rain_tomorrow_proba, p.max_temp_tomorrow,
    p.weather_type_tomorrow, p.comfort_score, p.heatwave_risk,
    p.frost_risk, p.storm_probability, p.predicted_at
FROM weather_raw r
LEFT JOIN weather_predictions p ON r.date = p.date AND r.city = p.city
"""


def write_sqlite(raw: pd.DataFrame, preds: pd.DataFrame) -> None:
    DEMO_DIR.mkdir(parents=True, exist_ok=True)
    SQLITE_PATH.unlink(missing_ok=True)
    with sqlite3.connect(SQLITE_PATH) as conn:
        conn.executescript(_DDL_SQLITE)
        raw.to_sql("weather_raw", conn, if_exists="append", index=False)
        preds.to_sql("weather_predictions", conn, if_exists="append", index=False)
        conn.execute(_DDL_VIEW)
    print(f"  SQLite  → {SQLITE_PATH.relative_to(ROOT)}  ({SQLITE_PATH.stat().st_size / 1024:.0f} KB)")


# ─── analytics marts ──────────────────────────────────────────────────────────


def _build_timeline(raw: pd.DataFrame, preds: pd.DataFrame) -> pd.DataFrame:
    """mart_forecast_vs_actual_timeline: predictions joined with next-day actuals + rolling KPIs."""
    raw_idx = raw.set_index(["date", "city"])
    state_map = raw.drop_duplicates("city").set_index("city")["state"].to_dict()
    date_list = sorted(raw["date"].unique())
    next_date_map = {d: date_list[i + 1] for i, d in enumerate(date_list[:-1])}

    rows: list[dict] = []
    for _, p in preds.iterrows():
        d, city = p["date"], p["city"]
        nd = next_date_map.get(d)
        if nd and (nd, city) in raw_idx.index:
            a = raw_idx.loc[(nd, city)]
            actual_rain = int(a["rain_today"])
            actual_max = float(a["max_temp"])
            actual_min = float(a["min_temp"])
            actual_rainfall = float(a["rainfall"])
            has_actuals = True
            rain_correct = int(int(p["rain_tomorrow"]) == actual_rain)
            temp_err = round(abs(float(p["max_temp_tomorrow"]) - actual_max), 2)
        else:
            actual_rain = actual_max = actual_min = actual_rainfall = None
            has_actuals = False
            rain_correct = temp_err = None

        rows.append({
            "prediction_date": d,
            "city": city,
            "state": state_map.get(city),
            "pred_rain_tomorrow": int(p["rain_tomorrow"]),
            "pred_rain_proba": float(p["rain_tomorrow_proba"]),
            "pred_max_temp_tomorrow": float(p["max_temp_tomorrow"]),
            "pred_weather_type_tomorrow": str(p["weather_type_tomorrow"]),
            "pred_heatwave_risk": float(p["heatwave_risk"]),
            "pred_frost_risk": float(p["frost_risk"]),
            "pred_storm_probability": float(p["storm_probability"]),
            "comfort_score": float(p["comfort_score"]),
            "actual_date": nd,
            "actual_rain": actual_rain,
            "actual_max_temp": actual_max,
            "actual_min_temp": actual_min,
            "actual_rainfall": actual_rainfall,
            "rain_correct": rain_correct,
            "temp_abs_error": temp_err,
            "has_actuals": has_actuals,
        })

    df = pd.DataFrame(rows).sort_values(["city", "prediction_date"]).reset_index(drop=True)

    for city_name in df["city"].unique():
        mask = df["city"] == city_name
        group = df.loc[mask]
        rc = group["rain_correct"].astype(float)
        te = group["temp_abs_error"].astype(float)
        df.loc[mask, "rain_accuracy_30d"] = rc.rolling(30, min_periods=1).mean().round(4).values
        df.loc[mask, "temp_mae_30d"] = te.rolling(30, min_periods=1).mean().round(3).values
        df.loc[mask, "rain_accuracy_7d"] = rc.rolling(7, min_periods=1).mean().round(4).values
        df.loc[mask, "temp_mae_7d"] = te.rolling(7, min_periods=1).mean().round(3).values

    return df.sort_values("prediction_date", ascending=False).reset_index(drop=True)


def _build_performance_overview(timeline: pd.DataFrame) -> pd.DataFrame:
    """mart_model_performance_overview: monthly aggregates across all cities."""
    t = timeline.copy()
    t["month"] = pd.to_datetime(t["prediction_date"]).dt.to_period("M").dt.to_timestamp()

    agg = t.groupby("month").agg(
        n_city_days=("prediction_date", "count"),
        n_days=("prediction_date", "nunique"),
        n_cities=("city", "nunique"),
        rain_accuracy=("rain_correct", "mean"),
        temp_mae=("temp_abs_error", "mean"),
        temp_max_error=("temp_abs_error", "max"),
        temp_min_error=("temp_abs_error", "min"),
    ).reset_index()

    pct_actuals = t.groupby("month")["has_actuals"].mean() * 100
    agg["pct_with_actuals"] = agg["month"].map(pct_actuals).round(1)
    agg["rain_accuracy"] = agg["rain_accuracy"].round(4)
    for col in ("temp_mae", "temp_max_error", "temp_min_error"):
        agg[col] = agg[col].round(3)
    agg["month"] = agg["month"].dt.strftime("%Y-%m-%d")

    return agg.sort_values("month", ascending=False).reset_index(drop=True)


def _build_mlops_health(raw: pd.DataFrame, timeline: pd.DataFrame) -> pd.DataFrame:
    """mart_mlops_health: daily ops snapshot for the last 30 days."""
    all_dates = sorted(raw["date"].unique())
    last_30 = all_dates[-30:]
    retrain_dates = ["2026-02-28", "2026-04-10"]

    tl_sorted = timeline.sort_values("prediction_date")

    rows: list[dict] = []
    for d in reversed(last_30):
        cities_present = int(raw[raw["date"] == d]["city"].nunique())
        expected = len(CITIES)
        missing = expected - cities_present

        # Rolling metrics up to and including date d (last 30 city-days)
        before = tl_sorted[tl_sorted["prediction_date"] <= d].tail(30 * expected)
        rain_acc = before["rain_correct"].mean()
        temp_mae = before["temp_abs_error"].mean()

        rain_acc_val = float(rain_acc) if not (rain_acc != rain_acc) else None  # NaN check
        temp_mae_val = float(temp_mae) if not (temp_mae != temp_mae) else None

        if rain_acc_val is not None and rain_acc_val < 0.75:
            action, reason = "trigger_retrain", "rain accuracy below threshold"
            n_drift, heavy, mild, low_acc = int(RNG.integers(3, 7)), True, True, True
        elif bool(RNG.random() < 0.15):
            action, reason = "alert_only", "mild feature drift detected"
            n_drift, heavy, mild, low_acc = int(RNG.integers(1, 3)), False, True, False
        else:
            action, reason = "no_action", "all metrics within thresholds"
            n_drift, heavy, mild, low_acc = 0, False, False, False

        high_mae = bool(temp_mae_val is not None and temp_mae_val > 2.0)
        last_retrain = max((r for r in retrain_dates if r <= d), default=None)
        days_since = (
            (pd.Timestamp(d) - pd.Timestamp(last_retrain)).days
            if last_retrain else None
        )

        rows.append({
            "date": d,
            "completeness_pct": round(cities_present / expected * 100, 1),
            "missing_city_count": missing,
            "present_cities": cities_present,
            "expected_cities": expected,
            "has_gap": missing > 0,
            "monitoring_action": action,
            "monitoring_reason": reason,
            "n_drifted_features": n_drift,
            "heavy_drift": heavy,
            "mild_drift": mild,
            "low_accuracy": low_acc,
            "high_mae": high_mae,
            "rain_accuracy_30d": round(rain_acc_val, 4) if rain_acc_val is not None else None,
            "temp_mae_30d": round(temp_mae_val, 3) if temp_mae_val is not None else None,
            "last_retrain_date": last_retrain,
            "days_since_retrain": int(days_since) if days_since is not None else None,
        })

    return pd.DataFrame(rows).reset_index(drop=True)


def _build_performance_by_city(timeline: pd.DataFrame) -> pd.DataFrame:
    """mart_model_performance_by_city: monthly performance per city with geo metadata."""
    t = timeline.copy()
    t["month"] = pd.to_datetime(t["prediction_date"]).dt.to_period("M").dt.to_timestamp()
    t_sorted = t.sort_values(["city", "month", "prediction_date"])

    agg = t_sorted.groupby(["month", "city"]).agg(
        n_days=("prediction_date", "nunique"),
        rain_accuracy=("rain_correct", "mean"),
        temp_mae=("temp_abs_error", "mean"),
        temp_max_error=("temp_abs_error", "max"),
    ).reset_index()

    eom = t_sorted.groupby(["month", "city"]).agg(
        rain_accuracy_30d_eom=("rain_accuracy_30d", "last"),
        temp_mae_30d_eom=("temp_mae_30d", "last"),
    ).reset_index()

    by_city = agg.merge(eom, on=["month", "city"])
    city_meta = pd.DataFrame([
        {"city": city, "state": c["state"], "latitude": c["lat"], "longitude": c["lon"]}
        for city, c in CITIES.items()
    ])
    by_city = by_city.merge(city_meta, on="city")

    by_city["rain_accuracy"] = by_city["rain_accuracy"].round(4)
    by_city["rain_accuracy_30d_eom"] = by_city["rain_accuracy_30d_eom"].round(4)
    for col in ("temp_mae", "temp_max_error", "temp_mae_30d_eom"):
        by_city[col] = by_city[col].round(3)
    by_city["month"] = by_city["month"].dt.strftime("%Y-%m-%d")

    return by_city.sort_values(["month", "city"], ascending=[False, True]).reset_index(drop=True)


def _build_retraining_history(timeline: pd.DataFrame) -> pd.DataFrame:
    """mart_retraining_history: audit of retrain events with before/after deltas."""
    events = [
        {
            "retrain_date": "2026-02-28",
            "source": "monitoring_dag",
            "trigger_action": "trigger_retrain",
            "trigger_reason": "heavy feature drift: humidity_9am, pressure_9am, vpd_9am (5 features)",
            "n_drifted_features": 5,
            "heavy_drift": True,
            "low_accuracy": False,
            "preceded_by_gap": False,
            "gap_days_in_7d_window": 0,
            "max_missing_cities_7d": 0,
            "avg_completeness_7d_pct": 100.0,
        },
        {
            "retrain_date": "2026-04-10",
            "source": "monitoring_dag",
            "trigger_action": "trigger_retrain",
            "trigger_reason": "rain_accuracy_30d below threshold (0.77) for 7 consecutive days",
            "n_drifted_features": 3,
            "heavy_drift": False,
            "low_accuracy": True,
            "preceded_by_gap": True,
            "gap_days_in_7d_window": 2,
            "max_missing_cities_7d": 1,
            "avg_completeness_7d_pct": 91.4,
        },
    ]

    tl = timeline.sort_values("prediction_date")
    for ev in events:
        rd = ev["retrain_date"]
        before = tl[tl["prediction_date"] <= rd].tail(30 * len(CITIES))
        ev["rain_accuracy_at_retrain"] = round(float(before["rain_correct"].mean()), 4)
        ev["temp_mae_at_retrain"] = round(float(before["temp_abs_error"].mean()), 3)

    for i, ev in enumerate(events):
        if i + 1 < len(events):
            nev = events[i + 1]
            ev["next_retrain_date"] = nev["retrain_date"]
            ev["next_rain_accuracy"] = nev["rain_accuracy_at_retrain"]
            ev["next_temp_mae"] = nev["temp_mae_at_retrain"]
        else:
            ev["next_retrain_date"] = None
            ev["next_rain_accuracy"] = None
            ev["next_temp_mae"] = None

        curr_acc = ev["rain_accuracy_at_retrain"]
        next_acc = ev["next_rain_accuracy"]
        ev["accuracy_delta"] = round(float(next_acc) - curr_acc, 4) if next_acc is not None else None
        curr_mae = ev["temp_mae_at_retrain"]
        next_mae = ev["next_temp_mae"]
        ev["mae_delta"] = round(curr_mae - float(next_mae), 3) if next_mae is not None else None
        ev["accuracy_degraded_after_retrain"] = bool(
            next_acc is not None and float(next_acc) < curr_acc - 0.02
        )

    return pd.DataFrame(events).sort_values("retrain_date", ascending=False).reset_index(drop=True)


# ─── DuckDB ───────────────────────────────────────────────────────────────────


def write_duckdb(
    timeline: pd.DataFrame,
    overview: pd.DataFrame,
    health: pd.DataFrame,
    by_city: pd.DataFrame,
    history: pd.DataFrame,
) -> None:
    DEMO_DIR.mkdir(parents=True, exist_ok=True)
    DUCKDB_PATH.unlink(missing_ok=True)

    con = duckdb.connect(str(DUCKDB_PATH))
    marts = {
        "mart_forecast_vs_actual_timeline": timeline,
        "mart_model_performance_overview": overview,
        "mart_mlops_health": health,
        "mart_model_performance_by_city": by_city,
        "mart_retraining_history": history,
    }
    for name, df in marts.items():
        con.register(f"_src_{name}", df)
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM _src_{name}")
    con.close()

    print(f"  DuckDB  → {DUCKDB_PATH.relative_to(ROOT)}  ({DUCKDB_PATH.stat().st_size / 1024:.0f} KB)")


# ─── main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    print(f"Generating demo data: {START_DATE} → {END_DATE}  ({N_DAYS} days × {len(CITIES)} cities)")

    print("  raw weather …")
    raw = generate_raw()

    print("  predictions …")
    preds = generate_predictions(raw)
    print(f"    {len(raw)} raw rows, {len(preds)} prediction rows")

    write_sqlite(raw, preds)

    print("  analytics marts …")
    timeline = _build_timeline(raw, preds)
    overview = _build_performance_overview(timeline)
    health = _build_mlops_health(raw, timeline)
    by_city = _build_performance_by_city(timeline)
    history = _build_retraining_history(timeline)
    print(
        f"    timeline={len(timeline)}  overview={len(overview)}"
        f"  health={len(health)}  by_city={len(by_city)}  history={len(history)}"
    )

    write_duckdb(timeline, overview, health, by_city, history)

    total_mb = (SQLITE_PATH.stat().st_size + DUCKDB_PATH.stat().st_size) / 1024 / 1024
    print(f"  Total size: {total_mb:.1f} MB  (target < 8 MB)")
    print("Done.")


if __name__ == "__main__":
    main()
