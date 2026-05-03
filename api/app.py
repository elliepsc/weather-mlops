"""
FastAPI — Weather data endpoint for Power BI and Streamlit.

Endpoints:
  GET /api/weather              — full historical + predictions table (Power BI Web connector)
  GET /api/weather/latest       — latest date per city with predictions
  GET /api/weather/predictions  — predictions-only table
  GET /api/cities               — list of available cities
  GET /api/analytics/{mart}     — DuckDB analytics mart (demo or production)
  GET /api/mlflow/runs          — last N MLflow training runs + metrics
  GET /api/mlflow/metrics       — latest metrics per model
  GET /api/export/csv           — download CSV file directly
  GET /health                   — health check
"""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from datetime import date, datetime
from typing import Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from prometheus_fastapi_instrumentator import Instrumentator

from pipeline.database import DB_PATH, get_connection

# ─── DEMO_MODE ────────────────────────────────────────────────────────────────
# Set DEMO_MODE=true on Render to serve pre-generated demo databases instead of
# the production SQLite / DuckDB files that live outside the repo.

DEMO_MODE: bool = os.getenv("DEMO_MODE", "false").lower() in ("1", "true", "yes")

_DEMO_DB = ROOT / "data" / "demo" / "weather_demo.db"
_DEMO_ANALYTICS = ROOT / "data" / "demo" / "analytics_demo.duckdb"
_PROD_ANALYTICS = ROOT / "data" / "analytics.duckdb"

APP_DB_PATH: Path = _DEMO_DB if DEMO_MODE else DB_PATH
ANALYTICS_DB_PATH: Path = _DEMO_ANALYTICS if DEMO_MODE else _PROD_ANALYTICS

app = FastAPI(
    title="Weather Australia API",
    description="Historical weather data + ML predictions for 26 Australian cities.",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Power BI Desktop needs this
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Expose /metrics endpoint for Prometheus scraping
Instrumentator().instrument(app).expose(app)

OUTPUT_CSV = ROOT / "data" / "output" / "weather_final.csv"


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame to JSON-serialisable records (NaN → None)."""
    records = df.where(pd.notnull(df), None).to_dict(orient="records")
    for row in records:
        for key, value in row.items():
            if isinstance(value, (pd.Timestamp, datetime, date)):
                row[key] = value.isoformat()
    return records


# ─── endpoints ───────────────────────────────────────────────────────────────


@app.get("/health")
def health():
    import json

    json_path = ROOT / "models" / "mlflow_latest.json"
    if json_path.exists():
        try:
            json.loads(json_path.read_text())
            mlflow_source = "json_cache"
        except Exception:
            mlflow_source = "unavailable"
    elif DEMO_MODE:
        mlflow_source = "json_cache"
    else:
        mlflow_source = "unknown"

    return {
        "status": "ok",
        "db": str(APP_DB_PATH),
        "db_exists": APP_DB_PATH.exists(),
        "demo_mode": DEMO_MODE,
        "mlflow_source": mlflow_source,
    }


@app.get("/api/cities")
def list_cities():
    """Return available cities and their coordinates."""
    from pipeline.locations import LOCATIONS

    return {
        "cities": [
            {"city": k, "state": v["state"], "lat": v["lat"], "lon": v["lon"]}
            for k, v in LOCATIONS.items()
        ]
    }


@app.get("/api/weather")
def get_weather(
    city: Optional[str] = Query(None, description="Filter by city name"),
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    limit: int = Query(50000, description="Max rows returned"),
):
    """
    Full weather + predictions table.
    Power BI: Data → Web → paste this URL → JSON → expand 'data'.
    """
    try:
        with get_connection(APP_DB_PATH) as conn:
            query = "SELECT * FROM v_weather_full WHERE 1=1"
            params = []
            if city:
                query += " AND city = ?"
                params.append(city)
            if start_date:
                query += " AND date >= ?"
                params.append(start_date)
            if end_date:
                query += " AND date <= ?"
                params.append(end_date)
            query += f" ORDER BY date DESC, city LIMIT {int(limit)}"
            df = pd.read_sql(query, conn, params=params)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return JSONResponse(
        {
            "count": len(df),
            "last_updated": date.today().isoformat(),
            "data": _df_to_records(df),
        }
    )


@app.get("/api/weather/latest")
def get_latest(city: Optional[str] = Query(None)):
    """Latest available date per city with all predictions — ideal for a Power BI dashboard."""
    try:
        with get_connection(APP_DB_PATH) as conn:
            base = """
                SELECT w.*
                FROM v_weather_full w
                INNER JOIN (
                    SELECT city, MAX(date) AS max_date
                    FROM v_weather_full
                    GROUP BY city
                ) m ON w.city = m.city AND w.date = m.max_date
            """
            params = []
            if city:
                base += " WHERE w.city = ?"
                params.append(city)
            df = pd.read_sql(base, conn, params=params)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return JSONResponse(
        {
            "count": len(df),
            "data": _df_to_records(df),
        }
    )


@app.get("/api/weather/predictions")
def get_predictions(
    city: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Predictions-only table (lighter payload for Power BI dashboards)."""
    try:
        with get_connection(APP_DB_PATH) as conn:
            query = """
                SELECT date, city,
                       rain_tomorrow, rain_tomorrow_proba,
                       max_temp_tomorrow, weather_type_tomorrow,
                       comfort_score,
                       heatwave_risk, frost_risk, storm_probability,
                       predicted_at
                FROM weather_predictions WHERE 1=1
            """
            params = []
            if city:
                query += " AND city = ?"
                params.append(city)
            if start_date:
                query += " AND date >= ?"
                params.append(start_date)
            if end_date:
                query += " AND date <= ?"
                params.append(end_date)
            query += " ORDER BY date DESC, city"
            df = pd.read_sql(query, conn, params=params)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return JSONResponse({"count": len(df), "data": _df_to_records(df)})


@app.get("/api/export/csv")
def export_csv():
    """
    Download the full dataset as CSV.
    Power BI: Data → Web → paste URL → load directly as CSV.
    """
    if not OUTPUT_CSV.exists():
        raise HTTPException(
            status_code=404,
            detail="CSV not yet generated. Run 'python pipeline/run_pipeline.py export'.",
        )
    return FileResponse(
        path=str(OUTPUT_CSV),
        media_type="text/csv",
        filename="weather_australia.csv",
    )


# ─── Analytics (DuckDB marts) ────────────────────────────────────────────────

_ANALYTICS_MARTS = {
    "forecast-timeline": "mart_forecast_vs_actual_timeline",
    "performance-overview": "mart_model_performance_overview",
    "health": "mart_mlops_health",
    "performance-by-city": "mart_model_performance_by_city",
    "retraining-history": "mart_retraining_history",
}


def _read_analytics_mart(mart: str) -> pd.DataFrame:
    if mart not in _ANALYTICS_MARTS:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown mart '{mart}'. Available: {list(_ANALYTICS_MARTS)}",
        )
    if not ANALYTICS_DB_PATH.exists():
        raise HTTPException(
            status_code=503,
            detail="Analytics database not available. Run pipeline/generate_demo_dataset.py or dbt.",
        )
    try:
        import duckdb

        con = duckdb.connect(str(ANALYTICS_DB_PATH), read_only=True)
        df = con.execute(f"SELECT * FROM {_ANALYTICS_MARTS[mart]}").df()
        con.close()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return df


@app.get("/api/analytics/{mart}.csv")
def get_analytics_mart_csv(mart: str):
    """
    Download one analytics mart as CSV for Power BI Web connector.
    Example: /api/analytics/health.csv
    """
    df = _read_analytics_mart(mart)
    filename = f"{_ANALYTICS_MARTS[mart]}.csv"
    return Response(
        content=df.to_csv(index=False),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/analytics/{mart}")
def get_analytics_mart(mart: str):
    """
    Read one of the five DuckDB analytics marts.
    Available slugs: forecast-timeline, performance-overview, health,
                     performance-by-city, retraining-history.
    """
    df = _read_analytics_mart(mart)
    return JSONResponse({"count": len(df), "data": _df_to_records(df)})


# ─── MLflow endpoints ────────────────────────────────────────────────────────


def _get_mlflow_client():
    import mlflow
    from pipeline.mlflow_config import get_mlflow_tracking_uri

    uri = get_mlflow_tracking_uri(ROOT)
    mlflow.set_tracking_uri(uri)
    return mlflow.MlflowClient()


@app.get("/api/mlflow/runs")
def get_mlflow_runs(n: int = Query(10, description="Number of most recent runs")):
    """
    Last N training runs from MLflow — useful for Streamlit or Power BI trend charts.
    Shows aggregate metrics per run (rain accuracy, temp MAE, etc.).
    """
    if DEMO_MODE:
        return JSONResponse(
            {"count": 0, "runs": [], "message": "MLflow not available in demo mode."}
        )
    try:
        client = _get_mlflow_client()
        experiment = client.get_experiment_by_name("weather_australia")
        if not experiment:
            return JSONResponse({"runs": [], "message": "No MLflow experiment found yet."})

        runs = client.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=["start_time DESC"],
            max_results=n,
            filter_string="tags.mlflow.runName NOT LIKE '%rain%' "
            "AND tags.mlflow.runName NOT LIKE '%temp%' "
            "AND tags.mlflow.runName NOT LIKE '%heatwave%' "
            "AND tags.mlflow.runName NOT LIKE '%frost%' "
            "AND tags.mlflow.runName NOT LIKE '%storm%' "
            "AND tags.mlflow.runName NOT LIKE '%weather_type%'",
        )

        result = []
        for r in runs:
            result.append(
                {
                    "run_id": r.info.run_id,
                    "run_name": r.info.run_name,
                    "status": r.info.status,
                    "start_time": r.info.start_time,
                    "metrics": r.data.metrics,
                    "params": {
                        k: v
                        for k, v in r.data.params.items()
                        if k
                        in (
                            "n_rows",
                            "n_features",
                            "n_cities",
                            "date_range_start",
                            "date_range_end",
                        )
                    },
                }
            )

        return JSONResponse({"count": len(result), "runs": result})

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/mlflow/metrics")
def get_latest_mlflow_metrics():
    """
    Latest MLflow metrics with cascade fallback:
      1. models/mlflow_latest.json  — committed JSON cache, works on Render
      2. MLflow tracking server     — local mlruns/ when available
      3. Degraded response          — status "unavailable", no HTTP 500
    Every response includes a 'source' field so /health can surface it.
    """
    import json

    # ── Case 1: committed JSON cache ─────────────────────────────────────────
    json_path = ROOT / "models" / "mlflow_latest.json"
    if json_path.exists():
        try:
            data = json.loads(json_path.read_text())
            data["source"] = "json_cache"
            return JSONResponse(data)
        except Exception:
            pass  # fall through

    # ── DEMO_MODE hardcoded fallback (no mlflow_latest.json yet) ─────────────
    if DEMO_MODE:
        return JSONResponse(
            {
                "source": "json_cache",
                "exported_at": "2026-05-01T06:00:00Z",
                "run_id": "demo_run_001",
                "run_name": "train_20260501_060000",
                "start_time": "2026-05-01T04:00:00Z",
                "duration_seconds": 823,
                "status": "FINISHED",
                "model_version": "20260501_060000",
                "metrics": {
                    "rain_accuracy": 0.7699,
                    "temp_mae": 1.628,
                    "temp_rmse": 2.31,
                    "rain_f1": 0.76,
                    "rain_precision": 0.78,
                    "rain_recall": 0.74,
                },
                "params": {"n_estimators": "200", "max_depth": "6", "learning_rate": "0.05"},
                "tags": {"trigger": "scheduled", "cities_count": "26"},
                "baseline_metrics": {"rain_accuracy": 0.75, "temp_mae": 1.89},
                "delta_vs_baseline": {"rain_accuracy": 0.0199, "temp_mae": -0.262},
                "model_metrics": {
                    "rain_tomorrow": {"accuracy": 0.7699, "auc": 0.8518},
                    "max_temp_tomorrow": {"mae": 1.628, "r2": 0.9068},
                    "weather_type_tomorrow": {"accuracy": 0.8216},
                    "heatwave_risk": {"auc": 0.9963},
                    "frost_risk": {"auc": 0.9891},
                    "storm_probability": {"auc": 0.8980},
                },
            }
        )

    # ── Case 2: live MLflow tracking server ───────────────────────────────────
    try:
        client = _get_mlflow_client()
        experiment = client.get_experiment_by_name("weather_australia")
        if experiment:
            runs = client.search_runs(
                experiment_ids=[experiment.experiment_id],
                filter_string="attributes.status = 'FINISHED'",
                order_by=["start_time DESC"],
                max_results=1,
            )
            if runs:
                run = runs[0]
                info = run.info
                _summary_keys = {
                    "rain_accuracy",
                    "temp_mae",
                    "temp_rmse",
                    "rain_f1",
                    "rain_precision",
                    "rain_recall",
                }
                summary = {
                    k: round(v, 6) if isinstance(v, float) else v
                    for k, v in run.data.metrics.items()
                    if k in _summary_keys
                }
                metrics_path = ROOT / "models" / "metrics.json"
                model_metrics = (
                    json.loads(metrics_path.read_text()) if metrics_path.exists() else None
                )
                return JSONResponse(
                    {
                        "source": "mlflow_live",
                        "exported_at": None,
                        "run_id": info.run_id,
                        "run_name": info.run_name or "",
                        "start_time": info.start_time,
                        "duration_seconds": (
                            round((info.end_time - info.start_time) / 1000)
                            if info.end_time
                            else None
                        ),
                        "status": info.status,
                        "model_version": info.run_name or info.run_id[:8],
                        "metrics": summary,
                        "params": dict(run.data.params),
                        "tags": {
                            k: v for k, v in run.data.tags.items() if not k.startswith("mlflow.")
                        },
                        "baseline_metrics": None,
                        "delta_vs_baseline": None,
                        "model_metrics": model_metrics,
                    }
                )
    except Exception:
        pass

    # ── Case 3: nothing available ─────────────────────────────────────────────
    return JSONResponse(
        {
            "source": "unavailable",
            "status": "unavailable",
            "message": "MLflow metrics not available. Run the training pipeline first.",
            "metrics": {},
            "model_metrics": None,
            "baseline_metrics": None,
            "delta_vs_baseline": None,
        }
    )


# ─── run ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("API_PORT", 8001))  # 8001 local (8083 = Airflow)
    host = os.getenv("API_HOST", "0.0.0.0")
    uvicorn.run("api.app:app", host=host, port=port, reload=True)
