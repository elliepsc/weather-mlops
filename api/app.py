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

from datetime import date
from typing import Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
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
    return df.where(pd.notnull(df), None).to_dict(orient="records")


# ─── endpoints ───────────────────────────────────────────────────────────────


@app.get("/health")
def health():
    return {
        "status": "ok",
        "db": str(APP_DB_PATH),
        "db_exists": APP_DB_PATH.exists(),
        "demo_mode": DEMO_MODE,
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
    "forecast-timeline":    "mart_forecast_vs_actual_timeline",
    "performance-overview": "mart_model_performance_overview",
    "health":               "mart_mlops_health",
    "performance-by-city":  "mart_model_performance_by_city",
    "retraining-history":   "mart_retraining_history",
}


@app.get("/api/analytics/{mart}")
def get_analytics_mart(mart: str):
    """
    Read one of the five DuckDB analytics marts.
    Available slugs: forecast-timeline, performance-overview, health,
                     performance-by-city, retraining-history.
    """
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

    return JSONResponse({"count": len(df), "data": _df_to_records(df)})


# ─── MLflow endpoints ────────────────────────────────────────────────────────


def _get_mlflow_client():
    from pipeline.mlflow_config import get_mlflow_tracking_uri
    import mlflow

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
        return JSONResponse({"count": 0, "runs": [], "message": "MLflow not available in demo mode."})
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
    Latest metrics from models/metrics.json — quick health check for Power BI.
    In DEMO_MODE returns canned values matching the trained model benchmarks.
    """
    if DEMO_MODE:
        return JSONResponse({
            "rain_tomorrow":         {"accuracy": 0.7699, "auc": 0.8518},
            "max_temp_tomorrow":     {"mae": 1.628, "r2": 0.9068},
            "weather_type_tomorrow": {"accuracy": 0.8216},
            "heatwave_risk":         {"auc": 0.9963},
            "frost_risk":            {"auc": 0.9891},
            "storm_probability":     {"auc": 0.8980},
        })
    metrics_path = ROOT / "models" / "metrics.json"
    if not metrics_path.exists():
        raise HTTPException(
            status_code=404, detail="No metrics file found. Run the pipeline first."
        )
    import json

    return JSONResponse(json.loads(metrics_path.read_text()))


# ─── run ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("API_PORT", 8083))
    host = os.getenv("API_HOST", "0.0.0.0")
    uvicorn.run("api.app:app", host=host, port=port, reload=True)
