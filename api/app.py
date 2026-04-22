"""
FastAPI — Weather data endpoint for Power BI.

Endpoints:
  GET /api/weather              — full historical + predictions table (Power BI Web connector)
  GET /api/weather/latest       — latest date per city with predictions
  GET /api/weather/predictions  — predictions-only table
  GET /api/cities               — list of available cities
  GET /api/mlflow/runs          — last N MLflow training runs + metrics
  GET /api/mlflow/metrics       — latest metrics per model
  GET /api/export/csv           — download CSV file directly
  GET /health                   — health check
"""
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

from pipeline.database import DB_PATH, read_all, get_connection

app = FastAPI(
    title="Weather Australia API",
    description="Historical weather data + ML predictions for 26 Australian cities.",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Power BI Desktop needs this
    allow_methods=["GET"],
    allow_headers=["*"],
)

OUTPUT_CSV = ROOT / "data" / "output" / "weather_final.csv"


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame to JSON-serialisable records (NaN → None)."""
    return df.where(pd.notnull(df), None).to_dict(orient="records")


# ─── endpoints ───────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "db": str(DB_PATH), "db_exists": DB_PATH.exists()}


@app.get("/api/cities")
def list_cities():
    """Return available cities and their coordinates."""
    from pipeline.locations import LOCATIONS
    return {"cities": [
        {"city": k, "state": v["state"], "lat": v["lat"], "lon": v["lon"]}
        for k, v in LOCATIONS.items()
    ]}


@app.get("/api/weather")
def get_weather(
    city:       Optional[str] = Query(None, description="Filter by city name"),
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end_date:   Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    limit:      int           = Query(50000, description="Max rows returned"),
):
    """
    Full weather + predictions table.
    Power BI: Data → Web → paste this URL → JSON → expand 'data'.
    """
    try:
        with get_connection() as conn:
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

    return JSONResponse({
        "count":        len(df),
        "last_updated": date.today().isoformat(),
        "data":         _df_to_records(df),
    })


@app.get("/api/weather/latest")
def get_latest(city: Optional[str] = Query(None)):
    """Latest available date per city with all predictions — ideal for a Power BI dashboard."""
    try:
        with get_connection() as conn:
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

    return JSONResponse({
        "count": len(df),
        "data":  _df_to_records(df),
    })


@app.get("/api/weather/predictions")
def get_predictions(
    city:       Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date:   Optional[str] = Query(None),
):
    """Predictions-only table (lighter payload for Power BI dashboards)."""
    try:
        with get_connection() as conn:
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


# ─── MLflow endpoints ────────────────────────────────────────────────────────

def _get_mlflow_client():
    import mlflow
    import os
    uri = os.getenv("MLFLOW_TRACKING_URI", f"sqlite:///{ROOT / 'mlflow' / 'mlflow.db'}")
    mlflow.set_tracking_uri(uri)
    return mlflow.MlflowClient()


@app.get("/api/mlflow/runs")
def get_mlflow_runs(n: int = Query(10, description="Number of most recent runs")):
    """
    Last N training runs from MLflow — useful for Streamlit or Power BI trend charts.
    Shows aggregate metrics per run (rain accuracy, temp MAE, etc.).
    """
    try:
        import mlflow
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
            result.append({
                "run_id":        r.info.run_id,
                "run_name":      r.info.run_name,
                "status":        r.info.status,
                "start_time":    r.info.start_time,
                "metrics":       r.data.metrics,
                "params":        {k: v for k, v in r.data.params.items()
                                  if k in ("n_rows", "n_features", "n_cities",
                                           "date_range_start", "date_range_end")},
            })

        return JSONResponse({"count": len(result), "runs": result})

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/mlflow/metrics")
def get_latest_mlflow_metrics():
    """
    Latest metrics from models/metrics.json — quick health check for Power BI.
    """
    metrics_path = ROOT / "models" / "metrics.json"
    if not metrics_path.exists():
        raise HTTPException(status_code=404,
                            detail="No metrics file found. Run the pipeline first.")
    import json
    return JSONResponse(json.loads(metrics_path.read_text()))


# ─── run ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.app:app", host="0.0.0.0", port=8080, reload=True)
