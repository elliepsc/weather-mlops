"""
Export all mart tables from DuckDB analytics.duckdb to data/analytics/*.csv.

Run after dbt run:
    python analytics/scripts/export_powerbi.py

Output files: one CSV per mart/BI model, ready to import in Power BI.

Power BI connection after export:
    Get Data -> Text/CSV -> select files in data/analytics/

Power BI ODBC connection, live with no export needed:
    1. Install DuckDB ODBC driver: https://duckdb.org/docs/api/odbc/overview
    2. Create DSN pointing to data/analytics.duckdb
    3. Get Data -> ODBC -> select DSN -> Import mode
    4. Tables are under schema main_marts.*
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent.parent
DUCKDB_PATH = ROOT / "data" / "analytics.duckdb"
OUTPUT_DIR = ROOT / "data" / "analytics"

MARTS = [
    "bi_dashboard_overview",
    "bi_data_quality_summary",
    "bi_powerbi_homepage_kpis",
    "bi_weather_alerts_summary_monthly",
    "bi_weather_current_snapshot",
    "bi_weather_extremes",
    "bi_weather_risk_alerts",
    "location_cities",
    "mart_city_weather_scorecard",
    "mart_climate_anomaly_vs_drift",
    "mart_climate_normals_city_month",
    "mart_current_weather_snapshot",
    "mart_data_freshness_by_city",
    "mart_data_quality_daily",
    "mart_extreme_events_performance",
    "mart_feature_drift_summary",
    "mart_forecast_accuracy_monthly",
    "mart_forecast_confusion_matrix",
    "mart_forecast_vs_actual_daily",
    "mart_forecast_vs_actual_timeline",
    "mart_mlops_health",
    "mart_model_calibration",
    "mart_model_performance_by_city",
    "mart_model_performance_overview",
    "mart_prediction_bias_report",
    "mart_rain_probability_calibration",
    "mart_retraining_history",
    "mart_seasonal_performance",
    "mart_weather_comfort_segments",
    "mart_weather_daily_clean",
    "mart_weather_extremes",
    "mart_weather_monthly_city",
    "mart_weather_risk_alerts",
    "mart_weather_seasonality",
    "mart_weather_state_monthly",
    "mart_weather_yearly_city",
]


def main() -> None:
    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(
            f"DuckDB database not found: {DUCKDB_PATH}\n"
            "Run first: python analytics/scripts/load_sources.py && cd analytics && dbt run"
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Exporting marts to {OUTPUT_DIR}")

    with duckdb.connect(str(DUCKDB_PATH), read_only=True) as duck:
        for mart in MARTS:
            out = OUTPUT_DIR / f"{mart}.csv"
            out_tmp = OUTPUT_DIR / f"{mart}.csv.tmp"
            try:
                duck.execute(
                    f"""
                    COPY (SELECT * FROM main_marts.{mart})
                    TO '{out_tmp.as_posix()}'
                    (HEADER, DELIMITER ',')
                    """
                )
                row_count = duck.execute(f"SELECT COUNT(*) FROM main_marts.{mart}").fetchone()[0]
                out_tmp.replace(out)
                logger.info(
                    "exported %s: %d rows, %.1f KB -> %s",
                    mart,
                    row_count,
                    out.stat().st_size / 1024,
                    out.name,
                )
                print(f"  {mart}: {row_count:,} rows -> {out.name}")
            except Exception:
                if out_tmp.exists():
                    out_tmp.unlink()
                raise

    for mart in MARTS:
        out = OUTPUT_DIR / f"{mart}.csv"
        if not out.exists() or out.stat().st_size == 0:
            raise RuntimeError(f"Export validation failed: {out} missing or empty")

    print(f"\nDone. Import in Power BI: Get Data -> Text/CSV -> {OUTPUT_DIR}")
    print("Or connect live via ODBC: see https://duckdb.org/docs/api/odbc/overview")


if __name__ == "__main__":
    main()
