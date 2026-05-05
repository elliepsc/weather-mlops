"""Create the BigQuery dataset and external tables pointing to GCS Parquet files.

Run this once after an initial GCS export to wire up BigQuery.

Usage:
    python analytics/scripts/setup_bigquery.py

Environment variables (required unless noted):
    GCP_PROJECT_ID              : GCP project ID
    GCS_BUCKET                  : GCS bucket containing the Parquet export
    GCP_DATASET                 : BigQuery dataset name (default: weather_mlops)
    GCP_LOCATION                : dataset region (default: EU)
    GOOGLE_APPLICATION_CREDENTIALS : service account JSON path (optional if using oauth)
"""

from __future__ import annotations

import logging
import os
import sys

logger = logging.getLogger(__name__)

GCS_PREFIX = "weather-mlops/marts"

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
    project = os.environ.get("GCP_PROJECT_ID", "")
    dataset = os.environ.get("GCP_DATASET", "weather_mlops")
    location = os.environ.get("GCP_LOCATION", "EU")
    bucket = os.environ.get("GCS_BUCKET", "")

    if not project:
        sys.exit("Error: GCP_PROJECT_ID is not set")
    if not bucket:
        sys.exit("Error: GCS_BUCKET is not set")

    try:
        from google.cloud import bigquery
    except ImportError:
        sys.exit(
            "google-cloud-bigquery is not installed. " "Run: pip install google-cloud-bigquery"
        )

    client = bigquery.Client(project=project)

    # Create dataset if it doesn't exist
    dataset_ref = bigquery.Dataset(f"{project}.{dataset}")
    dataset_ref.location = location
    client.create_dataset(dataset_ref, exists_ok=True)
    logger.info("Dataset %s.%s ready (location: %s)", project, dataset, location)

    # Create or replace external tables
    created: list[str] = []
    failed: list[str] = []
    for mart in MARTS:
        uri = f"gs://{bucket}/{GCS_PREFIX}/{mart}/*.parquet"
        table_id = f"`{project}.{dataset}.{mart}`"
        ddl = (
            f"CREATE OR REPLACE EXTERNAL TABLE {table_id}\n"
            f"OPTIONS (\n"
            f"  format = 'PARQUET',\n"
            f"  uris = ['{uri}']\n"
            f")"
        )
        try:
            client.query(ddl).result()
            logger.info("created %s -> %s", table_id, uri)
            created.append(mart)
        except Exception as exc:
            logger.error("failed %s: %s", table_id, exc)
            failed.append(mart)

    print(f"\nDone: {len(created)}/{len(MARTS)} external tables created in " f"{project}.{dataset}")
    if failed:
        print(f"Failed ({len(failed)}): {', '.join(failed)}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
