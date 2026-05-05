"""Export mart tables from DuckDB analytics.duckdb to GCS as Parquet files.

Usage:
    python pipeline/export_to_gcs.py

Environment variables:
    GCS_ENABLED                 : must be "true" to run (default: "false")
    GCS_BUCKET                  : GCS bucket name (required)
    GOOGLE_APPLICATION_CREDENTIALS : service account JSON path (optional — local uses oauth)
    ANALYTICS_DB_PATH           : override for DuckDB path (default: data/analytics.duckdb)
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
import time
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent
DUCKDB_PATH = Path(os.getenv("ANALYTICS_DB_PATH", str(ROOT / "data" / "analytics.duckdb")))

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

# Tables that get YYYY/MM/ date partitioning; value is the date column name.
PARTITIONED_MARTS: dict[str, str] = {
    "mart_forecast_vs_actual_timeline": "prediction_date",
}


def _export_mart(
    duck: duckdb.DuckDBPyConnection, mart: str, tmp_dir: Path
) -> list[tuple[Path, str]]:
    """Export one mart to local Parquet file(s).

    Returns a list of (local_path, gcs_relative_path) tuples.
    For partitioned marts, one tuple per year/month partition.
    """
    mart_dir = tmp_dir / mart
    mart_dir.mkdir(parents=True, exist_ok=True)

    if mart in PARTITIONED_MARTS:
        date_col = PARTITIONED_MARTS[mart]
        partitions = duck.execute(
            f"""
            SELECT DISTINCT
                CAST(EXTRACT(YEAR  FROM {date_col}) AS INTEGER) AS yr,
                CAST(EXTRACT(MONTH FROM {date_col}) AS INTEGER) AS mo
            FROM main_marts.{mart}
            ORDER BY yr, mo
            """
        ).fetchall()

        results: list[tuple[Path, str]] = []
        for yr, mo in partitions:
            part_dir = mart_dir / f"{yr:04d}" / f"{mo:02d}"
            part_dir.mkdir(parents=True, exist_ok=True)
            local_path = part_dir / "data.parquet"
            duck.execute(
                f"""
                COPY (
                    SELECT * FROM main_marts.{mart}
                    WHERE EXTRACT(YEAR  FROM {date_col}) = {yr}
                      AND EXTRACT(MONTH FROM {date_col}) = {mo}
                )
                TO '{local_path.as_posix()}' (FORMAT PARQUET)
                """
            )
            results.append((local_path, f"{mart}/{yr:04d}/{mo:02d}/data.parquet"))
        return results

    local_path = mart_dir / f"{mart}.parquet"
    duck.execute(
        f"""
        COPY (SELECT * FROM main_marts.{mart})
        TO '{local_path.as_posix()}' (FORMAT PARQUET)
        """
    )
    return [(local_path, f"{mart}/{mart}.parquet")]


def main() -> None:
    """Export all marts from DuckDB to GCS as Parquet files."""
    if os.getenv("GCS_ENABLED", "false").lower() != "true":
        logger.warning("GCS_ENABLED is not 'true' — skipping GCS export")
        return

    bucket_name = os.getenv("GCS_BUCKET", "")
    if not bucket_name:
        logger.warning("GCS_BUCKET is not set — skipping GCS export")
        return

    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""):
        logger.info(
            "GOOGLE_APPLICATION_CREDENTIALS not set — "
            "attempting Application Default Credentials (oauth)"
        )

    try:
        from google.cloud import storage
    except ImportError:
        logger.warning(
            "google-cloud-storage not installed — skipping GCS export. "
            "Install with: pip install google-cloud-storage"
        )
        return

    if not DUCKDB_PATH.exists():
        logger.warning("DuckDB not found at %s — skipping GCS export", DUCKDB_PATH)
        return

    try:
        gcs_client = storage.Client()
        bucket = gcs_client.bucket(bucket_name)
        bucket.reload()
    except Exception as exc:
        logger.warning("Cannot access GCS bucket %r: %s — skipping", bucket_name, exc)
        return

    t_start = time.monotonic()
    tmp_dir = Path(tempfile.mkdtemp(prefix="gcs_export_"))
    total_bytes = 0
    upload_count = 0

    try:
        with duckdb.connect(str(DUCKDB_PATH), read_only=True) as duck:
            for mart in MARTS:
                try:
                    file_pairs = _export_mart(duck, mart, tmp_dir)
                except Exception as exc:
                    logger.error("Export failed for %s: %s", mart, exc)
                    continue

                for local_path, gcs_rel in file_pairs:
                    blob_name = f"{GCS_PREFIX}/{gcs_rel}"
                    size = local_path.stat().st_size
                    bucket.blob(blob_name).upload_from_filename(str(local_path))
                    total_bytes += size
                    upload_count += 1
                    logger.info(
                        "uploaded gs://%s/%s (%.1f KB)",
                        bucket_name,
                        blob_name,
                        size / 1024,
                    )
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    elapsed = time.monotonic() - t_start
    logger.info(
        "GCS export done: %d files, %.1f MB, %.1fs",
        upload_count,
        total_bytes / 1_048_576,
        elapsed,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
