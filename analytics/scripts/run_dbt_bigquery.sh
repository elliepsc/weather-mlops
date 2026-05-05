#!/bin/bash
# Run dbt against BigQuery (Phase 2).
# Usage: ./analytics/scripts/run_dbt_bigquery.sh
#
# Prerequisites:
#   1. pip install dbt-bigquery
#   2. Set GCP_PROJECT_ID, GCP_DATASET, GCS_BUCKET in .env
#   3. Local : gcloud auth application-default login
#      CI/CD : export DBT_BIGQUERY_METHOD=service-account
#              export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
#   4. Export Parquet to GCS first: python pipeline/export_to_gcs.py

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ANALYTICS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

export DBT_TARGET=bigquery

echo "Running dbt run  --target bigquery ..."
dbt run  --target bigquery --profiles-dir "${ANALYTICS_DIR}" --project-dir "${ANALYTICS_DIR}"

echo "Running dbt test --target bigquery ..."
dbt test --target bigquery --profiles-dir "${ANALYTICS_DIR}" --project-dir "${ANALYTICS_DIR}"

echo "Done."
