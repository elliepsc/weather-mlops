"""
Power BI Python datasource — alternative to ODBC for live DuckDB queries.

Use this script inside Power BI Desktop when ODBC is not available:
  Get Data → Python script → paste the body of the function you need.

Requirements on the Power BI machine:
    pip install duckdb pandas

How to use:
  1. Power BI Desktop → Get Data → Python script
  2. Set Python home to your venv: File → Options → Python scripting
  3. Paste one of the dataset blocks below into the script editor
  4. Power BI will show a table preview — click Load

Each block reads directly from analytics.duckdb (no CSV needed).
"""

from pathlib import Path

import duckdb

# Adjust this path if running Power BI on Windows (not WSL)
DB_PATH = str(Path(__file__).parent.parent.parent / "data" / "analytics.duckdb")

# ── Copy any block below into Power BI "Python script" datasource ──────────

# Block 1 — Model performance overview (monthly, global)
with duckdb.connect(DB_PATH, read_only=True) as duck:
    dataset = duck.execute(
        "SELECT * FROM main_marts.mart_model_performance_overview ORDER BY month DESC"
    ).df()

# Block 2 — Model performance by city (monthly)
# with duckdb.connect(DB_PATH, read_only=True) as duck:
#     dataset = duck.execute(
#         "SELECT * FROM main_marts.mart_model_performance_by_city ORDER BY month DESC, city"
#     ).df()

# Block 3 — Forecast vs actual timeline (last 90 days)
# with duckdb.connect(DB_PATH, read_only=True) as duck:
#     dataset = duck.execute(
#         "SELECT * FROM main_marts.mart_forecast_vs_actual_timeline ORDER BY date DESC, city"
#     ).df()

# Block 4 — MLOps health (last 30 days)
# with duckdb.connect(DB_PATH, read_only=True) as duck:
#     dataset = duck.execute(
#         "SELECT * FROM main_marts.mart_mlops_health ORDER BY date DESC"
#     ).df()

# Block 5 — Retraining history (all events)
# with duckdb.connect(DB_PATH, read_only=True) as duck:
#     dataset = duck.execute(
#         "SELECT * FROM main_marts.mart_retraining_history ORDER BY retrain_date DESC"
#     ).df()

# Block 6 — Custom SQL (any schema)
# with duckdb.connect(DB_PATH, read_only=True) as duck:
#     dataset = duck.execute("""
#         SELECT city, month,
#                rain_accuracy_30d,
#                temp_mae_30d
#         FROM main_marts.mart_model_performance_by_city
#         WHERE month >= '2025-01-01'
#         ORDER BY month DESC, rain_accuracy_30d ASC
#     """).df()
