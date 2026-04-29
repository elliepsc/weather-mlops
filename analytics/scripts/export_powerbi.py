"""
Export all mart tables from DuckDB analytics.duckdb → data/powerbi/*.csv

Run after dbt run:
    python analytics/scripts/export_powerbi.py

Output files (one per mart, ready to import in Power BI):
    data/powerbi/mart_model_performance_overview.csv
    data/powerbi/mart_model_performance_by_city.csv
    data/powerbi/mart_forecast_vs_actual_timeline.csv
    data/powerbi/mart_mlops_health.csv
    data/powerbi/mart_retraining_history.csv

Power BI connection (after export):
    Get Data → Text/CSV → select any file in data/powerbi/

Power BI ODBC connection (live, no export needed):
    1. Install DuckDB ODBC driver: https://duckdb.org/docs/api/odbc/overview
    2. Create DSN pointing to data/analytics.duckdb
    3. Get Data → ODBC → select DSN → Import mode
    4. Tables are under schema main_marts.*
"""

from __future__ import annotations

from pathlib import Path

import duckdb

ROOT = Path(__file__).parent.parent.parent
DUCKDB_PATH = ROOT / "data" / "analytics.duckdb"
OUTPUT_DIR = ROOT / "data" / "powerbi"

MARTS = [
    "mart_model_performance_overview",
    "mart_model_performance_by_city",
    "mart_forecast_vs_actual_timeline",
    "mart_mlops_health",
    "mart_retraining_history",
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
            duck.execute(
                f"""
                COPY (SELECT * FROM main_marts.{mart})
                TO '{out.as_posix()}'
                (HEADER, DELIMITER ',')
            """
            )
            row_count = duck.execute(f"SELECT COUNT(*) FROM main_marts.{mart}").fetchone()[0]
            print(f"  {mart}: {row_count:,} rows → {out.name}")

    print(f"\nDone. Import in Power BI: Get Data → Text/CSV → {OUTPUT_DIR}")
    print("Or connect live via ODBC: see https://duckdb.org/docs/api/odbc/overview")


if __name__ == "__main__":
    main()
