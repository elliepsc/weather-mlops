"""
Export mart tables from DuckDB to data/analytics/*.csv.

The list of models to export comes from analytics/pipeline.yml (the `export` key).
Edit that file to add or remove CSV outputs — no code change needed here.

Usage (from project root):
    python analytics/scripts/export_powerbi.py                 # daily_bi (env default)
    python analytics/scripts/export_powerbi.py daily_bi
    python analytics/scripts/export_powerbi.py full_build
    ANALYTICS_PIPELINE=full_build python analytics/scripts/export_powerbi.py

Power BI connection after export:
    Get Data → Text/CSV → select files in data/analytics/

Power BI live connection (no export needed):
    1. Install DuckDB ODBC driver: https://duckdb.org/docs/api/odbc/overview
    2. Create DSN pointing to data/analytics.duckdb
    3. Get Data → ODBC → select DSN → Import mode
    4. Tables are under schema main_marts.*
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import duckdb
import yaml

logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent.parent
ANALYTICS_DIR = ROOT / "analytics"
PIPELINE_YML = ANALYTICS_DIR / "pipeline.yml"
DUCKDB_PATH = ROOT / "data" / "analytics.duckdb"
OUTPUT_DIR = ROOT / "data" / "analytics"
_SCHEMA = "main_marts"


def _load_export_list(pipeline: str) -> list[str]:
    config = yaml.safe_load(PIPELINE_YML.read_text())
    pipelines = config.get("pipelines", {})
    if pipeline not in pipelines:
        available = ", ".join(pipelines.keys())
        raise KeyError(f"Pipeline {pipeline!r} not in pipeline.yml. Available: {available}")
    return pipelines[pipeline].get("export", [])


def _export_model(duck: duckdb.DuckDBPyConnection, model: str, out: Path) -> None:
    out_tmp = out.with_suffix(".csv.tmp")
    try:
        duck.execute(
            f"COPY (SELECT * FROM {_SCHEMA}.{model}) "
            f"TO '{out_tmp.as_posix()}' (HEADER, DELIMITER ',')"
        )
        row_count = duck.execute(f"SELECT COUNT(*) FROM {_SCHEMA}.{model}").fetchone()[0]
        out_tmp.replace(out)
        logger.info("exported %s: %d rows → %s", model, row_count, out.name)
        print(f"  {model}: {row_count:,} rows -> {out.name}")
    except Exception:
        if out_tmp.exists():
            out_tmp.unlink()
        raise


def main(pipeline: str | None = None) -> None:
    if pipeline is None:
        pipeline = os.getenv("ANALYTICS_PIPELINE", "daily_bi")

    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(
            f"DuckDB not found: {DUCKDB_PATH}\n" "Run: make analytics-load && make analytics-run"
        )

    models = _load_export_list(pipeline)
    if not models:
        logger.warning("No models in export list for pipeline %r — nothing to export.", pipeline)
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Exporting {len(models)} model(s) [pipeline: {pipeline!r}] → {OUTPUT_DIR}")

    with duckdb.connect(str(DUCKDB_PATH), read_only=True) as duck:
        for model in models:
            _export_model(duck, model, OUTPUT_DIR / f"{model}.csv")

    print(f"\nDone — {len(models)} CSV(s) in {OUTPUT_DIR}")
    print("Power BI: Get Data → Text/CSV  |  live via ODBC: duckdb.org/docs/api/odbc")


if __name__ == "__main__":
    pipeline_arg = (sys.argv[1].strip() if len(sys.argv) > 1 else "") or None
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    main(pipeline=pipeline_arg)
