"""
Sync SQLite + JSON monitoring files → DuckDB analytics database.

Run before dbt:
    python analytics/scripts/load_sources.py

For BigQuery migration: replace the duckdb writes below with a loader
targeting BigQuery (e.g. pandas_gbq, google-cloud-bigquery, or Airbyte).
The dbt models themselves do not change when switching targets.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).parent.parent.parent
SQLITE_PATH = ROOT / "data" / "weather.db"
DUCKDB_PATH = ROOT / "data" / "analytics.duckdb"
MONITORING_DIR = ROOT / "data" / "monitoring"


def _sqlite_to_df(table: str) -> pd.DataFrame:
    with sqlite3.connect(SQLITE_PATH) as conn:
        return pd.read_sql(f"SELECT * FROM {table} ORDER BY date, city", conn)


def load_weather_tables(duck: duckdb.DuckDBPyConnection) -> None:
    for table in ("weather_raw", "weather_predictions"):
        df = _sqlite_to_df(table)
        duck.execute(f"CREATE OR REPLACE TABLE src_{table} AS SELECT * FROM df")
        print(f"  src_{table}: {len(df):,} rows")


def load_model_metrics_history(duck: duckdb.DuckDBPyConnection) -> None:
    path = MONITORING_DIR / "model_metrics_history.jsonl"
    if not path.exists():
        print("  src_model_metrics_history: file not found, skipped")
        return

    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if not rows:
        return

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["row_num"] = range(len(df))

    duck.execute("CREATE OR REPLACE TABLE src_model_metrics_history AS SELECT * FROM df")
    print(f"  src_model_metrics_history: {len(df):,} rows")


def load_monitoring_decisions(duck: duckdb.DuckDBPyConnection) -> None:
    path = MONITORING_DIR / "monitoring_decision.json"
    if not path.exists():
        return

    data = json.loads(path.read_text())
    duck.execute(
        """
        CREATE TABLE IF NOT EXISTS src_monitoring_decisions (
            date                DATE PRIMARY KEY,
            action              TEXT,
            reason              TEXT,
            n_drifted           INTEGER,
            drifted_features    TEXT,
            low_accuracy        BOOLEAN,
            high_mae            BOOLEAN,
            heavy_drift         BOOLEAN,
            mild_drift          BOOLEAN,
            rain_accuracy_30d   DOUBLE,
            temp_mae_30d        DOUBLE
        )
    """
    )

    duck.execute(
        """
        INSERT INTO src_monitoring_decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (date) DO NOTHING
    """,
        [
            data["date"],
            data.get("action"),
            data.get("reason"),
            data.get("n_drifted", 0),
            json.dumps(data.get("drifted_features", [])),
            bool(data.get("low_accuracy", False)),
            bool(data.get("high_mae", False)),
            bool(data.get("heavy_drift", False)),
            bool(data.get("mild_drift", False)),
            data.get("rain_accuracy_30d"),
            data.get("temp_mae_30d"),
        ],
    )
    print(f"  src_monitoring_decisions: upserted {data['date']}")


def load_drift_reports(duck: duckdb.DuckDBPyConnection) -> None:
    path = MONITORING_DIR / "drift_report.json"
    if not path.exists():
        return

    data = json.loads(path.read_text())
    run_date = data["date"]

    rows = [
        {
            "date": run_date,
            "feature_name": feature,
            "ks_stat": stats.get("ks_stat"),
            "p_value": stats.get("p_value"),
            "drifted": bool(stats.get("drifted", False)),
        }
        for feature, stats in data.get("features", {}).items()
    ]
    if not rows:
        return

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    duck.execute(
        """
        CREATE TABLE IF NOT EXISTS src_drift_reports (
            date            DATE,
            feature_name    TEXT,
            ks_stat         DOUBLE,
            p_value         DOUBLE,
            drifted         BOOLEAN,
            PRIMARY KEY (date, feature_name)
        )
    """
    )

    for row in rows:
        duck.execute(
            """
            INSERT INTO src_drift_reports VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (date, feature_name) DO NOTHING
        """,
            [row["date"], row["feature_name"], row["ks_stat"], row["p_value"], row["drifted"]],
        )

    print(f"  src_drift_reports: upserted {len(rows)} features for {run_date}")


def load_retrain_events(duck: duckdb.DuckDBPyConnection) -> None:
    path = MONITORING_DIR / "last_retrain.json"
    if not path.exists():
        return

    data = json.loads(path.read_text())
    duck.execute(
        """
        CREATE TABLE IF NOT EXISTS src_retrain_events (
            retrain_date    DATE PRIMARY KEY,
            written_at      TEXT,
            source          TEXT
        )
    """
    )

    duck.execute(
        """
        INSERT INTO src_retrain_events VALUES (?, ?, ?)
        ON CONFLICT (retrain_date) DO NOTHING
    """,
        [data["last_retrain"], data.get("written_at"), data.get("source")],
    )

    print(f"  src_retrain_events: upserted {data['last_retrain']}")


def write_pipeline_run(
    duck: duckdb.DuckDBPyConnection,
    data_date: str,
    dag_id: str,
    execution_date: str,
    rows_weather_raw: int,
    rows_weather_predictions: int,
    monitoring_decision_loaded: bool,
    drift_report_loaded: bool,
    retrain_event_loaded: bool,
) -> None:
    duck.execute(
        """
        CREATE TABLE IF NOT EXISTS src_pipeline_runs (
            run_id                          TEXT PRIMARY KEY,
            dag_id                          TEXT,
            data_date                       DATE,
            execution_date                  TEXT,
            sqlite_rows_weather_raw         INTEGER,
            sqlite_rows_weather_predictions INTEGER,
            monitoring_decision_loaded      BOOLEAN,
            drift_report_loaded             BOOLEAN,
            retrain_event_loaded            BOOLEAN,
            loaded_at                       TIMESTAMP DEFAULT now()
        )
        """
    )
    duck.execute(
        """
        INSERT INTO src_pipeline_runs (
            run_id, dag_id, data_date, execution_date,
            sqlite_rows_weather_raw, sqlite_rows_weather_predictions,
            monitoring_decision_loaded, drift_report_loaded, retrain_event_loaded
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (run_id) DO NOTHING
        """,
        [
            str(uuid.uuid4()),
            dag_id,
            data_date,
            execution_date,
            rows_weather_raw,
            rows_weather_predictions,
            monitoring_decision_loaded,
            drift_report_loaded,
            retrain_event_loaded,
        ],
    )
    print(f"  src_pipeline_runs: recorded run for {data_date}")


def main(dag_id: str = "manual", execution_date: str | None = None) -> None:
    if not SQLITE_PATH.exists():
        raise FileNotFoundError(f"SQLite database not found: {SQLITE_PATH}")

    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f"Loading sources into {DUCKDB_PATH}")

    with duckdb.connect(str(DUCKDB_PATH)) as duck:
        load_weather_tables(duck)
        rows_weather_raw = duck.execute("SELECT COUNT(*) FROM src_weather_raw").fetchone()[0]
        rows_weather_predictions = duck.execute(
            "SELECT COUNT(*) FROM src_weather_predictions"
        ).fetchone()[0]

        load_model_metrics_history(duck)
        load_monitoring_decisions(duck)
        load_drift_reports(duck)
        load_retrain_events(duck)

        data_date = (execution_date or datetime.now().isoformat())[:10]
        write_pipeline_run(
            duck=duck,
            data_date=data_date,
            dag_id=dag_id,
            execution_date=execution_date or datetime.now().isoformat(),
            rows_weather_raw=rows_weather_raw,
            rows_weather_predictions=rows_weather_predictions,
            monitoring_decision_loaded=(MONITORING_DIR / "monitoring_decision.json").exists(),
            drift_report_loaded=(MONITORING_DIR / "drift_report.json").exists(),
            retrain_event_loaded=(MONITORING_DIR / "last_retrain.json").exists(),
        )

    print("Done. Run: cd analytics && dbt run")


if __name__ == "__main__":
    main()
