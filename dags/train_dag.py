"""
Airflow DAG — Weekly model retraining
Schedule : every Monday at 02:00 UTC

Tasks:
  1. retrain_models  — retrain all 6 XGBoost models on the full DB history
  2. run_predictions — regenerate all predictions with the new models
  3. export_csv      — refresh CSV export for Power BI fallback
"""
import sys
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

default_args = {
    "owner":            "airflow",
    "retries":          1,
    "retry_delay_seconds": 600,
    "email_on_failure": False,
}


def step_train(**kwargs):
    from pipeline.run_pipeline import step_train as _f
    _f()

def step_predict(**kwargs):
    from pipeline.run_pipeline import step_predict as _f
    _f()

def step_export(**kwargs):
    from pipeline.run_pipeline import step_export as _f
    _f()


with DAG(
    dag_id="weather_weekly_train",
    description="Weekly retraining of 6 XGBoost models + prediction refresh",
    schedule="0 2 * * 1",            # every Monday at 02:00 UTC
    start_date=datetime(2026, 4, 16),
    catchup=False,
    default_args=default_args,
    tags=["weather", "training", "weekly"],
) as dag:

    t_train = PythonOperator(
        task_id="retrain_models",
        python_callable=step_train,
    )

    t_predict = PythonOperator(
        task_id="run_predictions",
        python_callable=step_predict,
    )

    t_export = PythonOperator(
        task_id="export_csv",
        python_callable=step_export,
    )

    t_train >> t_predict >> t_export
