"""Minimal Airflow compatibility layer for local unit tests.

When Airflow is installed, this module re-exports the real classes.
When it is not installed, it provides light stubs so DAG modules remain
importable and pure-Python business logic can still be tested.
"""

try:
    from airflow import DAG
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import (
        BranchPythonOperator,
        PythonOperator,
    )
    from airflow.providers.standard.operators.trigger_dagrun import (
        TriggerDagRunOperator,
    )
except ModuleNotFoundError:
    class DAG:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False


    class _DummyOperator:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            self.task_id = kwargs.get("task_id")
            self.python_callable = kwargs.get("python_callable")

        def __rshift__(self, other):
            return other

        def __rrshift__(self, other):
            return self


    class PythonOperator(_DummyOperator):
        pass


    class BranchPythonOperator(_DummyOperator):
        pass


    class EmptyOperator(_DummyOperator):
        pass


    class TriggerDagRunOperator(_DummyOperator):
        pass
