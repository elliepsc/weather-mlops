from pathlib import Path

from pipeline import mlflow_config


def test_explicit_tracking_uri_has_priority(monkeypatch):
    root = Path("/mnt/c/projects/weather-mlops")
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "sqlite:////tmp/custom.db")

    assert mlflow_config.get_mlflow_tracking_uri(root) == "sqlite:////tmp/custom.db"
    assert mlflow_config.get_mlflow_home(root) is None


def test_wsl_repo_on_windows_mount_uses_linux_home(monkeypatch):
    root = Path("/mnt/c/projects/weather-mlops")
    monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
    monkeypatch.setattr(mlflow_config, "_is_wsl", lambda: True)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: Path("/home/ellie")))

    assert mlflow_config.get_mlflow_home(root) == Path("/home/ellie/.weather-mlops/mlflow")
    assert mlflow_config.get_mlflow_tracking_uri(root) == (
        "sqlite:////home/ellie/.weather-mlops/mlflow/mlflow.db"
    )


def test_non_wsl_keeps_repo_local_mlflow_dir(monkeypatch):
    root = Path("C:/Users/Ellie/weather-mlops")
    monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
    monkeypatch.setattr(mlflow_config, "_is_wsl", lambda: False)

    assert mlflow_config.get_mlflow_home(root) == root / "mlflow"
    assert mlflow_config.get_mlflow_artifacts_dir(root) == root / "mlflow" / "artifacts"
