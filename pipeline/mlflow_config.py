"""Shared MLflow path resolution for local, Docker, and WSL runs."""
import os
from pathlib import Path


def _is_wsl() -> bool:
    if os.getenv("WSL_DISTRO_NAME") or os.getenv("WSL_INTEROP"):
        return True
    try:
        return "microsoft" in Path("/proc/version").read_text(encoding="utf-8").lower()
    except OSError:
        return False


def _is_windows_mount(path: Path) -> bool:
    candidates = [str(path)]
    try:
        candidates.append(str(path.resolve()))
    except OSError:
        pass
    return any(candidate.replace("\\", "/").startswith("/mnt/") for candidate in candidates)


def get_mlflow_home(root: Path) -> Path | None:
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    if tracking_uri:
        return None
    if _is_wsl() and _is_windows_mount(root):
        return Path.home() / ".weather-mlops" / "mlflow"
    return root / "mlflow"


def get_mlflow_tracking_uri(root: Path) -> str:
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    if tracking_uri:
        return tracking_uri
    db_path = (get_mlflow_home(root) / "mlflow.db").expanduser()
    normalized = str(db_path).replace("\\", "/")
    return f"sqlite:///{normalized}"


def get_mlflow_artifacts_dir(root: Path) -> Path | None:
    mlflow_home = get_mlflow_home(root)
    if mlflow_home is None:
        return None
    return mlflow_home / "artifacts"
