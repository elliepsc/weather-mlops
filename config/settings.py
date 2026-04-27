"""Typed configuration loading for weather-mlops.

Three layers:
  - Settings: environment-specific values loaded from .env and process env
  - MLOpsConfig: versioned operational thresholds loaded from config/mlops.yaml
  - ModelingConfig: model/training parameters loaded from config/modeling.yaml
"""

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, model_validator

ROOT = Path(__file__).parent.parent

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ModuleNotFoundError:
    class BaseSettings(BaseModel):
        """Small fallback so local tests can run without pydantic-settings."""

        def __init__(self, **data):
            values = {}
            env_file = ROOT / ".env"
            if env_file.exists():
                for line in env_file.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, value = line.split("=", 1)
                    values[key.strip().lower()] = value.strip()

            for key, value in os.environ.items():
                values[key.lower()] = value

            values.update(data)
            super().__init__(**values)


    def SettingsConfigDict(**kwargs):
        return kwargs


class MonitoringConfig(BaseModel):
    rain_accuracy_threshold: float
    temp_mae_threshold_celsius: float
    min_drift_features_alert: int
    min_drift_features_retrain: int
    retrain_cooldown_days: int
    drift_ks_alpha: float
    drift_window_days: int
    monitored_features: list[str]
    min_rows_for_metrics: int

    @model_validator(mode="after")
    def alert_lt_retrain(self):
        if self.min_drift_features_alert >= self.min_drift_features_retrain:
            raise ValueError(
                f"min_drift_features_alert ({self.min_drift_features_alert}) "
                f"must be < min_drift_features_retrain ({self.min_drift_features_retrain})"
            )
        return self


class IngestionConfig(BaseModel):
    min_cities_threshold: int
    backfill_delay_seconds: float
    required_non_null_columns: list[str]

    @model_validator(mode="after")
    def require_at_least_one_completeness_column(self):
        if not self.required_non_null_columns:
            raise ValueError("ingestion.required_non_null_columns must not be empty")
        return self


class BaselineToleranceConfig(BaseModel):
    rain_accuracy_pp: float
    temp_mae_celsius: float


class TrainingConfig(BaseModel):
    baseline_tolerance: BaselineToleranceConfig


class MLOpsConfig(BaseModel):
    monitoring: MonitoringConfig
    ingestion: IngestionConfig
    training: TrainingConfig


class FeatureConfig(BaseModel):
    numerical: list[str]
    categorical: list[str]

    @model_validator(mode="after")
    def no_duplicate_features(self):
        overlap = sorted(set(self.numerical) & set(self.categorical))
        if overlap:
            raise ValueError(f"Features cannot be both numerical and categorical: {overlap}")
        return self


class LabelConfig(BaseModel):
    rain_min_mm: float = Field(gt=0)
    heatwave_temp_celsius: float
    frost_temp_celsius: float
    storm_rainfall_mm: float = Field(gt=0)
    storm_gust_kmh: float = Field(gt=0)


class InferenceConfig(BaseModel):
    rain_probability_threshold: float = Field(ge=0, le=1)


class ModelingTrainingConfig(BaseModel):
    test_size: float = Field(gt=0, lt=1)
    random_state: int


class ModelingMlflowConfig(BaseModel):
    experiment_name: str


class XGBoostConfig(BaseModel):
    base: dict[str, Any]
    models: dict[str, dict[str, Any]]


class ModelingConfig(BaseModel):
    mlflow: ModelingMlflowConfig
    training: ModelingTrainingConfig
    inference: InferenceConfig
    labels: LabelConfig
    features: FeatureConfig
    xgboost: XGBoostConfig


class Settings(BaseSettings):
    """Secrets and environment-specific settings."""

    model_config = SettingsConfigDict(
        env_file=str(ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    env: str = "dev"
    db_path: str = "data/weather.db"
    openmeteo_base_url: str = "https://archive-api.open-meteo.com/v1/archive"
    slack_webhook_url: str = ""
    alert_email: str = ""


def load_mlops_config() -> MLOpsConfig:
    cfg_path = ROOT / "config" / "mlops.yaml"
    with open(cfg_path, encoding="utf-8") as file:
        return MLOpsConfig(**yaml.safe_load(file))


def load_modeling_config() -> ModelingConfig:
    cfg_path = ROOT / "config" / "modeling.yaml"
    with open(cfg_path, encoding="utf-8") as file:
        return ModelingConfig(**yaml.safe_load(file))


settings = Settings()
mlops_config = load_mlops_config()
modeling_config = load_modeling_config()
