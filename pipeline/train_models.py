"""
Train 6 XGBoost models and save them to disk.
Every run is tracked in MLflow (experiment: weather_australia).

Models trained:
  1. rain_tomorrow         — binary classification
  2. max_temp_tomorrow     — regression
  3. weather_type_tomorrow — multi-class (Sunny / Cloudy / Rainy / Stormy)
  4. heatwave_risk         — binary classification → probability
  5. frost_risk            — binary classification → probability
  6. storm_probability     — binary classification → probability
  (comfort_score is a formula, no model needed)
"""
import json
import logging
import pickle
from datetime import datetime
from pathlib import Path

import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
from sklearn.metrics import (accuracy_score, f1_score, mean_absolute_error,
                              r2_score, roc_auc_score)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBClassifier, XGBRegressor

from config.settings import modeling_config
from pipeline.mlflow_config import get_mlflow_artifacts_dir, get_mlflow_tracking_uri
from pipeline.process_weather import (encode_categoricals, get_feature_matrix)

logger = logging.getLogger(__name__)

ROOT         = Path(__file__).parent.parent
MODELS_DIR   = ROOT / "models"
METRICS_PATH = MODELS_DIR / "metrics.json"

def _get_xgb_params(model_name: str) -> dict:
    """Return XGBoost params for a given model (base merged with per-model overrides)."""
    cfg = modeling_config.xgboost
    params = dict(cfg.base)
    params.update(cfg.models.get(model_name) or {})
    # Fallback to hardcoded defaults if config is missing
    defaults = dict(n_estimators=300, max_depth=6, learning_rate=0.05,
                    subsample=0.8, colsample_bytree=0.8, random_state=42, n_jobs=-1)
    return {**defaults, **params}


def _get_training_config() -> dict:
    return {
        "test_size": modeling_config.training.test_size,
        "random_state": modeling_config.training.random_state,
    }


def _get_experiment_name() -> str:
    return modeling_config.mlflow.experiment_name


MLFLOW_EXPERIMENT = _get_experiment_name()


# ─── MLflow setup ────────────────────────────────────────────────────────────

def _setup_mlflow():
    tracking_uri = get_mlflow_tracking_uri(ROOT)
    artifacts_dir = get_mlflow_artifacts_dir(ROOT)

    if artifacts_dir is not None:
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        artifacts_uri = artifacts_dir.resolve().as_uri()
    else:
        artifacts_uri = None

    mlflow.set_tracking_uri(tracking_uri)
    client = mlflow.MlflowClient()
    experiment = client.get_experiment_by_name(MLFLOW_EXPERIMENT)
    if experiment is None and artifacts_uri is not None:
        client.create_experiment(MLFLOW_EXPERIMENT, artifact_location=artifacts_uri)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)
    logger.info("MLflow tracking URI: %s", tracking_uri)


# ─── model persistence ───────────────────────────────────────────────────────

def _save(obj, name: str):
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    with open(MODELS_DIR / f"{name}.pkl", "wb") as f:
        pickle.dump(obj, f)
    logger.info("Saved %s", name)


def _load(name: str):
    path = MODELS_DIR / f"{name}.pkl"
    if not path.exists():
        raise FileNotFoundError(f"Model not found: {path}")
    with open(path, "rb") as f:
        return pickle.load(f)


def load_all_models() -> dict:
    return {
        "rain_tomorrow":         _load("rain_tomorrow"),
        "max_temp_tomorrow":     _load("max_temp_tomorrow"),
        "weather_type_encoder":  _load("weather_type_encoder"),
        "weather_type_tomorrow": _load("weather_type_tomorrow"),
        "heatwave_risk":         _load("heatwave_risk"),
        "frost_risk":            _load("frost_risk"),
        "storm_probability":     _load("storm_probability"),
        "cat_mappings":          _load("cat_mappings"),
    }


# ─── individual trainers ─────────────────────────────────────────────────────

def _train_binary(X_tr, y_tr, X_te, y_te, name: str,
                  scale_pos_weight: float = 1.0,
                  parent_run_id: str = None) -> dict:
    params = {**_get_xgb_params(name), "scale_pos_weight": scale_pos_weight,
              "eval_metric": "logloss", "use_label_encoder": False}
    model = XGBClassifier(**params)
    model.fit(X_tr, y_tr, eval_set=[(X_te, y_te)], verbose=False)
    _save(model, name)

    preds  = model.predict(X_te)
    probas = model.predict_proba(X_te)[:, 1]
    metrics = {
        "accuracy": round(accuracy_score(y_te, preds), 4),
        "f1":       round(f1_score(y_te, preds, zero_division=0), 4),
        "auc_roc":  round(roc_auc_score(y_te, probas), 4)
                    if len(np.unique(y_te)) > 1 else None,
        "pos_rate": round(float(y_te.mean()), 4),
    }

    try:
        with mlflow.start_run(run_name=name, nested=True, parent_run_id=parent_run_id):
            mlflow.log_params({k: v for k, v in params.items() if k != "use_label_encoder"})
            mlflow.log_metrics({k: v for k, v in metrics.items() if v is not None})
            mlflow.xgboost.log_model(model, name=name,
                                     registered_model_name=f"weather_{name}")
            _log_feature_importance(model, X_tr.columns.tolist(), name)
    except Exception as mlflow_exc:
        logger.warning("[%s] MLflow logging failed (non-fatal): %s", name, mlflow_exc)

    logger.info("[%s] acc=%.3f  f1=%.3f  auc=%.3f",
                name, metrics["accuracy"], metrics["f1"], metrics.get("auc_roc") or 0)
    return metrics


def _train_regression(X_tr, y_tr, X_te, y_te, name: str,
                      parent_run_id: str = None) -> dict:
    params = {**_get_xgb_params(name), "eval_metric": "rmse"}
    model = XGBRegressor(**params)
    model.fit(X_tr, y_tr, eval_set=[(X_te, y_te)], verbose=False)
    _save(model, name)

    preds = model.predict(X_te)
    metrics = {
        "mae":  round(mean_absolute_error(y_te, preds), 3),
        "r2":   round(r2_score(y_te, preds), 4),
        "rmse": round(float(np.sqrt(((y_te - preds) ** 2).mean())), 3),
    }

    try:
        with mlflow.start_run(run_name=name, nested=True, parent_run_id=parent_run_id):
            mlflow.log_params(params)
            mlflow.log_metrics(metrics)
            mlflow.xgboost.log_model(model, name=name,
                                     registered_model_name=f"weather_{name}")
            _log_feature_importance(model, X_tr.columns.tolist(), name)
    except Exception as mlflow_exc:
        logger.warning("[%s] MLflow logging failed (non-fatal): %s", name, mlflow_exc)

    logger.info("[%s] MAE=%.2f  R²=%.3f", name, metrics["mae"], metrics["r2"])
    return metrics


def _train_multiclass(X_tr, y_tr, X_te, y_te, le: LabelEncoder,
                      name: str, parent_run_id: str = None) -> dict:
    params = {**_get_xgb_params(name), "objective": "multi:softmax",
              "num_class": len(le.classes_), "eval_metric": "mlogloss",
              "use_label_encoder": False}
    model = XGBClassifier(**params)
    model.fit(X_tr, y_tr, eval_set=[(X_te, y_te)], verbose=False)
    _save(model, name)

    preds = model.predict(X_te)
    # softmax returns class indices as float — cast to int for sklearn metrics
    preds = np.asarray(preds, dtype=int)
    metrics = {
        "accuracy":  round(accuracy_score(y_te, preds), 4),
        "f1_macro":  round(f1_score(y_te, preds, average="macro", zero_division=0), 4),
        "f1_weighted": round(f1_score(y_te, preds, average="weighted", zero_division=0), 4),
        "n_classes": len(le.classes_),
    }

    try:
        with mlflow.start_run(run_name=name, nested=True, parent_run_id=parent_run_id):
            mlflow.log_params({k: v for k, v in params.items()
                               if k not in ("use_label_encoder", "num_class")})
            mlflow.log_params({"classes": list(le.classes_)})
            mlflow.log_metrics({k: v for k, v in metrics.items() if isinstance(v, float)})
            mlflow.xgboost.log_model(model, name=name,
                                     registered_model_name=f"weather_{name}")
            _log_feature_importance(model, X_tr.columns.tolist(), name)
    except Exception as mlflow_exc:
        logger.warning("[%s] MLflow logging failed (non-fatal): %s", name, mlflow_exc)

    logger.info("[%s] acc=%.3f  f1_macro=%.3f", name, metrics["accuracy"], metrics["f1_macro"])
    return metrics


def _log_feature_importance(model, feature_names: list, run_name: str):
    """Log top-20 feature importances as a JSON artifact."""
    try:
        importances = model.feature_importances_
        fi = sorted(zip(feature_names, importances), key=lambda x: -x[1])[:20]
        fi_dict = {k: round(float(v), 6) for k, v in fi}
        path = MODELS_DIR / f"{run_name}_feature_importance.json"
        path.write_text(json.dumps(fi_dict, indent=2))
        mlflow.log_artifact(str(path), artifact_path="feature_importance")
    except Exception:
        pass


# ─── main trainer ────────────────────────────────────────────────────────────

def train_all(df: pd.DataFrame) -> dict:
    """
    Train all 6 models, log everything to MLflow, save .pkl files to models/.
    Returns aggregate metrics dict (also written to models/metrics.json).
    """
    _setup_mlflow()

    logger.info("Encoding categoricals...")
    df_enc, cat_mappings = encode_categoricals(df)
    _save(cat_mappings, "cat_mappings")

    X = get_feature_matrix(df_enc)
    required_targets = [
        "rain_tomorrow", "max_temp_tomorrow", "weather_type_tomorrow",
        "heatwave_risk", "frost_risk", "storm_label",
    ]
    missing = [t for t in required_targets if t not in df_enc.columns]
    if missing:
        raise ValueError(f"Missing target columns: {missing}")

    mask = df_enc[required_targets].notna().all(axis=1)
    X, df_enc = X[mask], df_enc[mask]
    logger.info("Training dataset: %d rows × %d features", len(X), X.shape[1])

    run_name = f"train_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    all_metrics = {}
    tcfg = _get_training_config()
    ts, rs = tcfg["test_size"], tcfg["random_state"]

    pid = None
    try:
        parent_run_ctx = mlflow.start_run(run_name=run_name)
        parent_run = parent_run_ctx.__enter__()
        mlflow.log_params({"n_rows": len(X), "n_features": X.shape[1],
                           "n_cities": df_enc["city"].nunique() if "city" in df_enc.columns else "n/a",
                           "date_range_start": df_enc["date"].min() if "date" in df_enc.columns else "n/a",
                           "date_range_end":   df_enc["date"].max() if "date" in df_enc.columns else "n/a"})
        pid = parent_run.info.run_id
    except Exception as mlflow_exc:
        logger.warning("MLflow parent run init failed (non-fatal): %s", mlflow_exc)
        parent_run_ctx = None

    # 1. rain_tomorrow — binary
    y = df_enc["rain_tomorrow"].astype(int)
    ratio = (y == 0).sum() / max((y == 1).sum(), 1)
    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=ts, random_state=rs, stratify=y)
    all_metrics["rain_tomorrow"] = _train_binary(Xtr, ytr, Xte, yte, "rain_tomorrow",
                                                  scale_pos_weight=ratio, parent_run_id=pid)

    # 2. max_temp_tomorrow — regression
    y = df_enc["max_temp_tomorrow"].astype(float)
    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=ts, random_state=rs)
    all_metrics["max_temp_tomorrow"] = _train_regression(Xtr, ytr, Xte, yte,
                                                          "max_temp_tomorrow", parent_run_id=pid)

    # 3. weather_type_tomorrow — multi-class
    le = LabelEncoder()
    y  = le.fit_transform(df_enc["weather_type_tomorrow"].fillna("Unknown"))
    _save(le, "weather_type_encoder")
    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=ts, random_state=rs, stratify=y)
    all_metrics["weather_type_tomorrow"] = _train_multiclass(Xtr, ytr, Xte, yte, le,
                                                              "weather_type_tomorrow", parent_run_id=pid)

    # 4. heatwave_risk — binary (rare)
    y = df_enc["heatwave_risk"].astype(int)
    ratio = max((y == 0).sum() / max((y == 1).sum(), 1), 1.0)
    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=ts, random_state=rs, stratify=y)
    all_metrics["heatwave_risk"] = _train_binary(Xtr, ytr, Xte, yte, "heatwave_risk",
                                                  scale_pos_weight=ratio, parent_run_id=pid)

    # 5. frost_risk — binary (rare)
    y = df_enc["frost_risk"].astype(int)
    ratio = max((y == 0).sum() / max((y == 1).sum(), 1), 1.0)
    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=ts, random_state=rs, stratify=y)
    all_metrics["frost_risk"] = _train_binary(Xtr, ytr, Xte, yte, "frost_risk",
                                               scale_pos_weight=ratio, parent_run_id=pid)

    # 6. storm_probability — binary
    y = df_enc["storm_label"].astype(int)
    ratio = max((y == 0).sum() / max((y == 1).sum(), 1), 1.0)
    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=ts, random_state=rs, stratify=y)
    all_metrics["storm_probability"] = _train_binary(Xtr, ytr, Xte, yte, "storm_probability",
                                                      scale_pos_weight=ratio, parent_run_id=pid)

    if parent_run_ctx is not None:
        try:
            mlflow.log_metrics({
                "rain_accuracy":       all_metrics["rain_tomorrow"]["accuracy"],
                "rain_auc":            all_metrics["rain_tomorrow"].get("auc_roc") or 0,
                "temp_mae":            all_metrics["max_temp_tomorrow"]["mae"],
                "temp_r2":             all_metrics["max_temp_tomorrow"]["r2"],
                "weather_type_acc":    all_metrics["weather_type_tomorrow"]["accuracy"],
                "heatwave_auc":        all_metrics["heatwave_risk"].get("auc_roc") or 0,
                "frost_auc":           all_metrics["frost_risk"].get("auc_roc") or 0,
                "storm_auc":           all_metrics["storm_probability"].get("auc_roc") or 0,
            })
            mlflow.log_artifact(str(MODELS_DIR), artifact_path="models")
            parent_run_ctx.__exit__(None, None, None)
        except Exception as mlflow_exc:
            logger.warning("MLflow parent run finalize failed (non-fatal): %s", mlflow_exc)
            try:
                parent_run_ctx.__exit__(None, None, None)
            except Exception:
                pass

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    with open(METRICS_PATH, "w") as f:
        json.dump(all_metrics, f, indent=2)

    logger.info("All 6 models trained. MLflow run: %s. Metrics: %s", run_name, METRICS_PATH)
    return all_metrics
