"""Unit tests for pipeline/train_models.py."""
import pickle
import tempfile
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBClassifier, XGBRegressor

from pipeline.process_weather import add_features, encode_categoricals, get_feature_matrix
from pipeline.train_models import _train_binary, _train_regression, _train_multiclass, MODELS_DIR


# ─── helpers ─────────────────────────────────────────────────────────────────

@pytest.fixture
def tiny_df():
    """Minimal synthetic dataset with all required columns for training."""
    n = 100
    rng = np.random.default_rng(0)
    dates = pd.date_range("2022-01-01", periods=n, freq="D")
    df = pd.DataFrame({
        "date":           dates.strftime("%Y-%m-%d"),
        "city":           ["Sydney"] * 50 + ["Melbourne"] * 50,
        "state":          ["NSW"] * 50 + ["VIC"] * 50,
        "max_temp":       rng.uniform(10, 40, n),
        "min_temp":       rng.uniform(0, 20, n),
        "rainfall":       rng.exponential(2, n),
        "evaporation":    rng.uniform(2, 8, n),
        "sunshine_hours": rng.uniform(3, 12, n),
        "wind_gust_speed":rng.uniform(10, 60, n),
        "wind_speed_9am": rng.uniform(5, 30, n),
        "wind_speed_3pm": rng.uniform(5, 30, n),
        "wind_gust_dir":  rng.choice(["N", "S", "E", "W"], n),
        "wind_dir_9am":   rng.choice(["N", "NE", "E"], n),
        "wind_dir_3pm":   rng.choice(["NW", "W", "SW"], n),
        "humidity_9am":   rng.uniform(30, 90, n),
        "humidity_3pm":   rng.uniform(20, 80, n),
        "pressure_9am":   rng.uniform(1005, 1025, n),
        "pressure_3pm":   rng.uniform(1000, 1020, n),
        "cloud_9am":      rng.integers(0, 8, n),
        "cloud_3pm":      rng.integers(0, 8, n),
        "temp_9am":       rng.uniform(8, 30, n),
        "temp_3pm":       rng.uniform(15, 38, n),
        "rain_today":     rng.integers(0, 2, n),
        "weather_code":   rng.choice([0, 2, 51, 80, 95], n),
    })
    return add_features(df)


@pytest.fixture
def xy(tiny_df):
    df_enc, _ = encode_categoricals(tiny_df)
    X = get_feature_matrix(df_enc)
    split = int(len(X) * 0.8)
    return X.iloc[:split], X.iloc[split:], df_enc.iloc[:split], df_enc.iloc[split:]


# ─── _train_binary ───────────────────────────────────────────────────────────

def test_train_binary_returns_metrics(xy):
    X_tr, X_te, df_tr, df_te = xy
    y_tr = df_tr["rain_tomorrow"].astype(int)
    y_te = df_te["rain_tomorrow"].astype(int)

    with patch("pipeline.train_models.mlflow"):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("pipeline.train_models.MODELS_DIR", Path(tmpdir)):
                metrics = _train_binary(X_tr, y_tr, X_te, y_te, "rain_tomorrow_test")

    assert "accuracy" in metrics
    assert "f1" in metrics
    assert 0.0 <= metrics["accuracy"] <= 1.0


# ─── _train_regression ───────────────────────────────────────────────────────

def test_train_regression_returns_metrics(xy):
    X_tr, X_te, df_tr, df_te = xy
    y_tr = df_tr["max_temp_tomorrow"].astype(float)
    y_te = df_te["max_temp_tomorrow"].astype(float)

    with patch("pipeline.train_models.mlflow"):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("pipeline.train_models.MODELS_DIR", Path(tmpdir)):
                metrics = _train_regression(X_tr, y_tr, X_te, y_te, "temp_test")

    assert "mae" in metrics
    assert "r2" in metrics
    assert metrics["mae"] >= 0.0


# ─── _train_multiclass ───────────────────────────────────────────────────────

def test_train_multiclass_returns_metrics(xy):
    X_tr, X_te, df_tr, df_te = xy
    le = LabelEncoder()
    y_all = pd.concat([df_tr["weather_type_tomorrow"], df_te["weather_type_tomorrow"]])
    le.fit(y_all.fillna("Unknown"))
    y_tr = le.transform(df_tr["weather_type_tomorrow"].fillna("Unknown"))
    y_te = le.transform(df_te["weather_type_tomorrow"].fillna("Unknown"))

    with patch("pipeline.train_models.mlflow"):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("pipeline.train_models.MODELS_DIR", Path(tmpdir)):
                metrics = _train_multiclass(X_tr, y_tr, X_te, y_te, le, "weather_type_test")

    assert "accuracy" in metrics
    assert "f1_macro" in metrics
    assert metrics["n_classes"] == len(le.classes_)


# ─── model save / load ───────────────────────────────────────────────────────

def test_save_and_load_model():
    from pipeline.train_models import _save, _load
    model = XGBClassifier(n_estimators=2)

    with tempfile.TemporaryDirectory() as tmpdir:
        with patch("pipeline.train_models.MODELS_DIR", Path(tmpdir)):
            _save(model, "test_model")
            loaded = _load("test_model")

    assert isinstance(loaded, XGBClassifier)


def test_load_missing_model_raises():
    from pipeline.train_models import _load
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch("pipeline.train_models.MODELS_DIR", Path(tmpdir)):
            with pytest.raises(FileNotFoundError):
                _load("nonexistent_model")
