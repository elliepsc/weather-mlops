"""Unit tests for pipeline/process_weather.py."""
import numpy as np
import pandas as pd
import pytest

from pipeline.process_weather import (
    CATEGORICAL_FEATURES,
    ML_FEATURES,
    add_features,
    compute_comfort_score,
    encode_categoricals,
    get_feature_matrix,
    wmo_to_weather_type,
)

# ─── wmo_to_weather_type ─────────────────────────────────────────────────────

@pytest.mark.parametrize("code,expected", [
    (0,  "Sunny"),
    (2,  "Sunny"),
    (45, "Cloudy"),
    (51, "Rainy"),
    (80, "Rainy"),
    (95, "Stormy"),
    (99, "Stormy"),
    (None, "Unknown"),
])
def test_wmo_to_weather_type(code, expected):
    assert wmo_to_weather_type(code) == expected


# ─── compute_comfort_score ───────────────────────────────────────────────────

def test_comfort_score_ideal():
    row = {"max_temp": 22, "humidity_3pm": 50, "wind_gust_speed": 10,
           "rain_today": 0, "sunshine_hours": 8}
    score = compute_comfort_score(row)
    assert 90 < score <= 100


def test_comfort_score_extreme_heat():
    row = {"max_temp": 45, "humidity_3pm": 90, "wind_gust_speed": 60,
           "rain_today": 1, "sunshine_hours": 0}
    score = compute_comfort_score(row)
    assert score < 50


def test_comfort_score_clamped():
    row = {"max_temp": -10, "humidity_3pm": 100, "wind_gust_speed": 100,
           "rain_today": 1, "sunshine_hours": 0}
    score = compute_comfort_score(row)
    assert score == 0.0


def test_comfort_score_missing_fields():
    row = {}
    score = compute_comfort_score(row)
    assert 0.0 <= score <= 100.0


# ─── add_features ────────────────────────────────────────────────────────────

@pytest.fixture
def raw_df():
    dates = pd.date_range("2023-01-01", periods=10, freq="D")
    return pd.DataFrame({
        "date":           dates.strftime("%Y-%m-%d"),
        "city":           ["Sydney"] * 10,
        "state":          ["NSW"] * 10,
        "max_temp":       [25, 26, 28, 38, 39, 24, 20, 18, 15, 30],
        "min_temp":       [18, 19, 20, 25, 26, 16, 12,  8,  1, 20],
        "rainfall":       [ 0,  0,  5,  0,  0,  2,  0,  0,  0,  1],
        "evaporation":    [ 5,  5,  4,  7,  8,  5,  4,  3,  3,  6],
        "sunshine_hours": [ 8,  9,  6, 10, 10,  7,  5,  4,  4,  8],
        "wind_gust_speed":[ 20, 25, 30, 40, 45, 20, 15, 12, 10, 25],
        "wind_speed_9am": [ 10, 12, 15, 20, 22, 10,  8,  7,  6, 12],
        "wind_speed_3pm": [ 15, 18, 20, 30, 32, 15, 10,  9,  8, 18],
        "wind_gust_dir":  ["N"] * 10,
        "wind_dir_9am":   ["NE"] * 10,
        "wind_dir_3pm":   ["NW"] * 10,
        "humidity_9am":   [60, 62, 70, 50, 48, 65, 70, 75, 80, 58],
        "humidity_3pm":   [45, 48, 55, 30, 28, 50, 60, 65, 70, 42],
        "pressure_9am":   [1015]*10,
        "pressure_3pm":   [1012, 1010, 1008, 1010, 1012, 1013, 1015, 1016, 1017, 1011],
        "cloud_9am":      [2, 2, 4, 1, 1, 3, 5, 6, 7, 2],
        "cloud_3pm":      [3, 3, 5, 2, 2, 4, 6, 7, 8, 3],
        "temp_9am":       [20, 21, 22, 30, 31, 19, 16, 13, 10, 23],
        "temp_3pm":       [24, 25, 27, 36, 37, 22, 18, 16, 13, 28],
        "rain_today":     [0, 0, 1, 0, 0, 1, 0, 0, 0, 1],
        "weather_code":   [0, 0, 61, 0, 0, 80, 45, 45, 45, 95],
        "precipitation_hours": [0, 0, 3, 0, 0, 2, 0, 0, 0, 1],
        "shortwave_radiation_sum": [200, 210, 150, 220, 230, 180, 100, 80, 60, 200],
        "vpd_9am":        [0.5, 0.6, 0.4, 1.2, 1.3, 0.5, 0.3, 0.2, 0.1, 0.7],
        "vpd_3pm":        [1.0, 1.1, 0.8, 2.5, 2.6, 1.0, 0.6, 0.4, 0.2, 1.2],
        "wind_speed_100m_9am": [12, 14, 18, 25, 28, 12, 10, 8, 7, 15],
        "wind_speed_100m_3pm": [18, 22, 25, 38, 40, 18, 12, 10, 9, 22],
    })


def test_add_features_returns_dataframe(raw_df):
    result = add_features(raw_df)
    assert isinstance(result, pd.DataFrame)


def test_add_features_drops_last_row(raw_df):
    result = add_features(raw_df)
    assert len(result) == len(raw_df) - 1


def test_add_features_target_columns(raw_df):
    result = add_features(raw_df)
    for col in ["rain_tomorrow", "max_temp_tomorrow", "weather_type_tomorrow",
                "heatwave_risk", "frost_risk", "storm_label"]:
        assert col in result.columns, f"Missing target column: {col}"


def test_add_features_calendar_cols(raw_df):
    result = add_features(raw_df)
    assert "month" in result.columns
    assert "day_of_year" in result.columns
    assert "season" in result.columns


def test_add_features_lag_cols(raw_df):
    result = add_features(raw_df)
    assert "max_temp_lag1" in result.columns
    assert "rainfall_lag2" in result.columns


def test_add_features_comfort_score_range(raw_df):
    result = add_features(raw_df)
    assert result["comfort_score"].between(0, 100).all()


def test_add_features_frost_risk(raw_df):
    result = add_features(raw_df)
    # Row with min_temp=1 on day 8 → frost_risk on day 8's row (predicts day 9)
    assert result["frost_risk"].sum() >= 1


def test_add_features_rainfall_intensity(raw_df):
    result = add_features(raw_df)
    assert "rainfall_intensity" in result.columns
    assert (result["rainfall_intensity"] >= 0).all()
    # Day 2 has rainfall=5, precipitation_hours=3 → intensity ≈ 1.67 mm/h
    day2 = result[result["date"] == "2023-01-03"]
    assert not day2.empty
    assert day2.iloc[0]["rainfall_intensity"] == pytest.approx(5 / 3, rel=1e-3)


# ─── encode_categoricals ─────────────────────────────────────────────────────

def test_encode_categoricals(raw_df):
    df_enc, mappings = encode_categoricals(raw_df)
    for col in CATEGORICAL_FEATURES:
        if col in df_enc.columns:
            assert df_enc[col].dtype in (np.int64, np.int32, int)
    assert "city" in mappings


# ─── get_feature_matrix ──────────────────────────────────────────────────────

def test_get_feature_matrix_no_nans(raw_df):
    df_feat = add_features(raw_df)
    df_enc, _ = encode_categoricals(df_feat)
    X = get_feature_matrix(df_enc)
    assert not X.isnull().any().any(), "Feature matrix contains NaN values"


def test_get_feature_matrix_columns(raw_df):
    df_feat = add_features(raw_df)
    df_enc, _ = encode_categoricals(df_feat)
    X = get_feature_matrix(df_enc)
    for col in ML_FEATURES:
        if col in df_enc.columns:
            assert col in X.columns
