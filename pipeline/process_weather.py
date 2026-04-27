"""
Feature engineering and label construction.
Adds lag features, derived columns, and ML target labels.
"""

import pandas as pd

from config.settings import modeling_config

# WMO weather code -> weather type category
_WMO_SUNNY = set(range(0, 4))  # 0-3: clear/partly cloudy
_WMO_CLOUDY = {45, 48} | set(range(10, 20))  # fog, mist
_WMO_RAINY = set(range(51, 68)) | set(range(80, 87))  # drizzle, rain, showers
_WMO_STORMY = set(range(95, 100))  # thunderstorm

LABELS = modeling_config.labels
ML_FEATURES = list(modeling_config.features.numerical)
CATEGORICAL_FEATURES = list(modeling_config.features.categorical)


def wmo_to_weather_type(code) -> str:
    if pd.isna(code):
        return "Unknown"
    code = int(code)
    if code in _WMO_STORMY:
        return "Stormy"
    if code in _WMO_RAINY:
        return "Rainy"
    if code in _WMO_CLOUDY:
        return "Cloudy"
    return "Sunny"  # covers codes 0-3 and anything else


def compute_comfort_score(row) -> float:
    """
    Composite comfort score 0-100.
    Ideal: temp 18-24C, humidity 40-60%, wind < 20 km/h, sunny.
    """
    score = 100.0

    temp = row.get("max_temp")
    if temp is not None and not pd.isna(temp):
        if temp < 10:
            score -= (10 - temp) * 3.5
        elif temp > 38:
            score -= (temp - 38) * 4.0
        elif temp > 30:
            score -= (temp - 30) * 1.5

    humidity = row.get("humidity_3pm")
    if humidity is not None and not pd.isna(humidity):
        if humidity > 85:
            score -= (humidity - 85) * 0.6
        elif humidity < 25:
            score -= (25 - humidity) * 0.4

    wind = row.get("wind_gust_speed")
    if wind is not None and not pd.isna(wind):
        if wind > 50:
            score -= (wind - 50) * 0.5
        elif wind > 25:
            score -= (wind - 25) * 0.3

    if row.get("rain_today") == 1:
        score -= 15

    sunshine = row.get("sunshine_hours")
    if sunshine is not None and not pd.isna(sunshine):
        if sunshine > 7:
            score += min((sunshine - 7) * 2, 10)

    return round(max(0.0, min(100.0, score)), 1)


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add all derived features needed for ML training and prediction.
    Input df must be sorted by (city, date).
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["city", "date"]).reset_index(drop=True)

    # Calendar features
    df["month"] = df["date"].dt.month
    df["day_of_year"] = df["date"].dt.dayofyear
    df["season"] = df["month"].map(
        {
            12: "Summer",
            1: "Summer",
            2: "Summer",
            3: "Autumn",
            4: "Autumn",
            5: "Autumn",
            6: "Winter",
            7: "Winter",
            8: "Winter",
            9: "Spring",
            10: "Spring",
            11: "Spring",
        }
    )

    # Lag features (per city)
    for col in ["max_temp", "min_temp", "rainfall", "pressure_3pm"]:
        df[f"{col}_lag1"] = df.groupby("city")[col].shift(1)
        df[f"{col}_lag2"] = df.groupby("city")[col].shift(2)

    # Rolling averages (7-day window per city)
    for col in ["max_temp", "rainfall", "humidity_3pm"]:
        df[f"{col}_rolling7"] = df.groupby("city")[col].transform(
            lambda x: x.shift(1).rolling(7, min_periods=3).mean()
        )

    # Pressure tendency (drop -> storm risk)
    df["pressure_tendency"] = df["pressure_3pm"] - df["pressure_3pm_lag1"]

    # Temperature anomaly vs rolling mean
    df["temp_anomaly"] = df["max_temp"] - df["max_temp_rolling7"]

    # Consecutive hot days (for heatwave detection)
    df["hot_day"] = (df["max_temp"] >= LABELS.heatwave_temp_celsius).astype(int)
    df["hot_day_lag1"] = df.groupby("city")["hot_day"].shift(1).fillna(0).astype(int)
    df["hot_day_lag2"] = df.groupby("city")["hot_day"].shift(2).fillna(0).astype(int)
    df["consec_hot_days"] = df["hot_day"] + df["hot_day_lag1"] + df["hot_day_lag2"]

    # Rainfall intensity: mm/h — distinguishes storm (10mm/1h) from drizzle (10mm/8h)
    df["rainfall_intensity"] = (
        df["rainfall"] / df["precipitation_hours"].replace(0, float("nan"))
    ).fillna(0.0)

    # Comfort score (formula, no ML)
    df["comfort_score"] = df.apply(compute_comfort_score, axis=1)

    # Weather type label for today (used as feature)
    df["weather_type"] = df["weather_code"].apply(wmo_to_weather_type)

    # ---- TARGET LABELS (shifted by -1 = tomorrow) ----
    grp = df.groupby("city")

    # rain_tomorrow: next day precipitation above configured wet-day threshold
    df["rain_tomorrow"] = (grp["rainfall"].shift(-1).fillna(0) > LABELS.rain_min_mm).astype(int)

    # max_temp_tomorrow: regression target
    df["max_temp_tomorrow"] = grp["max_temp"].shift(-1)

    # weather_type_tomorrow: multi-class target
    df["weather_type_tomorrow"] = grp["weather_code"].shift(-1).apply(wmo_to_weather_type)

    # heatwave_risk: 1 if tomorrow is hot AND today is already hot
    next_hot = (grp["max_temp"].shift(-1).fillna(0) >= LABELS.heatwave_temp_celsius).astype(int)
    df["heatwave_risk"] = ((next_hot == 1) & (df["consec_hot_days"] >= 1)).astype(int)

    # frost_risk: next day min_temp below configured frost threshold
    df["frost_risk"] = (grp["min_temp"].shift(-1).fillna(99) <= LABELS.frost_temp_celsius).astype(
        int
    )

    # storm_probability: next day has heavy rain and strong gusts
    next_rain = grp["rainfall"].shift(-1).fillna(0)
    next_gust = grp["wind_gust_speed"].shift(-1).fillna(0)
    df["storm_label"] = (
        (next_rain > LABELS.storm_rainfall_mm) & (next_gust > LABELS.storm_gust_kmh)
    ).astype(int)

    # Drop last row per city (no tomorrow available)
    last_dates = df.groupby("city")["date"].transform("max")
    df = df[df["date"] < last_dates].copy()

    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    return df


def encode_categoricals(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Label-encode categorical features. Returns (encoded_df, mapping_dict)."""
    df = df.copy()
    mappings = {}
    for col in CATEGORICAL_FEATURES:
        if col in df.columns:
            cats = sorted(df[col].dropna().unique().tolist())
            mapping = {value: index for index, value in enumerate(cats)}
            mapping[None] = -1
            df[col] = df[col].map(mapping).fillna(-1).astype(int)
            mappings[col] = mapping
    return df, mappings


def get_feature_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Return only the ML feature columns, filling NaN with column median."""
    all_features = ML_FEATURES + [column for column in CATEGORICAL_FEATURES if column in df.columns]
    X = df[[column for column in all_features if column in df.columns]].copy()
    X = X.fillna(X.median(numeric_only=True))
    return X
