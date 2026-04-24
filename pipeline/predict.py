"""
Generate all 8 prediction columns for each row in the DataFrame.

Predictions produced:
  1. rain_tomorrow        (0/1)
  2. rain_tomorrow_proba  (0.0–1.0)
  3. max_temp_tomorrow    (°C)
  4. weather_type_tomorrow (Sunny / Cloudy / Rainy / Stormy)
  5. comfort_score        (0–100, formula-based, no model needed)
  6. heatwave_risk        (0.0–1.0 probability)
  7. frost_risk           (0.0–1.0 probability)
  8. storm_probability    (0.0–1.0 probability)
"""
import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from config.settings import modeling_config
from pipeline.process_weather import (encode_categoricals, get_feature_matrix,
                                       compute_comfort_score)
from pipeline.train_models import load_all_models

logger = logging.getLogger(__name__)
RAIN_CLASSIFICATION_THRESHOLD = modeling_config.inference.rain_probability_threshold


def generate_predictions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run all models on df (already processed with add_features).
    Returns a DataFrame with columns: date, city, + 8 prediction columns.
    """
    models = load_all_models()

    df_enc, _ = encode_categoricals(df)
    X = get_feature_matrix(df_enc)

    # 1 & 2 — rain_tomorrow
    rain_model = models["rain_tomorrow"]
    rain_proba = rain_model.predict_proba(X)[:, 1]
    rain_pred  = (rain_proba >= RAIN_CLASSIFICATION_THRESHOLD).astype(int)

    # 3 — max_temp_tomorrow
    temp_pred = models["max_temp_tomorrow"].predict(X)

    # 4 — weather_type_tomorrow
    le      = models["weather_type_encoder"]
    wt_idx  = models["weather_type_tomorrow"].predict(X)
    wt_pred = le.inverse_transform(wt_idx)

    # 5 — comfort_score (formula, no model)
    comfort = df.apply(compute_comfort_score, axis=1).values

    # 6 — heatwave_risk
    hw_proba = models["heatwave_risk"].predict_proba(X)[:, 1]

    # 7 — frost_risk
    fr_proba = models["frost_risk"].predict_proba(X)[:, 1]

    # 8 — storm_probability
    st_proba = models["storm_probability"].predict_proba(X)[:, 1]

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    result = pd.DataFrame({
        "date":                 df["date"].values,
        "city":                 df["city"].values,
        "rain_tomorrow":        rain_pred,
        "rain_tomorrow_proba":  np.round(rain_proba, 4),
        "max_temp_tomorrow":    np.round(temp_pred, 1),
        "weather_type_tomorrow": wt_pred,
        "comfort_score":        comfort,
        "heatwave_risk":        np.round(hw_proba, 4),
        "frost_risk":           np.round(fr_proba, 4),
        "storm_probability":    np.round(st_proba, 4),
        "predicted_at":         now,
    })

    logger.info("Generated %d predictions for %d cities",
                len(result), result["city"].nunique())
    return result
