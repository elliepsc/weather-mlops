from config.settings import modeling_config
from pipeline.predict import RAIN_CLASSIFICATION_THRESHOLD
from pipeline.process_weather import CATEGORICAL_FEATURES, ML_FEATURES
from pipeline.train_models import _get_experiment_name, _get_training_config, _get_xgb_params


def test_feature_lists_are_loaded_from_modeling_config():
    assert ML_FEATURES == modeling_config.features.numerical
    assert CATEGORICAL_FEATURES == modeling_config.features.categorical


def test_train_models_reads_modeling_config():
    rain_params = _get_xgb_params("rain_tomorrow")
    weather_type_params = _get_xgb_params("weather_type_tomorrow")

    assert rain_params["n_estimators"] == modeling_config.xgboost.base["n_estimators"]
    assert weather_type_params["n_estimators"] == 400
    assert _get_training_config() == {
        "test_size": modeling_config.training.test_size,
        "random_state": modeling_config.training.random_state,
    }
    assert _get_experiment_name() == modeling_config.mlflow.experiment_name


def test_predict_uses_configured_rain_threshold():
    assert RAIN_CLASSIFICATION_THRESHOLD == modeling_config.inference.rain_probability_threshold
