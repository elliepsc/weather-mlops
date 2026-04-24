# Repo Backlog

## Implemented Repo State
- `tests/` is the only test directory to use. `tests_unitaires/` is obsolete and should not be recreated.
- `config/mlops.yaml` now owns MLOps thresholds, ingestion completeness rules, cooldowns, and alert/retrain policy.
- `config/modeling.yaml` now owns XGBoost hyperparameters, feature lists, label thresholds, and inference thresholds.
- `config/settings.py` is the single typed entrypoint for `.env`, `mlops.yaml`, and `modeling.yaml`.
- Monitoring DAG was refactored away from the old `branch_on_drift` / `no_drift` / `alert_drift` split. The current logic lives in `branch_on_monitoring_decision`.
- Monitoring now supports the explicit `insufficient_data` path and a real alert path instead of an `EmptyOperator`.
- Train DAG now includes baseline validation plus rollback behavior. A degraded retrain should not silently stay in prod.
- `last_retrain.json` is written from the train side after success, not from monitoring before the retrain actually runs.
- Daily ingestion is idempotent on the target date, uses Airflow execution context instead of `date.today()` for the orchestration decision, and re-fetches only missing or incomplete cities.
- Backfill is resumable by city/date and now runs an explicit `repair_gaps` phase to scan and replay missing or incomplete rows inside a requested interval.
- CLI now supports `repair --start-date ... --end-date ...` for targeted raw-data repair without retraining.
- `weather_raw` upsert is merge-safe: a partial re-fetch should not overwrite an existing non-null value with `NULL`.
- `init_db()` is also the schema migration path for raw and prediction tables. New columns must be added there so existing local databases are upgraded safely.
- `v_weather_full` is recreated from code during `init_db()` and must stay aligned with the raw/prediction schemas.
- A small Airflow compatibility layer exists in `dags/_airflow_compat.py` so tests can import DAG modules without a full Airflow install.
- Open-Meteo raw enrichment already added in storage/export: `rain_sum`, `precipitation_hours`, `dew_point_9am`, `dew_point_3pm`, `surface_pressure_9am`, `surface_pressure_3pm`.
- These extra raw fields are not yet ML features. They are available for later feature work, backfill, and export.
- The current full local validation target is `python -m pytest tests -q` with `59 passed` and 2 known XGBoost warnings about `use_label_encoder`.

## Open-Meteo Backlog
- High-value next raw fields to consider: `cloud_cover_low`, `cloud_cover_mid`, `cloud_cover_high`, `shortwave_radiation`, `dew_point_2m` aggregates, `surface_pressure` aggregates, and possibly `rain` vs `precipitation` separation for cleaner storm heuristics.
- More advanced candidates: soil moisture / soil temperature, vapor pressure deficit, apparent temperature, wet-bulb temperature. These are potentially useful for heatwave, comfort, and rainfall regimes, but should be added only after verifying archive + recent-path availability.
- Forecast-only or model-dependent variables such as CAPE / visibility can be interesting later, but they likely require a split ingestion design instead of the current unified historical/recent path.
- A future model upgrade worth considering is to move some of the newly stored raw fields into `config/modeling.yaml` feature lists and re-train on them explicitly.

## Backfill Impact Guide
- Safe without retrain: storing a new raw column that is not used in `process_weather.py`, not part of `required_non_null_columns`, and not part of model features. This still needs schema migration and usually a `repair` or backfill if you want history populated.
- Usually needs partial backfill + retrain: adding a new feature that can be derived for recent history only, or changing target thresholds / feature lists in `config/modeling.yaml`.
- Usually needs full backfill + retrain: adding a new raw field that should exist across the full training horizon, changing the feature engineering history in a way that affects all rows, or changing labels / targets that redefine the supervised dataset.
- Usually needs broad `repair` or even backfill replay: tightening `config/mlops.yaml` `ingestion.required_non_null_columns`, because previously acceptable rows can suddenly become incomplete.
- After any change that affects stored raw columns or model features, validate in this order: schema migration -> targeted fetch test -> `repair` on a narrow range -> full test suite -> only then consider a large backfill or retrain.
