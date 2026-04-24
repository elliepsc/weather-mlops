# Claude Code Guide

## Project Map
- `dags/`: Airflow orchestration.
- `pipeline/`: pure Python pipeline logic.
- `config/mlops.yaml`: versioned MLOps thresholds and policy.
- `config/modeling.yaml`: versioned model hyperparameters, feature lists, and label thresholds.
- `config/settings.py`: typed loader for `.env`, `mlops.yaml`, and `modeling.yaml`.
- `tests/`: single source of truth for unit and contract tests in this repo.

## Non-Negotiable Rules
- Read the existing DAG or pipeline file before editing it.
- Do not hardcode MLOps thresholds in DAGs. Read them from `mlops_config`.
- Do not use `date.today()` inside daily Airflow checks when `context["ds"]` is available.
- Do not write `last_retrain.json` from the monitoring DAG.
- Do not ship a no-op branch named `alert_*`; alerts must at least log explicitly and, when configured, hit Slack.
- Do not replace a whole DAG with generated code unless the existing file is clearly unusable.
- Do not add an Open-Meteo variable only in `DAILY_VARS` / `HOURLY_VARS`; schema, upsert, view, and tests must move together.

## Current MLOps Design
- Monitoring is 4-way: `trigger_retrain`, `alert_only`, `alert_insufficient_data`, `no_action`.
- `insufficient_data` is a real state in `monitoring_decision.json`, even if the task routed to is `alert_insufficient_data`.
- Cooldown is checked in monitoring, but persisted only at the end of a successful train DAG run.
- Retraining is gated by `compare_vs_baseline` in `train_dag.py`.
- If the newly trained model degrades beyond tolerance, the train DAG restores `models/baseline/` into `models/` and alerts.
- Current repo snapshot and longer-term backlog live in `docs/backlog.md`.

## Known Pitfalls
- `weather_predictions.max_temp_tomorrow` must be compared with next-day actual `weather_raw.max_temp`, not same-day.
- The rain metric also uses next-day alignment via `shift(-1)`.
- Drift detection uses seasonal features. `alert_only` on mild drift is expected and not sufficient on its own to justify a retrain.
- `config/settings.py` has a fallback path when `pydantic-settings` is missing, so tests can import DAG modules without a full Airflow env.
- `init_db()` is currently the schema migration path for `weather_raw`; when adding new raw columns, update both the base `CREATE TABLE`, the migration helper, and `v_weather_full`.

## Open-Meteo Extension Rules
- Current enriched raw fields beyond the original baseline are: `rain_sum`, `precipitation_hours`, `dew_point_9am`, `dew_point_3pm`, `surface_pressure_9am`, `surface_pressure_3pm`.
- These fields are stored in SQLite and exported through `v_weather_full`, but they are not yet model features or completeness gates.
- If you add another Open-Meteo field, you must update at least:
  1. `pipeline/fetch_weather.py`
  2. `pipeline/database.py` schema, migration, and view
  3. any relevant tests under `tests/`
- Prefer fields that exist on both the historical archive API and the daily recent path. Avoid forecast-only variables unless the ingestion path is split explicitly.

## Useful Later Work
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

## Expected Workflow
1. Read the relevant config file (`config/mlops.yaml` for ops, `config/modeling.yaml` for ML) and the target file before coding.
2. Make the smallest coherent change set.
3. Add or update tests when branching logic or rollback behavior changes.
4. Run the narrowest relevant test first.
5. If DAG behavior changed, run the broader unit suite before finishing.

## Validation Commands
- `python -m pytest tests/test_monitoring_branch.py -q`
- `python -m pytest tests/test_ingestion_backfill_flow.py -q`
- `python -m pytest tests/test_train_dag.py -q`
- `python -m pytest tests -q`

## Preferred Prompt Shape For Claude Code
- Scope one task at a time.
- Name the exact files Claude may edit.
- State the expected diff size.
- Require pytest output, not a paraphrase.

Example:

```text
Read CLAUDE.md, then read @dags/train_dag.py and @tests/test_train_dag.py.
Implement only the baseline-validation gate change.
Do not touch any other file.
After editing, run:
python -m pytest tests/test_train_dag.py -q
Return the diff summary and raw pytest output.
```
