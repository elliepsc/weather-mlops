"""
Main pipeline orchestrator.
Can be called directly (CLI) or imported by Airflow DAGs.

Steps:
  backfill  - fetch historical data + init DB + train models + predict
  daily     - fetch missing data for a target day + update predictions
  train     - retrain all models on full DB + regenerate all predictions
  predict   - regenerate predictions without retraining
  export    - export DB view to CSV for Power BI fallback
"""

import argparse
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import mlops_config
from pipeline.database import (
    find_weather_raw_gaps,
    get_existing_cities_for_date,
    get_latest_date,
    init_db,
    read_all,
    read_raw,
    upsert_predictions,
    upsert_weather_raw,
)
from pipeline.fetch_weather import fetch_city
from pipeline.locations import LOCATIONS
from pipeline.predict import generate_predictions
from pipeline.process_weather import add_features

(ROOT / "logs").mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stderr),
        logging.FileHandler(ROOT / "logs" / "pipeline.log", mode="a"),
    ],
)
logger = logging.getLogger("run_pipeline")

OUTPUT_CSV = ROOT / "data" / "output" / "weather_final.csv"
RAW_COMPLETENESS_COLUMNS = mlops_config.ingestion.required_non_null_columns


def step_init_db():
    logger.info("Initialising database schema...")
    (ROOT / "logs").mkdir(parents=True, exist_ok=True)
    init_db()


def _group_consecutive_dates(dates: list[str]) -> list[tuple[str, str]]:
    """Group sorted ISO dates into inclusive contiguous ranges."""
    if not dates:
        return []

    ordered = sorted(date.fromisoformat(day) for day in dates)
    ranges: list[tuple[str, str]] = []
    start = ordered[0]
    previous = ordered[0]

    for current in ordered[1:]:
        if current == previous + timedelta(days=1):
            previous = current
            continue
        ranges.append((start.isoformat(), previous.isoformat()))
        start = current
        previous = current

    ranges.append((start.isoformat(), previous.isoformat()))
    return ranges


def backfill_date_range(
    start_date: str,
    end_date: str,
    *,
    force: bool = False,
    delay_seconds: float = 10.0,
) -> dict:
    """Fetch a historical range incrementally, resuming per city when possible."""
    request_start = date.fromisoformat(start_date)
    request_end = date.fromisoformat(end_date)
    cities = list(LOCATIONS.keys())

    logger.info(
        "Backfill %s -> %s (%d cities, via Open-Meteo%s)",
        start_date,
        end_date,
        len(cities),
        ", force re-fetch" if force else "",
    )

    stored_total = 0
    fetched_cities: list[str] = []
    skipped_cities: list[str] = []
    failed_cities: dict[str, str] = {}

    for city in cities:
        city_start = request_start
        if not force:
            latest = get_latest_date(city, required_columns=RAW_COMPLETENESS_COLUMNS)
            if latest:
                latest_date = date.fromisoformat(latest)
                if latest_date >= request_end:
                    logger.info(
                        "Skip %s - already up to date with complete rows for requested range (%s)",
                        city,
                        latest,
                    )
                    skipped_cities.append(city)
                    continue
                city_start = max(request_start, latest_date + timedelta(days=1))

        logger.info("Fetching %s  (%s -> %s)", city, city_start.isoformat(), end_date)
        try:
            df = fetch_city(city, city_start.isoformat(), end_date)
            if df.empty:
                failed_cities[city] = (
                    f"No data returned for {city} ({city_start.isoformat()} -> {end_date})"
                )
                logger.warning("No data returned for %s on requested range", city)
            else:
                upsert_weather_raw(df)
                stored_total += len(df)
                fetched_cities.append(city)
                logger.info("Stored %d rows for %s", len(df), city)
        except Exception as exc:
            failed_cities[city] = str(exc)
            logger.error("Failed %s: %s - continuing", city, exc)

        if delay_seconds:
            time.sleep(delay_seconds)

    summary = {
        "start_date": start_date,
        "end_date": end_date,
        "stored_rows": stored_total,
        "fetched_cities": fetched_cities,
        "skipped_cities": skipped_cities,
        "failed_cities": failed_cities,
    }
    logger.info(
        "Backfill complete: stored=%d rows | fetched=%d | skipped=%d | failed=%d",
        stored_total,
        len(fetched_cities),
        len(skipped_cities),
        len(failed_cities),
    )
    return summary


def repair_gaps_in_range(
    start_date: str,
    end_date: str,
    *,
    delay_seconds: float = 10.0,
) -> dict:
    """
    Scan for missing or incomplete raw rows within an interval and replay only those gaps.
    """
    cities = list(LOCATIONS.keys())
    initial_gaps = find_weather_raw_gaps(
        start_date,
        end_date,
        expected_cities=cities,
        required_columns=RAW_COMPLETENESS_COLUMNS,
    )
    initial_gap_counts = {city: len(days) for city, days in initial_gaps.items()}

    if not initial_gaps:
        logger.info("No raw data gaps detected for %s -> %s", start_date, end_date)
        return {
            "start_date": start_date,
            "end_date": end_date,
            "status": "no_gaps",
            "stored_rows": 0,
            "initial_gap_counts": {},
            "repaired_ranges": [],
            "failed_ranges": {},
            "remaining_gap_counts": {},
        }

    stored_total = 0
    repaired_ranges: list[dict[str, int | str]] = []
    failed_ranges: dict[str, str] = {}

    logger.info(
        "Repairing raw gaps for %s -> %s (%d cities with gaps, %d missing/incomplete days)",
        start_date,
        end_date,
        len(initial_gaps),
        sum(initial_gap_counts.values()),
    )

    for city, gap_dates in initial_gaps.items():
        for range_start, range_end in _group_consecutive_dates(gap_dates):
            range_label = f"{city}:{range_start}->{range_end}"
            logger.info("Repair fetch %s", range_label)
            try:
                df = fetch_city(city, range_start, range_end)
                if df.empty:
                    failed_ranges[range_label] = (
                        f"No data returned for {city} ({range_start} -> {range_end})"
                    )
                    logger.warning("Repair fetch returned no rows for %s", range_label)
                else:
                    upsert_weather_raw(df)
                    stored_total += len(df)
                    repaired_ranges.append(
                        {
                            "city": city,
                            "start_date": range_start,
                            "end_date": range_end,
                            "gap_days": len(pd.date_range(range_start, range_end, freq="D")),
                        }
                    )
                    logger.info("Repair stored %d rows for %s", len(df), range_label)
            except Exception as exc:
                failed_ranges[range_label] = str(exc)
                logger.error("Repair failed for %s: %s", range_label, exc)

            if delay_seconds:
                time.sleep(delay_seconds)

    remaining_gaps = find_weather_raw_gaps(
        start_date,
        end_date,
        expected_cities=cities,
        required_columns=RAW_COMPLETENESS_COLUMNS,
    )
    remaining_gap_counts = {city: len(days) for city, days in remaining_gaps.items()}

    logger.info(
        "Gap repair complete: stored=%d rows | repaired_ranges=%d | failed_ranges=%d | remaining_gap_days=%d",
        stored_total,
        len(repaired_ranges),
        len(failed_ranges),
        sum(remaining_gap_counts.values()),
    )
    return {
        "start_date": start_date,
        "end_date": end_date,
        "status": "repaired" if repaired_ranges else "checked",
        "stored_rows": stored_total,
        "initial_gap_counts": initial_gap_counts,
        "repaired_ranges": repaired_ranges,
        "failed_ranges": failed_ranges,
        "remaining_gap_counts": remaining_gap_counts,
    }


def backfill_and_repair_date_range(
    start_date: str,
    end_date: str,
    *,
    force: bool = False,
    repair_gaps: bool = True,
    delay_seconds: float = 10.0,
) -> dict:
    """Run backfill resume logic, then repair internal gaps explicitly."""
    backfill_summary = backfill_date_range(
        start_date,
        end_date,
        force=force,
        delay_seconds=delay_seconds,
    )
    _skip_repair = not repair_gaps or (not force and backfill_summary["stored_rows"] == 0)
    if _skip_repair:
        reason = "repair_gaps=False" if not repair_gaps else "no new rows stored"
        logger.info("Gap repair skipped (%s).", reason)
        repair_summary = {
            "status": "skipped",
            "stored_rows": 0,
            "initial_gap_counts": {},
            "repaired_ranges": [],
            "failed_ranges": {},
            "remaining_gap_counts": {},
        }
    else:
        repair_summary = repair_gaps_in_range(
            start_date,
            end_date,
            delay_seconds=delay_seconds,
        )
    return {
        "start_date": start_date,
        "end_date": end_date,
        "stored_rows": backfill_summary["stored_rows"] + repair_summary["stored_rows"],
        "backfill": backfill_summary,
        "repair": repair_summary,
        "failed_cities": backfill_summary["failed_cities"],
        "failed_ranges": repair_summary["failed_ranges"],
        "remaining_gap_counts": repair_summary["remaining_gap_counts"],
    }


def step_ingest_backfill(force: bool = False):
    backfill_start = "2008-01-01"
    end = (date.today() - timedelta(days=1)).isoformat()
    summary = backfill_and_repair_date_range(
        backfill_start,
        end,
        force=force,
        delay_seconds=10.0,
    )
    if summary["failed_cities"] or summary["failed_ranges"] or summary["remaining_gap_counts"]:
        failed_cities = ", ".join(sorted(summary["failed_cities"])) or "none"
        failed_ranges = ", ".join(sorted(summary["failed_ranges"])) or "none"
        remaining = (
            ", ".join(
                f"{city}:{count}" for city, count in sorted(summary["remaining_gap_counts"].items())
            )
            or "none"
        )
        raise RuntimeError(
            "Backfill incomplete - "
            f"failed cities: {failed_cities}; "
            f"failed repair ranges: {failed_ranges}; "
            f"remaining gap days: {remaining}. "
            "Rerun the command to resume from persisted progress."
        )
    return summary


def step_ingest_daily(target_date: str | None = None, *, delay_seconds: float = 0.3) -> dict:
    """Fetch only missing cities for a target day and skip the API when complete."""
    target_date = target_date or (date.today() - timedelta(days=1)).isoformat()
    expected_cities = list(LOCATIONS.keys())
    existing_cities = get_existing_cities_for_date(target_date)
    complete_cities = get_existing_cities_for_date(
        target_date,
        required_columns=RAW_COMPLETENESS_COLUMNS,
    )
    incomplete_cities = sorted(existing_cities - complete_cities)
    missing_cities = [city for city in expected_cities if city not in complete_cities]

    if not missing_cities:
        logger.info(
            "Daily ingestion skipped: %d/%d cities already complete for %s",
            len(complete_cities),
            len(expected_cities),
            target_date,
        )
        return {
            "target_date": target_date,
            "status": "skipped",
            "stored_rows": 0,
            "fetched_cities": [],
            "existing_cities": sorted(existing_cities),
            "complete_cities": sorted(complete_cities),
            "incomplete_cities": incomplete_cities,
            "failed_cities": {},
        }

    logger.info(
        "Fetching daily update for %s (%d/%d cities incomplete or missing)...",
        target_date,
        len(missing_cities),
        len(expected_cities),
    )

    stored_total = 0
    fetched_cities: list[str] = []
    failed_cities: dict[str, str] = {}
    for city in missing_cities:
        logger.info("Fetching %s  (%s -> %s)", city, target_date, target_date)
        try:
            df = fetch_city(city, target_date, target_date)
            if df.empty:
                failed_cities[city] = f"No data returned for {city} on {target_date}"
                logger.warning("No data returned for %s on %s", city, target_date)
            else:
                upsert_weather_raw(df)
                stored_total += len(df)
                fetched_cities.append(city)
                logger.info("Stored %d rows for %s", len(df), city)
        except Exception as exc:
            failed_cities[city] = str(exc)
            logger.error("Failed %s: %s - continuing", city, exc)

        if delay_seconds:
            time.sleep(delay_seconds)

    summary = {
        "target_date": target_date,
        "status": "fetched",
        "stored_rows": stored_total,
        "fetched_cities": fetched_cities,
        "existing_cities": sorted(existing_cities),
        "complete_cities": sorted(complete_cities),
        "incomplete_cities": incomplete_cities,
        "failed_cities": failed_cities,
    }
    logger.info(
        "Daily ingestion complete for %s: stored=%d rows | fetched=%d | complete=%d | incomplete=%d | failed=%d",
        target_date,
        stored_total,
        len(fetched_cities),
        len(complete_cities),
        len(incomplete_cities),
        len(failed_cities),
    )
    return summary


def step_train():
    from pipeline.train_models import train_all

    logger.info("Loading raw data for training...")
    df_raw = read_raw()
    if len(df_raw) < 500:
        raise RuntimeError(f"Not enough data to train ({len(df_raw)} rows). Run backfill first.")
    df_feat = add_features(df_raw)
    logger.info("Training all 6 XGBoost models on %d rows...", len(df_feat))
    metrics = train_all(df_feat)
    logger.info("Training complete. Metrics: %s", metrics)
    return metrics


def step_predict():
    logger.info("Generating predictions for all rows...")
    df_raw = read_raw()
    df_feat = add_features(df_raw)
    preds = generate_predictions(df_feat)
    upsert_predictions(preds)
    logger.info("Predictions stored: %d rows", len(preds))


def step_export():
    logger.info("Exporting final dataset to CSV...")
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df = read_all("v_weather_full")
    df.to_csv(OUTPUT_CSV, index=False)
    logger.info("Exported %d rows -> %s", len(df), OUTPUT_CSV)


def run_backfill(force: bool = False):
    """Full first-run: DB init -> fetch -> train -> predict -> export."""
    step_init_db()
    step_ingest_backfill(force=force)
    step_train()
    step_predict()
    step_export()
    logger.info("=== Backfill complete ===")


def run_daily():
    """Daily task: fetch missing cities for yesterday -> predict -> export."""
    step_init_db()
    step_ingest_daily()
    step_predict()
    step_export()
    logger.info("=== Daily update complete ===")


def run_weekly_train():
    """Weekly task: retrain on full history -> regenerate predictions -> export."""
    step_train()
    step_predict()
    step_export()
    logger.info("=== Weekly retraining complete ===")


def run_repair_gaps(start_date: str, end_date: str):
    """Repair only the missing or incomplete raw rows for a target range."""
    step_init_db()
    summary = repair_gaps_in_range(
        start_date,
        end_date,
        delay_seconds=mlops_config.ingestion.backfill_delay_seconds,
    )
    if summary["failed_ranges"] or summary["remaining_gap_counts"]:
        failed_ranges = ", ".join(sorted(summary["failed_ranges"])) or "none"
        remaining = (
            ", ".join(
                f"{city}:{count}" for city, count in sorted(summary["remaining_gap_counts"].items())
            )
            or "none"
        )
        raise RuntimeError(
            "Gap repair incomplete - "
            f"failed ranges: {failed_ranges}; "
            f"remaining gap days: {remaining}."
        )
    logger.info("=== Gap repair complete ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weather pipeline orchestrator")
    parser.add_argument(
        "command",
        choices=["backfill", "daily", "train", "predict", "export", "repair"],
        help=(
            "backfill : full first-run setup (historical data + initial training)\n"
            "daily    : incremental daily update\n"
            "train    : retrain models only\n"
            "predict  : regenerate predictions only (models must already exist)\n"
            "export   : export SQLite view to CSV\n"
            "repair   : scan and repair missing/incomplete raw rows in a date range"
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="backfill: re-fetch all cities from scratch, overwriting existing data",
    )
    parser.add_argument(
        "--start-date",
        dest="start_date",
        help="repair: inclusive ISO start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        dest="end_date",
        help="repair: inclusive ISO end date (YYYY-MM-DD)",
    )
    args = parser.parse_args()

    commands = {
        "backfill": run_backfill,
        "daily": run_daily,
        "train": run_weekly_train,
        "predict": step_predict,
        "export": step_export,
    }
    if args.command == "backfill":
        run_backfill(force=args.force)
    elif args.command == "repair":
        if not args.start_date or not args.end_date:
            parser.error("repair requires --start-date and --end-date")
        run_repair_gaps(args.start_date, args.end_date)
    else:
        commands[args.command]()
