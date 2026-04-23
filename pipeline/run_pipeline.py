"""
Main pipeline orchestrator.
Can be called directly (CLI) or imported by Airflow DAGs.

Steps:
  backfill  — fetch 2 years of history + init DB + train models + predict
  ingest    — fetch yesterday's data + update predictions for new rows
  train     — retrain all models on full DB + regenerate all predictions
  predict   — regenerate predictions without retraining (uses saved models)
  export    — export DB view to CSV for Power BI fallback
"""
import argparse
import logging
import sys
import time
from pathlib import Path

# Make sure the project root is on sys.path when run directly
ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.database import init_db, upsert_weather_raw, upsert_predictions, read_raw, read_all, get_latest_date
from pipeline.fetch_weather import fetch_city, fetch_today
from pipeline.locations import LOCATIONS
from pipeline.process_weather import add_features
from pipeline.train_models import train_all
from pipeline.predict import generate_predictions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(ROOT / "logs" / "pipeline.log", mode="a"),
    ],
)
logger = logging.getLogger("run_pipeline")

OUTPUT_CSV = ROOT / "data" / "output" / "weather_final.csv"


# ─── individual steps ────────────────────────────────────────────────────────

def step_init_db():
    logger.info("Initialising database schema...")
    (ROOT / "logs").mkdir(parents=True, exist_ok=True)
    init_db()


def step_ingest_backfill(force: bool = False):
    from datetime import date, timedelta

    BACKFILL_START = "2008-01-01"
    # Open-Meteo archive lags ~1 day
    end = (date.today() - timedelta(days=1)).isoformat()

    cities = list(LOCATIONS.keys())
    logger.info("Backfill %s -> %s (%d cities, via Open-Meteo%s)",
                BACKFILL_START, end, len(cities), ", force re-fetch" if force else "")

    stored_total = 0
    skipped = 0
    for city in cities:
        if not force:
            latest = get_latest_date(city)
            if latest and latest >= end:
                logger.info("Skip %s — already up to date (%s)", city, latest)
                skipped += 1
                continue
            start = (date.fromisoformat(latest) + timedelta(days=1)).isoformat() if latest else BACKFILL_START
        else:
            start = BACKFILL_START

        logger.info("Fetching %s  (%s -> %s)", city, start, end)
        try:
            df = fetch_city(city, start, end)
            if df.empty:
                logger.warning("No data for %s", city)
            else:
                upsert_weather_raw(df)
                stored_total += len(df)
                logger.info("Stored %d rows for %s", len(df), city)
        except Exception as exc:
            logger.error("Failed %s: %s — continuing", city, exc)
        time.sleep(10)

    logger.info("Backfill complete: %d rows stored, %d cities skipped", stored_total, skipped)


def step_ingest_daily():
    logger.info("Fetching daily update (yesterday)...")
    df = fetch_today()
    if df.empty:
        logger.warning("Daily fetch returned no data.")
        return
    upsert_weather_raw(df)
    logger.info("Daily update stored: %d rows", len(df))


def step_train():
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
    df_raw  = read_raw()
    df_feat = add_features(df_raw)
    preds   = generate_predictions(df_feat)
    upsert_predictions(preds)
    logger.info("Predictions stored: %d rows", len(preds))


def step_export():
    logger.info("Exporting final dataset to CSV...")
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df = read_all("v_weather_full")
    df.to_csv(OUTPUT_CSV, index=False)
    logger.info("Exported %d rows -> %s", len(df), OUTPUT_CSV)


# ─── compound workflows ──────────────────────────────────────────────────────

def run_backfill(force: bool = False):
    """Full first-run: DB init -> fetch -> train -> predict -> export."""
    step_init_db()
    step_ingest_backfill(force=force)
    step_train()
    step_predict()
    step_export()
    logger.info("=== Backfill complete ===")


def run_daily():
    """Daily Airflow task: fetch yesterday -> update predictions -> export."""
    step_init_db()          # no-op if already exists
    step_ingest_daily()
    step_predict()
    step_export()
    logger.info("=== Daily update complete ===")


def run_weekly_train():
    """Weekly Airflow task: retrain on full history -> regenerate predictions -> export."""
    step_train()
    step_predict()
    step_export()
    logger.info("=== Weekly retraining complete ===")


# ─── CLI entry point ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weather pipeline orchestrator")
    parser.add_argument(
        "command",
        choices=["backfill", "daily", "train", "predict", "export"],
        help=(
            "backfill : full first-run setup (historical data + initial training)\n"
            "daily    : incremental daily update\n"
            "train    : retrain models only\n"
            "predict  : regenerate predictions only (models must already exist)\n"
            "export   : export SQLite view to CSV"
        ),
    )
    parser.add_argument(
        "--force", action="store_true",
        help="backfill: re-fetch all cities from scratch, overwriting existing data",
    )
    args = parser.parse_args()

    commands = {
        "backfill": run_backfill,
        "daily":    run_daily,
        "train":    run_weekly_train,
        "predict":  step_predict,
        "export":   step_export,
    }
    if args.command == "backfill":
        run_backfill(force=args.force)
    else:
        commands[args.command]()
