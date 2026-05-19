"""
Run or test dbt models driven by analytics/pipeline.yml.

Usage (from project root):
    python analytics/scripts/dbt_runner.py run
    python analytics/scripts/dbt_runner.py test
    python analytics/scripts/dbt_runner.py run daily_bi
    python analytics/scripts/dbt_runner.py test full_build
    ANALYTICS_PIPELINE=full_build python analytics/scripts/dbt_runner.py run

The optional pipeline argument overrides the ANALYTICS_PIPELINE env var (default: daily_bi).
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).parent.parent.parent
ANALYTICS_DIR = ROOT / "analytics"
PIPELINE_YML = ANALYTICS_DIR / "pipeline.yml"

_DBT_FLAGS = [
    "--profiles-dir",
    str(ANALYTICS_DIR),
    "--project-dir",
    str(ANALYTICS_DIR),
    "--no-partial-parse",
]


def _load(pipeline: str) -> dict:
    config = yaml.safe_load(PIPELINE_YML.read_text())
    pipelines = config.get("pipelines", {})
    if pipeline not in pipelines:
        available = ", ".join(pipelines.keys())
        sys.exit(f"ERROR: Pipeline {pipeline!r} not in pipeline.yml. Available: {available}")
    return pipelines[pipeline]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    if cmd not in ("run", "test"):
        sys.exit(f"ERROR: Unknown command {cmd!r}. Use 'run' or 'test'.")

    pipeline = (sys.argv[2].strip() if len(sys.argv) > 2 else "") or os.getenv(
        "ANALYTICS_PIPELINE", "daily_bi"
    )

    cfg = _load(pipeline)
    key = "run_select" if cmd == "run" else "test_select"
    selects = cfg.get(key, [])
    if isinstance(selects, str):
        selects = [selects]

    select_args: list[str] = []
    for s in selects:
        select_args += ["--select", s]

    dbt_cmd = ["dbt", cmd] + select_args + _DBT_FLAGS
    print(f"[pipeline={pipeline}] dbt {cmd}: {selects}")
    result = subprocess.run(dbt_cmd, cwd=str(ANALYTICS_DIR))
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
