.DEFAULT_GOAL := help

PYTHON := python
PYTEST  := $(PYTHON) -m pytest

.PHONY: help install install-dev test test-monitoring test-ingestion test-train \
        lint format format-github format-check ci docker-build docker-up docker-down \
        analytics-install analytics-load analytics-run analytics-test \
        analytics-export analytics-docs analytics-all analytics-odbc

help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Setup"
	@echo "  install             Install runtime dependencies"
	@echo "  install-dev         Install runtime + dev tools (ruff, black, pre-commit)"
	@echo ""
	@echo "Tests"
	@echo "  test                Run all tests"
	@echo "  test-monitoring       tests/test_monitoring_branch.py"
	@echo "  test-ingestion        tests/test_ingestion_backfill_flow.py"
	@echo "  test-train            tests/test_train_dag.py"
	@echo ""
	@echo "Code quality"
	@echo "  lint                Lint with ruff"
	@echo "  format              Format with black"
	@echo "  format-github       Auto-fix ruff issues, then format with black"
	@echo "  format-check        Check formatting (no writes, for CI)"
	@echo "  ci                  lint + format-check + test"
	@echo ""
	@echo "Analytics (dbt + DuckDB)"
	@echo "  analytics-install   pip install dbt-core dbt-duckdb duckdb (pinned versions)"
	@echo "  analytics-load      Sync SQLite + JSON monitoring files → DuckDB"
	@echo "  analytics-run       Load sources + dbt deps + dbt run (full pipeline)"
	@echo "  analytics-test      Run dbt schema/data tests"
	@echo "  analytics-export    Export all mart tables as CSV → data/analytics/"
	@echo "  analytics-docs      Generate + serve dbt docs at http://localhost:8080"
	@echo "  analytics-all       analytics-run + analytics-export (end-to-end)"
	@echo "  analytics-odbc      Print instructions to set up ODBC DSN for Power BI"
	@echo ""
	@echo "Docker"
	@echo "  docker-build        Build API + Airflow images"
	@echo "  docker-up           Start all services (detached)"
	@echo "  docker-down         Stop all services"

# ── Setup ─────────────────────────────────────────────────────────────────────

install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements.txt ruff black pre-commit
	pre-commit install

# ── Tests ─────────────────────────────────────────────────────────────────────

test:
	$(PYTEST) tests/ -q --tb=short

test-monitoring:
	$(PYTEST) tests/test_monitoring_branch.py -q

test-ingestion:
	$(PYTEST) tests/test_ingestion_backfill_flow.py -q

test-train:
	$(PYTEST) tests/test_train_dag.py -q

# ── Code quality ──────────────────────────────────────────────────────────────

lint:
	ruff check .

lint-fix:
	ruff check --fix .

format:
	black .

format-github:
	ruff check --fix .
	black .

format-check:
	black --check .

ci: lint format-check test

# ── Docker ────────────────────────────────────────────────────────────────────

docker-build:
	docker build -t weather-mlops-api:latest .
	docker build -f Dockerfile.airflow -t weather-mlops-airflow:latest .

docker-up:
	docker compose up -d

docker-down:
	docker compose down

# ── Analytics ─────────────────────────────────────────────────────────────────

analytics-install:
	pip install dbt-core==1.11.8 dbt-duckdb==1.10.1 duckdb==1.5.2

analytics-load:
	python analytics/scripts/load_sources.py

analytics-run: analytics-load
	cd analytics && dbt deps && dbt run --no-partial-parse

analytics-test:
	cd analytics && dbt test --no-partial-parse

analytics-export:
	python analytics/scripts/export_powerbi.py

analytics-docs:
	cd analytics && dbt docs generate --no-partial-parse && dbt docs serve

analytics-all: analytics-run analytics-export

analytics-odbc:
	@echo "Run as Administrator in PowerShell:"
	@echo "  powershell -ExecutionPolicy Bypass -File analytics\\scripts\\setup_odbc_dsn.ps1"
	@echo ""
	@echo "Prerequisite — install DuckDB ODBC driver first:"
	@echo "  https://github.com/duckdb/duckdb/releases/latest"
	@echo "  File: duckdb_odbc-windows-amd64.zip -> odbc_install.exe"
