.DEFAULT_GOAL := help

PYTHON := python
PYTEST  := $(PYTHON) -m pytest

.PHONY: help install install-dev test test-monitoring test-ingestion test-train \
        lint format format-check ci docker-build docker-up docker-down

help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Setup"
	@echo "  install        Install runtime dependencies"
	@echo "  install-dev    Install runtime + dev tools (ruff, black, pre-commit)"
	@echo ""
	@echo "Tests"
	@echo "  test           Run all tests"
	@echo "  test-monitoring  tests/test_monitoring_branch.py"
	@echo "  test-ingestion   tests/test_ingestion_backfill_flow.py"
	@echo "  test-train       tests/test_train_dag.py"
	@echo ""
	@echo "Code quality"
	@echo "  lint           Lint with ruff"
	@echo "  format         Format with black"
	@echo "  format-check   Check formatting (no writes, for CI)"
	@echo "  ci             lint + format-check + test"
	@echo ""
	@echo "Docker"
	@echo "  docker-build   Build API + Airflow images"
	@echo "  docker-up      Start all services (detached)"
	@echo "  docker-down    Stop all services"

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

format:
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
