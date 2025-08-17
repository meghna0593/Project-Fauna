# ---- config ----
PYTHON ?= python3
VENV := .venv
PIP := $(VENV)/bin/pip
PY := $(VENV)/bin/python
PYTEST := $(VENV)/bin/pytest

.PHONY: help setup test run clean

help:
	@echo "Available targets:"
	@echo "  make setup     - create virtualenv and install deps"
	@echo "  make test      - run pytest"
	@echo "  make run ARGS='--base-url http://localhost:3123 --concurrency=10...' - run the app"
	@echo "  make clean     - remove venv and caches"

setup:
	@$(PYTHON) -m venv $(VENV)
	@$(PIP) install --upgrade pip
	@$(PIP) install -r requirements.txt
	@$(PIP) install -e .

test:
	@$(PYTEST) -q

run:
	@$(VENV)/bin/animals-etl $(ARGS)

clean:
	@rm -rf $(VENV) .pytest_cache
	@find . -type d -name "__pycache__" -exec rm -rf {} +
	@find . -type f -name "*.pyc" -delete
	@find . -type d -name "*.egg-info" -exec rm -rf {} +
