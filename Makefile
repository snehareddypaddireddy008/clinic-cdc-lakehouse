# Run `make help` for the list. Everything runs from the repo root.
SHELL := /bin/bash
.DEFAULT_GOAL := help
export ICEBERG_CATALOG_URI ?= sqlite:///$(CURDIR)/.lakehouse/catalog.db
export DUCKDB_PATH ?= $(CURDIR)/.lakehouse/analytics.duckdb

help:  ## show this list
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

install:  ## create a virtualenv and install everything
	python3 -m venv .venv && . .venv/bin/activate && pip install -U pip && pip install -e ".[dev]"

up:  ## start Postgres, Kafka, Kafka Connect and S3 (SeaweedFS)
	docker compose up -d
	@echo "waiting for Kafka Connect..."; until curl -sf localhost:8083/connectors > /dev/null; do sleep 3; done; echo "ready"

infra:  ## create the lakehouse bucket with Terraform
	cd terraform && terraform init -input=false && terraform apply -auto-approve

register:  ## register the Debezium Postgres connector
	curl -s -X POST -H "Content-Type: application/json" --data @connect/register-postgres.json localhost:8083/connectors | python3 -m json.tool
	@sleep 3; curl -s localhost:8083/connectors/clinic-postgres-cdc/status | python3 -m json.tool

simulate:  ## write booking activity (with some bad rows) into Postgres
	python scripts/simulate_activity.py --actions 300
	python scripts/simulate_activity.py --actions 60 --dirty

consume:  ## stream Kafka into Iceberg; exits after 20s without messages
	python -m lakehouse.consumer --max-idle-seconds 20

dbt:  ## build and test the dbt models
	cd dbt && dbt build --profiles-dir .

test:  ## lint and run the Python tests
	ruff check src tests scripts && pytest -q

offline:  ## no Docker: replay sample events into a local-file Iceberg warehouse, then dbt
	mkdir -p .lakehouse/warehouse
	python scripts/generate_sample_events.py
	ICEBERG_WAREHOUSE=file://$(CURDIR)/.lakehouse/warehouse python -m lakehouse.replay sample_data/cdc_events.jsonl
	cd dbt && ICEBERG_WAREHOUSE=file://$(CURDIR)/.lakehouse/warehouse dbt build --profiles-dir .

demo: up infra register simulate consume dbt  ## the whole pipeline, end to end

down:  ## stop everything and delete volumes and local state
	docker compose down -v
	rm -rf .lakehouse

.PHONY: help install up infra register simulate consume dbt test offline demo down
