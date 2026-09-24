PYTHON := python3
CLIENT := docker exec -i ch_container clickhouse-client --user chuser --password Royal15Raccoon --multiquery

.PHONY: all up wait init upgrade generate test import pdf down clean

all: up wait init generate

up:
	docker compose up -d

wait:
	@echo "Attente de ClickHouse..."
	@until docker exec ch_container clickhouse-client --user chuser --password Royal15Raccoon -q "SELECT 1" >/dev/null 2>&1; do sleep 1; done
	@echo "ClickHouse prêt."

init:
	$(CLIENT) < sql/02_optimized.sql
	@echo "Schéma optimisé créé."

# Mise à jour de ClickHouse vers la version de docker-compose.yml (volume conservé)
upgrade:
	docker compose pull
	docker compose up -d
	@$(MAKE) --no-print-directory wait
	@docker exec ch_container clickhouse-client --user chuser --password Royal15Raccoon -q "SELECT 'ClickHouse ' || version()"

generate: .venv
	$(PYTHON) scripts/generate_data.py

test: .venv
	$(PYTHON) scripts/test.py

import:
	@test -n "$(FILE)" || { echo "Usage : make import FILE=<archive.zip|fichier|dossier>"; exit 1; }
	$(PYTHON) scripts/import_data.py "$(FILE)"

pdf: .venv
	$(PYTHON) scripts/make_pdf.py

.venv:
	python3 -m venv .venv
	.venv/bin/pip install -q --upgrade pip
	.venv/bin/pip install -q clickhouse-connect matplotlib numpy

down:
	docker compose down

clean: down
	docker compose down -v
	rm -rf .venv results/*.png results/results.json
