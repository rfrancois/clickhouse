PYTHON := python3
CLIENT := docker exec -i bench_clickhouse clickhouse-client --user bench --password bench --multiquery

.PHONY: all up wait init migrate upgrade generate test import pdf down clean

all: up wait init generate

up:
	docker compose up -d

wait:
	@echo "Attente de ClickHouse..."
	@until docker exec bench_clickhouse clickhouse-client --user bench --password bench -q "SELECT 1" >/dev/null 2>&1; do sleep 1; done
	@echo "ClickHouse prêt."

init:
	$(CLIENT) < sql/02_optimized.sql
	@echo "Schéma optimisé créé."

# Mise à jour de ClickHouse vers la version de docker-compose.yml (volume conservé)
upgrade:
	docker compose pull
	docker compose up -d
	@$(MAKE) --no-print-directory wait
	@docker exec bench_clickhouse clickhouse-client --user bench --password bench -q "SELECT 'ClickHouse ' || version()"

# Mise à jour d'une base existante (sans perte) : projection p_rank pour ORDER BY rank
migrate:
	$(CLIENT) < sql/06_rank_projection.sql
	@echo "Projection p_rank ajoutée — construction en arrière-plan (voir system.mutations)."

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
