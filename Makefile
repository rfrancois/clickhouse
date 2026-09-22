PYTHON := python3
CLIENT := docker exec -i bench_clickhouse clickhouse-client --user bench --password bench --multiquery

.PHONY: all up wait init migrate migrate-swap migrate-ip migrate-ip-swap text-index upgrade generate test import pdf down clean

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

# Base existante : copie de fqdn_search triée par nom inversé (table actuelle intacte)
migrate:
	$(CLIENT) < sql/07_migrate_copy.sql

# Bascule vers la nouvelle table (ancienne gardée sous fqdn_search_old)
migrate-swap:
	$(CLIENT) < sql/08_migrate_swap.sql

# Même migration pour ip_search (tri par valeur, sans index ngram)
migrate-ip:
	$(CLIENT) < sql/10_migrate_ip_copy.sql

migrate-ip-swap:
	$(CLIENT) < sql/11_migrate_ip_swap.sql

# Ajout de l'index texte exact sur fqdn_search (sans copie, en arrière-plan)
text-index:
	$(CLIENT) < sql/09_add_text_index.sql
	@echo "Index texte en construction — suivi : system.mutations (voir sql/09_add_text_index.sql)."

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
