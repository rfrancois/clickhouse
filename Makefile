PYTHON := python3
CLIENT := docker exec -i ch_container clickhouse-client --user chuser --password Royal15Raccoon --multiquery

.PHONY: all up wait init migration migration-swap migration-nodes migration-nodes-swap migration-property migration-property-swap migration-property-int upgrade generate test import pdf down clean

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

# Base existante : link → AggregatingMergeTree (première date de détection
# conservée). Copie dans link_new, link intacte ; relançable.
migration:
	$(CLIENT) < sql/06_migrate_link_copy.sql

# Bascule vers link_new (ancienne gardée sous link_old_replacing), après
# contrôle des comptes.
migration-swap:
	$(CLIENT) < sql/07_migrate_link_swap.sql

# Base existante : les 9 tables de valeurs (fqdn, ip, application, ...) →
# AggregatingMergeTree, une ligne par valeur (id le plus ancien conservé).
# Copies dans <type>_new, tables intactes ; relançable.
migration-nodes:
	$(CLIENT) < sql/08_migrate_nodes_copy.sql

# Bascule vers les <type>_new (anciennes gardées sous <type>_old_replacing),
# après contrôle des comptes.
migration-nodes-swap:
	$(CLIENT) < sql/09_migrate_nodes_swap.sql

# Base existante (après migration / migration-swap) : property +
# property_detection → une seule property en AggregatingMergeTree (payload du
# dernier insert, première date de détection). Copie dans property_new ;
# relançable.
migration-property:
	$(CLIENT) < sql/10_migrate_property_copy.sql

# Bascule vers property_new (anciennes gardées sous property_old_replacing et
# property_detection_old), après contrôle des comptes.
migration-property-swap:
	$(CLIENT) < sql/11_migrate_property_swap.sql

# Base dont property est déjà en AggregatingMergeTree avec detection_date en
# DateTime : passage en Int32 (timestamp Unix), en place (ALTER).
migration-property-int:
	$(CLIENT) < sql/12_migrate_property_detection_int.sql

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
