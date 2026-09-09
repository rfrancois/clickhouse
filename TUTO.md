# Tuto rapide — ClickHouse FQDN/IP (schéma optimisé)

## Prérequis

- Docker Desktop démarré
- Python 3.10+
- ~20 Go de disque libre (plus selon la volumétrie importée)

## Installation

```bash
cd bench
make up        # démarre le conteneur ClickHouse (ports 8123 / 9000)
```

## Importer des données réelles

```bash
make import FILE=archive.zip   # zip contenant *node*.csv / *link*.csv / *.json[.gz]
                               # (un fichier seul ou un dossier marchent aussi)
```

Les données sont chargées en staging puis distribuées dans les tables
optimisées (`fqdn_search`, `ip_search`, `link_opt`). Pas besoin d'avoir
lancé `make init` au préalable : si les tables optimisées sont absentes,
le schéma est créé automatiquement au début de l'import. La résolution
des liens (valeur → id) utilise `grace_hash` pour ne pas saturer la
mémoire à grande échelle. Voir la section « Import de données réelles »
du README pour le détail des formats et des choix d'import.

## Données factices + test rapide (optionnel)

```bash
make all       # up + schéma optimisé + 500k FQDN / 500k IP / 1M liens factices
make test      # recherche LIKE + jointure chronométrées sur les tables optimisées
```

## Commandes équivalentes sans make

```bash
docker compose up -d
python3 -m venv .venv
.venv/bin/pip install clickhouse-connect matplotlib numpy

docker exec -i bench_clickhouse clickhouse-client --user bench --password bench --multiquery < sql/02_optimized.sql

.venv/bin/python3 scripts/generate_data.py   # données factices
.venv/bin/python3 scripts/test.py            # test rapide
.venv/bin/python3 scripts/import_data.py archive.zip   # import réel
```

## Arrêter / nettoyer

```bash
make down    # stoppe le conteneur (données conservées)
make clean   # stoppe + supprime le volume de données et le venv
```

Identifiants ClickHouse : `bench` / `bench` — HTTP `localhost:8123`, natif `localhost:9000`.
