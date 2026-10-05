#!/usr/bin/env python3
"""Envoie les ranks d'une semaine (une source, ~1M nœuds d'un type classé :
fqdn ou ip) dans la table rank :

    send_ranks(ranks=[("google.com", 1), ...], source_uuid="...", week=...,
               node_type="fqdn")

ou en ligne de commande :

    python scripts/send_ranks.py <fichier.csv[.gz]> <source_uuid>
                                 [--week AAAA-MM-JJ] [--type fqdn|ip]

Fichier : CSV à deux colonnes, rank et valeur dans n'importe quel ordre
(détecté : la colonne numérique est le rank), séparateur ',' ou ';',
en-tête facultatif (ignoré s'il n'a pas de colonne numérique).

Semaine : date (datetime, date ou chaîne ISO) du relevé, ramenée au lundi ;
absente → aujourd'hui (UTC). rank garde une ligne par (nœud, source,
semaine) : renvoyer une semaine remplace ses ranks.

Tout se fait côté serveur, sans aller-retour des ids vers Python :
  1. chargement brut dans stg_rank
  2. normalisation et validation → stg_rank_norm, une ligne par valeur (le
     meilleur rank si elle est en double) ; rank ≤ 0 rejeté
       fqdn : espaces, minuscules, point final retirés ; IP, wildcards,
              noms invalides rejetés (mêmes règles que domains.json,
              sql/05_import_distribute.sql)
       ip   : IPv4 / IPv6 valide, en forme canonique (2001:DB8:0::1 →
              2001:db8::1)
  3. valeurs absentes de toutes les parts de la table du type
     → stg_rank_missing
  4. création de ces nœuds, nouvel id auto-incrémenté (max(id) + 1, ...),
     rank NULL (remplacé à l'étape 6)
  5. résolution valeur → min(id) (l'id qui survit aux merges), une seule
     jointure → stg_rank_ids
  6. depuis stg_rank_ids : insertion dans rank, et rank de la table du type
     = le rank de cet envoi (anyLast : le DERNIER envoi, toutes sources
     confondues, donne le rank)
Les tables stg_rank* sont supprimées à la fin.

Un seul envoi à la fois (deux envois concurrents liraient le même
max(id)). Prérequis : table rank (make init, ou make migration-rank sur une
base existante).

Dépendance : pip install clickhouse-connect
"""
import argparse
import csv
import gzip
import sys
import time
from datetime import date, datetime, timedelta, timezone
from itertools import islice

import clickhouse_connect

from send import HOST, PORT, USER, PASSWORD, norm_uuid, resolve_sources

# types classés → colonne id de leur table (la table porte le nom du type)
RANKED = {"fqdn": "id_fqdn", "ip": "id_ip"}
STAGING = ("stg_rank", "stg_rank_norm", "stg_rank_missing", "stg_rank_ids")
BATCH = 100_000     # lignes par insert dans stg_rank
# nom d'hôte valide, même règle que sql/05_import_distribute.sql (étape 0)
FQDN_RE = (r"^(?:[a-z0-9_]|[^\x00-\x7f])(?:[a-z0-9_-]|[^\x00-\x7f]){0,62}"
           r"(?:\.(?:[a-z0-9_]|[^\x00-\x7f])(?:[a-z0-9_-]|[^\x00-\x7f]){0,62})+$")
# normalisation (étape 2) : valeur brute → v, puis valeur retenue et filtre
NORMALIZE = {
    "fqdn": ("trim(TRAILING '.' FROM lowerUTF8(trimBoth(value)))",
             "v",
             "length(v) <= 253 AND match(v, {re:String}) "
             "AND NOT match(v, '\\\\.[0-9]+$')"),  # dernier label numérique (999.1.1.1)
    "ip":   ("lower(trimBoth(value))",
             "if(isIPv4String(v), toString(toIPv4(v)), toString(toIPv6(v)))",
             "isIPv4String(v) OR isIPv6String(v)"),
}
# l'index primaire / les jointures suffisent : inutile de charger les index
# ngram / texte de fqdn
SETTINGS = {"use_skip_indexes": 0}


def to_monday(v) -> date:
    """Date (datetime, date, chaîne ISO ou None = aujourd'hui UTC) → lundi
    de sa semaine."""
    if v is None:
        v = datetime.now(timezone.utc).date()
    elif isinstance(v, str):
        v = datetime.fromisoformat(v.strip())
    if isinstance(v, datetime):
        v = v.date()
    return v - timedelta(days=v.weekday())


def is_int(s: str) -> bool:
    return s.strip().lstrip("-").isdigit()


def read_csv(path: str):
    """(valeur, rank) de chaque ligne d'un CSV rank/valeur. Colonne du rank :
    la colonne numérique de la première ligne de données ; une première
    ligne sans colonne numérique est un en-tête, ignoré. Lignes illisibles
    ignorées."""
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8-sig", newline="") as f:
        first = f.readline()
        delim = ";" if first.count(";") > first.count(",") else ","
        rank_col = None
        for row in csv.reader([first], delimiter=delim):
            if len(row) >= 2 and (is_int(row[0]) or is_int(row[1])):
                rank_col = 0 if is_int(row[0]) else 1
                yield row[1 - rank_col], int(row[rank_col])
        for row in csv.reader(f, delimiter=delim):
            if len(row) < 2:
                continue
            if rank_col is None:  # première ligne de données après l'en-tête
                rank_col = 0 if is_int(row[0]) else 1
            if is_int(row[rank_col]):
                yield row[1 - rank_col], int(row[rank_col])


def send_ranks(ranks, source_uuid: str, week=None, node_type: str = "fqdn") -> None:
    """ranks : itérable de (valeur, rank), consommé par tranches (un fichier
    de 1M lignes n'est jamais entièrement en mémoire)."""
    typ = str(node_type).lower()
    if typ not in RANKED:
        sys.exit(f"ERREUR : type {typ} sans rank (types classés : {', '.join(RANKED)})")
    idcol = RANKED[typ]
    raw_expr, value_expr, valid = NORMALIZE[typ]

    client = clickhouse_connect.get_client(host=HOST, port=PORT,
                                           username=USER, password=PASSWORD)
    if not int(client.command("EXISTS TABLE rank")):
        sys.exit("ERREUR : table rank absente (make migration-rank)")

    uuid = norm_uuid(source_uuid)
    src = resolve_sources(client, {uuid}).get(uuid)
    if src is None:
        sys.exit(f"ERREUR : source_uuid {uuid + ' inconnu' if uuid else 'absent'}")

    monday = to_monday(week)
    if monday + timedelta(days=730) <= datetime.now(timezone.utc).date():
        sys.exit(f"ERREUR : semaine du {monday} vieille de plus de 2 ans "
                 "(supprimée par le TTL de rank)")
    params = {"src": src, "week": monday, "re": FQDN_RE}
    print(f"{typ}, semaine du {monday}, source {uuid} (id_source {src})")

    def cmd(sql: str):
        return client.command(sql, parameters=params, settings=SETTINGS)

    def count(sql: str) -> int:
        return int(client.command(sql, parameters=params, settings=SETTINGS))

    t = time.monotonic()
    try:
        # 1. chargement brut
        for tbl in STAGING:
            cmd(f"DROP TABLE IF EXISTS {tbl}")
        cmd("CREATE TABLE stg_rank (value String, rank Int32) "
            "ENGINE = MergeTree ORDER BY tuple()")
        it = iter(ranks)
        while batch := [[str(v), int(r)] for v, r in islice(it, BATCH)]:
            client.insert("stg_rank", batch, column_names=["value", "rank"])
        raw = count("SELECT count() FROM stg_rank")

        # 2. normalisation, validation, une ligne par valeur (meilleur rank)
        cmd("CREATE TABLE stg_rank_norm (value String, rank Int32) "
            "ENGINE = MergeTree ORDER BY value")
        cmd("INSERT INTO stg_rank_norm "
            f"SELECT {value_expr} AS c, min(rank) FROM ("
            f" SELECT {raw_expr} AS v, rank FROM stg_rank) "
            f"WHERE rank > 0 AND ({valid}) "
            "GROUP BY c")
        norm = count("SELECT count() FROM stg_rank_norm")
        print(f"stg_rank : {raw:,} lignes, {norm:,} valeurs valides distinctes, "
              f"{raw - norm:,} rejetées ou en double")
        if not norm:
            return

        # 3. valeurs absentes de la table du type (toutes parts, fusionnées ou non)
        cmd("CREATE TABLE stg_rank_missing (value String) "
            "ENGINE = MergeTree ORDER BY value")
        cmd("INSERT INTO stg_rank_missing "
            "SELECT value FROM stg_rank_norm WHERE value NOT IN ("
            f" SELECT value FROM {typ} WHERE value IN (SELECT value FROM stg_rank_norm))")
        missing = count("SELECT count() FROM stg_rank_missing")

        # 4. création, ids à la suite du max existant
        if missing:
            base = count(f"SELECT max({idcol}) FROM {typ}")
            params["base"] = base
            # rank non fourni : NULL, remplacé par celui de l'envoi à l'étape 6
            cmd(f"INSERT INTO {typ} (value, {idcol}, version) "
                "SELECT value, toInt64({base:Int64} + row_number() OVER (ORDER BY value)), "
                "toUnixTimestamp(now()) FROM stg_rank_missing")
            print(f"{typ} : {norm - missing:,} existants, {missing:,} créés "
                  f"(ids {base + 1:,} à {base + missing:,})")
        else:
            print(f"{typ} : {norm:,} existants, aucun créé")

        # 5. valeur → id (le min : celui qui survit aux merges), une seule
        # passe sur la table du type pour les deux inserts suivants
        cmd("CREATE TABLE stg_rank_ids (value String, id_node Int64, rank Int32) "
            "ENGINE = MergeTree ORDER BY value")
        cmd("INSERT INTO stg_rank_ids "
            "SELECT s.value, f.id_node, s.rank "
            "FROM stg_rank_norm AS s INNER JOIN ("
            f" SELECT value, min({idcol}) AS id_node FROM {typ}"
            " WHERE value IN (SELECT value FROM stg_rank_norm) GROUP BY value"
            ") AS f USING value")
        resolved = count("SELECT count() FROM stg_rank_ids")
        if resolved < norm:
            print(f"ATTENTION : {norm - resolved:,} valeurs sans id résolu", file=sys.stderr)

        # 6. historique, puis rank courant de la table du type (anyLast :
        # celui-ci remplace le précédent ; id inchangé, c'est déjà le min)
        cmd("INSERT INTO rank (node_type, id_node, id_source, week, rank) "
            f"SELECT '{typ}', id_node, {{src:Int32}}, {{week:Date}}, rank FROM stg_rank_ids")
        cmd(f"INSERT INTO {typ} (value, {idcol}, rank, version) "
            "SELECT value, id_node, rank, toUnixTimestamp(now()) FROM stg_rank_ids")
        print(f"rank : {resolved:,} ranks {typ} pour la semaine du {monday}, "
              f"rank de {typ} mis à jour, en {time.monotonic() - t:.0f} s")
    finally:
        for tbl in STAGING:
            cmd(f"DROP TABLE IF EXISTS {tbl}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Ranks d'une semaine → table rank")
    ap.add_argument("file", help="CSV rank/valeur (.csv ou .csv.gz)")
    ap.add_argument("source_uuid")
    ap.add_argument("--week", help="date du relevé, ramenée au lundi (défaut : aujourd'hui, UTC)")
    ap.add_argument("--type", default="fqdn", choices=sorted(RANKED),
                    help="type des nœuds classés (défaut : fqdn)")
    args = ap.parse_args()
    send_ranks(read_csv(args.file), args.source_uuid, args.week, args.type)
