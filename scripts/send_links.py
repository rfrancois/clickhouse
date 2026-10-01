#!/usr/bin/env python3
"""Envoie des liens vers ClickHouse (table link), sans passer par links.csv.

Version autonome et simplifiée de distribute_links() (import_data.py) :
  - chaque lien est un dict avec les champs :
      value_1, value_2, type_1, type_2, id_source, creation_date, update_date
    (value_1 / value_2 : "google.com", "8.8.8.8", ...)
    creation_date et update_date sont optionnels : update_date absent → now ;
    creation_date absent → update_date. link (AggregatingMergeTree) garde la
    plus ANCIENNE creation_date et la plus RÉCENTE update_date d'un lien :
    un lien déjà connu garde donc sa date de création.
  - chaque valeur est résolue en id dans la table de son type ; une valeur
    inconnue reçoit un nouvel id auto-incrémenté (max(id) + 1, ...), avec
    rank 1000000 pour fqdn / ip
  - chaque lien est inséré dans les deux sens (A→B et B→A)
  - ignorés : type inconnu, valeur vide, auto-lien (même type, même valeur)

Un seul envoi à la fois (deux envois concurrents liraient le même max(id)).

Dépendance : pip install clickhouse-connect
"""
from datetime import datetime, timezone

import clickhouse_connect

HOST, PORT, USER, PASSWORD = "localhost", 8123, "chuser", "Royal15Raccoon"

# type de nœud → colonne id de sa table (la table porte le nom du type)
NODE_TABLES = {
    "application":       "id_application",
    "capture":           "id_capture",
    "fqdn":              "id_fqdn",
    "ip":                "id_ip",
    "plugin":            "id_plugin",
    "organization_name": "id_organization_name",
    "organization_id":   "id_organization_id",
    "phone":             "id_phone",
    "social_id":         "id_social_id",
}
RANKED = {"fqdn", "ip"}
NEW_RANK = 1000000
CHUNK = 1000  # nombre de valeurs par requête de résolution


def to_ts(v, default: int) -> int:
    """Date (datetime, timestamp ou chaîne ISO) → timestamp Unix (UTC si pas
    de fuseau). Illisible ou vide (dont 0000-00-00 MySQL) → default."""
    if isinstance(v, (int, float)):
        return int(v)
    if isinstance(v, str):
        try:
            v = datetime.fromisoformat(v.strip())
        except ValueError:
            return default
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return int(v.timestamp())
    return default


def send_links(links: list[dict]) -> None:
    client = clickhouse_connect.get_client(host=HOST, port=PORT,
                                           username=USER, password=PASSWORD)
    now = int(datetime.now(timezone.utc).timestamp())

    # 1. nettoyage
    rows = []
    for l in links:
        t1, t2 = str(l["type_1"]).lower(), str(l["type_2"]).lower()
        v1, v2 = str(l["value_1"]), str(l["value_2"])
        if t1 not in NODE_TABLES or t2 not in NODE_TABLES or not v1 or not v2:
            continue
        if t1 == t2 and v1 == v2:
            continue
        ver = to_ts(l.get("update_date"), now)
        det = to_ts(l.get("creation_date"), ver)
        rows.append((t1, v1, t2, v2, int(l.get("id_source") or 0), det, ver))

    # 2. valeur → id, par type (le plus ancien si la valeur en a plusieurs,
    # nouvel id si elle est inconnue)
    ids = {}  # (type, valeur) → id
    for typ, idcol in NODE_TABLES.items():
        values = sorted({v1 for t1, v1, *_ in rows if t1 == typ}
                        | {v2 for _, _, t2, v2, *_ in rows if t2 == typ})
        if not values:
            continue
        for i in range(0, len(values), CHUNK):
            res = client.query(
                f"SELECT value, min({idcol}) FROM {typ} "
                "WHERE value IN {vals:Array(String)} GROUP BY value",
                parameters={"vals": values[i:i + CHUNK]},
                # l'index primaire suffit (égalité) : inutile de charger les
                # index ngram / texte de fqdn, qui ne filtrent rien de plus
                settings={"use_skip_indexes": 0})
            for value, id_ in res.result_rows:
                ids[(typ, value)] = id_

        new = [v for v in values if (typ, v) not in ids]
        if new:
            next_id = int(client.command(f"SELECT max({idcol}) FROM {typ}")) + 1
            new_rows = []
            for v in new:
                ids[(typ, v)] = next_id
                new_rows.append([v, next_id] + ([NEW_RANK] if typ in RANKED else []) + [now])
                next_id += 1
            cols = ["value", idcol] + (["rank"] if typ in RANKED else []) + ["version"]
            client.insert(typ, new_rows, column_names=cols)
        print(f"{typ} : {len(values) - len(new):,} existants, {len(new):,} créés")

    # 3. liens dans les deux sens
    data = []
    for t1, v1, t2, v2, src, det, ver in rows:
        id1, id2 = ids[(t1, v1)], ids[(t2, v2)]
        data.append([t1, id1, t2, id2, src, det, ver])
        data.append([t2, id2, t1, id1, src, det, ver])
    if data:
        client.insert("link", data, column_names=[
            "type_1", "id_1", "type_2", "id_2", "id_source", "detection_date", "version"])
    print(f"link : {len(rows):,} liens envoyés ({len(data):,} lignes), "
          f"{len(links) - len(rows):,} ignorés")


if __name__ == "__main__":
    send_links([
        {"value_1": "example.com", "value_2": "93.184.216.34",
         "type_1": "fqdn", "type_2": "ip", "id_source": 1,
         "creation_date": "2024-01-15 10:00:00", "update_date": "2024-06-01 12:00:00"},
    ])
