#!/usr/bin/env python3
"""Envoie des liens (table link) et des propriétés (table property) vers
ClickHouse, sans passer par links.csv / properties.csv, avec une seule
connexion :

    send(links=[...], properties=[...])

Version autonome et simplifiée de distribute_links() / distribute_properties()
(import_data.py). Les nœuds sont désignés par leur VALEUR ("google.com",
"8.8.8.8", ...) et leur type, dans les liens comme dans les propriétés :
  - chaque valeur est résolue en id dans la table de son type (une seule
    résolution pour les liens et les propriétés) ; une valeur inconnue
    reçoit un nouvel id auto-incrémenté (max(id) + 1, ...), avec rank
    1000000 pour fqdn / ip

Liens : chaque lien est un dict avec les champs
      value_1, value_2, type_1, type_2, id_source, creation_date, update_date
  - creation_date et update_date sont optionnels : update_date absent → now ;
    creation_date absent → update_date. link (AggregatingMergeTree) garde la
    plus ANCIENNE creation_date et la plus RÉCENTE update_date d'un lien :
    un lien déjà connu garde donc sa date de création.
  - chaque lien est inséré dans les deux sens (A→B et B→A)
  - ignorés : type inconnu, valeur vide, auto-lien (même type, même valeur)

Propriétés : chaque propriété est un dict avec les champs (mêmes noms de
dates que properties.csv)
      value, type, id_source, payload, version, detection_date
  - payload : chaîne (JSON déjà sérialisé, envoyée telle quelle) ou objet
    Python (dict, list, ...) sérialisé en JSON
  - version et detection_date sont optionnels : version absente → now ;
    detection_date absente → version. property (AggregatingMergeTree) garde
    le payload du DERNIER insert, la plus RÉCENTE version et la plus
    ANCIENNE detection_date d'un couple (nœud, source) : une propriété déjà
    connue garde donc sa date de détection.
  - un même (type, valeur, source) présent plusieurs fois dans la liste :
    une seule ligne, avec le dernier payload de la liste
  - ignorées : type inconnu, valeur vide, payload absent (None)

Dates (creation_date, update_date, version, detection_date) : datetime,
timestamp Unix ou chaîne ISO, UTC si pas de fuseau.

Un seul envoi à la fois (deux envois concurrents liraient le même max(id)).

Dépendance : pip install clickhouse-connect
"""
import json
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
NEW_RANK = 1000001
CHUNK = 1000  # nombre de valeurs par requête de résolution
INT32_MAX = 2**31 - 1  # property.detection_date en Int32 : jusqu'au 2038-01-19


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


def resolve_ids(client, nodes: set[tuple[str, str]], now: int) -> dict:
    """{(type, valeur)} → {(type, valeur): id}. Valeur connue : son id le plus
    ancien (min, si elle en a plusieurs) ; inconnue : nouvel id
    auto-incrémenté (max(id) + 1, ...), créé dans la table de son type."""
    ids = {}  # (type, valeur) → id
    for typ, idcol in NODE_TABLES.items():
        values = sorted(v for t, v in nodes if t == typ)
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
    return ids


def send(links: list[dict] = (), properties: list[dict] = ()) -> None:
    client = clickhouse_connect.get_client(host=HOST, port=PORT,
                                           username=USER, password=PASSWORD)
    now = int(datetime.now(timezone.utc).timestamp())

    # 1. nettoyage des liens
    link_rows = []
    for l in links:
        t1, t2 = str(l["type_1"]).lower(), str(l["type_2"]).lower()
        v1, v2 = str(l["value_1"]), str(l["value_2"])
        if t1 not in NODE_TABLES or t2 not in NODE_TABLES or not v1 or not v2:
            continue
        if t1 == t2 and v1 == v2:
            continue
        ver = to_ts(l.get("update_date"), now)
        det = to_ts(l.get("creation_date"), ver)
        link_rows.append((t1, v1, t2, v2, int(l.get("id_source") or 0), det, ver))

    # 2. nettoyage des propriétés ; un seul (type, valeur, source) : dernier
    # payload, version max, detection_date min (comme la fusion de property)
    prop_rows = {}  # (type, valeur, source) → [payload, det, ver]
    for p in properties:
        typ, value = str(p["type"]).lower(), str(p["value"])
        payload = p.get("payload")
        if typ not in NODE_TABLES or not value or payload is None:
            continue
        if not isinstance(payload, str):
            payload = json.dumps(payload, ensure_ascii=False)
        ver = to_ts(p.get("version"), now)
        det = min(to_ts(p.get("detection_date"), ver), INT32_MAX)
        key = (typ, value, int(p.get("id_source") or 0))
        if key in prop_rows:
            _, det0, ver0 = prop_rows[key]
            det, ver = min(det, det0), max(ver, ver0)
        prop_rows[key] = [payload, det, ver]

    # 3. valeur → id, par type, pour tous les nœuds (liens et propriétés)
    ids = resolve_ids(client,
                      {(t1, v1) for t1, v1, *_ in link_rows}
                      | {(t2, v2) for _, _, t2, v2, *_ in link_rows}
                      | {(typ, value) for typ, value, _ in prop_rows}, now)

    # 4. liens dans les deux sens
    data = []
    for t1, v1, t2, v2, src, det, ver in link_rows:
        id1, id2 = ids[(t1, v1)], ids[(t2, v2)]
        data.append([t1, id1, t2, id2, src, det, ver])
        data.append([t2, id2, t1, id1, src, det, ver])
    if data:
        client.insert("link", data, column_names=[
            "type_1", "id_1", "type_2", "id_2", "id_source", "detection_date", "version"])
    print(f"link : {len(link_rows):,} liens envoyés ({len(data):,} lignes), "
          f"{len(links) - len(link_rows):,} ignorés")

    # 5. propriétés
    data = [[typ, ids[(typ, value)], src, payload, ver, det]
            for (typ, value, src), (payload, det, ver) in prop_rows.items()]
    if data:
        client.insert("property", data, column_names=[
            "node_type", "id_node", "id_source", "payload", "version", "detection_date"])
    print(f"property : {len(prop_rows):,} propriétés envoyées, "
          f"{len(properties) - len(prop_rows):,} ignorées ou fusionnées")


if __name__ == "__main__":
    send(
        links=[
            {"value_1": "example.com", "value_2": "93.184.216.34",
             "type_1": "fqdn", "type_2": "ip", "id_source": 1,
             "update_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
        ],
        properties=[
            {"value": "example.com", "type": "fqdn", "id_source": 1,
             "payload": {"registrar": "IANA", "country": "US"},
             "version": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
        ],
    )
