#!/usr/bin/env python3
"""Import de fichiers réels vers les tables optimisées
(<type> pour chaque type de nœud, link — les tables naïves ne sont
plus alimentées).

Usage : make import FILE=<archive.zip | fichier | dossier>
        make import-resume   (CSV : reprend la distribution après un échec,
                              sans recharger les fichiers)

Fichiers reconnus (classification par nom) :
  - *node*.csv   : id;value;type;creation_date;rank        (';' + quotes)
  - *link*.csv   : id_node_1;id_node_2;type_1;type_2;id_source;creation_date;update_date
  - *propert*.csv : id_node;type;id_source;payload;version;detection_date
                   (';' + quotes, guillemets internes échappés en \\")
  - *.json/.json.gz : {"cn":..., "dns":[...]|null, "ip":...|null}  (JSONEachRow)

domains.json[.gz] — import INCRÉMENTAL, lot par lot (import_domains) :
  le fichier est lu par lots de IMPORT_BATCH_LINES lignes (100 000 par
  défaut) ; chaque lot est entièrement traité par ClickHouse avant de lire
  le suivant : nettoyage / validation (sql/05_import_normalize.sql ; un cn
  ou dns qui est une IP est typé ip ; wildcards, IP non routables, noms
  invalides rejetés et comptés), nœuds créés dans fqdn / ip, liens
  cn ↔ dns / cn ↔ ip écrits dans link (deux sens). fqdn, ip et link
  grossissent donc dès le premier lot, et la mémoire ne dépend que de la
  taille d'un lot.
  L'id d'une valeur existante est retrouvé dans fqdn_ids / ip_ids : tables
  valeur → id (moteur EmbeddedRocksDB, recherche directe par clé, sans
  relire fqdn), construites au premier import puis tenues à jour. Une
  valeur inconnue reçoit un nouvel id AUTO-INCRÉMENTÉ (max + 1, ...),
  rank 1000000.
  Reprise : chaque lot validé est enregistré dans import_state (lignes
  traitées, ids réservés) ; relancer la même commande reprend après le
  dernier lot validé. Un lot interrompu est rejoué à l'identique (mêmes
  ids ; lignes en double fusionnées par ReplacingMergeTree).

CSV — chargement en staging puis distribution (tient le milliard de
lignes) :
  1. extraction du .zip le cas échéant
  2. création du schéma optimisé si absent (équivalent de `make init`,
     jamais de DROP sur un schéma existant)
  3. création des tables de staging (sql/04_import_staging.sql)
  4. chargement brut de chaque fichier (gunzip à la volée si nécessaire),
     un seul INSERT en streaming via clickhouse-client
  5. distribution vers les tables optimisées :
     - node.csv → <type> selon le type (fqdn, ip, application,
       plugin, ... cf. NODE_TABLES), avec ses propres ids (distribute_nodes)
     - properties.csv → property, id_node déjà numérique, sans résolution
       (distribute_properties)
     - links.csv → distribute_links, en tranches (pour tenir en RAM sur une
       VM Docker modeste) : résolution valeur → id ; toute valeur absente
       de sa table reçoit un nouvel id AUTO-INCRÉMENTÉ ; chaque lien est
       inséré dans link DANS LES DEUX SENS, sauf les auto-liens. Chaque
       étape ne s'exécute que si sa table d'entrée existe encore :
       `make import-resume` reprend à l'étape interrompue.

link n'a pas de projection inverse : chaque lien est physiquement dupliqué
(A→B et B→A) pour qu'un simple filtre sur (type_1, id_1) retrouve les voisins
dans les deux sens. Toute future écriture sur link (update, suppression) doit
donc traiter les deux lignes ensemble pour rester cohérente.
"""
import http.client
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
import zipfile
import zlib
from pathlib import Path
from urllib.parse import urlencode

ROOT = Path(__file__).resolve().parent.parent
SQL_STAGING = ROOT / "sql" / "04_import_staging.sql"
SQL_NORMALIZE = ROOT / "sql" / "05_import_normalize.sql"
# tables de travail d'un import CSV (staging + intermédiaires de
# distribute_links) : leur présence indique un import interrompu, à reprendre
STAGING = ("stg_node", "stg_property", "stg_link", "tmp_node_map_done",
           "tmp_node_ids", "tmp_link_1", "tmp_link_2")

# Types de nœuds (ceux de l'Enum8 de link / property) → colonne id de leur
# table de valeurs (la table porte le nom du type). Seuls fqdn et ip ont un
# rank. Nouveau type : l'ajouter ici, à la fin de l'Enum8, et créer sa table
# dans sql/02_optimized.sql.
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
NEW_RANK = 1000000  # rank des nœuds sans rank connu (fin de ORDER BY rank)
TYPES_SQL = ", ".join(f"'{t}'" for t in NODE_TABLES)

USER, PASSWORD = "chuser", "Royal15Raccoon"
CLIENT = ["docker", "exec", "-i", "ch_container", "clickhouse-client",
          "--user", USER, "--password", PASSWORD]

# Interface HTTP (port exposé par docker-compose.yml) : import des JSON lot
# par lot, ~1 ms par requête au lieu de ~100 ms pour un `docker exec`.
HTTP_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
HTTP_PORT = int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123"))

# tolérance aux lignes malformées (données réelles). Pour les JSON, elle
# s'applique à chaque lot.
TOLER_SETTINGS = {"input_format_allow_errors_num": 1000,
                  "input_format_allow_errors_ratio": 0.001}
TOLER = [f"--{k}={v}" for k, v in TOLER_SETTINGS.items()]
# domains.json : ne PAS lire un objet JSON comme une chaîne (réglage actif
# par défaut) : sinon, après une ligne tronquée ("dns": [ jamais fermé), les
# lignes suivantes sont avalées comme éléments de dns jusqu'à la fin du bloc
# de lecture, puis rejetées ensemble — des milliers de lignes perdues.
JSON_SETTINGS = {**TOLER_SETTINGS, "input_format_json_read_objects_as_strings": 0}

# domains.json : lignes par lot (surchargeable via IMPORT_BATCH_LINES). Plus
# gros = plus rapide au total (moins de requêtes), progression moins fine.
BATCH_LINES = int(os.environ.get("IMPORT_BATCH_LINES", "100000"))
# tables valeur → id (EmbeddedRocksDB) des types alimentés par domains.json
LOOKUP = {"fqdn": "fqdn_ids", "ip": "ip_ids"}
# Requête d'un lot refusée faute de ressources (un lot = un bloc = une part,
# atomique : rien n'est inséré) → on attend et on la renvoie.
#   241 MEMORY_LIMIT_EXCEEDED, 252 TOO_MANY_PARTS (merges en retard)
RETRY_CODES = {"241", "252"}
RETRIES = 8

# Les tables de staging (stg_domain surtout) peuvent dépasser la limite de
# sécurité par défaut (50 Gio) : on lève le garde-fou pour les DROP du script,
# qui sont voulus (staging jetable, recréé à chaque import).
DROP_OK = ["--max_table_size_to_drop=0", "--max_partition_size_to_drop=0"]

# Garde-fous mémoire pour la résolution des liens en tranches (cf.
# distribute_links). Passés en ligne de commande car ces requêtes sont
# lancées une par une, hors du fichier SQL.
#   use_skip_indexes=0 : fqdn est triée sur (id_fqdn, value), donc
#   l'index ngram sur `value` n'élague rien pour une égalité — inutile de
#   charger ~2 Gio de filtres de Bloom pour un scan qui sera complet.
#   min_insert_block_size_* : un INSERT ... SELECT crée une part par bloc
#   (~1 M de lignes par défaut) ; sur des milliards de lignes, des blocs de
#   1 Gio évitent TOO_MANY_PARTS (cf. 05_import_normalize.sql).
MEM = ["--max_threads=1",
       "--max_memory_usage=11000000000",
       "--max_bytes_before_external_group_by=536870912",
       "--max_bytes_before_external_sort=536870912",
       "--use_skip_indexes=0",
       "--join_algorithm=full_sorting_merge",
       "--min_insert_block_size_rows=100000000",
       "--min_insert_block_size_bytes=1073741824"]
# Mêmes garde-fous pour les écritures dans fqdn_ids / ip_ids (EmbeddedRocksDB),
# mais avec les blocs d'insertion par défaut (1 M lignes / 256 Mio) : chaque
# bloc y est trié et sérialisé d'un seul tenant, un bloc de 1 Gio dépasse les
# 10 Gio (mesuré : 388 Mio de pic avec les blocs par défaut).
LOOKUP_MEM = [a for a in MEM if not a.startswith("--min_insert_block_size")]

# Nombre de tranches pour la distribution des liens : chaque requête ne traite
# que 1/N des lignes → l'empreinte mémoire reste bornée même sur une VM Docker
# à faible RAM. Surchargeable via l'environnement (IMPORT_LINK_SLICES).
LINK_SLICES = int(os.environ.get("IMPORT_LINK_SLICES", "16"))
# Tranches pour l'écriture des liens (jointures liens × correspondance) : les
# deux côtés sont PARTITIONNÉS par hash de la valeur, chaque jointure ne lit
# que sa partition — 1/N des liens contre 1/N de la correspondance, en
# jointure par hachage (seul le côté correspondance est en RAM). Plus de
# tranches = moins de RAM par requête, sans relecture supplémentaire.
JOIN_SLICES = int(os.environ.get("IMPORT_JOIN_SLICES", "64"))

FREE_WARN_GIB = 15


_progress_open = False  # ligne de progression en cours (terminal, sans \n)


def log(msg: str) -> None:
    global _progress_open
    if _progress_open:
        print(flush=True)
        _progress_open = False
    print(msg, flush=True)


def duration(s: float) -> str:
    s = int(s)
    if s >= 3600:
        return f"{s // 3600} h {s % 3600 // 60:02d} min"
    if s >= 60:
        return f"{s // 60} min {s % 60:02d} s"
    return f"{s} s"


def progress(done: int, total: int, lines: int, elapsed: float,
             end: bool = False) -> None:
    """Ligne de progression (réécrite sur place dans un terminal).
    done / total : octets lus du fichier BRUT (compressé si .gz)."""
    global _progress_open
    pct = 100 * done / total if total else 100.0
    rate = lines / elapsed if elapsed > 0 else 0
    msg = (f"  [{pct:5.1f} %] {lines:,} lignes · {rate:,.0f} lignes/s · "
           f"{duration(elapsed)}")
    if not end and done:
        msg += f" · reste ~{duration(elapsed * (total - done) / done)}"
    if sys.stdout.isatty():
        print("\r" + msg.ljust(100), end="\n" if end else "", flush=True)
        _progress_open = not end
    else:
        print(msg, flush=True)


def query(sql: str, mem=False, qid: str = "") -> str:
    """mem : False, True (réglages MEM) ou une liste de réglages."""
    if mem is True:
        mem = MEM
    r = subprocess.run(CLIENT + DROP_OK + (mem or [])
                       + ([f"--query_id={qid}"] if qid else []) + ["-q", sql],
                       capture_output=True, text=True, encoding="utf-8")
    if r.returncode != 0:
        # message du serveur visible (sinon perdu dans capture_output)
        log(r.stderr.strip())
        raise subprocess.CalledProcessError(r.returncode, sql[:200])
    return r.stdout.strip()


def exists(table: str) -> bool:
    return query(f"EXISTS TABLE {table}") == "1"


def run_sql_file(path: Path) -> None:
    with open(path, "rb") as f:
        subprocess.run(CLIENT + DROP_OK + ["--multiquery"], stdin=f, check=True)


def distribute_nodes() -> None:
    """node.csv (stg_node) → <type>, avec les ids fournis par le fichier.

    Un type absent de NODE_TABLES n'a pas de table : ses lignes sont ignorées
    (et comptées)."""
    if not exists("stg_node"):
        return
    counts = query("SELECT lower(node_type), count() FROM stg_node "
                   "WHERE value != '' GROUP BY 1 ORDER BY 1 FORMAT TSV")
    if not counts:
        query("DROP TABLE IF EXISTS stg_node")
        return
    log("Distribution des nœuds (node.csv)...")
    for line in counts.splitlines():
        typ, cnt = line.split("\t")
        if typ not in NODE_TABLES:
            log(f"  {typ or '(vide)'} : {int(cnt):,} lignes IGNORÉES (type inconnu)")
            continue
        idcol = NODE_TABLES[typ]
        ranked = typ in RANKED
        query(
            f"INSERT INTO {typ} (value, {idcol}, {'rank, ' if ranked else ''}version) "
            "SELECT value, toInt64OrZero(id), "
            + (f"if(toInt32OrZero(rank) = 0, {NEW_RANK}, toInt32OrZero(rank)), "
               if ranked else "")
            + "coalesce(toUnixTimestamp(parseDateTimeBestEffortOrNull(creation_date)), "
            "toUnixTimestamp(now())) "
            f"FROM stg_node WHERE lower(node_type) = '{typ}' AND value != ''",
            mem=True)
        if typ in LOOKUP and exists(LOOKUP[typ]):  # table valeur → id à jour
            query(f"INSERT INTO {LOOKUP[typ]} SELECT value, toInt64OrZero(id) "
                  f"FROM stg_node WHERE lower(node_type) = '{typ}' AND value != ''",
                  mem=LOOKUP_MEM)
        log(f"  {typ} : {int(cnt):,} lignes")
    query("DROP TABLE IF EXISTS stg_node")


def distribute_properties() -> None:
    """properties.csv (stg_property) → property.

    id_node est déjà l'id du nœud dans la table de son type : aucune
    résolution, pas de tranches (simple INSERT ... SELECT en streaming).
    L'existence du nœud n'est pas vérifiée. Un type hors NODE_TABLES (absent
    de l'Enum8) ou un id_node non numérique : ligne ignorée (et comptée).

    version (date de mise à jour) → timestamp Unix, now() si illisible ;
    detection_date illisible (dont la date zéro MySQL 0000-00-00) → date de
    version."""
    if not exists("stg_property"):
        return
    counts = query("SELECT lower(node_type), toInt64OrZero(id_node) != 0, count() "
                   "FROM stg_property GROUP BY 1, 2 ORDER BY 1, 2 FORMAT TSV")
    if not counts:
        query("DROP TABLE IF EXISTS stg_property")
        return
    log("Distribution des propriétés (properties.csv)...")
    for line in counts.splitlines():
        typ, valid, cnt = line.split("\t")
        if typ not in NODE_TABLES:
            log(f"  {typ or '(vide)'} : {int(cnt):,} lignes IGNORÉES (type inconnu)")
        elif valid != "1":
            log(f"  {typ} : {int(cnt):,} lignes IGNORÉES (id_node non numérique)")
    # date zéro MySQL : parseDateTimeBestEffortOrNull('0000-00-00 00:00:00')
    # ne renvoie PAS NULL (il renvoie le 1er janvier de l'année en cours)
    def date(col: str) -> str:
        return (f"if(startsWith({col}, '0000-00-00'), NULL, "
                f"parseDateTimeBestEffortOrNull({col}))")
    query(
        "INSERT INTO property "
        "(node_type, id_node, id_source, payload, detection_date, version) "
        "SELECT lower(node_type), toInt64OrZero(id_node), toInt32OrZero(id_source), "
        "payload, "
        f"coalesce({date('detection_date')}, {date('version')}, now()), "
        f"coalesce(toUnixTimestamp({date('version')}), toUnixTimestamp(now())) "
        f"FROM stg_property WHERE lower(node_type) IN ({TYPES_SQL}) "
        "AND toInt64OrZero(id_node) != 0",
        mem=True)
    for line in counts.splitlines():
        typ, valid, cnt = line.split("\t")
        if typ in NODE_TABLES and valid == "1":
            log(f"  {typ} : {int(cnt):,} propriétés")
    query("DROP TABLE IF EXISTS stg_property")


def build_node_map(n: int) -> None:
    """3a / 3a bis : table de correspondance (type, valeur) → id des valeurs
    citées par stg_link, en n tranches, avec création des nœuds
    inconnus (ids auto-incrémentés). Terminée, elle est renommée
    tmp_node_map_done : une reprise ne la reconstruit pas (et ne recrée pas
    de nœuds)."""
    # 3a — table de correspondance (type, valeur) → id, restreinte aux valeurs
    # citées par les liens, construite tranche par tranche.
    query("DROP TABLE IF EXISTS tmp_link_values")
    query("CREATE TABLE tmp_link_values (node_type String, value String) "
          "ENGINE = MergeTree ORDER BY (node_type, value)")
    query("DROP TABLE IF EXISTS tmp_node_map")
    query("CREATE TABLE tmp_node_map (node_type String, value String, id Int64) "
          "ENGINE = MergeTree ORDER BY (node_type, value)")

    t = time.monotonic()
    for k in range(n):
        query(
            "INSERT INTO tmp_link_values SELECT node_type, value FROM ("
            " SELECT lower(type_1) AS node_type, id_node_1 AS value FROM stg_link"
            f"  WHERE cityHash64(id_node_1) % {n} = {k}"
            " UNION ALL"
            " SELECT lower(type_2), id_node_2 FROM stg_link"
            f"  WHERE cityHash64(id_node_2) % {n} = {k}"
            f") WHERE value != '' AND node_type IN ({TYPES_SQL}) "
            "GROUP BY node_type, value", mem=True)
    # types réellement cités : inutile de parcourir les tables des autres
    present = set(query("SELECT DISTINCT node_type FROM tmp_link_values").split())
    types = [(typ, idcol) for typ, idcol in NODE_TABLES.items() if typ in present]
    for k in range(n):
        for typ, idcol in types:
            query(
                f"INSERT INTO tmp_node_map SELECT '{typ}', value, "
                f"argMax({idcol}, version) FROM {typ} "
                "WHERE value IN (SELECT value FROM tmp_link_values "
                f"WHERE node_type = '{typ}' AND cityHash64(value) % {n} = {k}) "
                "GROUP BY value", mem=True)
    log(f"  correspondance valeur → id construite en {time.monotonic() - t:.0f} s")

    # 3a bis — nouveaux ids pour les valeurs absentes de leur table <type> :
    # auto-incrément à partir du max(id) existant du type. Chaque tranche
    # numérote ses valeurs inconnues à la suite de la précédente (row_number()
    # + dernier id attribué), puis les nœuds créés (id > max initial) sont
    # insérés dans la table du type.
    t = time.monotonic()
    for typ, idcol in types:
        base = start = int(query(f"SELECT max({idcol}) FROM {typ}"))
        for k in range(n):
            query(
                f"INSERT INTO tmp_node_map SELECT '{typ}', value, "
                f"toInt64({base} + row_number() OVER ()) "
                "FROM tmp_link_values "
                f"WHERE node_type = '{typ}' AND cityHash64(value) % {n} = {k} "
                "AND value NOT IN (SELECT value FROM tmp_node_map "
                f"WHERE node_type = '{typ}' AND cityHash64(value) % {n} = {k})",
                mem=True)
            base = max(base, int(query(
                f"SELECT max(id) FROM tmp_node_map WHERE node_type = '{typ}'")))
        ranked = typ in RANKED
        query(
            f"INSERT INTO {typ} (value, {idcol}, {'rank, ' if ranked else ''}version) "
            f"SELECT value, id, {f'{NEW_RANK}, ' if ranked else ''}"
            "toUnixTimestamp(now()) "
            f"FROM tmp_node_map WHERE node_type = '{typ}' AND id > {start}",
            mem=True)
        if typ in LOOKUP and exists(LOOKUP[typ]):  # table valeur → id à jour
            query(f"INSERT INTO {LOOKUP[typ]} SELECT value, id FROM tmp_node_map "
                  f"WHERE node_type = '{typ}' AND id > {start}", mem=LOOKUP_MEM)
        log(f"  {base - start:,} nœuds {typ} créés "
            f"(ids {start + 1:,} → {base:,})" if base > start
            else f"  aucun nœud {typ} créé")
    log(f"  nouveaux ids attribués en {time.monotonic() - t:.0f} s")

    check(count("tmp_node_map") > 0, "correspondance valeur → id vide")
    query("RENAME TABLE tmp_node_map TO tmp_node_map_done")


def partitions(table: str) -> list:
    """Tranches (partitions h) restant à traiter dans une table tmp_*."""
    return [int(p) for p in query(
        "SELECT DISTINCT partition FROM system.parts WHERE active "
        f"AND database = currentDatabase() AND table = '{table}' "
        "ORDER BY toUInt32(partition)").split()]


def count(table: str, where: str = "") -> int:
    return int(query(f"SELECT count() FROM {table}"
                     + (f" WHERE {where}" if where else "")))


def check(ok: bool, msg: str) -> None:
    """Garde-fou : arrête l'import AVANT toute suppression de données dont
    on n'a pas vérifié la copie (les tables restent en place pour analyse,
    make import-resume reprend après correction)."""
    if not ok:
        raise RuntimeError(f"Garde-fou : {msg} — import arrêté, rien n'a été "
                           "supprimé.")


def to_ts(col: str) -> str:
    return (f"coalesce(toUnixTimestamp(parseDateTimeBestEffortOrNull({col})), "
            "toUnixTimestamp(now()))")


def distribute_links(n: int = LINK_SLICES, m: int = JOIN_SLICES) -> None:
    """Attribue les ids manquants puis résout stg_link → link.

    Valeurs traitées : les extrémités de stg_link (links.csv), pour tous les
    types de NODE_TABLES. Une valeur déjà présente dans la table <type>
    garde son id ; une valeur absente est créée avec un nouvel id
    AUTO-INCRÉMENTÉ à partir du max(id) existant du type (max + 1, max + 2,
    ...), rank = 1000000 pour fqdn / ip. Un type hors NODE_TABLES n'a pas de
    table : ses liens sont ignorés (et comptés).

    Suppose un seul import à la fois : deux imports concurrents liraient le
    même max(id) et attribueraient les mêmes ids.

    Étapes, chacune bornée en mémoire et reprenable (make import-resume) :
      1. correspondance valeur → id + nœuds créés (build_node_map, n
         tranches), puis tmp_node_ids : la même, PARTITIONNÉE en m tranches
         par cityHash64(valeur) ;
      2. tmp_link_1 : stg_link typé et nettoyé, partitionné par le hash de
         l'extrémité 1 ;
      3. pour chaque tranche k : tmp_link_1[k] ⋈ tmp_node_ids[k] → id_1,
         vers tmp_link_2, partitionnée par le hash de l'extrémité 2 ;
      4. pour chaque tranche k : tmp_link_2[k] ⋈ tmp_node_ids[k] → id_2,
         vers link, DANS LES DEUX SENS (pas de projection inverse sur link,
         cf. 02_optimized.sql), sauf les auto-liens.
    Chaque jointure ne voit que 1/m des liens et 1/m de la correspondance
    (jointure par hachage : seul ce 1/m de correspondance est en RAM). Une
    tranche terminée est supprimée (DROP PARTITION) : une reprise repart de
    la première tranche restante. Une tranche interrompue puis rejouée
    réinsère des lignes identiques, fusionnées par ReplacingMergeTree.
    """
    for tbl in ("tmp_node_ids_build", "tmp_link_1_build"):
        query(f"DROP TABLE IF EXISTS {tbl}")

    if exists("tmp_node_ids"):
        # reprise : le découpage doit rester celui des tables déjà construites
        m = int(query("SELECT comment FROM system.tables WHERE database = "
                      "currentDatabase() AND name = 'tmp_node_ids'")
                .removeprefix("slices="))
        log(f"Reprise de l'écriture des liens ({m} tranches)...")
        check(count("tmp_node_ids") > 0, "tmp_node_ids est vide")
    else:
        if not exists("stg_link"):
            return
        if count("stg_link") == 0:
            query("DROP TABLE IF EXISTS stg_link")
            return
        log("Résolution des nœuds et des liens → link...")
        log(f"  stg_link : {count('stg_link'):,} lignes")
        if exists("tmp_node_map_done"):
            log("  correspondance valeur → id déjà construite (reprise)")
        else:
            build_node_map(n)
        t = time.monotonic()
        query("CREATE TABLE tmp_node_ids_build (h UInt16, "
              "node_type LowCardinality(String), value String, id Int64) "
              "ENGINE = MergeTree PARTITION BY h ORDER BY (node_type, value) "
              f"COMMENT 'slices={m}'")
        query(f"INSERT INTO tmp_node_ids_build SELECT cityHash64(value) % {m}, "
              "node_type, value, id FROM tmp_node_map_done", mem=True)
        src, dst = count("tmp_node_map_done"), count("tmp_node_ids_build")
        check(src > 0 and dst == src,
              f"copie de la correspondance incomplète ({dst:,} / {src:,} lignes)")
        query("RENAME TABLE tmp_node_ids_build TO tmp_node_ids")
        for tbl in ("tmp_node_map_done", "tmp_link_values"):
            query(f"DROP TABLE IF EXISTS {tbl}")
        log(f"  correspondance : {dst:,} valeurs, découpée en {m} tranches en "
            f"{duration(time.monotonic() - t)}")

    if exists("stg_link"):
        t = time.monotonic()
        ok = (f"t1 IN ({TYPES_SQL}) AND t2 IN ({TYPES_SQL}) "
              "AND v1 != '' AND v2 != ''")
        raw = ("SELECT lower(type_1) AS t1, id_node_1 AS v1, lower(type_2) AS t2, "
               "id_node_2 AS v2, id_source, creation_date, update_date "
               "FROM stg_link")
        total, bad, auto = map(int, query(
            f"SELECT count(), countIf(NOT ({ok})), "
            f"countIf(({ok}) AND t1 = t2 AND v1 = v2) FROM ({raw}) "
            "FORMAT TSV", mem=True).split("\t"))
        log(f"  liens à résoudre : {total - bad - auto:,} / {total:,}")
        if bad:
            log(f"  liens ignorés (type inconnu ou valeur vide) : {bad:,}")
        if auto:
            log(f"  auto-liens ignorés (nœud lié à lui-même) : {auto:,}")
        query("CREATE TABLE tmp_link_1_build (h UInt16, "
              "t1 LowCardinality(String), v1 String, "
              "t2 LowCardinality(String), v2 String, id_source Int32, "
              "detection_date UInt64, version UInt64) "
              "ENGINE = MergeTree PARTITION BY h ORDER BY tuple()")
        query(f"INSERT INTO tmp_link_1_build SELECT cityHash64(v1) % {m}, "
              "t1, v1, t2, v2, toInt32OrZero(id_source), "
              f"{to_ts('creation_date')}, {to_ts('update_date')} "
              f"FROM ({raw}) WHERE {ok} AND NOT (t1 = t2 AND v1 = v2)",
              mem=True)
        dst = count("tmp_link_1_build")
        check(dst == total - bad - auto,
              f"copie des liens incomplète ({dst:,} / {total - bad - auto:,})")
        query("RENAME TABLE tmp_link_1_build TO tmp_link_1")
        query("DROP TABLE IF EXISTS stg_link")
        log(f"  liens découpés en {m} tranches en {duration(time.monotonic() - t)}")

    # jointure d'une tranche de liens avec la même tranche de correspondance
    def join(src: str, k: int, t: str, v: str) -> str:
        return (f"FROM (SELECT * FROM {src} WHERE h = {k}) AS l "
                "INNER JOIN (SELECT node_type, value, id FROM tmp_node_ids "
                f"WHERE h = {k}) AS m ON m.node_type = l.{t} AND m.value = l.{v} ")
    hash_join = " SETTINGS join_algorithm = 'hash'"

    if exists("tmp_link_1"):
        query("CREATE TABLE IF NOT EXISTS tmp_link_2 (h UInt16, "
              "t1 LowCardinality(String), id_1 Int64, "
              "t2 LowCardinality(String), v2 String, id_source Int32, "
              "detection_date UInt64, version UInt64) "
              "ENGINE = MergeTree PARTITION BY h ORDER BY tuple()")
        todo = partitions("tmp_link_1")
        log(f"Liens, extrémité 1 → id : {len(todo)} tranches restantes...")
        t = time.monotonic()
        for i, k in enumerate(todo, 1):
            src, before = count("tmp_link_1", f"h = {k}"), count("tmp_link_2")
            query(f"INSERT INTO tmp_link_2 SELECT cityHash64(l.v2) % {m}, l.t1, "
                  "m.id, l.t2, l.v2, l.id_source, l.detection_date, l.version "
                  + join("tmp_link_1", k, "t1", "v1") + hash_join, mem=True)
            # la correspondance a été construite à partir des extrémités des
            # liens (une valeur = un id) : chaque lien doit être résolu
            got = count("tmp_link_2") - before
            check(got == src, f"tranche {k} : {got:,} liens résolus sur {src:,} "
                  "(extrémité 1)")
            query(f"ALTER TABLE tmp_link_1 DROP PARTITION {k}")
            el = time.monotonic() - t
            log(f"  tranche {i}/{len(todo)} · {duration(el)} · "
                f"reste ~{duration(el / i * (len(todo) - i))}")
        query("DROP TABLE tmp_link_1")

    if exists("tmp_link_2"):
        todo = partitions("tmp_link_2")
        log(f"Liens, extrémité 2 → id, écriture dans link (2 sens) : "
            f"{len(todo)} tranches restantes...")
        t = time.monotonic()
        for i, k in enumerate(todo, 1):
            src, before = count("tmp_link_2", f"h = {k}"), count("link")
            query("INSERT INTO link "
                  "(type_1, id_1, type_2, id_2, id_source, detection_date, version) "
                  "SELECT d.1, d.2, d.3, d.4, id_source, detection_date, version "
                  "FROM (SELECT arrayJoin([(toString(l.t1), l.id_1, toString(l.t2), m.id), "
                  "(toString(l.t2), m.id, toString(l.t1), l.id_1)]) AS d, "
                  "l.id_source AS id_source, l.detection_date AS detection_date, "
                  "l.version AS version "
                  + join("tmp_link_2", k, "t2", "v2")
                  + "WHERE NOT (l.t1 = l.t2 AND l.id_1 = m.id))" + hash_join,
                  mem=True)
            # link est une ReplacingMergeTree (ses merges peuvent retirer des
            # doublons en parallèle) : contrôle grossier, rien d'écrit = arrêt
            check(src == 0 or count("link") > before,
                  f"tranche {k} : aucun lien écrit dans link sur {src:,}")
            query(f"ALTER TABLE tmp_link_2 DROP PARTITION {k}")
            el = time.monotonic() - t
            log(f"  tranche {i}/{len(todo)} · {duration(el)} · "
                f"reste ~{duration(el / i * (len(todo) - i))}")
        query("DROP TABLE tmp_link_2")

    query("DROP TABLE IF EXISTS tmp_node_ids")


def distribute(resume: bool = False) -> None:
    """staging CSV → tables optimisées. Chaque étape ne s'exécute que si sa table
    d'entrée existe encore (elle la supprime une fois finie) : après un
    échec, resume=True reprend à l'étape interrompue."""
    log("Distribution vers les tables optimisées...")
    t = time.monotonic()
    # Les tables de staging chargées ne sont plus que lues : on suspend leurs
    # merges, qui concurrencent la distribution en mémoire. PAS de SYSTEM STOP MERGES global : les tables
    # écrites ici (stg_link, link, fqdn...) reçoivent des milliards de lignes
    # et finissent en TOO_MANY_PARTS si leurs parts ne sont pas fusionnées.
    for tbl in ("stg_node", "stg_property"):
        if exists(tbl):
            query(f"SYSTEM STOP MERGES {tbl}")
    try:
        query("SYSTEM DROP MARK CACHE")
        query("SYSTEM DROP UNCOMPRESSED CACHE")
        distribute_nodes()  # node.csv → <type>
        distribute_properties()  # properties.csv → property
        distribute_links()  # links.csv : ids auto-incrémentés + liens
    finally:
        query("SYSTEM START MERGES")
    log(f"  fait en {time.monotonic() - t:.0f} s")


def print_counts(t0: float) -> None:
    log("\nCompteurs après import :")
    counts = query(
        "SELECT * FROM ("
        + " UNION ALL ".join(f"SELECT '{t}' AS tbl, count() AS n FROM {t}"
                             for t in NODE_TABLES)
        + " UNION ALL SELECT 'link', count() FROM link"
        " UNION ALL SELECT 'property', count() FROM property"
        ") ORDER BY tbl FORMAT PrettyCompactMonoBlock")
    log(counts)
    log(f"\nImport terminé en {time.monotonic() - t0:.0f} s "
        "(dédup ReplacingMergeTree asynchrone en arrière-plan).")


def iter_blocks(path: Path):
    """Itère les blocs du fichier, décompressés à la volée si gzip, sous la
    forme (bloc, octets lus du fichier brut) — pour la progression.

    Tolérant aux archives .gz tronquées : tout ce qui est lisible est
    émis (contrairement à gzip.GzipFile.read qui perd le bloc en cours),
    puis un avertissement est affiché."""
    with open(path, "rb") as f:
        magic = f.read(2)
        f.seek(0)
        if magic != b"\x1f\x8b":
            while True:
                buf = f.read(1024 * 1024)
                if not buf:
                    break
                yield buf, f.tell()
            return
        d = zlib.decompressobj(31)  # 16 + 15 = conteneur gzip
        truncated = False
        while True:
            buf = f.read(1024 * 1024)
            if not buf:
                break
            try:
                out = d.decompress(buf)
            except zlib.error:
                truncated = True
                break
            if out:
                yield out, f.tell()
        if not d.eof:
            truncated = True
        if truncated:
            log(f"  ⚠️  ARCHIVE TRONQUÉE : {path.name}\n"
                "     → les données de fin de fichier sont perdues.\n"
                "     → re-télécharge le fichier si possible "
                "(gzip -t pour vérifier).")


def iter_line_chunks(path: Path, n: int, skip: int = 0):
    """Découpe le fichier (décompressé à la volée) en lots de n lignes
    complètes, après avoir sauté les `skip` premières lignes (reprise).
    Émet (lot en bytes, nb de lignes, octets lus du fichier brut).
    Seuls un bloc de 1 Mio et un lot sont en mémoire à la fois."""
    pending = b""
    lines = []
    pos = 0
    for block, pos in iter_blocks(path):
        if skip:  # lignes déjà importées : on compte les \n sans découper
            c = block.count(b"\n")
            if c < skip:
                skip -= c
                continue
            i = -1
            for _ in range(skip):
                i = block.index(b"\n", i + 1)
            block, skip = block[i + 1:], 0
        parts = (pending + block).split(b"\n")
        pending = parts.pop()  # dernière ligne, incomplète
        lines.extend(parts)
        while len(lines) >= n:
            yield b"\n".join(lines[:n]) + b"\n", n, pos
            del lines[:n]
    if pending.strip():
        lines.append(pending)
    if lines:
        yield b"\n".join(lines) + b"\n", len(lines), pos


class Http:
    """Requêtes via l'interface HTTP de ClickHouse (connexion keep-alive
    réutilisée d'une requête à l'autre).

    ClickHouse ferme une connexion inactive après keep_alive_timeout (30 s
    par défaut) : une connexion restée inactive plus de IDLE s est rouverte
    avant la requête, et une connexion réutilisée coupée malgré tout est
    rouverte et la requête renvoyée une fois (les étapes d'un lot sont
    rejouables)."""

    IDLE = 5  # s

    def __init__(self):
        self.headers = {"X-ClickHouse-User": USER,
                        "X-ClickHouse-Key": PASSWORD,
                        "Content-Type": "application/octet-stream"}
        self.conn = None
        self.last = 0.0  # fin de la dernière réponse
        self.written = 0  # lignes écrites par la dernière requête

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def _send(self, path: str, body: bytes):
        if self.conn is None:
            self.conn = http.client.HTTPConnection(HTTP_HOST, HTTP_PORT,
                                                   timeout=3600)
        self.conn.request("POST", path, body, self.headers)
        r = self.conn.getresponse()
        msg = r.read().decode("utf-8", "replace").strip()
        self.last = time.monotonic()
        return r, msg

    def run(self, sql: str, data: bytes = None, **settings) -> str:
        """Exécute sql (ou `INSERT ... FORMAT x` avec data en corps)."""
        params = {"wait_end_of_query": 1, **settings}
        if data is None:
            body = sql.encode()
        else:
            params["query"] = sql
            body = data
        path = "/?" + urlencode(params)
        for attempt in range(RETRIES):
            if self.conn is not None and time.monotonic() - self.last > self.IDLE:
                self.close()  # probablement déjà fermée par le serveur
            reused = self.conn is not None
            try:
                try:
                    r, msg = self._send(path, body)
                except (OSError, http.client.HTTPException):
                    if not reused:
                        raise
                    self.close()  # connexion keep-alive fermée côté serveur
                    r, msg = self._send(path, body)
            except (OSError, http.client.HTTPException) as e:
                self.close()
                raise RuntimeError(
                    f"ClickHouse HTTP injoignable ({HTTP_HOST}:{HTTP_PORT}) : {e}\n"
                    "  → ClickHouse tourne-t-il ? port 8123 exposé ? "
                    "(docker-compose.yml)") from e
            if r.status == 200:
                summary = json.loads(r.getheader("X-ClickHouse-Summary") or "{}")
                self.written = int(summary.get("written_rows", 0))
                return msg
            code = r.getheader("X-ClickHouse-Exception-Code", "")
            if code not in RETRY_CODES:
                raise RuntimeError(f"Requête refusée (HTTP {r.status}) : "
                                   f"{msg[:2000]}\n  requête : {sql[:300]}")
            wait = min(60, 2 ** attempt)
            log(f"  requête refusée (code {code}), nouvel essai dans {wait} s : "
                f"{msg.splitlines()[0][:200] if msg else ''}")
            time.sleep(wait)
        raise RuntimeError(f"Requête refusée après {RETRIES} essais : {msg[:2000]}")


def query_with_progress(sql: str, total: int, mem=True) -> None:
    """Longue requête INSERT ... SELECT (clickhouse-client) de `total` lignes,
    avec progression sur les lignes ÉCRITES lue dans system.processes (la
    lecture peut avoir beaucoup d'avance sur l'écriture)."""
    qid = f"import_{uuid.uuid4().hex}"
    err = []

    def target():
        try:
            query(sql, mem=mem, qid=qid)
        except Exception as e:  # relancée dans le thread principal
            err.append(e)

    th = threading.Thread(target=target)
    th.start()
    t0 = time.monotonic()
    while th.is_alive():
        th.join(5)
        row = query("SELECT written_rows FROM system.processes "
                    f"WHERE query_id = '{qid}'")
        if row:
            progress(int(row), total, int(row), time.monotonic() - t0)
    if err:
        raise err[0]
    log(f"  fait en {duration(time.monotonic() - t0)}")


def build_lookups() -> None:
    """fqdn_ids / ip_ids : tables valeur → id (EmbeddedRocksDB, clé = valeur)
    construites une fois depuis fqdn / ip, puis tenues à jour par chaque
    import. Construites sous un nom provisoire puis renommées : une table
    présente est toujours complète."""
    for typ, lk in LOOKUP.items():
        if exists(lk):
            continue
        n = count(typ)
        log(f"Construction de {lk} (valeur → id) depuis {typ} : {n:,} lignes "
            "(une seule fois)...")
        query(f"DROP TABLE IF EXISTS {lk}_build")
        query(f"CREATE TABLE {lk}_build (value String, id Int64) "
              "ENGINE = EmbeddedRocksDB PRIMARY KEY value")
        query_with_progress(f"INSERT INTO {lk}_build SELECT value, {NODE_TABLES[typ]} "
                            f"FROM {typ}", n, mem=LOOKUP_MEM)
        query(f"RENAME TABLE {lk}_build TO {lk}")


def sql_str(v: str) -> str:
    return "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"


# Un lot de domains.json, une fois dans stg_domain puis normalisé dans
# stg_domain_norm (05_import_normalize.sql) :
#   valeurs retenues (fqdn / ip) du lot
DOM_VALUES = """
INSERT INTO dom_values
SELECT DISTINCT t.1, t.2
FROM (SELECT arrayJoin(arrayConcat([ip, cn], dns)) AS t FROM stg_domain_norm)
WHERE t.1 IN ('fqdn', 'ip')"""
#   liens cn ↔ dns et cn ↔ ip, résolus par dom_map (valeur → id du lot),
#   écrits dans les DEUX sens, sans auto-liens
DOM_LINKS = """
INSERT INTO link (type_1, id_1, type_2, id_2, id_source, detection_date, version)
SELECT l.1, l.2, l.3, l.4, 0, toUnixTimestamp(now()), toUnixTimestamp(now())
FROM (
    SELECT arrayJoin([(p.t1, a.id, p.t2, b.id), (p.t2, b.id, p.t1, a.id)]) AS l
    FROM (
        SELECT DISTINCT t1, v1, t2, v2 FROM (
            SELECT cn.1 AS t1, cn.2 AS v1, x.1 AS t2, x.2 AS v2
            FROM (SELECT cn, arrayJoin(dns) AS x FROM stg_domain_norm
                  WHERE cn.1 IN ('fqdn', 'ip'))
            WHERE x.1 IN ('fqdn', 'ip') AND x != cn
            UNION ALL
            SELECT cn.1, cn.2, 'ip', ip.2 FROM stg_domain_norm
            WHERE cn.1 IN ('fqdn', 'ip') AND ip.1 = 'ip' AND ip != cn)
    ) AS p
    INNER JOIN dom_map AS a ON a.node_type = p.t1 AND a.value = p.v1
    INNER JOIN dom_map AS b ON b.node_type = p.t2 AND b.value = p.v2
    WHERE NOT (p.t1 = p.t2 AND a.id = b.id))"""
#   valeurs rejetées par la validation, par champ et par raison
DOM_REJECTS = """
SELECT field, reason, count() FROM (
    SELECT 'ip' AS field, ip.1 AS reason FROM stg_domain_norm
    UNION ALL SELECT 'cn', cn.1 FROM stg_domain_norm
    UNION ALL SELECT 'dns', arrayJoin(dns).1 FROM stg_domain_norm)
WHERE startsWith(reason, 'x_') GROUP BY 1, 2"""
DOM_TABLES = {
    "stg_domain": "cn Nullable(String), dns Array(Nullable(String)), "
                  "ip Nullable(String)",
    "stg_domain_norm": "ip Tuple(String, String), cn Tuple(String, String), "
                       "dns Array(Tuple(String, String))",
    "dom_values": "node_type String, value String",
    "dom_map": "node_type String, value String, id Int64",
    "dom_new": "node_type String, value String, id Int64",
}


def import_domains(path: Path, n: int = BATCH_LINES) -> None:
    """domains.json[.gz] → fqdn / ip / link, lot par lot (cf. docstring du
    module). Relancer la même commande reprend après le dernier lot validé."""
    h = Http()
    q = h.run
    key = f"{path.name}:{path.stat().st_size}"  # identifie le fichier

    def state(k: str) -> int:
        v = q(f"SELECT value FROM import_state WHERE key = {sql_str(k)}")
        return int(v) if v else 0

    def save(**kv) -> None:
        q("INSERT INTO import_state VALUES "
          + ", ".join(f"({sql_str(k)}, {v})" for k, v in kv.items()))

    def tsv(sql: str) -> list:
        return [line.split("\t") for line in q(sql + " FORMAT TSV").splitlines()]

    try:
        q("CREATE TABLE IF NOT EXISTS import_state (key String, value Int64) "
          "ENGINE = EmbeddedRocksDB PRIMARY KEY key")
        if state(f"done:{key}"):
            log(f"  déjà importé entièrement ({state(f'lines:{key}'):,} lignes) : "
                "ignoré.\n  → pour le réimporter : DELETE FROM import_state "
                f"WHERE key LIKE '%:{key}'")
            return
        build_lookups()
        for tbl, cols in DOM_TABLES.items():  # tables d'un lot, en mémoire
            q(f"CREATE OR REPLACE TABLE {tbl} ({cols}) ENGINE = Memory")
        normalize = SQL_NORMALIZE.read_text(encoding="utf-8").strip().rstrip(";")

        # alloc : plus grand id réservé (enregistré AVANT d'écrire les nœuds)
        # nodes : plus grand id dont le nœud est écrit dans sa table
        # Un lot interrompu entre les deux est rejoué : ses nœuds (ids entre
        # nodes et alloc) sont retrouvés dans fqdn_ids / ip_ids et réécrits.
        alloc, nodes = {}, {}
        for typ in LOOKUP:
            top = int(q(f"SELECT max({NODE_TABLES[typ]}) FROM {typ}"))
            alloc[typ] = max(top, state(f"alloc:{typ}"))
            nodes[typ] = max(top, state(f"nodes:{typ}"))
        done = state(f"lines:{key}")
        if done:
            log(f"  reprise après {done:,} lignes déjà importées "
                "(lecture du début du fichier pour les sauter)...")
    except Exception:
        h.close()
        raise

    created = dict.fromkeys(LOOKUP, 0)
    links = lines = unreadable = 0
    rejects = {}
    total = path.stat().st_size
    t0 = last = time.monotonic()
    every = 0 if sys.stdout.isatty() else 30
    pos = 0

    def show(end: bool = False) -> None:
        global _progress_open
        el = time.monotonic() - t0
        pct = 100 * pos / total if total else 100.0
        msg = (f"  [{pct:5.1f} %] {done:,} lignes · fqdn +{created['fqdn']:,} · "
               f"ip +{created['ip']:,} · liens +{links:,} · "
               f"{lines / el if el else 0:,.0f} lignes/s")
        if not end and first and pos > first[1]:
            # estimation sur ce qui a été traité pendant CETTE exécution
            # (une reprise saute le début du fichier sans le traiter)
            t1, p1 = first
            msg += (f" · reste ~"
                    f"{duration((time.monotonic() - t1) * (total - pos) / (pos - p1))}")
        if sys.stdout.isatty():
            print("\r" + msg.ljust(110), end="\n" if end else "", flush=True)
            _progress_open = not end
        else:
            print(msg, flush=True)

    first = None  # (instant, position) à la fin du premier lot de cette exécution
    try:
        for body, cnt, pos in iter_line_chunks(path, n, skip=done):
            if not lines:
                t0 = time.monotonic()  # sans le temps passé à sauter le début
            for tbl in DOM_TABLES:
                q(f"TRUNCATE TABLE {tbl}")
            q("INSERT INTO stg_domain FORMAT JSONEachRow", body, **JSON_SETTINGS)
            blank = sum(1 for line in body.split(b"\n")[:-1] if not line.strip())
            unreadable += cnt - blank - h.written
            q(normalize)
            q(DOM_VALUES)
            # ids existants : recherche directe par clé dans fqdn_ids / ip_ids
            for typ, lk in LOOKUP.items():
                q(f"INSERT INTO dom_map SELECT '{typ}', v.value, r.id "
                  f"FROM (SELECT value FROM dom_values WHERE node_type = '{typ}') AS v "
                  f"INNER JOIN {lk} AS r ON r.value = v.value "
                  "SETTINGS join_algorithm = 'direct'")
            # valeurs inconnues : nouveaux ids à la suite du plus grand connu
            seen = dict(tsv("SELECT node_type, max(id) FROM dom_map GROUP BY node_type"))
            for typ in LOOKUP:
                base = max(alloc[typ], int(seen.get(typ, 0)))
                q(f"INSERT INTO dom_new SELECT '{typ}', value, "
                  f"toInt64({base} + row_number() OVER (ORDER BY value)) "
                  f"FROM dom_values WHERE node_type = '{typ}' AND value NOT IN "
                  f"(SELECT value FROM dom_map WHERE node_type = '{typ}')")
            new = {t: int(mx) for t, mx in tsv(
                "SELECT node_type, max(id) FROM dom_new GROUP BY node_type")}
            if new:
                for typ, mx in new.items():
                    alloc[typ] = max(alloc[typ], mx)
                save(**{f"alloc:{t}": alloc[t] for t in LOOKUP})  # réservation
                for typ in new:
                    q(f"INSERT INTO {LOOKUP[typ]} SELECT value, id FROM dom_new "
                      f"WHERE node_type = '{typ}'")
                q("INSERT INTO dom_map SELECT * FROM dom_new")
            # nœuds pas encore écrits (créés par ce lot, ou par un essai
            # interrompu de ce même lot)
            for typ in LOOKUP:
                q(f"INSERT INTO {typ} (value, {NODE_TABLES[typ]}, rank, version) "
                  f"SELECT value, id, {NEW_RANK}, toUnixTimestamp(now()) FROM dom_map "
                  f"WHERE node_type = '{typ}' AND id > {nodes[typ]}")
                created[typ] += h.written
            q(DOM_LINKS)
            links += h.written
            for field, reason, c in tsv(DOM_REJECTS):
                rejects[(field, reason)] = rejects.get((field, reason), 0) + int(c)
            # lot validé
            nodes = dict(alloc)
            done += cnt
            lines += cnt
            save(**{f"lines:{key}": done}, **{f"nodes:{t}": nodes[t] for t in LOOKUP})
            now = time.monotonic()
            if first is None:
                first = (now, pos)
            if now - last >= every:
                show()
                last = now
        show(end=True)
        save(**{f"done:{key}": 1})
    except BaseException:
        log(f"\nImport de {path.name} interrompu : {done:,} lignes validées "
            "(fqdn / ip / link à jour jusque-là).\n  → relancer la même commande "
            "reprend après le dernier lot validé.")
        raise
    finally:
        try:
            for tbl in DOM_TABLES:
                q(f"DROP TABLE IF EXISTS {tbl}")
        except Exception:
            pass
        h.close()
        if unreadable:
            log(f"Lignes JSON illisibles, ignorées : {unreadable:,}")
        if rejects:
            log("Valeurs rejetées (ni nœud ni lien) :")
            for (field, reason), c in sorted(rejects.items()):
                log(f"  {field:<3} {reason[2:]:<12} : {c:,}")


def classify(path: Path):
    """Retourne ('stg_node'|'stg_link'|'stg_property'|'domains', format,
    settings) ou None."""
    n = path.name.lower()
    csv_settings = ["--format_csv_delimiter=;",
                    "--input_format_csv_skip_first_lines=1"]
    # properties.csv échappe les guillemets du payload par backslash (\"),
    # que le format CSV de ClickHouse ne comprend pas (il attend "") : chaque
    # champ est lu comme une chaîne JSON ("..." avec échappements \).
    # En-tête sauté, colonnes prises dans l'ordre (pas par nom).
    property_settings = ["--format_custom_escaping_rule=JSON",
                         "--format_custom_field_delimiter=;",
                         "--input_format_with_names_use_header=0"]
    # testé avant node / link : "node_properties.csv" est un fichier de propriétés
    if n.endswith(".csv") and "propert" in n:
        return "stg_property", "CustomSeparatedWithNames", property_settings
    if n.endswith(".csv") and "node" in n:
        return "stg_node", "CSV", csv_settings
    if n.endswith(".csv") and ("link" in n or "edge" in n):
        return "stg_link", "CSV", csv_settings
    if ".json" in n:
        return "domains", "JSONEachRow", []
    return None


def collect_files(src: Path):
    """Extrait un zip si besoin et retourne (fichiers, dossier_temporaire)."""
    tmp = None
    if src.suffix.lower() == ".zip":
        tmp = Path(tempfile.mkdtemp(prefix="import_", dir=ROOT))
        log(f"Extraction de {src.name} → {tmp}")
        with zipfile.ZipFile(src) as z:
            z.extractall(tmp)
        files = [p for p in sorted(tmp.rglob("*")) if p.is_file()
                 and "__MACOSX" not in p.parts]
    elif src.is_dir():
        files = [p for p in sorted(src.rglob("*")) if p.is_file()]
    else:
        files = [src]
    return files, tmp


def main() -> None:
    if len(sys.argv) != 2 or not sys.argv[1]:
        sys.exit("Usage : make import FILE=<archive.zip|fichier|dossier>\n"
                 "        make import-resume")
    resume = sys.argv[1] == "--resume"
    src = None if resume else Path(sys.argv[1]).expanduser().resolve()
    if src is not None and not src.exists():
        sys.exit(f"Fichier introuvable : {src}")

    free = shutil.disk_usage(ROOT).free / 2**30
    if free < FREE_WARN_GIB:
        sys.exit(f"Espace disque insuffisant ({free:.0f} Gio libres).")
    log(f"Disque libre : {free:.0f} Gio")

    # ClickHouse doit être là
    try:
        subprocess.run(CLIENT + ["-q", "SELECT 1"],
                       capture_output=True, check=True, text=True)
    except subprocess.CalledProcessError as e:
        sys.exit("ClickHouse injoignable.\n"
                 f"  {e.stderr.strip() or e}\n"
                 "  → vérifie le conteneur : docker ps | grep ch_container\n"
                 "  → puis relance-le si besoin : make up")

    # Schéma optimisé requis : créé ici si `make init` n'a jamais été lancé.
    # (02_optimized.sql DROP + CREATE : on ne l'exécute que si la table est
    # absente, jamais sur un schéma existant — l'import reste idempotent.)
    if query("EXISTS TABLE fqdn") != "1":
        log("Tables optimisées absentes → création du schéma "
            "(sql/02_optimized.sql)...")
        run_sql_file(ROOT / "sql" / "02_optimized.sql")

    if resume:
        present = [t for t in STAGING if exists(t)]
        if not present:
            sys.exit("Rien à reprendre : aucune table de staging "
                     "(import terminé, ou jamais lancé).")
        log("Reprise, tables de staging présentes :")
        for t in present:
            log(f"  {t} : {int(query(f'SELECT count() FROM {t}')):,} lignes")
        t0 = time.monotonic()
        distribute(resume=True)
        print_counts(t0)
        return

    files, tmp = collect_files(src)
    try:
        jobs = []
        for f in files:
            c = classify(f)
            if c is None:
                log(f"  ignoré (type non reconnu) : {f.name}")
                continue
            jobs.append((f, *c))
        if not jobs:
            sys.exit("Aucun fichier importable trouvé "
                     "(attendus : *node*.csv, *link*.csv, *propert*.csv, "
                     "*.json[.gz]).")

        t0 = time.monotonic()
        csv_jobs = [j for j in jobs if j[1] != "domains"]
        if csv_jobs:
            log("Création des tables de staging...")
            run_sql_file(SQL_STAGING)
        for f, table, fmt, settings in csv_jobs:
            size = f.stat().st_size / 2**20
            log(f"Chargement {f.name} ({size:.1f} Mio) → {table} [{fmt}]...")
            t = time.monotonic()
            cmd = CLIENT + TOLER + settings \
                + ["--query", f"INSERT INTO {table} FORMAT {fmt}"]
            # ATTENTION : on ne peut PAS passer un gzip.GzipFile en
            # stdin= de subprocess — il a un fileno() qui pointe vers le
            # fichier BRUT compressé, la décompression serait contournée.
            # On copie donc les blocs (décompressés) dans un vrai pipe.
            proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
            # CustomSeparated attend exactement '\n' en fin de ligne (le CSV
            # tolère '\r\n', pas lui) : on retire les '\r' des fichiers de
            # propriétés. Sans risque : un '\r' dans une valeur y est échappé
            # (texte \r), un '\r' brut ne peut être qu'une fin de ligne.
            strip_cr = table == "stg_property"
            try:
                for block, _ in iter_blocks(f):
                    proc.stdin.write(block.replace(b"\r", b"") if strip_cr
                                     else block)
                proc.stdin.close()
            except BrokenPipeError:
                pass  # le client a échoué, on récupère le code retour
            if proc.wait() != 0:
                raise subprocess.CalledProcessError(proc.returncode, cmd)
            n = query(f"SELECT count() FROM {table}")
            log(f"  {int(n):,} lignes en staging en {time.monotonic() - t:.0f} s")

        if csv_jobs:
            try:
                distribute()
            except (subprocess.CalledProcessError, RuntimeError):
                log("\nDistribution interrompue. Les données chargées sont "
                    "conservées en staging :\n  → après correction, reprendre "
                    "sans recharger : make import-resume")
                raise
        # domains.json après les CSV : node.csv a pu créer des nœuds
        for f, table, fmt, settings in jobs:
            if table == "domains":
                log(f"Import de {f.name} ({f.stat().st_size / 2**20:.1f} Mio) "
                    f"par lots de {BATCH_LINES:,} lignes...")
                import_domains(f)
        print_counts(t0)
    finally:
        if tmp is not None:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
