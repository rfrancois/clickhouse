#!/usr/bin/env python3
"""Import de fichiers réels vers les tables optimisées
(<type> pour chaque type de nœud, link — les tables naïves ne sont
plus alimentées).

Usage : make import FILE=<archive.zip | fichier | dossier>

Fichiers reconnus (classification par nom) :
  - *node*.csv   : id;value;type;creation_date;rank        (';' + quotes)
  - *link*.csv   : id_node_1;id_node_2;type_1;type_2;id_source;creation_date;update_date
  - *propert*.csv : id_node;type;id_source;payload;version;detection_date
                   (';' + quotes, guillemets internes échappés en \\")
  - *.json/.json.gz : {"cn":..., "dns":[...]|null, "ip":...|null}  (JSONEachRow)

Pipeline (aucun parsing Python : streaming direct vers clickhouse-client,
tient le milliard de lignes) :
  1. extraction du .zip le cas échéant
  2. création du schéma optimisé si absent (équivalent de `make init`,
     jamais de DROP sur un schéma existant)
  3. création des tables de staging (sql/04_import_staging.sql)
  4. chargement brut de chaque fichier (FORMAT CSV / JSONEachRow, gunzip
     à la volée si nécessaire)
  5. distribution vers les tables optimisées :
     - node.csv → <type> selon le type (fqdn, ip, application,
       plugin, ... cf. NODE_TABLES), avec ses propres ids (distribute_nodes)
     - properties.csv → property, id_node déjà numérique, sans résolution
       (distribute_properties)
     - domains.json → valeurs (stg_value) et liens cn ↔ dns / cn ↔ ip
       (stg_link), sans id (sql/05_import_distribute.sql)
     - puis distribute_links, en N tranches (pour tenir en RAM sur une VM
       Docker modeste) : résolution valeur → id ; toute valeur absente de
       sa table reçoit un nouvel id AUTO-INCRÉMENTÉ à partir du max(id) du
       type (rank 1000000 pour fqdn/ip) ; chaque lien est inséré dans link
       DANS LES DEUX SENS

link n'a pas de projection inverse : chaque lien est physiquement dupliqué
(A→B et B→A) pour qu'un simple filtre sur (type_1, id_1) retrouve les voisins
dans les deux sens. Toute future écriture sur link (update, suppression) doit
donc traiter les deux lignes ensemble pour rester cohérente.
"""
import os
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SQL_STAGING = ROOT / "sql" / "04_import_staging.sql"
SQL_DISTRIBUTE = ROOT / "sql" / "05_import_distribute.sql"

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

CLIENT = ["docker", "exec", "-i", "ch_container", "clickhouse-client",
          "--user", "chuser", "--password", "Royal15Raccoon"]

# tolérance aux lignes malformées (données réelles)
TOLER = ["--input_format_allow_errors_num=1000",
         "--input_format_allow_errors_ratio=0.001"]

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
MEM = ["--max_threads=1",
       "--max_memory_usage=11000000000",
       "--max_bytes_before_external_group_by=536870912",
       "--max_bytes_before_external_sort=536870912",
       "--use_skip_indexes=0",
       "--join_algorithm=full_sorting_merge"]

# Nombre de tranches pour la distribution des liens : chaque requête ne traite
# que 1/N des lignes → l'empreinte mémoire reste bornée même sur une VM Docker
# à faible RAM. Surchargeable via l'environnement (IMPORT_LINK_SLICES).
LINK_SLICES = int(os.environ.get("IMPORT_LINK_SLICES", "16"))

FREE_WARN_GIB = 15


def log(msg: str) -> None:
    print(msg, flush=True)


def query(sql: str, mem: bool = False) -> str:
    r = subprocess.run(CLIENT + (MEM if mem else []) + ["-q", sql],
                       capture_output=True, text=True, check=True)
    return r.stdout.strip()


def run_sql_file(path: Path) -> None:
    with open(path, "rb") as f:
        subprocess.run(CLIENT + DROP_OK + ["--multiquery"], stdin=f, check=True)


def distribute_nodes() -> None:
    """node.csv (stg_node) → <type>, avec les ids fournis par le fichier.

    Un type absent de NODE_TABLES n'a pas de table : ses lignes sont ignorées
    (et comptées)."""
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


def distribute_links(n: int = LINK_SLICES) -> None:
    """Attribue les ids manquants puis résout stg_link → link, en N tranches
    (empreinte mémoire bornée).

    Tranche par cityHash64 : pour la tranche k on ne traite que les valeurs de
    nœuds (resp. les liens) dont le hash % n == k. Aucune requête ne voit donc
    plus de 1/n des données à la fois. Le hash ne sert qu'au découpage, jamais
    d'id.

    Valeurs traitées : les extrémités de stg_link (links.csv + liens de
    domains.json) et les valeurs de domains.json (stg_value), pour tous les
    types de NODE_TABLES. Une valeur déjà présente dans la table <type>
    garde son id ; une valeur absente est créée avec un nouvel id
    AUTO-INCRÉMENTÉ à partir du max(id) existant du type (max + 1, max + 2,
    ...), rank = 1000000 pour fqdn / ip. Un type hors NODE_TABLES n'a pas de
    table : ses liens sont ignorés (et comptés).

    Suppose un seul import à la fois : deux imports concurrents liraient le
    même max(id) et attribueraient les mêmes ids.

    Chaque lien résolu est inséré dans les deux sens (pas de projection
    inverse sur link, cf. 02_optimized.sql).
    """
    if query("SELECT (SELECT count() FROM stg_link) + "
             "(SELECT count() FROM stg_value)") == "0":
        for tbl in ("stg_link", "stg_value"):
            query(f"DROP TABLE IF EXISTS {tbl}")
        return

    log(f"Résolution des nœuds et des liens → link en {n} tranches...")
    for t in ("stg_link", "stg_value"):
        log(f"  {t} : {int(query(f'SELECT count() FROM {t}')):,} lignes")

    # 3a — table de correspondance (type, valeur) → id, restreinte aux valeurs
    # citées par les liens ou par domains.json, construite tranche par tranche.
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
            " UNION ALL"
            " SELECT node_type, value FROM stg_value"
            f"  WHERE cityHash64(value) % {n} = {k}"
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
        log(f"  {base - start:,} nœuds {typ} créés "
            f"(ids {start + 1:,} → {base:,})" if base > start
            else f"  aucun nœud {typ} créé")
    log(f"  nouveaux ids attribués en {time.monotonic() - t:.0f} s")

    # 3b — réécriture des liens avec les ids, tranche par tranche. Table link
    # sans projection inverse (cf. 02_optimized.sql) : chaque lien résolu est
    # inséré dans les DEUX sens (n1→n2 et n2→n1).
    t = time.monotonic()
    for k in range(n):
        base = (
            "FROM (SELECT id_node_1, id_node_2, id_source, creation_date, update_date, "
            "lower(type_1) AS t1, lower(type_2) AS t2 FROM stg_link "
            f"WHERE cityHash64(id_node_1, id_node_2) % {n} = {k}) AS l "
            "INNER JOIN tmp_node_map AS n1 ON n1.node_type = l.t1 AND n1.value = l.id_node_1 "
            "INNER JOIN tmp_node_map AS n2 ON n2.node_type = l.t2 AND n2.value = l.id_node_2"
        )
        query(
            "INSERT INTO link "
            "(type_1, id_1, type_2, id_2, id_source, detection_date, version) "
            "SELECT l.t1, n1.id, l.t2, n2.id, toInt32OrZero(l.id_source), "
            "coalesce(toUnixTimestamp(parseDateTimeBestEffortOrNull(l.creation_date)), toUnixTimestamp(now())), "
            "coalesce(toUnixTimestamp(parseDateTimeBestEffortOrNull(l.update_date)), toUnixTimestamp(now())) "
            + base, mem=True)
        query(
            "INSERT INTO link "
            "(type_1, id_1, type_2, id_2, id_source, detection_date, version) "
            "SELECT l.t2, n2.id, l.t1, n1.id, toInt32OrZero(l.id_source), "
            "coalesce(toUnixTimestamp(parseDateTimeBestEffortOrNull(l.creation_date)), toUnixTimestamp(now())), "
            "coalesce(toUnixTimestamp(parseDateTimeBestEffortOrNull(l.update_date)), toUnixTimestamp(now())) "
            + base, mem=True)
    log(f"  liens réécrits (2 sens) en {time.monotonic() - t:.0f} s")

    staged = int(query("SELECT count() FROM stg_link"))
    resolus = int(query(
        "SELECT count() FROM stg_link AS l WHERE "
        "(lower(l.type_1), l.id_node_1) IN (SELECT node_type, value FROM tmp_node_map) AND "
        "(lower(l.type_2), l.id_node_2) IN (SELECT node_type, value FROM tmp_node_map)",
        mem=True))
    log(f"  liens résolus : {resolus:,} / {staged:,} "
        f"(ignorés, nœud inconnu : {staged - resolus:,})")

    for tbl in ("tmp_node_map", "tmp_link_values", "stg_link", "stg_value"):
        query(f"DROP TABLE IF EXISTS {tbl}")


def iter_blocks(path: Path):
    """Itère les blocs du fichier, décompressés à la volée si gzip.

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
                yield buf
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
                yield out
        if not d.eof:
            truncated = True
        if truncated:
            log(f"  ⚠️  ARCHIVE TRONQUÉE : {path.name}\n"
                "     → les données de fin de fichier sont perdues.\n"
                "     → re-télécharge le fichier si possible "
                "(gzip -t pour vérifier).")


def classify(path: Path):
    """Retourne ('stg_node'|'stg_link'|'stg_property'|'stg_domain', format,
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
        return "stg_domain", "JSONEachRow", []
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
        sys.exit("Usage : make import FILE=<archive.zip|fichier|dossier>")
    src = Path(sys.argv[1]).expanduser().resolve()
    if not src.exists():
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

        log("Création des tables de staging...")
        run_sql_file(SQL_STAGING)

        t0 = time.monotonic()
        for f, table, fmt, settings in jobs:
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
                for block in iter_blocks(f):
                    proc.stdin.write(block.replace(b"\r", b"") if strip_cr
                                     else block)
                proc.stdin.close()
            except BrokenPipeError:
                pass  # le client a échoué, on récupère le code retour
            if proc.wait() != 0:
                raise subprocess.CalledProcessError(proc.returncode, cmd)
            n = query(f"SELECT count() FROM {table}")
            log(f"  {int(n):,} lignes en staging en {time.monotonic() - t:.0f} s")

        log("Distribution vers les tables optimisées...")
        t = time.monotonic()
        # Les merges en arrière-plan (déclenchés par le chargement de staging)
        # sont le principal concurrent mémoire pendant la distribution : on les
        # met en pause, on les reprend quoi qu'il arrive.
        query("SYSTEM STOP MERGES")
        try:
            query("SYSTEM DROP MARK CACHE")
            query("SYSTEM DROP UNCOMPRESSED CACHE")
            distribute_nodes()  # node.csv → <type>
            distribute_properties()  # properties.csv → property
            run_sql_file(SQL_DISTRIBUTE)  # domains.json → stg_value / stg_link
            distribute_links()  # ids auto-incrémentés + liens (CSV et domains.json)
        finally:
            query("SYSTEM START MERGES")
        log(f"  fait en {time.monotonic() - t:.0f} s")

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
    finally:
        if tmp is not None:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
