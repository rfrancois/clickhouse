#!/usr/bin/env python3
"""Import de fichiers réels vers les tables optimisées
(fqdn_search / ip_search / link_opt — les tables naïves ne sont plus alimentées).

Usage : make import FILE=<archive.zip | fichier | dossier>

Fichiers reconnus (classification par nom) :
  - *node*.csv   : id;value;type;creation_date;rank        (';' + quotes)
  - *link*.csv   : id_node_1;id_node_2;type_1;type_2;id_source;creation_date;update_date
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
     - node.csv / domains.json → fqdn_search / ip_search
       (sql/05_import_distribute.sql)
     - liens : résolution valeur → id puis stg_link → link_opt, en N tranches
       (distribute_links, pour tenir en RAM sur une VM Docker modeste)
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

CLIENT = ["docker", "exec", "-i", "bench_clickhouse", "clickhouse-client",
          "--user", "bench", "--password", "bench"]

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
MEM = ["--max_threads=1",
       "--max_memory_usage=6000000000",
       "--max_bytes_before_external_group_by=536870912",
       "--max_bytes_before_external_sort=536870912",
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


def distribute_links(n: int = LINK_SLICES) -> None:
    """Résout stg_link → link_opt en N tranches (empreinte mémoire bornée).

    Tranche par cityHash64 : pour la tranche k on ne traite que les valeurs de
    nœuds (resp. les liens) dont le hash % n == k. Aucune requête ne voit donc
    plus de 1/n des données à la fois.
    """
    log(f"Résolution des liens → link_opt en {n} tranches...")
    for t in ("fqdn_search", "ip_search", "stg_link"):
        log(f"  {t} : {int(query(f'SELECT count() FROM {t}')):,} lignes")

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
            ") WHERE value != '' GROUP BY node_type, value", mem=True)
    for k in range(n):
        for typ, tbl, idcol in (("fqdn", "fqdn_search", "id_fqdn"),
                                ("ip", "ip_search", "id_ip")):
            query(
                f"INSERT INTO tmp_node_map SELECT '{typ}', value, "
                f"argMax(toInt64({idcol}), version) FROM {tbl} "
                "WHERE value IN (SELECT value FROM tmp_link_values "
                f"WHERE node_type = '{typ}' AND cityHash64(value) % {n} = {k}) "
                "GROUP BY value", mem=True)
    log(f"  correspondance valeur → id construite en {time.monotonic() - t:.0f} s")

    # 3b — réécriture des liens avec les ids, tranche par tranche.
    t = time.monotonic()
    for k in range(n):
        query(
            "INSERT INTO link_opt "
            "(id_node_1, id_node_2, source_id, detection_date, version) "
            "SELECT n1.id, n2.id, toInt32OrZero(l.id_source), "
            "coalesce(toUnixTimestamp(parseDateTimeBestEffortOrNull(l.creation_date)), toUnixTimestamp(now())), "
            "coalesce(toUnixTimestamp(parseDateTimeBestEffortOrNull(l.update_date)), toUnixTimestamp(now())) "
            "FROM (SELECT id_node_1, id_node_2, id_source, creation_date, update_date, "
            "lower(type_1) AS t1, lower(type_2) AS t2 FROM stg_link "
            f"WHERE cityHash64(id_node_1, id_node_2) % {n} = {k}) AS l "
            "INNER JOIN tmp_node_map AS n1 ON n1.node_type = l.t1 AND n1.value = l.id_node_1 "
            "INNER JOIN tmp_node_map AS n2 ON n2.node_type = l.t2 AND n2.value = l.id_node_2",
            mem=True)
    log(f"  liens réécrits en {time.monotonic() - t:.0f} s")

    staged = int(query("SELECT count() FROM stg_link"))
    resolus = int(query(
        "SELECT count() FROM stg_link AS l WHERE "
        "(lower(l.type_1), l.id_node_1) IN (SELECT node_type, value FROM tmp_node_map) AND "
        "(lower(l.type_2), l.id_node_2) IN (SELECT node_type, value FROM tmp_node_map)",
        mem=True))
    log(f"  liens résolus : {resolus:,} / {staged:,} "
        f"(ignorés, nœud inconnu : {staged - resolus:,})")

    for tbl in ("tmp_node_map", "tmp_link_values", "stg_link"):
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
    """Retourne ('stg_node'|'stg_link'|'stg_domain', format, settings) ou None."""
    n = path.name.lower()
    csv_settings = ["--format_csv_delimiter=;",
                    "--input_format_csv_skip_first_lines=1"]
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
                 "  → vérifie le conteneur : docker ps | grep bench_clickhouse\n"
                 "  → puis relance-le si besoin : make up")

    # Schéma optimisé requis : créé ici si `make init` n'a jamais été lancé.
    # (02_optimized.sql DROP + CREATE : on ne l'exécute que si la table est
    # absente, jamais sur un schéma existant — l'import reste idempotent.)
    if query("EXISTS TABLE fqdn_search") != "1":
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
                     "(attendus : *node*.csv, *link*.csv, *.json[.gz]).")

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
            try:
                for block in iter_blocks(f):
                    proc.stdin.write(block)
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
            run_sql_file(SQL_DISTRIBUTE)  # node.csv / domains.json → fqdn / ip
            if any(table == "stg_link" for _, table, _, _ in jobs):
                distribute_links()
            else:
                query("DROP TABLE IF EXISTS stg_link")
        finally:
            query("SYSTEM START MERGES")
        log(f"  fait en {time.monotonic() - t:.0f} s")

        log("\nCompteurs après import :")
        counts = query(
            "SELECT * FROM ("
            "SELECT 'fqdn_search' AS tbl, count() AS n FROM fqdn_search UNION ALL "
            "SELECT 'ip_search', count() FROM ip_search UNION ALL "
            "SELECT 'link_opt', count() FROM link_opt"
            ") ORDER BY tbl FORMAT PrettyCompactMonoBlock")
        log(counts)
        log(f"\nImport terminé en {time.monotonic() - t0:.0f} s "
            "(dédup ReplacingMergeTree asynchrone en arrière-plan).")
    finally:
        if tmp is not None:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
