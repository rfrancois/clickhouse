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
  5. distribution SQL vers les tables optimisées
     (sql/05_import_distribute.sql) avec résolution valeur → id pour link_opt
"""
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

FREE_WARN_GIB = 15


def log(msg: str) -> None:
    print(msg, flush=True)


def query(sql: str) -> str:
    r = subprocess.run(CLIENT + ["-q", sql],
                       capture_output=True, text=True, check=True)
    return r.stdout.strip()


def run_sql_file(path: Path) -> None:
    with open(path, "rb") as f:
        subprocess.run(CLIENT + ["--multiquery"], stdin=f, check=True)


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
        run_sql_file(SQL_DISTRIBUTE)
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
