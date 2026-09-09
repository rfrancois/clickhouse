#!/usr/bin/env python3
"""
Test rapide du bench (~100 lignes retournées) :
  1. requête simple   : recherche LIKE '%youtube%' sur les FQDN
  2. requête jointure : FQDN -> link -> IP
Affiche le temps de chaque requête et la taille de la base.
"""
import time

import clickhouse_connect

HOST = "localhost"
PORT = 8123
USER = "bench"
PASSWORD = "bench"

TERM = "youtube"
LIMIT = 100

SIMPLE_QUERY = f"""
SELECT id_fqdn, value
FROM fqdn_search
WHERE value LIKE '%{TERM}%'
ORDER BY id_fqdn
LIMIT {LIMIT}
"""

JOIN_QUERY = f"""
SELECT DISTINCT ip.id_ip, ip.value
FROM ip_search AS ip
WHERE ip.id_ip IN (
    SELECT l.id_node_2
    FROM link_opt AS l
    WHERE l.id_node_1 IN (
        SELECT DISTINCT f.id_fqdn
        FROM fqdn_search AS f
        WHERE f.value LIKE '%{TERM}%'
    )
)
ORDER BY ip.id_ip
LIMIT {LIMIT}
"""

SIZE_QUERY = """
SELECT
    database,
    table,
    formatReadableSize(sum(bytes_on_disk)) AS size,
    sum(rows) AS rows
FROM system.parts
WHERE active AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
GROUP BY database, table
ORDER BY sum(bytes_on_disk) DESC
"""


def main():
    client = clickhouse_connect.get_client(
        host=HOST, port=PORT, username=USER, password=PASSWORD
    )
    client.command("SELECT 1")  # vérifie la connexion

    # --- Taille de la base ------------------------------------------------
    size_rows = client.query(SIZE_QUERY).result_rows
    total_bytes = client.query(
        """
        SELECT sum(bytes_on_disk)
        FROM system.parts
        WHERE active AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
        """
    ).result_rows[0][0] or 0

    def human(n):
        for unit in ["o", "Kio", "Mio", "Gio", "Tio"]:
            if n < 1024 or unit == "Tio":
                return f"{n:.2f} {unit}" if unit != "o" else f"{n} o"
            n /= 1024

    print()
    print("=" * 58)
    print(f"  TEST CLICKHOUSE — recherche '{TERM}' (~{LIMIT} lignes)")
    print("=" * 58)
    print(f"\n  Taille totale de la base : {human(total_bytes)}\n")
    print(f"  {'Table':<20} {'Taille':>12} {'Lignes':>15}")
    print(f"  {'-' * 20} {'-' * 12} {'-' * 15}")
    for db, table, size, rows in size_rows:
        print(f"  {db + '.' + table:<20} {size:>12} {rows:>15,}")

    # --- Requêtes chronométrées -------------------------------------------
    print()
    for label, query in [
        ("Requête simple   (fqdn_search LIKE)", SIMPLE_QUERY),
        ("Requête jointure (FQDN -> link -> IP)", JOIN_QUERY),
    ]:
        t0 = time.perf_counter()
        result = client.query(query)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        print(f"  ▸ {label}")
        print(f"      Résultats : {result.row_count} lignes")
        print(f"      Temps     : {elapsed_ms:.2f} ms")
        print()

    print("=" * 58)
    print("  Test terminé.")
    print("=" * 58)
    print()


if __name__ == "__main__":
    main()
