#!/usr/bin/env python3
"""Génère 500k FQDN, 500k IP et 1M liens factices dans les tables optimisées.

- 2% des FQDN contiennent des "hot terms" (youtube, shop, bank, mail...),
  agrégés par blocs contigus de 10k ids (distribution réaliste d'un crawl)
  pour que les recherches LIKE '%term%' retournent un volume réaliste.
- value = String (une valeur par ligne, comme les données réelles).
- Les liens relient des id_fqdn à des id_ip (ids Int64, schéma link_opt).
"""
import random
import ipaddress
import clickhouse_connect

HOST, PORT, USER, PASSWORD = "localhost", 8123, "bench", "bench"
N_FQDN = 500_000
N_IP = 500_000
N_LINKS = 1_000_000
BATCH = 50_000
SEED = 42

HOT_TERMS = ["youtube", "google", "facebook", "amazon", "shop", "bank",
             "mail", "cloud", "video", "music", "game", "news"]
WORDS = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf",
         "hotel", "india", "juliet", "kilo", "lima", "mike", "november",
         "oscar", "papa", "quebec", "romeo", "sierra", "tango", "urban",
         "victor", "whiskey", "xray", "yankee", "zulu", "pixel", "nova",
         "orbit", "quantum", "rocket", "solar", "titan", "umbra", "vortex"]
TLDS = ["com", "net", "org", "io", "fr", "de", "dev", "app"]


def make_domain(rng: random.Random, i: int) -> str:
    # 2% des domaines avec un "hot term", AGRÉGÉS par blocs contigus de 10k
    # (comme en réalité : un crawl découvre les domaines liés à un site en
    # rafales) — c'est ce qui permet à l'index ngram d'éluder les granules.
    if (i // 10_000) % 50 == 0:
        core = rng.choice(HOT_TERMS)
        if rng.random() < 0.5:
            core = f"{core}{rng.choice(WORDS)}"
        else:
            core = f"{rng.choice(WORDS)}-{core}"
    else:
        core = f"{rng.choice(WORDS)}{rng.choice(WORDS)}{rng.randint(0, 9999)}"
    return f"{core}.{rng.choice(TLDS)}"


def main() -> None:
    rng = random.Random(SEED)
    client = clickhouse_connect.get_client(
        host=HOST, port=PORT, username=USER, password=PASSWORD
    )

    # ---------- FQDN ----------
    print(f"[1/3] Génération de {N_FQDN:,} FQDN...")
    rows = []
    for i in range(1, N_FQDN + 1):
        rows.append((make_domain(rng, i), i, rng.randint(0, 1000), 1))
        if len(rows) >= BATCH:
            client.insert("fqdn_search", rows,
                          column_names=["value", "id_fqdn", "rank", "version"])
            rows.clear()
    if rows:
        client.insert("fqdn_search", rows,
                      column_names=["value", "id_fqdn", "rank", "version"])

    # ---------- IP ----------
    print(f"[2/3] Génération de {N_IP:,} IP...")
    rows = []
    for i in range(1, N_IP + 1):
        ip = str(ipaddress.IPv4Address(rng.randint(0x0A000001, 0xDFFFFFFF)))
        rows.append((ip, i, rng.randint(0, 1000), 1))
        if len(rows) >= BATCH:
            client.insert("ip_search", rows,
                          column_names=["value", "id_ip", "rank", "version"])
            rows.clear()
    if rows:
        client.insert("ip_search", rows,
                      column_names=["value", "id_ip", "rank", "version"])

    # ---------- LINKS (fqdn -> ip, ids Int64) ----------
    print(f"[3/3] Génération de {N_LINKS:,} liens...")
    now = 1_700_000_000
    rows = []
    for i in range(N_LINKS):
        rows.append((rng.randint(1, N_FQDN), rng.randint(1, N_IP),
                     rng.randint(1, 100), now - rng.randint(0, 31_536_000), 1))
        if len(rows) >= BATCH:
            client.insert("link_opt", rows,
                          column_names=["id_node_1", "id_node_2", "source_id",
                                        "detection_date", "version"])
            rows.clear()
    if rows:
        client.insert("link_opt", rows,
                      column_names=["id_node_1", "id_node_2", "source_id",
                                    "detection_date", "version"])

    for t in ("fqdn_search", "ip_search", "link_opt"):
        n = client.command(f"SELECT count() FROM {t}")
        print(f"  {t}: {n:,} lignes")
    print("OK — données chargées dans les tables optimisées.")


if __name__ == "__main__":
    main()
