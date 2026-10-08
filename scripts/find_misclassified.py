#!/usr/bin/env python3
"""FQDN mal classifiés : FQDN qui devraient être liés à un plugin et ne le
sont pas (non_classe), ou qui sont liés à un autre plugin que celui que
leurs voisins désignent (conflit). Résultat dans classification_candidate.

    python scripts/find_misclassified.py [--holdout PCT] [--database DB]

Un FQDN est « classifié P » s'il a un lien fqdn ↔ plugin P. Le nom du
plugin n'est jamais comparé au nom du FQDN (youtube.com peut être google) :
seuls comptent les FQDN déjà classifiés et le graphe link.

Signaux, chacun propose des couples (FQDN, plugin) :
  ancestor     le plus proche ancêtre classifié : coucou.youtube.com hérite
               du plugin de youtube.com. Ancêtre écarté pour P si ses
               descendants classifiés sont surtout d'autres plugins
               (plateforme partagée : x.amazonaws.com, y.amazonaws.com...)
  ip, application, capture, fqdn
               pivot : nœud relié au FQDN, « attribué » à P
                 - direct : pivot lié lui-même au plugin P (ip / application /
                   capture ↔ plugin, ou FQDN pivot classifié P)
                 - vote   : au moins MIN_VOTES FQDN classifiés parmi ses voisins,
                   dont au moins MIN_PURITY dans P
               pivot écarté s'il a plus de MAX_DEGREE FQDN voisins (CDN,
               mutualisé), ou s'il est direct mais que ses voisins classifiés
               sont surtout d'autres plugins
Score d'un couple = somme des poids de ses signaux distincts (WEIGHTS) ;
gardé si score >= MIN_SCORE (une IP seule ne suffit pas).

Volumétrie (600M FQDN, 1G liens) : une seule passe complète, sur fqdn
(ancêtre : recherche des suffixes dans une table Join en mémoire, sans
jointure ni GROUP BY sur les 600M lignes). link n'est lu que par sa clé
primaire (type_1, id_1) : liens des FQDN classifiés, des pivots retenus et
du type plugin, jamais une partition entière. Mémoire : FQDN classifiés
et pivots (tables Join, ensembles IN) ; le regroupement des signaux suit
l'ordre de tri (optimize_aggregation_in_order).

--holdout PCT : mode évaluation. PCT % des FQDN classifiés sont cachés
(tirage déterministe sur l'id), le calcul tourne sans eux, puis on mesure
combien sont retrouvés avec leur bon plugin (rappel), au total et par
signal. classification_candidate n'est pas modifiée.

Tout se fait côté serveur, tables de travail mc_* supprimées à la fin.
Lecture seule sur les tables de données.

Dépendance : pip install clickhouse-connect
"""
import argparse
import time

import clickhouse_connect

HOST, PORT, USER, PASSWORD = "localhost", 8123, "chuser", "Royal15Raccoon"

PIVOTS = ("ip", "application", "capture", "fqdn")
WEIGHTS = {"ancestor": 3, "fqdn": 2, "application": 2, "capture": 2, "ip": 1}
MIN_SCORE = 2
MIN_VOTES = 3       # FQDN classifiés minimum pour qu'un pivot vote
MIN_PURITY = 0.9    # part du plugin majoritaire pour un vote
GUARD_PURITY = 0.5  # en dessous, un pivot direct / un ancêtre est écarté
MAX_DEGREE = 1000   # FQDN voisins maximum d'un pivot
STAGING = ("mc_classified", "mc_hidden", "mc_own", "mc_anchor", "mc_ancestor",
           "mc_anchor_reject", "mc_pivot_vote", "mc_pivot_direct",
           "mc_pivot_degree", "mc_pivot_label", "mc_signal", "mc_candidate")
SETTINGS = {"use_skip_indexes": 0}
# même Enum8 que link.type_1 / type_2 : jointures et IN sur la clé primaire
# de link sans conversion
NODE_TYPE = ("Enum8('application' = 1, 'capture' = 2, 'fqdn' = 3, 'ip' = 4, "
             "'plugin' = 5, 'organization_name' = 6, 'organization_id' = 7, "
             "'phone' = 8, 'social_id' = 9)")

PIVOT_IN = ", ".join(f"'{p}'" for p in PIVOTS)
SCORE = ("arraySum(s -> transform(s, {sig:Array(String)}, {w:Array(UInt8)}, 0), "
         "signals)")


def find_misclassified(holdout: float = 0, database: str = "default") -> None:
    client = clickhouse_connect.get_client(host=HOST, port=PORT, username=USER,
                                           password=PASSWORD, database=database)
    params = {"bp": round(holdout * 100), "min_votes": MIN_VOTES,
              "min_purity": MIN_PURITY, "guard": GUARD_PURITY,
              "max_degree": MAX_DEGREE, "min_score": MIN_SCORE,
              "sig": list(WEIGHTS), "w": list(WEIGHTS.values())}

    def cmd(sql: str, **settings):
        return client.command(sql, parameters=params, settings={**SETTINGS, **settings})

    def count(sql: str) -> int:
        return int(cmd(sql))

    t = time.monotonic()

    def step(msg: str):
        print(f"[{time.monotonic() - t:6.0f} s] {msg}", flush=True)

    try:
        for tbl in STAGING:
            cmd(f"DROP TABLE IF EXISTS {tbl}")

        # 1. FQDN classifiés (fqdn ↔ plugin), moins ceux cachés par --holdout
        #    (bp = points de base : PCT × 100 sur 10 000). Partition plugin
        #    de link, clé primaire. mc_own : plugins de chaque FQDN classifié,
        #    en mémoire (joinGet)
        for tbl, op in (("mc_classified", ">="), ("mc_hidden", "<")):
            cmd(f"CREATE TABLE {tbl} (id_fqdn Int64, id_plugin Int64) "
                "ENGINE = MergeTree ORDER BY (id_fqdn, id_plugin)")
            cmd(f"INSERT INTO {tbl} SELECT DISTINCT id_2, id_1 FROM link "
                "WHERE type_1 = 'plugin' AND type_2 = 'fqdn' "
                f"AND cityHash64(id_2) % 10000 {op} {{bp:UInt32}}")
        n_cls = count("SELECT uniqExact(id_fqdn) FROM mc_classified")
        n_hid = count("SELECT uniqExact(id_fqdn) FROM mc_hidden")
        step(f"FQDN classifiés : {n_cls:,}" + (f", {n_hid:,} cachés" if holdout else ""))
        if not n_cls:
            return
        cmd("CREATE TABLE mc_own (id_fqdn Int64, own Array(Int64)) "
            "ENGINE = Join(ANY, LEFT, id_fqdn)")
        cmd("INSERT INTO mc_own SELECT id_fqdn, groupArray(id_plugin) "
            "FROM mc_classified GROUP BY id_fqdn")

        # 2. ancêtre. mc_anchor : valeur de chaque FQDN classifié → ses
        #    plugins, en mémoire (lecture de fqdn par la projection p_id).
        #    Puis UNE passe sur fqdn : premier parent présent dans mc_anchor
        #    = le plus proche (a.b.google.com → b.google.com, google.com ;
        #    jamais le TLD seul). Pas de jointure ni de GROUP BY : le flux
        #    de 600M lignes n'est jamais matérialisé
        cmd("CREATE TABLE mc_anchor (value String, plugins Array(Int64)) "
            "ENGINE = Join(ANY, LEFT, value)")
        cmd("INSERT INTO mc_anchor "
            "SELECT f.value, groupUniqArray(c.id_plugin) "
            "FROM fqdn AS f INNER JOIN mc_classified AS c ON c.id_fqdn = f.id_fqdn "
            "WHERE f.id_fqdn IN (SELECT id_fqdn FROM mc_classified) "
            "GROUP BY f.value")
        cmd("CREATE TABLE mc_ancestor (value String, id_fqdn Int64, anchor String, "
            "plugins Array(Int64), own Array(Int64)) "
            "ENGINE = MergeTree ORDER BY id_fqdn")
        cmd("INSERT INTO mc_ancestor "
            "SELECT value, id_fqdn, anchor, joinGet('mc_anchor', 'plugins', anchor), "
            "       joinGet('mc_own', 'own', id_fqdn) "
            "FROM ("
            " SELECT value, id_fqdn, arrayFirst("
            "   s -> notEmpty(joinGet('mc_anchor', 'plugins', s)),"
            "   arrayMap(i -> arrayStringConcat(arraySlice(p, i), '.'), range(2, length(p)))"
            " ) AS anchor"
            " FROM (SELECT value, id_fqdn, splitByChar('.', value) AS p FROM fqdn"
            "       WHERE countSubstrings(value, '.') >= 2)"
            ") WHERE anchor != ''")
        # ancêtre écarté pour P : au moins MIN_VOTES descendants classifiés,
        # dont moins de GUARD_PURITY dans P
        cmd("CREATE TABLE mc_anchor_reject (anchor String, id_plugin Int64) "
            "ENGINE = MergeTree ORDER BY anchor")
        cmd("INSERT INTO mc_anchor_reject "
            "SELECT anchor, id_plugin FROM ("
            " SELECT anchor, any(plugins) AS plugins, count() AS total,"
            "        groupArrayArray(own) AS desc_plugins"
            " FROM mc_ancestor WHERE notEmpty(own) GROUP BY anchor"
            ") ARRAY JOIN plugins AS id_plugin "
            "WHERE total >= {min_votes:UInt32} "
            "AND countEqual(desc_plugins, id_plugin) / total < {guard:Float64}")
        step(f"ancêtre : {count('SELECT count() FROM mc_ancestor'):,} FQDN sous un "
             f"FQDN classifié, {count('SELECT count() FROM mc_anchor_reject'):,} "
             "couples (ancêtre, plugin) écartés (plateformes partagées)")

        # 3. pivots. Votes : liens des FQDN classifiés vers les pivots, lus
        #    côté fqdn (link a les deux sens : clé primaire type_1 = 'fqdn',
        #    id_1 IN classifiés). Directs : partition plugin. Degré : liens
        #    des seuls pivots trouvés, par la clé primaire
        cmd(f"CREATE TABLE mc_pivot_vote (pivot_type {NODE_TYPE}, "
            "id_pivot Int64, id_plugin Int64, n UInt64) "
            "ENGINE = MergeTree ORDER BY (pivot_type, id_pivot)")
        cmd("INSERT INTO mc_pivot_vote "
            "SELECT l.type_2, l.id_2, c.id_plugin, uniqExact(l.id_1) "
            "FROM link AS l INNER JOIN mc_classified AS c ON c.id_fqdn = l.id_1 "
            "WHERE l.type_1 = 'fqdn' AND l.id_1 IN (SELECT id_fqdn FROM mc_classified) "
            f"AND l.type_2 IN ({PIVOT_IN}) "
            "GROUP BY l.type_2, l.id_2, c.id_plugin")
        cmd(f"CREATE TABLE mc_pivot_direct (pivot_type {NODE_TYPE}, "
            "id_pivot Int64, id_plugin Int64) "
            "ENGINE = MergeTree ORDER BY (pivot_type, id_pivot)")
        cmd("INSERT INTO mc_pivot_direct "
            "SELECT DISTINCT type_2, id_2, id_1 FROM link "
            f"WHERE type_1 = 'plugin' AND type_2 IN ({PIVOT_IN}) AND type_2 != 'fqdn'")
        cmd("INSERT INTO mc_pivot_direct "
            "SELECT 'fqdn', id_fqdn, id_plugin FROM mc_classified")
        cmd(f"CREATE TABLE mc_pivot_degree (pivot_type {NODE_TYPE}, "
            "id_pivot Int64, degree UInt64) "
            "ENGINE = MergeTree ORDER BY (pivot_type, id_pivot)")
        cmd("INSERT INTO mc_pivot_degree "
            "SELECT type_1, id_1, uniqExact(id_2) FROM link "
            "WHERE (type_1, id_1) IN ("
            " SELECT pivot_type, id_pivot FROM mc_pivot_vote"
            " UNION DISTINCT SELECT pivot_type, id_pivot FROM mc_pivot_direct) "
            "AND type_2 = 'fqdn' "
            "GROUP BY type_1, id_1")

        # 4. pivots attribués à un plugin (direct ou vote, filtres de degré
        #    et de pureté)
        cmd(f"CREATE TABLE mc_pivot_label (pivot_type {NODE_TYPE}, "
            "id_pivot Int64, id_plugin Int64, direct UInt8, n UInt64, "
            "purity Float64, degree UInt64) "
            "ENGINE = MergeTree ORDER BY (pivot_type, id_pivot)")
        cmd("INSERT INTO mc_pivot_label "
            "SELECT pivot_type, id_pivot, id_plugin, direct, n, "
            "       if(total = 0, 1., n / total) AS purity, degree "
            "FROM ("
            " SELECT k.pivot_type AS pivot_type, k.id_pivot AS id_pivot,"
            "        k.id_plugin AS id_plugin, k.direct AS direct, k.n AS n,"
            "        sum(k.n) OVER (PARTITION BY k.pivot_type, k.id_pivot) AS total,"
            "        g.degree AS degree"
            " FROM ("
            "  SELECT pivot_type, id_pivot, id_plugin, max(d) AS direct, max(v) AS n"
            "  FROM ("
            "   SELECT pivot_type, id_pivot, id_plugin, 0 AS d, n AS v FROM mc_pivot_vote"
            "   UNION ALL"
            "   SELECT pivot_type, id_pivot, id_plugin, 1, 0 FROM mc_pivot_direct)"
            "  GROUP BY pivot_type, id_pivot, id_plugin"
            " ) AS k INNER JOIN mc_pivot_degree AS g"
            "   ON g.pivot_type = k.pivot_type AND g.id_pivot = k.id_pivot"
            ") "
            "WHERE degree <= {max_degree:UInt64} AND ("
            " (direct AND (total < {min_votes:UInt32} OR purity >= {guard:Float64}))"
            " OR (n >= {min_votes:UInt32} AND purity >= {min_purity:Float64}))")
        step("pivots attribués : " + (", ".join(
            f"{t} {n:,}" for t, n in client.query(
                "SELECT pivot_type, uniqExact(id_pivot) FROM mc_pivot_label "
                "GROUP BY pivot_type ORDER BY pivot_type").result_rows) or "aucun"))

        # 5. signaux (FQDN, plugin, signal), sans les couples déjà liés.
        #    Pivots : leurs liens seulement (clé primaire), au plus
        #    MAX_DEGREE FQDN chacun
        cmd("CREATE TABLE mc_signal (id_fqdn Int64, id_plugin Int64, "
            "signal LowCardinality(String), fqdn String, anchor String, "
            "nb_pivots UInt64) "
            "ENGINE = MergeTree ORDER BY (id_fqdn, id_plugin)")
        cmd("INSERT INTO mc_signal "
            "SELECT id_fqdn, id_plugin, 'ancestor', value, anchor, 0 "
            "FROM mc_ancestor ARRAY JOIN plugins AS id_plugin "
            "WHERE NOT has(own, id_plugin) "
            "AND (anchor, id_plugin) NOT IN (SELECT anchor, id_plugin FROM mc_anchor_reject)")
        cmd("INSERT INTO mc_signal "
            "SELECT l.id_2, p.id_plugin, toString(p.pivot_type), '', '', uniqExact(l.id_1) "
            "FROM link AS l INNER JOIN mc_pivot_label AS p "
            "  ON p.pivot_type = l.type_1 AND p.id_pivot = l.id_1 "
            "WHERE (l.type_1, l.id_1) IN (SELECT pivot_type, id_pivot FROM mc_pivot_label) "
            "AND l.type_2 = 'fqdn' "
            "AND NOT has(joinGet('mc_own', 'own', l.id_2), p.id_plugin) "
            "GROUP BY l.id_2, p.id_plugin, p.pivot_type")

        # 6. candidats : score suffisant. Regroupement dans l'ordre de tri de
        #    mc_signal (pas de table de hachage sur tous les couples)
        cmd("CREATE TABLE mc_candidate (id_fqdn Int64, id_plugin Int64, fqdn String, "
            "status Enum8('non_classe' = 1, 'conflit' = 2), score UInt8, "
            "signals Array(LowCardinality(String)), anchor String, nb_pivots UInt64) "
            "ENGINE = MergeTree ORDER BY (id_plugin, id_fqdn)")
        cmd("INSERT INTO mc_candidate "
            "SELECT id_fqdn, id_plugin, fqdn, "
            "       if(empty(joinGet('mc_own', 'own', id_fqdn)), 'non_classe', 'conflit'), "
            "       score, signals, anchor, nb_pivots "
            "FROM ("
            f" SELECT id_fqdn, id_plugin, groupUniqArray(signal) AS signals, {SCORE} AS score,"
            "        anyIf(fqdn, fqdn != '') AS fqdn, anyIf(anchor, anchor != '') AS anchor,"
            "        sum(nb_pivots) AS nb_pivots"
            " FROM mc_signal GROUP BY id_fqdn, id_plugin"
            ") WHERE score >= {min_score:UInt8}",
            optimize_aggregation_in_order=1)

        if holdout:
            evaluate(client, params)
            return

        # 7. résultat. Valeur du FQDN déjà connue par le signal ancêtre ;
        #    recherche dans fqdn (projection p_id) pour les seuls candidats
        #    venus des pivots
        cmd("CREATE OR REPLACE TABLE classification_candidate ("
            " id_fqdn Int64, fqdn String, id_plugin Int64, plugin String,"
            " status Enum8('non_classe' = 1, 'conflit' = 2), score UInt8,"
            " signals Array(LowCardinality(String)), anchor String, nb_pivots UInt64,"
            " computed_at DateTime) "
            "ENGINE = MergeTree ORDER BY (plugin, status, fqdn)")
        cmd("INSERT INTO classification_candidate "
            "SELECT c.id_fqdn, if(c.fqdn != '', c.fqdn, f.fqdn), c.id_plugin, p.plugin, "
            "       c.status, c.score, c.signals, c.anchor, c.nb_pivots, now() "
            "FROM mc_candidate AS c "
            "LEFT JOIN (SELECT id_fqdn, any(value) AS fqdn FROM fqdn"
            "           WHERE id_fqdn IN (SELECT id_fqdn FROM mc_candidate WHERE fqdn = '')"
            "           GROUP BY id_fqdn) AS f ON f.id_fqdn = c.id_fqdn "
            "LEFT JOIN (SELECT id_plugin, any(value) AS plugin FROM plugin"
            "           GROUP BY id_plugin) AS p ON p.id_plugin = c.id_plugin")
        for status, n in client.query(
                "SELECT status, count() FROM classification_candidate "
                "GROUP BY status ORDER BY status").result_rows:
            step(f"classification_candidate : {n:,} {status}")
    finally:
        for tbl in STAGING:
            cmd(f"DROP TABLE IF EXISTS {tbl}")


def evaluate(client, params) -> None:
    """Rappel sur les FQDN cachés : couple (FQDN, plugin) caché retrouvé par
    les candidats (score suffisant) et par chaque signal seul. Précision
    approchée : part des propositions faites sur les FQDN cachés qui
    donnent le bon plugin."""
    def q(sql):
        return client.query(sql, parameters=params, settings=SETTINGS).result_rows

    hidden = q("SELECT count() FROM mc_hidden")[0][0]
    if not hidden:
        print("aucun FQDN caché : augmenter --holdout")
        return
    rows = q(
        "SELECT 'candidats (score >= ' || toString({min_score:UInt8}) || ')' AS s,"
        "       countIf((id_fqdn, id_plugin) IN (SELECT id_fqdn, id_plugin FROM mc_hidden)),"
        "       count() "
        "FROM mc_candidate WHERE id_fqdn IN (SELECT id_fqdn FROM mc_hidden) "
        "UNION ALL "
        "SELECT signal,"
        "       countIf((id_fqdn, id_plugin) IN (SELECT id_fqdn, id_plugin FROM mc_hidden)),"
        "       count() "
        "FROM mc_signal WHERE id_fqdn IN (SELECT id_fqdn FROM mc_hidden) "
        "GROUP BY signal")
    print(f"\n{hidden:,} couples (FQDN, plugin) cachés\n")
    print(f"{'signal':<24}{'retrouvés':>10}{'rappel':>9}{'propositions':>14}{'précision':>11}")
    for sig, found, proposed in sorted(rows, key=lambda r: (not r[0].startswith("cand"), r[0])):
        prec = f"{found / proposed:.0%}" if proposed else "-"
        print(f"{sig:<24}{found:>10,}{found / hidden:>9.0%}{proposed:>14,}{prec:>11}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="FQDN mal classifiés → classification_candidate")
    ap.add_argument("--holdout", type=float, default=0, metavar="PCT",
                    help="évaluation : %% de FQDN classifiés cachés, rappel mesuré, "
                         "classification_candidate inchangée")
    ap.add_argument("--database", default="default")
    args = ap.parse_args()
    if not 0 <= args.holdout < 100:
        ap.error("--holdout entre 0 et 100")
    find_misclassified(args.holdout, args.database)
