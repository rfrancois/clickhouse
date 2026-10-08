#!/usr/bin/env python3
"""FQDN mal classifiés et clients des plateformes multi-clients.

    python scripts/find_misclassified.py [--holdout PCT] [--database DB]

Un FQDN est « classifié P » s'il a un lien fqdn ↔ plugin P. Le nom du
plugin n'est jamais comparé au nom du FQDN (youtube.com peut être google) :
seuls comptent les FQDN déjà classifiés et le graphe link.

Héritage implicite : un sous-domaine d'un FQDN classifié P est considéré
comme P (www.google.com sous google.com) ; ce plugin ne lui est jamais
proposé. Un sous-domaine lié à un autre plugin que son ancêtre n'est pas un
conflit (teams.microsoft.com classifié teams sous microsoft.com classifié
microsoft : marque découpée en plusieurs plugins).

Plateforme multi-clients (zendesk.com, slack.com...) : FQDN classifié dont
les sous-domaines sont des clients (monapp.zendesk.com). Ce qui la
distingue d'une grande marque découpée en plusieurs plugins
(teams.microsoft.com, azure.microsoft.com...) : la DIVERSITÉ, presque un
plugin différent par client. Détectée automatiquement si au moins un des
deux critères est rempli :
  - ses sous-domaines classifiés dans d'autres plugins que la plateforme
    couvrent au moins PLATFORM_MIN_PLUGINS plugins distincts, soit au moins
    PLATFORM_MIN_DIVERSITY plugin distinct par sous-domaine
    (x.amazonaws.com, y.amazonaws.com... classifiés chacun ailleurs) ;
  - ses noms de clients qui sont la marque d'un domaine classifié ailleurs
    (acme.zendesk.com ↔ acme.com) désignent au moins PLATFORM_MIN_PLUGINS
    plugins distincts, soit au moins PLATFORM_MIN_DIVERSITY plugin par nom,
    et ces noms font au moins PLATFORM_MIN_RATIO des noms de clients
    distincts.
Nom générique : nom de client (data, cloud, news...) trouvé comme marque et
présent sous au moins GENERIC_MIN_DOMAINS domaines enregistrables
classifiés différents (data.microsoft.com, data.google.com...) ; jamais
utilisé comme marque (ni proposition, ni détection de plateforme). Calculé
sur les données, pas de liste.
Nom du client = label juste avant la plateforme (x.monapp.zendesk.com →
monapp), sauf labels techniques (TECH_LABELS : www, api, mail...).
Marque = premier label du domaine enregistrable, pour les seuls FQDN
classifiés qui SONT un domaine enregistrable ou son www (monapp.com,
www.monapp.com → monapp ; jamais acme.zendesk.com → zendesk). Le nom de
client est cherché entier, puis sa partie avant le premier « - »
(monapp-support → monapp).

Résultats :
  classification_candidate  couples (FQDN, plugin) proposés
    non_classe  FQDN sans plugin
    conflit     FQDN lié à un autre plugin que celui de ses pivots
    complement  FQDN déjà lié (ex. à zendesk) dont le nom de client est la
                marque d'un autre plugin
  classification_tenant     tous les clients des plateformes : FQDN, nom
                            du client, plugins proposés, domaine de la marque
  classification_platform_detected
                            plateformes détectées et leurs compteurs,
                            pour revue
Signaux, chacun propose des couples (FQDN, plugin) :
  tenant       sous une plateforme, nom de client = marque d'un domaine
               classifié dans un autre plugin
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
(ancêtre, nom de client et marque : recherches dans des tables Join en
mémoire, sans jointure ni GROUP BY sur les 600M lignes). Les sous-domaines
de FQDN classifiés sont écrits sur disque (mc_ancestor). link n'est lu que
par sa clé primaire (type_1, id_1). Mémoire : FQDN classifiés, marques,
plateformes et pivots (tables Join, ensembles IN) ; le regroupement des
signaux suit l'ordre de tri (optimize_aggregation_in_order).

--holdout PCT : mode évaluation. PCT % des FQDN classifiés sont cachés
(tirage déterministe sur l'id), le calcul tourne sans eux, puis on mesure
combien sont retrouvés avec leur bon plugin (héritage implicite compris),
au total et par signal. Les tables de résultats ne sont pas modifiées.

Tout se fait côté serveur, tables de travail mc_* supprimées à la fin.
Lecture seule sur les tables de données.

Dépendance : pip install clickhouse-connect
"""
import argparse
import time

import clickhouse_connect

HOST, PORT, USER, PASSWORD = "localhost", 8123, "chuser", "Royal15Raccoon"

PIVOTS = ("ip", "application", "capture", "fqdn")
WEIGHTS = {"tenant": 2, "fqdn": 2, "application": 2, "capture": 2,
           "ip": 1}
MIN_SCORE = 2
MIN_VOTES = 3       # FQDN classifiés minimum pour qu'un pivot vote
MIN_PURITY = 0.9    # part du plugin majoritaire pour un vote
GUARD_PURITY = 0.5  # pivot direct écarté / plateforme détectée en dessous
MAX_DEGREE = 1000   # FQDN voisins maximum d'un pivot
PLATFORM_MIN_PLUGINS = 10     # plugins distincts parmi les clients d'une plateforme
PLATFORM_MIN_DIVERSITY = 0.5  # plugins distincts par client (1 = un plugin par client)
PLATFORM_MIN_RATIO = 0.05     # noms de clients = marque, parmi les noms distincts
GENERIC_MIN_DOMAINS = 20      # nom présent sous autant de domaines → générique
# labels qui ne sont jamais un nom de client ni une marque
TECH_LABELS = (
    "www", "www1", "www2", "www3", "m", "mobile", "mail", "webmail", "smtp", "imap",
    "pop", "pop3", "mx", "ns", "ns1", "ns2", "ns3", "dns", "api", "app", "apps",
    "cdn", "static", "img", "images", "media", "assets", "files", "download",
    "admin", "login", "auth", "sso", "id", "account", "accounts", "my", "dev",
    "test", "staging", "stage", "preprod", "prod", "beta", "demo", "sandbox",
    "support", "help", "status", "blog", "shop", "store", "docs", "portal",
    "secure", "vpn", "remote", "ftp", "autodiscover", "autoconfig", "cpanel",
    "webdisk", "whm", "default", "web", "en", "fr", "de", "es", "it", "nl", "us",
    "eu", "uk")
STAGING = ("mc_classified", "mc_hidden", "mc_own", "mc_cls_value", "mc_anchor",
           "mc_brand", "mc_ancestor", "mc_generic", "mc_platform_stat", "mc_platform",
           "mc_plugin_name", "mc_pivot_vote", "mc_pivot_direct", "mc_pivot_degree",
           "mc_pivot_label", "mc_pivot_signal", "mc_signal", "mc_candidate")
SETTINGS = {"use_skip_indexes": 0}
# même Enum8 que link.type_1 / type_2 : jointures et IN sur la clé primaire
# de link sans conversion
NODE_TYPE = ("Enum8('application' = 1, 'capture' = 2, 'fqdn' = 3, 'ip' = 4, "
             "'plugin' = 5, 'organization_name' = 6, 'organization_id' = 7, "
             "'phone' = 8, 'social_id' = 9)")
STATUS = "Enum8('non_classe' = 1, 'conflit' = 2, 'complement' = 3)"

PIVOT_IN = ", ".join(f"'{p}'" for p in PIVOTS)
# plugins de la marque du nom de client, sauf nom générique
BRAND_PLUGINS = ("if(joinGet('mc_generic', 'generic', brand) = 1, "
                 "CAST([] AS Array(Int64)), brand_plugins)")
SCORE = ("arraySum(s -> transform(s, {sig:Array(String)}, {w:Array(UInt8)}, 0), "
         "signals)")


def find_misclassified(holdout: float = 0, database: str = "default") -> None:
    client = clickhouse_connect.get_client(host=HOST, port=PORT, username=USER,
                                           password=PASSWORD, database=database)
    params = {"bp": round(holdout * 100), "min_votes": MIN_VOTES,
              "min_purity": MIN_PURITY, "guard": GUARD_PURITY,
              "max_degree": MAX_DEGREE, "min_score": MIN_SCORE,
              "sig": list(WEIGHTS), "w": list(WEIGHTS.values()),
              "tech": list(TECH_LABELS), "plat_plugins": PLATFORM_MIN_PLUGINS,
              "plat_div": PLATFORM_MIN_DIVERSITY, "plat_ratio": PLATFORM_MIN_RATIO,
              "generic": GENERIC_MIN_DOMAINS}

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
        cmd("CREATE TABLE mc_plugin_name (id_plugin Int64, value String) "
            "ENGINE = Join(ANY, LEFT, id_plugin)")
        cmd("INSERT INTO mc_plugin_name SELECT id_plugin, any(value) FROM plugin "
            "GROUP BY id_plugin")

        # 2. ancêtre. mc_cls_value : valeur de chaque FQDN classifié → ses
        #    plugins (lecture de fqdn par la projection p_id). En mémoire :
        #    mc_anchor = FQDN classifiés → plugins ; mc_brand =
        #    marque → plugins, des FQDN classifiés qui sont un domaine
        #    enregistrable ou son www (monapp.com → monapp)
        cmd("CREATE TABLE mc_cls_value (value String, plugins Array(Int64)) "
            "ENGINE = MergeTree ORDER BY value")
        cmd("INSERT INTO mc_cls_value "
            "SELECT f.value, groupUniqArray(c.id_plugin) "
            "FROM fqdn AS f INNER JOIN mc_classified AS c ON c.id_fqdn = f.id_fqdn "
            "WHERE f.id_fqdn IN (SELECT id_fqdn FROM mc_classified) "
            "GROUP BY f.value")
        cmd("CREATE TABLE mc_anchor (value String, plugins Array(Int64)) "
            "ENGINE = Join(ANY, LEFT, value)")
        cmd("INSERT INTO mc_anchor SELECT value, plugins FROM mc_cls_value")
        cmd("CREATE TABLE mc_brand (brand String, plugins Array(Int64), domain String) "
            "ENGINE = Join(ANY, LEFT, brand)")
        cmd("INSERT INTO mc_brand "
            "SELECT brand, groupUniqArray(id_plugin), min(reg) FROM ("
            " SELECT cutToFirstSignificantSubdomain(value) AS reg,"
            "        splitByChar('.', reg)[1] AS brand, arrayJoin(plugins) AS id_plugin"
            " FROM mc_cls_value WHERE value = reg OR value = 'www.' || reg"
            ") WHERE length(brand) >= 3 AND NOT has({tech:Array(String)}, brand) "
            "GROUP BY brand")

        #    UNE passe sur fqdn : premier parent présent dans mc_anchor = le
        #    plus proche (a.b.google.com → b.google.com, google.com ; jamais
        #    le TLD seul) ; nom de client = label juste avant l'ancêtre ;
        #    marque correspondante (nom entier, sinon partie avant le premier
        #    « - »), hors plugins de l'ancêtre. Pas de jointure ni de GROUP BY
        cmd("CREATE TABLE mc_ancestor (value String, id_fqdn Int64, anchor String, "
            "plugins Array(Int64), own Array(Int64), tenant String, brand String, "
            "brand_plugins Array(Int64)) "
            "ENGINE = MergeTree ORDER BY id_fqdn")
        cmd("INSERT INTO mc_ancestor "
            "SELECT value, id_fqdn, anchor, plugins, own, tenant, "
            "       multiIf(notEmpty(b1), tenant, notEmpty(b2), head, ''), "
            "       arrayFilter(x -> NOT has(plugins, x), if(notEmpty(b1), b1, b2)) "
            "FROM ("
            " SELECT value, id_fqdn, anchor,"
            "        joinGet('mc_anchor', 'plugins', anchor) AS plugins,"
            "        joinGet('mc_own', 'own', id_fqdn) AS own,"
            "        splitByChar('.', substring(value, 1,"
            "                    length(value) - length(anchor) - 1))[-1] AS tenant,"
            "        splitByChar('-', tenant)[1] AS head,"
            "        if(has({tech:Array(String)}, tenant), [],"
            "           joinGet('mc_brand', 'plugins', tenant)) AS b1,"
            "        if(has({tech:Array(String)}, tenant) OR head = tenant"
            "           OR length(head) < 3 OR has({tech:Array(String)}, head), [],"
            "           joinGet('mc_brand', 'plugins', head)) AS b2"
            " FROM ("
            "  SELECT value, id_fqdn, arrayFirst("
            "    s -> notEmpty(joinGet('mc_anchor', 'plugins', s)),"
            "    arrayMap(i -> arrayStringConcat(arraySlice(p, i), '.'), range(2, length(p)))"
            "  ) AS anchor"
            "  FROM (SELECT value, id_fqdn, splitByChar('.', value) AS p FROM fqdn"
            "        WHERE countSubstrings(value, '.') >= 2)"
            " ) WHERE anchor != ''"
            ")")

        #    noms génériques : nom de client trouvé comme marque, présent sous
        #    au moins GENERIC_MIN_DOMAINS domaines enregistrables classifiés
        cmd("CREATE TABLE mc_generic (brand String, generic UInt8) "
            "ENGINE = Join(ANY, LEFT, brand)")
        cmd("INSERT INTO mc_generic "
            "SELECT brand, 1 FROM mc_ancestor WHERE brand != '' GROUP BY brand "
            "HAVING uniq(cutToFirstSignificantSubdomain(anchor)) >= {generic:UInt32}")

        #    plateformes : compteurs par ancêtre (uniq approché : mémoire fixe
        #    par ancêtre ; plugins distincts exacts, peu nombreux)
        cmd("CREATE TABLE mc_platform_stat (anchor String, plugins Array(Int64), "
            "nb_tenants UInt64, nb_brand UInt64, nb_brand_plugins UInt64, "
            "nb_classified UInt64, nb_classified_other UInt64, nb_plugins_other UInt64) "
            "ENGINE = MergeTree ORDER BY anchor")
        cmd("INSERT INTO mc_platform_stat "
            "SELECT anchor, any(plugins), "
            "       uniqIf(tenant, NOT has({tech:Array(String)}, tenant)), "
            f"       uniqIf(tenant, notEmpty({BRAND_PLUGINS})), "
            f"       uniqExactArray({BRAND_PLUGINS}), "
            "       countIf(notEmpty(own)), "
            "       countIf(notEmpty(own) AND NOT hasAll(plugins, own)), "
            "       uniqExactArray(arrayFilter(x -> NOT has(plugins, x), own)) "
            "FROM mc_ancestor GROUP BY anchor")
        cmd("CREATE TABLE mc_platform (anchor String, platform UInt8) "
            "ENGINE = Join(ANY, LEFT, anchor)")
        cmd("INSERT INTO mc_platform "
            "SELECT anchor, 1 FROM mc_platform_stat "
            "WHERE (nb_plugins_other >= {plat_plugins:UInt32}"
            "       AND nb_plugins_other / nb_classified_other >= {plat_div:Float64})"
            "   OR (nb_brand_plugins >= {plat_plugins:UInt32}"
            "       AND nb_brand_plugins / nb_brand >= {plat_div:Float64}"
            "       AND nb_brand / nb_tenants >= {plat_ratio:Float64})")
        step(f"ancêtre : {count('SELECT count() FROM mc_ancestor'):,} FQDN sous un "
             f"FQDN classifié, {count('SELECT count() FROM mc_generic'):,} noms "
             f"génériques, {count('SELECT count() FROM mc_platform'):,} plateformes")

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
        #    tenant : marque du nom de client (hors nom générique), sous une
        #    plateforme. Pivots : leurs liens seulement (clé primaire), au plus
        #    MAX_DEGREE FQDN chacun, sans les plugins hérités de l'ancêtre
        cols = ("(id_fqdn Int64, id_plugin Int64, signal LowCardinality(String), "
                "fqdn String, anchor String, nb_pivots UInt64) "
                "ENGINE = MergeTree ORDER BY (id_fqdn, id_plugin)")
        cmd(f"CREATE TABLE mc_signal {cols}")
        cmd(f"CREATE TABLE mc_pivot_signal {cols}")
        cmd("INSERT INTO mc_signal "
            "SELECT id_fqdn, id_plugin, 'tenant', value, anchor, 0 "
            "FROM mc_ancestor "
            f"ARRAY JOIN arrayFilter(x -> NOT has(own, x), {BRAND_PLUGINS}) AS id_plugin "
            "WHERE joinGet('mc_platform', 'platform', anchor) = 1")
        cmd("INSERT INTO mc_pivot_signal "
            "SELECT l.id_2, p.id_plugin, toString(p.pivot_type), '', '', uniqExact(l.id_1) "
            "FROM link AS l INNER JOIN mc_pivot_label AS p "
            "  ON p.pivot_type = l.type_1 AND p.id_pivot = l.id_1 "
            "WHERE (l.type_1, l.id_1) IN (SELECT pivot_type, id_pivot FROM mc_pivot_label) "
            "AND l.type_2 = 'fqdn' "
            "AND NOT has(joinGet('mc_own', 'own', l.id_2), p.id_plugin) "
            "GROUP BY l.id_2, p.id_plugin, p.pivot_type")
        cmd("INSERT INTO mc_signal SELECT * FROM mc_pivot_signal "
            "WHERE (id_fqdn, id_plugin) NOT IN ("
            " SELECT id_fqdn, arrayJoin(plugins) FROM mc_ancestor"
            " WHERE id_fqdn IN (SELECT id_fqdn FROM mc_pivot_signal))")

        # 6. candidats : score suffisant. Regroupement dans l'ordre de tri de
        #    mc_signal (pas de table de hachage sur tous les couples)
        cmd("CREATE TABLE mc_candidate (id_fqdn Int64, id_plugin Int64, fqdn String, "
            f"status {STATUS}, score UInt8, "
            "signals Array(LowCardinality(String)), anchor String, nb_pivots UInt64) "
            "ENGINE = MergeTree ORDER BY (id_plugin, id_fqdn)")
        cmd("INSERT INTO mc_candidate "
            "SELECT id_fqdn, id_plugin, fqdn, "
            "       multiIf(empty(joinGet('mc_own', 'own', id_fqdn)), 'non_classe', "
            "               has(signals, 'tenant'), 'complement', 'conflit'), "
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

        # 7. résultats. Valeur du FQDN déjà connue par l'ancêtre ; recherche
        #    dans fqdn (projection p_id) pour les seuls candidats des pivots
        cmd("CREATE OR REPLACE TABLE classification_candidate ("
            " id_fqdn Int64, fqdn String, id_plugin Int64, plugin String,"
            f" status {STATUS}, score UInt8,"
            " signals Array(LowCardinality(String)), anchor String, nb_pivots UInt64,"
            " computed_at DateTime) "
            "ENGINE = MergeTree ORDER BY (plugin, status, fqdn)")
        cmd("INSERT INTO classification_candidate "
            "SELECT c.id_fqdn, if(c.fqdn != '', c.fqdn, f.fqdn), c.id_plugin, "
            "       joinGet('mc_plugin_name', 'value', c.id_plugin), "
            "       c.status, c.score, c.signals, c.anchor, c.nb_pivots, now() "
            "FROM mc_candidate AS c "
            "LEFT JOIN (SELECT id_fqdn, any(value) AS fqdn FROM fqdn"
            "           WHERE id_fqdn IN (SELECT id_fqdn FROM mc_candidate WHERE fqdn = '')"
            "           GROUP BY id_fqdn) AS f ON f.id_fqdn = c.id_fqdn")
        for status, n in client.query(
                "SELECT status, count() FROM classification_candidate "
                "GROUP BY status ORDER BY status").result_rows:
            step(f"classification_candidate : {n:,} {status}")

        #    clients des plateformes (hors labels techniques), plateformes
        cmd("CREATE OR REPLACE TABLE classification_tenant ("
            " id_fqdn Int64, fqdn String, platform String, tenant String,"
            " nom_generique UInt8, plugins_proposes Array(String), domaine_marque String,"
            " plugins_lies Array(String), computed_at DateTime) "
            "ENGINE = MergeTree ORDER BY (platform, tenant, fqdn)")
        cmd("INSERT INTO classification_tenant "
            "SELECT id_fqdn, value, anchor, tenant, "
            "       joinGet('mc_generic', 'generic', brand), "
            "       arrayMap(x -> joinGet('mc_plugin_name', 'value', x),"
            f"                arrayFilter(x -> NOT has(own, x), {BRAND_PLUGINS})), "
            "       if(brand != '', joinGet('mc_brand', 'domain', brand), ''), "
            "       arrayMap(x -> joinGet('mc_plugin_name', 'value', x), own), now() "
            "FROM mc_ancestor "
            "WHERE joinGet('mc_platform', 'platform', anchor) = 1 "
            "AND NOT has({tech:Array(String)}, tenant)")
        cmd("CREATE OR REPLACE TABLE classification_platform_detected ("
            " platform String, plugins Array(String),"
            " nb_tenants UInt64, nb_brand UInt64, nb_brand_plugins UInt64,"
            " nb_classified UInt64, nb_classified_other UInt64, nb_plugins_other UInt64,"
            " computed_at DateTime) "
            "ENGINE = MergeTree ORDER BY platform")
        cmd("INSERT INTO classification_platform_detected "
            "SELECT anchor, arrayMap(x -> joinGet('mc_plugin_name', 'value', x), plugins), "
            "       nb_tenants, nb_brand, nb_brand_plugins, nb_classified, "
            "       nb_classified_other, nb_plugins_other, now() "
            "FROM mc_platform_stat WHERE joinGet('mc_platform', 'platform', anchor) = 1")
        n_ten = count("SELECT count() FROM classification_tenant")
        n_tm = count("SELECT countIf(notEmpty(plugins_proposes)) FROM classification_tenant")
        step(f"classification_tenant : {n_ten:,} clients de plateformes, "
             f"{n_tm:,} avec un plugin proposé")
    finally:
        for tbl in STAGING:
            cmd(f"DROP TABLE IF EXISTS {tbl}")


def evaluate(client, params) -> None:
    """Rappel sur les FQDN cachés : couple (FQDN, plugin) caché retrouvé par
    héritage implicite (ancêtre hors plateforme), par les candidats (score
    suffisant) et par chaque signal seul. Précision approchée : part des
    propositions faites sur les FQDN cachés qui donnent le bon plugin."""
    def q(sql):
        return client.query(sql, parameters=params, settings=SETTINGS).result_rows

    hidden = q("SELECT count() FROM mc_hidden")[0][0]
    if not hidden:
        print("aucun FQDN caché : augmenter --holdout")
        return
    inherited = ("SELECT id_fqdn, arrayJoin(plugins) FROM mc_ancestor "
                 "WHERE id_fqdn IN (SELECT id_fqdn FROM mc_hidden) "
                 "AND joinGet('mc_platform', 'platform', anchor) = 0")
    candidates = "SELECT id_fqdn, id_plugin FROM mc_candidate"
    rows = q(
        "SELECT 'total (hérité ou candidat)',"
        f"       countIf((id_fqdn, id_plugin) IN ({inherited})"
        f"            OR (id_fqdn, id_plugin) IN ({candidates})), 0 "
        "FROM mc_hidden "
        "UNION ALL "
        f"SELECT 'hérité (implicite)', countIf((id_fqdn, id_plugin) IN ({inherited})), 0 "
        "FROM mc_hidden "
        "UNION ALL "
        "SELECT 'candidats (score >= ' || toString({min_score:UInt8}) || ')',"
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
    print(f"{'signal':<28}{'retrouvés':>10}{'rappel':>9}{'propositions':>14}{'précision':>11}")
    order = {"total": 0, "hérité": 1, "candidats": 2}
    for sig, found, proposed in sorted(rows, key=lambda r: (order.get(r[0].split()[0], 3), r[0])):
        prec = f"{found / proposed:.0%}" if proposed else "-"
        print(f"{sig:<28}{found:>10,}{found / hidden:>9.0%}{proposed:>14,}{prec:>11}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="FQDN mal classifiés → classification_candidate, "
                                             "clients des plateformes → classification_tenant")
    ap.add_argument("--holdout", type=float, default=0, metavar="PCT",
                    help="évaluation : %% de FQDN classifiés cachés, rappel mesuré, "
                         "tables de résultats inchangées")
    ap.add_argument("--database", default="default")
    args = ap.parse_args()
    if not 0 <= args.holdout < 100:
        ap.error("--holdout entre 0 et 100")
    find_misclassified(args.holdout, args.database)
