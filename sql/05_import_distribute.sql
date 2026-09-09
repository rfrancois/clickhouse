-- ============================================================
-- DISTRIBUTION staging → tables optimisées uniquement
-- (fqdn_search / ip_search / link_opt — les tables naïves ne sont
--  plus alimentées par l'import)
-- ============================================================
-- Hypothèses (cf. README) :
--  * node.csv fournit id/rank/date ; domains.json n'a ni id ni rank
--    → id synthétique = cityHash64(valeur) tronqué à 31 bits, rank = 0,
--      version = now()
--  * les liens référencent des VALEURS de nœuds : résolution valeur → id
--    par jointure sur les tables optimisées. Un lien dont une extrémité
--    est inconnue est ignoré (jointure INNER).

-- ------------------------------------------------------------
-- Garde-fous mémoire (machine mono-nœud ~30 Gio de RAM).
-- Appliqués à toute la session --multiquery : chaque requête lourde
-- plafonne sa RAM et déborde sur disque au lieu de faire tuer le
-- process par l'OOM killer du noyau (exit 137).
-- ------------------------------------------------------------
-- La VM Docker a peu de RAM : au-delà de ~8 Gio/requête le noyau tue le
-- serveur (exit 137) AVANT que ClickHouse ne voie sa limite. On vise donc
-- une empreinte minimale : 1 thread, plafond bas, débordement disque très tôt.
SET max_threads = 1;                                  -- un seul jeu de tampons
SET max_memory_usage = 6000000000;                   -- 6 Gio / requête (échec propre bien avant l'OOM noyau)
SET max_bytes_before_external_group_by = 536870912;   -- 512 Mio → spill disque
SET max_bytes_before_external_sort = 536870912;       -- 512 Mio → spill disque
SET join_algorithm = 'full_sorting_merge';            -- jointure par tri-fusion externe

-- ---------- 1) node.csv → fqdn / ip ----------

INSERT INTO fqdn_search (value, id_fqdn, rank, version)
SELECT value,
       toInt32OrZero(id),
       toUInt32OrZero(rank),
       coalesce(toUnixTimestamp(parseDateTimeBestEffortOrNull(creation_date)),
                toUnixTimestamp(now()))
FROM stg_node
WHERE lower(node_type) = 'fqdn' AND value != '';

INSERT INTO ip_search (value, id_ip, rank, version)
SELECT value,
       toInt32OrZero(id),
       toUInt32OrZero(rank),
       coalesce(toUnixTimestamp(parseDateTimeBestEffortOrNull(creation_date)),
                toUnixTimestamp(now()))
FROM stg_node
WHERE lower(node_type) = 'ip' AND value != '';

-- ---------- 2) domains.json → fqdn (cn + chaque entrée dns) et ip ----------

INSERT INTO fqdn_search (value, id_fqdn, rank, version)
SELECT cn,
       toInt32(bitAnd(cityHash64(cn), 0x7FFFFFFF)),
       0,
       toUnixTimestamp(now())
FROM stg_domain
WHERE cn IS NOT NULL AND cn != '';

INSERT INTO fqdn_search (value, id_fqdn, rank, version)
SELECT value,
       toInt32(bitAnd(cityHash64(value), 0x7FFFFFFF)),
       0,
       toUnixTimestamp(now())
FROM (SELECT arrayJoin(dns) AS value FROM stg_domain)
WHERE value IS NOT NULL AND value != '';

INSERT INTO ip_search (value, id_ip, rank, version)
SELECT ip,
       toInt32(bitAnd(cityHash64(ip), 0x7FFFFFFF)),
       0,
       toUnixTimestamp(now())
FROM stg_domain
WHERE ip IS NOT NULL AND ip != '';

-- ---------- 3) liens : résolution valeur → id typé → link_opt (ids Int64) ----------
-- links.csv référence des VALEURS de nœuds ("google.com", "1.2.3.4") + le type
-- de chaque extrémité (type_1 / type_2 = 'fqdn' | 'ip'). On remplace chaque
-- valeur par son id :
--   type = 'fqdn' → id_fqdn depuis fqdn_search
--   type = 'ip'   → id_ip   depuis ip_search
-- Les ids ne sont JAMAIS créés ici : une extrémité absente de ces tables
-- fait ignorer le lien (INNER JOIN). Importer d'abord le *node*.csv
-- correspondant si besoin.

-- Étape 3a : on ne résout QUE les valeurs de nœuds réellement citées par les
-- liens à importer — surtout PAS toute la table fqdn_search / ip_search : un
-- GROUP BY value global sur des dizaines de millions de String fait exploser
-- la RAM (MEMORY_LIMIT_EXCEEDED).

-- 3a-1 : ensemble (type, valeur) référencé par stg_link (petit : borné par le
-- nombre de nœuds distincts du fichier, pas par la taille des tables).
DROP TABLE IF EXISTS tmp_link_values;
CREATE TABLE tmp_link_values (node_type String, value String)
ENGINE = MergeTree
ORDER BY (node_type, value);

INSERT INTO tmp_link_values
SELECT node_type, value
FROM (
    SELECT lower(type_1) AS node_type, id_node_1 AS value FROM stg_link
    UNION ALL
    SELECT lower(type_2) AS node_type, id_node_2 AS value FROM stg_link
)
WHERE value != ''
GROUP BY node_type, value;

-- 3a-2 : correspondance (type, valeur) → id, restreinte aux valeurs ci-dessus.
-- `value IN (sous-requête)` (et NON un JOIN) : la grande table est parcourue en
-- streaming avec un simple test d'appartenance, sans tri global (le JOIN, lui,
-- force full_sorting_merge à trier toute fqdn_search/ip_search → OOM).
-- Le set du IN est petit : tmp_link_values est déjà dédupliqué.
-- Deux INSERT séparés (pas d'UNION ALL) pour ne pas cumuler les deux
-- agrégations en mémoire. GROUP BY argMax : on garde l'id de la version la
-- plus récente ; il déborde sur disque (réglages session).
DROP TABLE IF EXISTS tmp_node_map;
CREATE TABLE tmp_node_map (node_type String, value String, id Int64)
ENGINE = MergeTree
ORDER BY (node_type, value);

INSERT INTO tmp_node_map
SELECT 'fqdn', value, argMax(toInt64(id_fqdn), version)
FROM fqdn_search
WHERE value IN (SELECT value FROM tmp_link_values WHERE node_type = 'fqdn')
GROUP BY value;

INSERT INTO tmp_node_map
SELECT 'ip', value, argMax(toInt64(id_ip), version)
FROM ip_search
WHERE value IN (SELECT value FROM tmp_link_values WHERE node_type = 'ip')
GROUP BY value;

-- Étape 3b : résolution des deux extrémités, UNE jointure simple à la fois.
-- On évite la jointure chaînée (l ⋈ n1 ⋈ n2) qui empile deux tris externes
-- dans la même requête ; ici chaque étape = 1 tri-fusion, mémoire bornée.

-- 3b-1 : remplace l'extrémité 1 par son id. tmp_link_r1 est trié sur (t2,
-- node_2) pour que la jointure 3b-2 n'ait plus rien à trier de ce côté.
DROP TABLE IF EXISTS tmp_link_r1;
CREATE TABLE tmp_link_r1
(
    id_node_1     Int64,
    node_2        String,
    t2            String,
    id_source     String,
    creation_date String,
    update_date   String
)
ENGINE = MergeTree
ORDER BY (t2, node_2);

INSERT INTO tmp_link_r1
SELECT n1.id, l.id_node_2, l.t2, l.id_source, l.creation_date, l.update_date
FROM (
    SELECT id_node_1, id_node_2, id_source, creation_date, update_date,
           lower(type_1) AS t1, lower(type_2) AS t2
    FROM stg_link
) AS l
INNER JOIN tmp_node_map AS n1 ON n1.node_type = l.t1 AND n1.value = l.id_node_1;

-- 3b-2 : remplace l'extrémité 2 par son id.
DROP TABLE IF EXISTS tmp_link_r2;
CREATE TABLE tmp_link_r2
(
    id_node_1     Int64,
    id_node_2     Int64,
    id_source     String,
    creation_date String,
    update_date   String
)
ENGINE = MergeTree
ORDER BY (id_node_1, id_node_2);

INSERT INTO tmp_link_r2
SELECT r.id_node_1, n2.id, r.id_source, r.creation_date, r.update_date
FROM tmp_link_r1 AS r
INNER JOIN tmp_node_map AS n2 ON n2.node_type = r.t2 AND n2.value = r.node_2;

-- Diagnostic (affiché pendant l'import) : combien de liens perdus, et où.
SELECT (SELECT count() FROM stg_link)                                AS liens_staging,
       (SELECT count() FROM tmp_link_r1)                             AS noeud1_resolu,
       (SELECT count() FROM tmp_link_r2)                             AS deux_noeuds_resolus,
       (SELECT count() FROM stg_link) - (SELECT count() FROM tmp_link_r2)
                                                                     AS liens_ignores_noeud_inconnu;

-- 3b-3 : typage final → link_opt.
INSERT INTO link_opt (id_node_1, id_node_2, source_id, detection_date, version)
SELECT id_node_1,
       id_node_2,
       toInt32OrZero(id_source),
       coalesce(toUnixTimestamp(parseDateTimeBestEffortOrNull(creation_date)),
                toUnixTimestamp(now())),
       coalesce(toUnixTimestamp(parseDateTimeBestEffortOrNull(update_date)),
                toUnixTimestamp(now()))
FROM tmp_link_r2;

DROP TABLE tmp_node_map;
DROP TABLE IF EXISTS tmp_link_values;
DROP TABLE IF EXISTS tmp_link_r1;
DROP TABLE IF EXISTS tmp_link_r2;

-- ---------- 4) staging nettoyé ----------

DROP TABLE stg_node;
DROP TABLE stg_link;
DROP TABLE stg_domain;
