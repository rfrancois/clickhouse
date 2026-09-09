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
GROUP BY node_type, value
SETTINGS max_bytes_before_external_group_by = 4294967296; -- 4 Gio

-- 3a-2 : correspondance (type, valeur) → id, restreinte aux valeurs ci-dessus.
-- ReplacingMergeTree : on garde l'id de la version la plus récente.
DROP TABLE IF EXISTS tmp_node_map;
CREATE TABLE tmp_node_map (node_type String, value String, id Int64)
ENGINE = MergeTree
ORDER BY (node_type, value);

INSERT INTO tmp_node_map
SELECT 'fqdn', value, argMax(toInt64(id_fqdn), version)
FROM fqdn_search
WHERE value IN (SELECT value FROM tmp_link_values WHERE node_type = 'fqdn')
GROUP BY value
UNION ALL
SELECT 'ip', value, argMax(toInt64(id_ip), version)
FROM ip_search
WHERE value IN (SELECT value FROM tmp_link_values WHERE node_type = 'ip')
GROUP BY value
SETTINGS max_bytes_before_external_group_by = 4294967296; -- 4 Gio

-- Diagnostic (affiché pendant l'import) : liens dont au moins une extrémité
-- est inconnue de fqdn_search / ip_search → ils ne seront PAS insérés.
SELECT count() AS liens_ignores_noeud_inconnu
FROM stg_link AS l
WHERE (lower(l.type_1), l.id_node_1) NOT IN (SELECT node_type, value FROM tmp_node_map)
   OR (lower(l.type_2), l.id_node_2) NOT IN (SELECT node_type, value FROM tmp_node_map);

-- Étape 3b : jointure par tri-fusion externe (partial_merge). Les deux côtés
-- sont triés avec débordement disque puis fusionnés : la mémoire reste bornée
-- quel que soit le volume (grace_hash échoue ici : il alloue > 6 Gio d'un coup
-- avant que son spill ne s'amorce → MEMORY_LIMIT_EXCEEDED).
-- Le type est normalisé (lower) dans une sous-requête pour que les clés de
-- jointure restent de simples colonnes (requis par partial_merge).
INSERT INTO link_opt (id_node_1, id_node_2, source_id, detection_date, version)
SELECT n1.id,
       n2.id,
       toInt32OrZero(l.id_source),
       coalesce(toUnixTimestamp(parseDateTimeBestEffortOrNull(l.creation_date)),
                toUnixTimestamp(now())),
       coalesce(toUnixTimestamp(parseDateTimeBestEffortOrNull(l.update_date)),
                toUnixTimestamp(now()))
FROM (
    SELECT id_node_1, id_node_2, id_source, creation_date, update_date,
           lower(type_1) AS t1, lower(type_2) AS t2
    FROM stg_link
) AS l
INNER JOIN tmp_node_map AS n1 ON n1.node_type = l.t1 AND n1.value = l.id_node_1
INNER JOIN tmp_node_map AS n2 ON n2.node_type = l.t2 AND n2.value = l.id_node_2
SETTINGS join_algorithm = 'partial_merge',
         max_bytes_before_external_sort = 8589934592; -- 8 Gio

DROP TABLE tmp_node_map;
DROP TABLE IF EXISTS tmp_link_values;

-- ---------- 4) staging nettoyé ----------

DROP TABLE stg_node;
DROP TABLE stg_link;
DROP TABLE stg_domain;
