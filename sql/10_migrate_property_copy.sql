-- ============================================================
-- MIGRATION property 1/2 — copie (property_new)
-- ============================================================
-- property (ReplacingMergeTree(version)) + property_detection
-- (AggregatingMergeTree, min) → une seule table property en
-- AggregatingMergeTree (cf. sql/02_optimized.sql), même clé
-- (node_type, id_node, id_source) :
--   * payload        : celui du DERNIER insert (anyLast) ;
--   * version        : la plus récente (max) ;
--   * detection_date : la plus ancienne (min), en timestamp Unix Int32
--     (property_detection est en DateTime : converti ici).
-- À lancer sur une base où property_detection existe (après make migration
-- et make migration-swap).
--
-- Rien n'est modifié ici : on remplit property_new à côté, puis
-- 11_migrate_property_swap.sql échange property / property_new (instantané)
-- et met property_detection de côté.
-- Prérequis : aucun import pendant la copie ni jusqu'à la bascule ; espace
-- disque ≈ taille actuelle de property.
-- Relançable : relancer le fichier ENTIER (les deux passes, dans l'ordre) ;
-- les lignes copiées deux fois fusionnent (même résultat).
--
-- Copie en flux, sans jointure, en DEUX PASSES dont l'ordre compte (anyLast
-- = dernier inséré) :
--   1. property_detection → detection_date seule (payload '', version 0) ;
--   2. property FINAL (dernière version de chaque ligne) → payload, version,
--      avec une detection_date au maximum de Int32 (2147483647, 2038-01-19)
--      que min() écarte au profit de la date de la passe 1.
-- Une ligne présente dans une seule des deux tables (anormal : l'import
-- écrit toujours les deux) garderait payload '' / version 0, ou la date
-- 2147483647 : le contrôle en fin de fichier les compte.

-- garde-fous : base déjà migrée, ou property_detection absente → arrêt
SELECT throwIf(engine = 'AggregatingMergeTree',
               'property est déjà en AggregatingMergeTree : migration déjà faite')
FROM system.tables WHERE database = currentDatabase() AND name = 'property'
FORMAT Null;
SELECT throwIf(count() = 0,
               'property_detection absente : lancer d''abord make migration puis make migration-swap')
FROM system.tables WHERE database = currentDatabase() AND name = 'property_detection'
FORMAT Null;

CREATE TABLE IF NOT EXISTS property_new
(
    node_type      Enum8('application' = 1, 'capture' = 2, 'fqdn' = 3, 'ip' = 4,
                         'plugin' = 5, 'organization_name' = 6, 'organization_id' = 7,
                         'phone' = 8, 'social_id' = 9),
    id_node        Int64,
    id_source      Int32,
    payload        SimpleAggregateFunction(anyLast, String) CODEC(ZSTD(3)),
    version        SimpleAggregateFunction(max, UInt64),
    detection_date SimpleAggregateFunction(min, Int32),
    PROJECTION p_source (SELECT _part_offset ORDER BY (id_source, node_type))
)
ENGINE = AggregatingMergeTree
PARTITION BY node_type
ORDER BY (node_type, id_node, id_source)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

-- passe 1 : dates de première détection
INSERT INTO property_new (node_type, id_node, id_source, payload, version, detection_date)
SELECT node_type, id_node, id_source, '', 0, toInt32(detection_date) FROM property_detection
SETTINGS max_threads = 8, max_insert_threads = 4;

-- passe 2 : payloads, APRÈS la passe 1 (anyLast garde ceux-ci)
INSERT INTO property_new (node_type, id_node, id_source, payload, version, detection_date)
SELECT node_type, id_node, id_source, payload, version, toInt32(2147483647)
FROM property FINAL
SETTINGS max_threads = 8, max_insert_threads = 4;

-- Contrôle : lignes_new ≈ distinctes de property (le double après une
-- relance, avant fusion) ; sans_payload / sans_detection doivent valoir 0
SELECT 'property' AS tbl, count() AS lignes, uniq(node_type, id_node, id_source) AS distinctes_approx FROM property
UNION ALL
SELECT 'property_detection', count(), uniq(node_type, id_node, id_source) FROM property_detection
UNION ALL
SELECT 'property_new', count(), uniq(node_type, id_node, id_source) FROM property_new
FORMAT PrettyCompactMonoBlock;

SELECT countIf(version = 0) AS sans_payload,
       countIf(detection_date = 2147483647) AS sans_detection
FROM property_new FINAL
FORMAT PrettyCompactMonoBlock;

SELECT table, formatReadableSize(sum(bytes_on_disk)) AS disque
FROM system.parts
WHERE active AND database = currentDatabase()
  AND table IN ('property', 'property_detection', 'property_new')
GROUP BY table
ORDER BY table
FORMAT PrettyCompactMonoBlock;
