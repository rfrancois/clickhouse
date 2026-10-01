-- ============================================================
-- MIGRATION 1/2 — copies (link_new, property_detection)
-- ============================================================
-- Dates de première détection conservées (cf. sql/02_optimized.sql) :
--  * link passe de ReplacingMergeTree(version) à AggregatingMergeTree avec
--    detection_date = min et version = max : un lien ré-envoyé garde sa
--    PREMIÈRE date de détection et prend la DERNIÈRE date de mise à jour ;
--  * property.detection_date part dans une table à part, property_detection
--    (AggregatingMergeTree, min) ; property reste en ReplacingMergeTree.
--
-- Rien n'est modifié ici : on remplit link_new et property_detection à côté,
-- puis 07_migrate_link_swap.sql échange link / link_new (instantané) et
-- supprime property.detection_date.
-- Prérequis : aucun import pendant la copie ni jusqu'à la bascule (ce qui
-- serait écrit entre-temps dans link ou property ne serait pas copié) ;
-- espace disque ≈ taille actuelle de link.
-- Relançable : relancer le fichier ENTIER ; les lignes copiées deux fois
-- sont fusionnées par AggregatingMergeTree (min / max : même résultat).

-- garde-fou : base déjà migrée → arrêt (rien n'est créé)
SELECT throwIf(engine = 'AggregatingMergeTree',
               'link est déjà en AggregatingMergeTree : migration déjà faite')
FROM system.tables WHERE database = currentDatabase() AND name = 'link'
FORMAT Null;

CREATE TABLE IF NOT EXISTS link_new
(
    type_1         Enum8('application' = 1, 'capture' = 2, 'fqdn' = 3, 'ip' = 4,
                         'plugin' = 5, 'organization_name' = 6, 'organization_id' = 7,
                         'phone' = 8, 'social_id' = 9),
    id_1           Int64,
    type_2         Enum8('application' = 1, 'capture' = 2, 'fqdn' = 3, 'ip' = 4,
                         'plugin' = 5, 'organization_name' = 6, 'organization_id' = 7,
                         'phone' = 8, 'social_id' = 9),
    id_2           Int64,
    id_source      Int32,
    detection_date SimpleAggregateFunction(min, Int32),  -- comme 02_optimized.sql
    version        SimpleAggregateFunction(max, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY type_1
PRIMARY KEY (type_1, id_1)
ORDER BY (type_1, id_1, type_2, id_2, id_source);

-- Copie en streaming (pas d'agrégation à l'insertion : mémoire bornée par
-- la taille des blocs, pas par la taille de la table)
INSERT INTO link_new
SELECT type_1, id_1, type_2, id_2, id_source, toInt32(detection_date), version FROM link
SETTINGS max_threads = 8, max_insert_threads = 4;

-- property.detection_date → property_detection (min : les doublons de
-- property pas encore fusionnés donnent leur plus ancienne date)
CREATE TABLE IF NOT EXISTS property_detection
(
    node_type      Enum8('application' = 1, 'capture' = 2, 'fqdn' = 3, 'ip' = 4,
                         'plugin' = 5, 'organization_name' = 6, 'organization_id' = 7,
                         'phone' = 8, 'social_id' = 9),
    id_node        Int64,
    id_source      Int32,
    detection_date SimpleAggregateFunction(min, DateTime)
)
ENGINE = AggregatingMergeTree
PARTITION BY node_type
ORDER BY (node_type, id_node, id_source);

INSERT INTO property_detection
SELECT node_type, id_node, id_source, detection_date FROM property
SETTINGS max_threads = 8, max_insert_threads = 4;

-- Contrôle : mêmes nombres de lignes attendus (link_new peut être plus petit
-- si link contenait des doublons pas encore fusionnés)
SELECT 'link' AS tbl, count() AS lignes, uniq(type_1, id_1, type_2, id_2, id_source) AS distinctes_approx FROM link
UNION ALL
SELECT 'link_new', count(), uniq(type_1, id_1, type_2, id_2, id_source) FROM link_new
UNION ALL
SELECT 'property', count(), uniq(node_type, id_node, id_source) FROM property
UNION ALL
SELECT 'property_detection', count(), uniq(node_type, id_node, id_source) FROM property_detection
FORMAT PrettyCompactMonoBlock;

SELECT table, formatReadableSize(sum(bytes_on_disk)) AS disque
FROM system.parts
WHERE active AND database = currentDatabase() AND table IN ('link', 'link_new')
GROUP BY table
FORMAT PrettyCompactMonoBlock;
