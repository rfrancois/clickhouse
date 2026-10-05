-- ============================================================
-- MIGRATION : création de la table rank
-- ============================================================
-- Pour une base existante (make init la crée déjà, mais supprime tout) :
-- même table que sql/02_optimized.sql (commentaires détaillés là-bas),
-- IF NOT EXISTS, aucune table existante modifiée. Relançable.
-- Prérequis de scripts/send_ranks.py (make ranks).

CREATE TABLE IF NOT EXISTS rank
(
    node_type  Enum8('application' = 1, 'capture' = 2, 'fqdn' = 3, 'ip' = 4,
                     'plugin' = 5, 'organization_name' = 6, 'organization_id' = 7,
                     'phone' = 8, 'social_id' = 9),
    id_node    Int64 CODEC(Delta, ZSTD),
    id_source  Int32,
    creation_date  Int32 CODEC(DoubleDelta, ZSTD),
    rank       Int32 CODEC(Delta, ZSTD)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(toDateTime(creation_date, 'UTC'))
ORDER BY (node_type, id_node, id_source, creation_date)
TTL toDateTime(creation_date, 'UTC') + INTERVAL 2 YEAR DELETE
SETTINGS ttl_only_drop_parts = 1;

SELECT name AS table_, engine FROM system.tables
WHERE database = currentDatabase() AND name = 'rank';
