-- ============================================================
-- CRÉATION de la table property sur une base existante
-- ============================================================
-- property contient les informations (payload) de chaque nœud, pour chaque
-- source : une ligne par (node_type, id_node, id_source), dernière
-- version seulement. Aucune donnée à migrer : la table est créée vide.
-- Relançable (IF NOT EXISTS).
--
-- Lecture de la dernière version (FINAL : doublons pas encore fusionnés) :
--   SELECT id_source, payload, detection_date FROM property FINAL
--   WHERE node_type = 'fqdn' AND id_node = 123456;

CREATE TABLE IF NOT EXISTS property
(
    -- même Enum8 que link : ajouter un nouveau type À LA FIN, partout
    node_type      Enum8('application' = 1, 'capture' = 2, 'fqdn' = 3, 'ip' = 4,
                         'plugin' = 5, 'organization_name' = 6, 'organization_id' = 7,
                         'phone' = 8, 'social_id' = 9),
    -- même id que fqdn_search.id_fqdn / ip_search.id_ip / link.id_1|id_2
    id_node        Int64,
    id_source      Int32,
    -- renvoyé tel quel, jamais filtré : String compressé plutôt que JSON typé
    payload        String CODEC(ZSTD(3)),
    detection_date DateTime,
    version        UInt64,
    -- « tout ce qu'a produit la source X » : projection légère (positions
    -- des lignes seulement, le payload n'est pas dupliqué)
    PROJECTION p_source (SELECT _part_offset ORDER BY (id_source, node_type))
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY node_type
-- une ligne par (nœud, source) : une nouvelle détection d'une même source
-- remplace l'ancienne (dernière version), pas d'historique
ORDER BY (node_type, id_node, id_source)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

SELECT create_table_query FROM system.tables
WHERE database = currentDatabase() AND name = 'property' FORMAT Vertical;
