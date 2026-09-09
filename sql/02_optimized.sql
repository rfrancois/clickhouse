-- ============================================================
-- SCHÉMA OPTIMISÉ — la "solution" (cf. SOLUTION.md)
-- ============================================================

DROP TABLE IF EXISTS fqdn_search;
DROP TABLE IF EXISTS ip_search;
DROP TABLE IF EXISTS link_opt;

-- 1) Même schéma que la naïve (value String) + index de saut n-grammes :
--    c'est l'index qui fait toute la différence, pas un changement de format
CREATE TABLE fqdn_search
(
    value    String,
    id_fqdn  Int32,
    rank     UInt32,
    version  UInt64,
    INDEX idx_ngram value TYPE ngrambf_v1(3, 16384, 4, 0) GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (id_fqdn, value);

CREATE TABLE ip_search
(
    value    String,
    id_ip    Int32,
    rank     UInt32,
    version  UInt64,
    INDEX idx_ngram value TYPE ngrambf_v1(3, 16384, 4, 0) GRANULARITY 1
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (id_ip, value);

-- 2) link avec ids TYPÉS (Int64) + projection inversée pour le sens inverse
CREATE TABLE link_opt
(
    id_node_1      Int64,
    id_node_2      Int64,
    source_id      Int32,
    detection_date UInt64,
    version        UInt64,
    PROJECTION p_reverse
    (
        SELECT id_node_2, id_node_1, source_id, detection_date, version
        ORDER BY (id_node_2, id_node_1)
    )
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (id_node_1, id_node_2)
SETTINGS deduplicate_merge_projection_mode = 'drop';
