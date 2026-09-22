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
    INDEX idx_ngram value TYPE ngrambf_v1(3, 16384, 4, 0) GRANULARITY 1,
    -- index texte EXACT pour LIKE '%…%' : lit ~5x moins de blocs que le ngram
    -- (probabiliste). Le ngram est gardé tant que les imports avec l'index
    -- texte ne sont pas validés (cf. sql/09_add_text_index.sql)
    INDEX idx_text value TYPE text(tokenizer = ngrams(3)),
    -- recherche par id (jointure FQDN → liens → IP) : projection légère
    -- (positions des lignes seulement), triée par id_fqdn
    PROJECTION p_id (SELECT _part_offset ORDER BY id_fqdn)
)
ENGINE = ReplacingMergeTree(version)
-- tri par nom INVERSÉ : tous les *.google.com, google.com.br... sont côte à
-- côte, donc LIKE '%google.com%' ne lit que quelques blocs, et ORDER BY rank
-- (même sans LIMIT) ne trie que ces lignes. Même identité de ligne
-- (id_fqdn, value) qu'avant : la déduplication est inchangée.
ORDER BY (reverse(value), id_fqdn)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

CREATE TABLE ip_search
(
    value    String,
    id_ip    Int32,
    rank     UInt32,
    version  UInt64,
    -- pas d'index ngram/texte : une IP n'a que des chiffres et des points, les
    -- trigrammes sont partout, l'index ne filtre rien (testé, plus lent avec)
    -- recherche par id (jointure liens → IP) : projection légère
    PROJECTION p_id (SELECT _part_offset ORDER BY id_ip)
)
ENGINE = ReplacingMergeTree(version)
-- tri par valeur (ordre normal) : un sous-réseau est contigu, donc
-- LIKE '192.168.%' passe par la clé primaire
ORDER BY (value, id_ip)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

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
