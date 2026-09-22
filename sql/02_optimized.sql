-- ============================================================
-- SCHÉMA OPTIMISÉ — la "solution" (cf. SOLUTION.md)
-- ============================================================

DROP TABLE IF EXISTS fqdn_search;
DROP TABLE IF EXISTS ip_search;
DROP TABLE IF EXISTS link;

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

-- 2) link : une seule table pour tous les couples d'entités, TYPÉS.
--    Les ids ne sont uniques qu'à l'intérieur d'un type (id_fqdn = 42 et
--    id_ip = 42 coexistent) : (type, id) identifie un nœud, pas l'id seul.
--    Nouveau type d'entité → ajouter la valeur À LA FIN des deux Enum8
--    (ALTER ... MODIFY COLUMN, métadonnées seules, aucune réécriture).
CREATE TABLE link
(
    type_1         Enum8('application' = 1, 'capture' = 2, 'fqdn' = 3, 'ip' = 4,
                         'plugin' = 5, 'organization_name' = 6, 'organization_id' = 7,
                         'phone' = 8, 'social_id' = 9),
    id_1           Int64,
    type_2         Enum8('application' = 1, 'capture' = 2, 'fqdn' = 3, 'ip' = 4,
                         'plugin' = 5, 'organization_name' = 6, 'organization_id' = 7,
                         'phone' = 8, 'social_id' = 9),
    id_2           Int64,
    source_id      Int32,
    detection_date UInt64,
    version        UInt64,
    -- sens inverse (voisins de l'extrémité 2) : même données, autre tri
    PROJECTION p_reverse
    (
        SELECT type_2, id_2, type_1, id_1, source_id, detection_date, version
        ORDER BY (type_2, id_2, type_1, id_1)
    )
)
ENGINE = ReplacingMergeTree(version)
-- une partition par type de l'extrémité 1 : suppression / réimport d'un type
-- entier par ALTER TABLE link DROP PARTITION 'xxx'
PARTITION BY type_1
-- index primaire (en RAM) réduit à ce qu'on filtre vraiment ; le tri complet
-- reste la clé de déduplication (un couple = une ligne)
PRIMARY KEY (type_1, id_1)
ORDER BY (type_1, id_1, type_2, id_2)
-- 'rebuild' et non 'drop' : avec 'drop', chaque fusion de dédoublonnage
-- supprime p_reverse de la part fusionnée → le sens inverse retombe en scan
-- complet au fil des merges
SETTINGS deduplicate_merge_projection_mode = 'rebuild';
