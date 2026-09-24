-- ============================================================
-- SCHÉMA OPTIMISÉ — la "solution" (cf. SOLUTION.md)
-- ============================================================

DROP TABLE IF EXISTS fqdn;
DROP TABLE IF EXISTS ip;
DROP TABLE IF EXISTS application;
DROP TABLE IF EXISTS capture;
DROP TABLE IF EXISTS plugin;
DROP TABLE IF EXISTS organization_name;
DROP TABLE IF EXISTS organization_id;
DROP TABLE IF EXISTS phone;
DROP TABLE IF EXISTS social_id;
DROP TABLE IF EXISTS link;
DROP TABLE IF EXISTS property;

-- 1) Même schéma que la naïve (value String) + index de saut n-grammes :
--    c'est l'index qui fait toute la différence, pas un changement de format
CREATE TABLE fqdn
(
    value    String,
    id_fqdn  Int64,
    rank     UInt32,
    version  UInt64,
    INDEX idx_ngram value TYPE ngrambf_v1(3, 16384, 4, 0) GRANULARITY 1,
    -- index texte EXACT pour LIKE '%…%' : lit ~5x moins de blocs que le ngram
    -- (probabiliste). Le ngram est gardé tant que les imports avec l'index
    -- texte ne sont pas validés
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

CREATE TABLE ip
(
    value    String,
    id_ip    Int64,
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

-- 1 bis) une table de valeurs par autre type de nœud (application, capture,
--    plugin, organization_name, organization_id, phone, social_id).
--    Même modèle que la table ip (value, id, version), SANS rank : ces types
--    ne sont pas classés. Tri par valeur (résolution valeur → id à l'import),
--    projection légère par id (jointure liens → valeur). Nouveau type de
--    nœud → nouvelle table <type> ici + NODE_TABLES dans
--    scripts/import_data.py.

CREATE TABLE application
(
    value                String,
    id_application       Int64,
    version              UInt64,
    PROJECTION p_id (SELECT _part_offset ORDER BY id_application)
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (value, id_application)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

CREATE TABLE capture
(
    value                String,
    id_capture           Int64,
    version              UInt64,
    PROJECTION p_id (SELECT _part_offset ORDER BY id_capture)
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (value, id_capture)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

CREATE TABLE plugin
(
    value                String,
    id_plugin            Int64,
    version              UInt64,
    PROJECTION p_id (SELECT _part_offset ORDER BY id_plugin)
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (value, id_plugin)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

CREATE TABLE organization_name
(
    value                String,
    id_organization_name Int64,
    version              UInt64,
    PROJECTION p_id (SELECT _part_offset ORDER BY id_organization_name)
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (value, id_organization_name)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

CREATE TABLE organization_id
(
    value                String,
    id_organization_id   Int64,
    version              UInt64,
    PROJECTION p_id (SELECT _part_offset ORDER BY id_organization_id)
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (value, id_organization_id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

CREATE TABLE phone
(
    value                String,
    id_phone             Int64,
    version              UInt64,
    PROJECTION p_id (SELECT _part_offset ORDER BY id_phone)
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (value, id_phone)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

CREATE TABLE social_id
(
    value                String,
    id_social_id         Int64,
    version              UInt64,
    PROJECTION p_id (SELECT _part_offset ORDER BY id_social_id)
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (value, id_social_id)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

-- 2) link : une seule table pour tous les couples de nœuds, TYPÉS.
--    Les ids ne sont uniques qu'à l'intérieur d'un type (id_fqdn = 42 et
--    id_ip = 42 coexistent) : (type, id) identifie un nœud, pas l'id seul.
--    Nouveau type de nœud → ajouter la valeur À LA FIN des deux Enum8
--    (ALTER ... MODIFY COLUMN, métadonnées seules, aucune réécriture).
--    Chaque lien est inséré DEUX FOIS (A→B et B→A, cf. sql/05_import_distribute.sql
--    et distribute_links() dans scripts/import_data.py) : un simple filtre sur
--    (type_1, id_1) retrouve les voisins dans les deux sens, sans projection ni
--    UNION à la lecture. Pas de projection inverse ici : dupliquer la ligne
--    coûte à peu près la même place disque qu'une projection qui recopiait déjà
--    toutes les colonnes triées dans l'autre sens — la contrepartie est que
--    l'import doit toujours écrire les deux lignes ensemble (ClickHouse ne les
--    garde plus synchronisées automatiquement).
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
    version        UInt64
)
ENGINE = ReplacingMergeTree(version)
-- une partition par type de l'extrémité 1 : suppression / réimport d'un type
-- entier par ALTER TABLE link DROP PARTITION 'xxx'
PARTITION BY type_1
-- index primaire (en RAM) réduit à ce qu'on filtre vraiment ; le tri complet
-- reste la clé de déduplication : un couple ORIENTÉ PAR SOURCE = une ligne
-- (A→B et B→A sont deux lignes distinctes ; un même lien vu par deux sources
-- aussi). Une nouvelle détection d'une même source remplace l'ancienne,
-- comme dans property. Voisins distincts : DISTINCT / GROUP BY à la lecture.
PRIMARY KEY (type_1, id_1)
ORDER BY (type_1, id_1, type_2, id_2, source_id);

-- 3) property : les informations (payload) de chaque nœud, par source.
--    (node_type, id_node) identifie le nœud, comme dans link.
CREATE TABLE property
(
    -- même Enum8 que link : ajouter un nouveau type À LA FIN, partout
    node_type      Enum8('application' = 1, 'capture' = 2, 'fqdn' = 3, 'ip' = 4,
                         'plugin' = 5, 'organization_name' = 6, 'organization_id' = 7,
                         'phone' = 8, 'social_id' = 9),
    -- même id que fqdn.id_fqdn / ip.id_ip / link.id_1|id_2
    id_node        Int64,
    id_source      Int64,
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
