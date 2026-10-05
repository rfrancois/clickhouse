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
DROP TABLE IF EXISTS property_detection;
DROP TABLE IF EXISTS rank;

-- 1) Même schéma que la naïve (value String) + index de saut n-grammes :
--    c'est l'index qui fait toute la différence, pas un changement de format
CREATE TABLE fqdn
(
    value    String,
    -- une ligne par nom, fusion colonne par colonne (AggregatingMergeTree) :
    -- un nom ré-inséré avec un autre id garde son id le plus ANCIEN (ids
    -- auto-incrémentés → min), le rank du DERNIER insert (anyLast, ordre
    -- d'insertion, pas version) et la plus récente version (max).
    -- rank NULL = inconnu (en fin de ORDER BY rank) ; anyLast ignore les
    -- NULL : un insert sans rank n'efface jamais un rank connu
    id_fqdn  SimpleAggregateFunction(min, Int64),
    rank     SimpleAggregateFunction(anyLast, Nullable(Int32)),
    version  SimpleAggregateFunction(max, UInt64),
    INDEX idx_ngram value TYPE ngrambf_v1(3, 16384, 4, 0) GRANULARITY 1,
    -- index texte EXACT pour LIKE '%…%' : lit ~5x moins de blocs que le ngram
    -- (probabiliste). Le ngram est gardé tant que les imports avec l'index
    -- texte ne sont pas validés
    INDEX idx_text value TYPE text(tokenizer = ngrams(3)),
    -- recherche par id (jointure FQDN → liens → IP) : projection légère
    -- (positions des lignes seulement), triée par id_fqdn
    PROJECTION p_id (SELECT _part_offset ORDER BY id_fqdn)
)
ENGINE = AggregatingMergeTree
-- tri par nom INVERSÉ : tous les *.google.com, google.com.br... sont côte à
-- côte, donc LIKE '%google.com%' ne lit que quelques blocs, et ORDER BY rank
-- (même sans LIMIT) ne trie que ces lignes. Identité de ligne = le nom seul
-- (plus id_fqdn) : deux ids pour un même nom fusionnent.
ORDER BY reverse(value)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

CREATE TABLE ip
(
    value    String,
    -- une ligne par IP, même fusion que fqdn (id min, rank anyLast, NULL =
    -- inconnu, version max)
    id_ip    SimpleAggregateFunction(min, Int64),
    rank     SimpleAggregateFunction(anyLast, Nullable(Int32)),
    version  SimpleAggregateFunction(max, UInt64),
    -- pas d'index ngram/texte : une IP n'a que des chiffres et des points, les
    -- trigrammes sont partout, l'index ne filtre rien (testé, plus lent avec)
    -- recherche par id (jointure liens → IP) : projection légère
    PROJECTION p_id (SELECT _part_offset ORDER BY id_ip)
)
ENGINE = AggregatingMergeTree
-- tri par valeur (ordre normal) : un sous-réseau est contigu, donc
-- LIKE '192.168.%' passe par la clé primaire
ORDER BY value
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

-- 1 bis) une table de valeurs par autre type de nœud (application, capture,
--    plugin, organization_name, organization_id, phone, social_id).
--    Même modèle que la table ip (value, id, version), SANS rank : ces types
--    ne sont pas classés. Tri par valeur (résolution valeur → id à l'import),
--    une ligne par valeur (id le plus ancien : min, version : max),
--    projection légère par id (jointure liens → valeur). Nouveau type de
--    nœud → nouvelle table <type> ici + NODE_TABLES dans
--    scripts/import_data.py.

CREATE TABLE application
(
    value                String,
    id_application       SimpleAggregateFunction(min, Int64),
    version              SimpleAggregateFunction(max, UInt64),
    PROJECTION p_id (SELECT _part_offset ORDER BY id_application)
)
ENGINE = AggregatingMergeTree
ORDER BY value
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

CREATE TABLE capture
(
    value                String,
    id_capture           SimpleAggregateFunction(min, Int64),
    version              SimpleAggregateFunction(max, UInt64),
    PROJECTION p_id (SELECT _part_offset ORDER BY id_capture)
)
ENGINE = AggregatingMergeTree
ORDER BY value
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

CREATE TABLE plugin
(
    value                String,
    id_plugin            SimpleAggregateFunction(min, Int64),
    version              SimpleAggregateFunction(max, UInt64),
    PROJECTION p_id (SELECT _part_offset ORDER BY id_plugin)
)
ENGINE = AggregatingMergeTree
ORDER BY value
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

CREATE TABLE organization_name
(
    value                String,
    id_organization_name SimpleAggregateFunction(min, Int64),
    version              SimpleAggregateFunction(max, UInt64),
    PROJECTION p_id (SELECT _part_offset ORDER BY id_organization_name)
)
ENGINE = AggregatingMergeTree
ORDER BY value
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

CREATE TABLE organization_id
(
    value                String,
    id_organization_id   SimpleAggregateFunction(min, Int64),
    version              SimpleAggregateFunction(max, UInt64),
    PROJECTION p_id (SELECT _part_offset ORDER BY id_organization_id)
)
ENGINE = AggregatingMergeTree
ORDER BY value
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

CREATE TABLE phone
(
    value                String,
    id_phone             SimpleAggregateFunction(min, Int64),
    version              SimpleAggregateFunction(max, UInt64),
    PROJECTION p_id (SELECT _part_offset ORDER BY id_phone)
)
ENGINE = AggregatingMergeTree
ORDER BY value
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

CREATE TABLE social_id
(
    value                String,
    id_social_id         SimpleAggregateFunction(min, Int64),
    version              SimpleAggregateFunction(max, UInt64),
    PROJECTION p_id (SELECT _part_offset ORDER BY id_social_id)
)
ENGINE = AggregatingMergeTree
ORDER BY value
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
    id_source      Int32,
    -- fusion colonne par colonne (pas ligne entière comme Replacing) : un
    -- lien ré-envoyé garde sa PREMIÈRE date de détection et prend la DERNIÈRE
    -- date de mise à jour, quel que soit l'ordre d'insertion. Lecture : FINAL
    -- ou GROUP BY avec min(detection_date) / max(version).
    -- timestamp Unix (secondes) ; Int32 comme property.detection_date :
    -- jusqu'au 2038-01-19
    detection_date SimpleAggregateFunction(min, Int32),
    version        SimpleAggregateFunction(max, UInt64)
)
ENGINE = AggregatingMergeTree
-- une partition par type de l'extrémité 1 : suppression / réimport d'un type
-- entier par ALTER TABLE link DROP PARTITION 'xxx'
PARTITION BY type_1
-- index primaire (en RAM) réduit à ce qu'on filtre vraiment ; le tri complet
-- reste la clé de déduplication : un couple ORIENTÉ PAR SOURCE = une ligne
-- (A→B et B→A sont deux lignes distinctes ; un même lien vu par deux sources
-- aussi). Une nouvelle détection d'une même source fusionne avec l'ancienne
-- (min / max des dates ci-dessus). Voisins distincts : DISTINCT / GROUP BY à
-- la lecture.
PRIMARY KEY (type_1, id_1)
ORDER BY (type_1, id_1, type_2, id_2, id_source);

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
    id_source      Int32,
    -- une ligne par (nœud, source), fusion colonne par colonne
    -- (AggregatingMergeTree) : une nouvelle détection d'une même source
    -- donne le payload du DERNIER insert (anyLast, ordre d'insertion, pas
    -- version), la plus récente version (max) et la PREMIÈRE date de
    -- détection (min)
    -- renvoyé tel quel, jamais filtré : String compressé plutôt que JSON typé
    payload        SimpleAggregateFunction(anyLast, String) CODEC(ZSTD(3)),
    version        SimpleAggregateFunction(max, UInt64),
    -- timestamp Unix (secondes) ; Int32 : jusqu'au 2038-01-19
    detection_date SimpleAggregateFunction(min, Int32),
    -- « tout ce qu'a produit la source X » : projection légère (positions
    -- des lignes seulement, le payload n'est pas dupliqué)
    PROJECTION p_source (SELECT _part_offset ORDER BY (id_source, node_type))
)
ENGINE = AggregatingMergeTree
PARTITION BY node_type
ORDER BY (node_type, id_node, id_source)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

-- 4) rank : un rank par (nœud classé, source, semaine), sur 2 ans glissants
--    (scripts/send_ranks.py). (node_type, id_node) identifie le nœud, comme
--    dans link et property ; seuls fqdn et ip ont un rank. Lignes étroites
--    plutôt qu'un tableau JSON dans property : l'import hebdomadaire n'écrit
--    que ~16 octets par nœud, sans relire ni réécrire l'historique.
CREATE TABLE rank
(
    -- même Enum8 que link / property : ajouter un nouveau type À LA FIN, partout
    node_type  Enum8('application' = 1, 'capture' = 2, 'fqdn' = 3, 'ip' = 4,
                     'plugin' = 5, 'organization_name' = 6, 'organization_id' = 7,
                     'phone' = 8, 'social_id' = 9),
    -- même id que fqdn.id_fqdn / ip.id_ip (le min, celui qui survit aux merges)
    id_node    Int64 CODEC(Delta, ZSTD),
    id_source  Int32,
    -- timestamp Unix (secondes) du SAMEDI 00:00 UTC de la semaine du relevé
    -- (fin de la semaine anglaise, dimanche → samedi) ; Int32 comme
    -- link / property.detection_date : jusqu'au 2038-01-19. Pas de 7 jours
    -- constant d'une ligne à l'autre : DoubleDelta le code en quasi rien
    creation_date  Int32 CODEC(DoubleDelta, ZSTD),
    -- d'une semaine à l'autre, le rank d'un nœud varie peu : Delta
    rank       Int32 CODEC(Delta, ZSTD)
)
-- (nœud, source, semaine) ré-envoyé = une seule ligne, celle du dernier
-- insert : relancer une semaine corrige ses ranks
ENGINE = ReplacingMergeTree
-- une partition par mois (UTC) : l'import d'une semaine ne touche (et ne
-- fait merger) que le mois en cours, jamais les 2 ans d'historique
PARTITION BY toYYYYMM(toDateTime(creation_date, 'UTC'))
-- tout l'historique d'un nœud, toutes sources, est contigu
ORDER BY (node_type, id_node, id_source, creation_date)
-- purge par mois entier (suppression de parts, aucune réécriture) ; à la
-- lecture, filtrer quand même sur creation_date (une part expirée n'est pas
-- supprimée instantanément)
TTL toDateTime(creation_date, 'UTC') + INTERVAL 2 YEAR DELETE
SETTINGS ttl_only_drop_parts = 1;
