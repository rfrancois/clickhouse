-- ============================================================
-- STAGING D'IMPORT — fichiers réels (make import FILE=...)
-- ============================================================
-- Tout est stocké brut (String / Nullable) tel que lu dans les fichiers.
-- La typisation et la distribution vers les tables optimisées
-- sont faites par 05_import_distribute.sql.

DROP TABLE IF EXISTS stg_node;
DROP TABLE IF EXISTS stg_link;
DROP TABLE IF EXISTS stg_domain;
DROP TABLE IF EXISTS stg_value;
DROP TABLE IF EXISTS stg_property;

-- node.csv : id;value;type;creation_date;rank  (séparateur ';', champs quotés)
CREATE TABLE stg_node
(
    id            String,
    value         String,
    node_type     String,
    creation_date String,
    rank          String
)
ENGINE = MergeTree
ORDER BY tuple();

-- links.csv : id_node_1;id_node_2;type_1;type_2;id_source;creation_date;update_date
-- ATTENTION : id_node_* sont des VALEURS de nœuds (ex. "netflix.com"),
-- pas des ids numériques — la résolution valeur → id est faite à la distribution.
CREATE TABLE stg_link
(
    id_node_1     String,
    id_node_2     String,
    type_1        String,
    type_2        String,
    id_source     String,
    creation_date String,
    update_date   String
)
ENGINE = MergeTree
ORDER BY tuple();

-- properties.csv : id_node;type;id_source;payload;version;detection_date
-- id_node est un ID numérique (même id que <type>.id_<type>), pas une valeur :
-- aucune résolution, distribution directe vers property (distribute_properties).
-- Champs entre guillemets, guillemets internes échappés par backslash (\")
-- → chargé en CustomSeparated avec la règle d'échappement JSON, pas en CSV.
CREATE TABLE stg_property
(
    id_node        String,
    node_type      String,
    id_source      String,
    payload        String,
    version        String,
    detection_date String
)
ENGINE = MergeTree
ORDER BY tuple();

-- domains.json[.gz] : une ligne JSON par enregistrement
-- {"cn": "...", "dns": ["...", ...] | null, "ip": "..." | null}
CREATE TABLE stg_domain
(
    cn  Nullable(String),
    dns Array(Nullable(String)),
    ip  Nullable(String)
)
ENGINE = MergeTree
ORDER BY tuple();

-- valeurs fqdn/ip sans id (issues de domains.json) : alimentée par
-- 05_import_distribute.sql, un id leur est attribué (auto-incrément à partir
-- du max existant) par distribute_links() dans scripts/import_data.py
CREATE TABLE stg_value
(
    node_type String,
    value     String
)
ENGINE = MergeTree
ORDER BY tuple();
