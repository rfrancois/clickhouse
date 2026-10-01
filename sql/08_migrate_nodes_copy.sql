-- ============================================================
-- MIGRATION tables de valeurs 1/2 — copies (<type>_new)
-- ============================================================
-- Les 9 tables de valeurs (une par type de l'Enum8 : application, capture,
-- fqdn, ip, plugin, organization_name, organization_id, phone, social_id)
-- passent de ReplacingMergeTree(version), clé (valeur, id), à
-- AggregatingMergeTree, clé valeur seule (cf. sql/02_optimized.sql) : une
-- valeur ré-insérée avec un autre id fusionne avec l'existante et garde
--   * id      : le plus ancien (min) ;
--   * rank    : celui du dernier insert (anyLast) — fqdn et ip seulement ;
--   * version : la plus récente (max).
--
-- Rien n'est modifié ici : on remplit <type>_new à côté, puis
-- 09_migrate_nodes_swap.sql échange les 9 tables (instantané).
-- Prérequis : aucun import pendant la copie ni jusqu'à la bascule (ce qui
-- serait écrit entre-temps ne serait pas copié) ; espace disque ≈ taille
-- actuelle des tables de valeurs.
-- Relançable : relancer le fichier ENTIER ; les lignes copiées deux fois
-- fusionnent dans <type>_new (min / anyLast / max : même résultat).
--
-- La copie AGRÈGE par valeur (au lieu d'un simple flux) : anyLast dépend de
-- l'ordre d'insertion, qu'une copie en parallèle ne respecte pas ; on fixe
-- donc ici le rank de la plus récente version (argMax) pour les valeurs qui
-- ont plusieurs lignes (doublons pas encore fusionnés, ou plusieurs ids).
--
-- ATTENTION : une valeur présente sous plusieurs ids ne garde que le plus
-- ancien ; les liens (link) et propriétés (property) qui citent les autres
-- ids deviennent orphelins. Ces ids sont listés dans node_id_remap
-- (type, id écarté → id gardé) et le contrôle en fin de fichier compte les
-- orphelins.

-- garde-fou : base déjà migrée (même en partie) → arrêt (rien n'est créé)
SELECT throwIf(count() > 0,
               'tables de valeurs déjà en AggregatingMergeTree : migration déjà faite')
FROM system.tables
WHERE database = currentDatabase() AND engine = 'AggregatingMergeTree'
  AND name IN ('application', 'capture', 'fqdn', 'ip', 'plugin',
               'organization_name', 'organization_id', 'phone', 'social_id')
FORMAT Null;

-- Mémoire bornée : les agrégations débordent sur disque au-delà de 512 Mio
SET max_threads = 4;
SET max_memory_usage = 11000000000;
SET max_bytes_before_external_group_by = 536870912;
SET use_skip_indexes = 0;

-- ---------- tables classées (rank) ----------

CREATE TABLE IF NOT EXISTS fqdn_new
(
    value    String,
    id_fqdn  SimpleAggregateFunction(min, Int64),
    rank     SimpleAggregateFunction(anyLast, Int32),
    version  SimpleAggregateFunction(max, UInt64),
    INDEX idx_ngram value TYPE ngrambf_v1(3, 16384, 4, 0) GRANULARITY 1,
    INDEX idx_text value TYPE text(tokenizer = ngrams(3)),
    PROJECTION p_id (SELECT _part_offset ORDER BY id_fqdn)
)
ENGINE = AggregatingMergeTree
ORDER BY reverse(value)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO fqdn_new (value, id_fqdn, rank, version)
SELECT value, min(id_fqdn), argMax(rank, version), max(version) FROM fqdn GROUP BY value;

CREATE TABLE IF NOT EXISTS ip_new
(
    value    String,
    id_ip    SimpleAggregateFunction(min, Int64),
    rank     SimpleAggregateFunction(anyLast, Int32),
    version  SimpleAggregateFunction(max, UInt64),
    PROJECTION p_id (SELECT _part_offset ORDER BY id_ip)
)
ENGINE = AggregatingMergeTree
ORDER BY value
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO ip_new (value, id_ip, rank, version)
SELECT value, min(id_ip), argMax(rank, version), max(version) FROM ip GROUP BY value;

-- ---------- autres types (sans rank) ----------

CREATE TABLE IF NOT EXISTS application_new
(
    value                String,
    id_application       SimpleAggregateFunction(min, Int64),
    version              SimpleAggregateFunction(max, UInt64),
    PROJECTION p_id (SELECT _part_offset ORDER BY id_application)
)
ENGINE = AggregatingMergeTree
ORDER BY value
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO application_new (value, id_application, version)
SELECT value, min(id_application), max(version) FROM application GROUP BY value;

CREATE TABLE IF NOT EXISTS capture_new
(
    value                String,
    id_capture           SimpleAggregateFunction(min, Int64),
    version              SimpleAggregateFunction(max, UInt64),
    PROJECTION p_id (SELECT _part_offset ORDER BY id_capture)
)
ENGINE = AggregatingMergeTree
ORDER BY value
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO capture_new (value, id_capture, version)
SELECT value, min(id_capture), max(version) FROM capture GROUP BY value;

CREATE TABLE IF NOT EXISTS plugin_new
(
    value                String,
    id_plugin            SimpleAggregateFunction(min, Int64),
    version              SimpleAggregateFunction(max, UInt64),
    PROJECTION p_id (SELECT _part_offset ORDER BY id_plugin)
)
ENGINE = AggregatingMergeTree
ORDER BY value
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO plugin_new (value, id_plugin, version)
SELECT value, min(id_plugin), max(version) FROM plugin GROUP BY value;

CREATE TABLE IF NOT EXISTS organization_name_new
(
    value                String,
    id_organization_name SimpleAggregateFunction(min, Int64),
    version              SimpleAggregateFunction(max, UInt64),
    PROJECTION p_id (SELECT _part_offset ORDER BY id_organization_name)
)
ENGINE = AggregatingMergeTree
ORDER BY value
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO organization_name_new (value, id_organization_name, version)
SELECT value, min(id_organization_name), max(version) FROM organization_name GROUP BY value;

CREATE TABLE IF NOT EXISTS organization_id_new
(
    value                String,
    id_organization_id   SimpleAggregateFunction(min, Int64),
    version              SimpleAggregateFunction(max, UInt64),
    PROJECTION p_id (SELECT _part_offset ORDER BY id_organization_id)
)
ENGINE = AggregatingMergeTree
ORDER BY value
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO organization_id_new (value, id_organization_id, version)
SELECT value, min(id_organization_id), max(version) FROM organization_id GROUP BY value;

CREATE TABLE IF NOT EXISTS phone_new
(
    value                String,
    id_phone             SimpleAggregateFunction(min, Int64),
    version              SimpleAggregateFunction(max, UInt64),
    PROJECTION p_id (SELECT _part_offset ORDER BY id_phone)
)
ENGINE = AggregatingMergeTree
ORDER BY value
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO phone_new (value, id_phone, version)
SELECT value, min(id_phone), max(version) FROM phone GROUP BY value;

CREATE TABLE IF NOT EXISTS social_id_new
(
    value                String,
    id_social_id         SimpleAggregateFunction(min, Int64),
    version              SimpleAggregateFunction(max, UInt64),
    PROJECTION p_id (SELECT _part_offset ORDER BY id_social_id)
)
ENGINE = AggregatingMergeTree
ORDER BY value
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO social_id_new (value, id_social_id, version)
SELECT value, min(id_social_id), max(version) FROM social_id GROUP BY value;

-- ---------- ids écartés → id gardé ----------
-- Petite table (seulement les valeurs à plusieurs ids), conservée après la
-- bascule pour réécrire plus tard les liens / propriétés qui citent un id
-- écarté. (node_type, id) identifie un nœud, comme dans link / property.
CREATE TABLE IF NOT EXISTS node_id_remap
(
    node_type  Enum8('application' = 1, 'capture' = 2, 'fqdn' = 3, 'ip' = 4,
                     'plugin' = 5, 'organization_name' = 6, 'organization_id' = 7,
                     'phone' = 8, 'social_id' = 9),
    id_ecarte  Int64,
    id_garde   Int64
)
ENGINE = ReplacingMergeTree
ORDER BY (node_type, id_ecarte);

INSERT INTO node_id_remap
SELECT 'fqdn', arrayJoin(arrayFilter(i -> i != id_garde, ids)), id_garde
FROM (SELECT groupUniqArray(id_fqdn) AS ids, min(id_fqdn) AS id_garde
      FROM fqdn GROUP BY value HAVING length(ids) > 1);

INSERT INTO node_id_remap
SELECT 'ip', arrayJoin(arrayFilter(i -> i != id_garde, ids)), id_garde
FROM (SELECT groupUniqArray(id_ip) AS ids, min(id_ip) AS id_garde
      FROM ip GROUP BY value HAVING length(ids) > 1);

INSERT INTO node_id_remap
SELECT 'application', arrayJoin(arrayFilter(i -> i != id_garde, ids)), id_garde
FROM (SELECT groupUniqArray(id_application) AS ids, min(id_application) AS id_garde
      FROM application GROUP BY value HAVING length(ids) > 1);

INSERT INTO node_id_remap
SELECT 'capture', arrayJoin(arrayFilter(i -> i != id_garde, ids)), id_garde
FROM (SELECT groupUniqArray(id_capture) AS ids, min(id_capture) AS id_garde
      FROM capture GROUP BY value HAVING length(ids) > 1);

INSERT INTO node_id_remap
SELECT 'plugin', arrayJoin(arrayFilter(i -> i != id_garde, ids)), id_garde
FROM (SELECT groupUniqArray(id_plugin) AS ids, min(id_plugin) AS id_garde
      FROM plugin GROUP BY value HAVING length(ids) > 1);

INSERT INTO node_id_remap
SELECT 'organization_name', arrayJoin(arrayFilter(i -> i != id_garde, ids)), id_garde
FROM (SELECT groupUniqArray(id_organization_name) AS ids, min(id_organization_name) AS id_garde
      FROM organization_name GROUP BY value HAVING length(ids) > 1);

INSERT INTO node_id_remap
SELECT 'organization_id', arrayJoin(arrayFilter(i -> i != id_garde, ids)), id_garde
FROM (SELECT groupUniqArray(id_organization_id) AS ids, min(id_organization_id) AS id_garde
      FROM organization_id GROUP BY value HAVING length(ids) > 1);

INSERT INTO node_id_remap
SELECT 'phone', arrayJoin(arrayFilter(i -> i != id_garde, ids)), id_garde
FROM (SELECT groupUniqArray(id_phone) AS ids, min(id_phone) AS id_garde
      FROM phone GROUP BY value HAVING length(ids) > 1);

INSERT INTO node_id_remap
SELECT 'social_id', arrayJoin(arrayFilter(i -> i != id_garde, ids)), id_garde
FROM (SELECT groupUniqArray(id_social_id) AS ids, min(id_social_id) AS id_garde
      FROM social_id GROUP BY value HAVING length(ids) > 1);

-- ---------- contrôle ----------
-- lignes_new doit valoir le nombre de valeurs distinctes de l'ancienne table
-- (noms_approx, approché ; le double après une relance, avant fusion)
SELECT 'fqdn' AS tbl, count() AS lignes, uniq(value) AS noms_approx, (SELECT count() FROM fqdn_new) AS lignes_new FROM fqdn
UNION ALL SELECT 'ip', count(), uniq(value), (SELECT count() FROM ip_new) FROM ip
UNION ALL SELECT 'application', count(), uniq(value), (SELECT count() FROM application_new) FROM application
UNION ALL SELECT 'capture', count(), uniq(value), (SELECT count() FROM capture_new) FROM capture
UNION ALL SELECT 'plugin', count(), uniq(value), (SELECT count() FROM plugin_new) FROM plugin
UNION ALL SELECT 'organization_name', count(), uniq(value), (SELECT count() FROM organization_name_new) FROM organization_name
UNION ALL SELECT 'organization_id', count(), uniq(value), (SELECT count() FROM organization_id_new) FROM organization_id
UNION ALL SELECT 'phone', count(), uniq(value), (SELECT count() FROM phone_new) FROM phone
UNION ALL SELECT 'social_id', count(), uniq(value), (SELECT count() FROM social_id_new) FROM social_id
FORMAT PrettyCompactMonoBlock;

-- ids écartés par type, et liens / propriétés qui deviendront orphelins
-- après la bascule (aucune ligne : aucun id écarté)
SELECT node_type, ids_ecartes,
       ifNull(liens_orphelins, 0) AS liens_orphelins,
       ifNull(proprietes_orphelines, 0) AS proprietes_orphelines
FROM (SELECT node_type, count() AS ids_ecartes FROM node_id_remap FINAL GROUP BY node_type) AS r
LEFT JOIN (SELECT type_1 AS node_type, count() AS liens_orphelins FROM link
           WHERE (type_1, id_1) IN (SELECT node_type, id_ecarte FROM node_id_remap)
           GROUP BY type_1) AS l USING (node_type)
LEFT JOIN (SELECT node_type, count() AS proprietes_orphelines FROM property
           WHERE (node_type, id_node) IN (SELECT node_type, id_ecarte FROM node_id_remap)
           GROUP BY node_type) AS p USING (node_type)
ORDER BY node_type
SETTINGS join_use_nulls = 1
FORMAT PrettyCompactMonoBlock;

SELECT table, formatReadableSize(sum(bytes_on_disk)) AS disque
FROM system.parts
WHERE active AND database = currentDatabase()
  AND replaceRegexpOne(table, '_new$', '') IN ('application', 'capture', 'fqdn', 'ip', 'plugin',
                                                'organization_name', 'organization_id', 'phone', 'social_id')
GROUP BY table
ORDER BY table
FORMAT PrettyCompactMonoBlock;
