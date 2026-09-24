-- ============================================================
-- MIGRATION LINK — source_id dans la clé de déduplication (1/2 : copie)
-- ============================================================
-- Base existante dont la table link est triée (type_1, id_1, type_2, id_2) :
-- un même lien vu par deux sources n'y garde qu'une ligne (la plus grande
-- version), l'autre source est perdue au merge. Le nouveau schéma
-- (02_optimized.sql) trie par (type_1, id_1, type_2, id_2, source_id) : une
-- ligne par lien orienté ET par source.
--
-- ClickHouse ne sait pas ajouter une colonne existante à ORDER BY
-- (MODIFY ORDER BY n'accepte que des colonnes créées dans le même ALTER) :
-- on remplit link_new à côté, puis 17_migrate_link_source_swap.sql bascule.
--
-- Les sources déjà fusionnées par les merges passés ne reviennent pas : pour
-- les retrouver, réimporter les liens après la bascule. Import/génération
-- doivent être arrêtés pendant la migration. Espace disque : prévoir la
-- taille actuelle de link.

DROP TABLE IF EXISTS link_new;

CREATE TABLE link_new
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
PARTITION BY type_1
PRIMARY KEY (type_1, id_1)
ORDER BY (type_1, id_1, type_2, id_2, source_id);

-- Copie en 16 tranches (positiveModulo : ids négatifs possibles). Les deux
-- sens sont déjà présents dans link : copie ligne à ligne.
SET max_threads = 8;
SET max_memory_usage = 11000000000;
SET max_bytes_before_external_sort = 536870912;
INSERT INTO link_new SELECT type_1, id_1, type_2, id_2, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 0;
INSERT INTO link_new SELECT type_1, id_1, type_2, id_2, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 1;
INSERT INTO link_new SELECT type_1, id_1, type_2, id_2, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 2;
INSERT INTO link_new SELECT type_1, id_1, type_2, id_2, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 3;
INSERT INTO link_new SELECT type_1, id_1, type_2, id_2, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 4;
INSERT INTO link_new SELECT type_1, id_1, type_2, id_2, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 5;
INSERT INTO link_new SELECT type_1, id_1, type_2, id_2, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 6;
INSERT INTO link_new SELECT type_1, id_1, type_2, id_2, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 7;
INSERT INTO link_new SELECT type_1, id_1, type_2, id_2, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 8;
INSERT INTO link_new SELECT type_1, id_1, type_2, id_2, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 9;
INSERT INTO link_new SELECT type_1, id_1, type_2, id_2, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 10;
INSERT INTO link_new SELECT type_1, id_1, type_2, id_2, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 11;
INSERT INTO link_new SELECT type_1, id_1, type_2, id_2, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 12;
INSERT INTO link_new SELECT type_1, id_1, type_2, id_2, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 13;
INSERT INTO link_new SELECT type_1, id_1, type_2, id_2, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 14;
INSERT INTO link_new SELECT type_1, id_1, type_2, id_2, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 15;

-- Contrôle : mêmes comptes de lignes (link_new peut en avoir un peu moins
-- tant que link n'a pas fini de fusionner ses doublons exacts).
SELECT 'link' AS tbl, count() AS lignes, uniq(type_1, id_1, type_2, id_2) AS couples FROM link
UNION ALL
SELECT 'link_new', count(), uniq(type_1, id_1, type_2, id_2) FROM link_new;
