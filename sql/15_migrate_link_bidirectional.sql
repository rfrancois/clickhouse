-- ============================================================
-- MIGRATION LINK — bascule vers des liens dupliqués dans les deux sens
-- ============================================================
-- Base existante dont la table link a été créée par une version antérieure
-- du schéma : liens à SENS UNIQUE (A→B seulement) + projection p_reverse
-- pour le sens inverse. La nouvelle version (02_optimized.sql) n'a plus de
-- projection : chaque lien doit exister physiquement dans les deux sens.
--
-- Cette migration :
--   1. insère le sens B→A manquant pour chaque ligne déjà présente
--      (en tranches, comme 12_migrate_link_copy.sql, pour borner la RAM) ;
--   2. supprime la projection p_reverse, devenue inutile (et redondante :
--      la garder doublerait le coût disque du nouveau design).
--
-- À lancer UNE SEULE FOIS sur une table encore à sens unique. Relancer après
-- coup réinsère des lignes déjà dupliquées (fusionnées sans dégât par
-- ReplacingMergeTree, mais inutile). Import/génération doivent être arrêtés
-- pendant la migration. Espace disque : prévoir environ la taille actuelle
-- de link (le sens ajouté double son volume de lignes, mais remplace la
-- place occupée par l'ancienne projection, cf. README).

SET max_threads = 8;
SET max_memory_usage = 11000000000;
SET max_bytes_before_external_sort = 536870912;

SELECT 'avant' AS etape, count() AS lignes FROM link;

-- Ajout du sens inverse, en 16 tranches (positiveModulo : ids négatifs possibles).
INSERT INTO link SELECT type_2, id_2, type_1, id_1, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 0;
INSERT INTO link SELECT type_2, id_2, type_1, id_1, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 1;
INSERT INTO link SELECT type_2, id_2, type_1, id_1, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 2;
INSERT INTO link SELECT type_2, id_2, type_1, id_1, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 3;
INSERT INTO link SELECT type_2, id_2, type_1, id_1, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 4;
INSERT INTO link SELECT type_2, id_2, type_1, id_1, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 5;
INSERT INTO link SELECT type_2, id_2, type_1, id_1, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 6;
INSERT INTO link SELECT type_2, id_2, type_1, id_1, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 7;
INSERT INTO link SELECT type_2, id_2, type_1, id_1, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 8;
INSERT INTO link SELECT type_2, id_2, type_1, id_1, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 9;
INSERT INTO link SELECT type_2, id_2, type_1, id_1, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 10;
INSERT INTO link SELECT type_2, id_2, type_1, id_1, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 11;
INSERT INTO link SELECT type_2, id_2, type_1, id_1, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 12;
INSERT INTO link SELECT type_2, id_2, type_1, id_1, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 13;
INSERT INTO link SELECT type_2, id_2, type_1, id_1, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 14;
INSERT INTO link SELECT type_2, id_2, type_1, id_1, source_id, detection_date, version FROM link WHERE positiveModulo(id_1, 16) = 15;

-- Projection inverse : plus nécessaire, et redondante avec la duplication ci-dessus.
ALTER TABLE link DROP PROJECTION IF EXISTS p_reverse;

SELECT 'après' AS etape, count() AS lignes FROM link;
