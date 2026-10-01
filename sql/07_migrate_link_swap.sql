-- ============================================================
-- MIGRATION 2/2 — bascule vers la nouvelle link, nettoyage de property
-- ============================================================
-- À lancer seulement après 06_migrate_link_copy.sql et le contrôle des
-- comptes (« distinctes_approx » proches entre link et link_new, et entre
-- property et property_detection).
-- Échange instantané : l'ancienne table est gardée sous link_old_replacing.
-- Puis suppression de property.detection_date, désormais dans
-- property_detection (sans retour arrière : relire property_detection).
--
-- Retour arrière pour link tant que link_old_replacing existe :
--   EXCHANGE TABLES link AND link_old_replacing;
-- Libérer le disque une fois satisfait :
--   DROP TABLE link_old_replacing SETTINGS max_table_size_to_drop = 0;

-- garde-fous AVANT l'échange : sinon un RENAME en échec laisserait les
-- tables à moitié basculées
SELECT throwIf(count() = 0, 'link_new absente : lancer d''abord make migration')
FROM system.tables WHERE database = currentDatabase() AND name = 'link_new'
FORMAT Null;
SELECT throwIf(count() > 0, 'link_old_replacing existe déjà : la supprimer ou la renommer avant')
FROM system.tables WHERE database = currentDatabase() AND name = 'link_old_replacing'
FORMAT Null;
SELECT throwIf(count() = 0, 'property_detection absente : lancer d''abord make migration')
FROM system.tables WHERE database = currentDatabase() AND name = 'property_detection'
FORMAT Null;

EXCHANGE TABLES link AND link_new;
RENAME TABLE link_new TO link_old_replacing;

ALTER TABLE property DROP COLUMN IF EXISTS detection_date;

SELECT name, engine, total_rows FROM system.tables
WHERE database = currentDatabase()
  AND name IN ('link', 'link_old_replacing', 'property', 'property_detection')
ORDER BY name
FORMAT PrettyCompactMonoBlock;
