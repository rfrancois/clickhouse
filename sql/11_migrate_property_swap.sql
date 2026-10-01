-- ============================================================
-- MIGRATION property 2/2 — bascule vers la nouvelle property
-- ============================================================
-- À lancer seulement après 10_migrate_property_copy.sql et le contrôle
-- (sans_payload = sans_detection = 0).
-- Échange instantané : l'ancienne property est gardée sous
-- property_old_replacing, property_detection sous property_detection_old.
--
-- Retour arrière tant que ces deux tables existent :
--   EXCHANGE TABLES property AND property_old_replacing;
--   RENAME TABLE property_detection_old TO property_detection;
-- Libérer le disque une fois satisfait :
--   DROP TABLE property_old_replacing SETTINGS max_table_size_to_drop = 0;
--   DROP TABLE property_detection_old SETTINGS max_table_size_to_drop = 0;

-- garde-fous AVANT l'échange : sinon un RENAME en échec laisserait les
-- tables à moitié basculées
SELECT throwIf(count() = 0, 'property_new absente : lancer d''abord make migration-property')
FROM system.tables WHERE database = currentDatabase() AND name = 'property_new'
FORMAT Null;
SELECT throwIf(count() > 0, 'property_old_replacing ou property_detection_old existe déjà : la supprimer ou la renommer avant')
FROM system.tables WHERE database = currentDatabase()
  AND name IN ('property_old_replacing', 'property_detection_old')
FORMAT Null;

EXCHANGE TABLES property AND property_new;
RENAME TABLE property_new TO property_old_replacing,
             property_detection TO property_detection_old;

SELECT name, engine, total_rows FROM system.tables
WHERE database = currentDatabase()
  AND name IN ('property', 'property_old_replacing', 'property_detection_old')
ORDER BY name
FORMAT PrettyCompactMonoBlock;
