-- ============================================================
-- MIGRATION LINK — source_id dans la clé de déduplication (2/2 : bascule)
-- ============================================================
-- À lancer seulement après 16_migrate_link_source_copy.sql et le contrôle
-- des comptes. link devient link_old, link_new devient link.
--
-- Retour arrière tant que link_old existe :
--   RENAME TABLE link TO link_new, link_old TO link;
-- Libérer le disque une fois satisfait :
--   DROP TABLE link_old SETTINGS max_table_size_to_drop = 0;

RENAME TABLE link TO link_old, link_new TO link;

SELECT count() AS lignes, uniq(type_1, id_1, type_2, id_2) AS couples FROM link;
