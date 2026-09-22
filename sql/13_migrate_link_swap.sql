-- ============================================================
-- MIGRATION LINK 2/2 — mise de côté de link_opt
-- ============================================================
-- À lancer seulement après 12_migrate_link_copy.sql et le contrôle des
-- comptes (« distinctes_approx » proches entre link_opt et link).
-- Les scripts (import, generate, test) écrivent et lisent désormais link :
-- link_opt est renommée pour que plus rien ne l'utilise par erreur.
--
-- Retour arrière tant que link_opt_old existe :
--   RENAME TABLE link_opt_old TO link_opt;
-- Libérer le disque une fois satisfait :
--   DROP TABLE link_opt_old SETTINGS max_table_size_to_drop = 0;

RENAME TABLE link_opt TO link_opt_old;

SELECT type_1, type_2, count() AS liens
FROM link
GROUP BY type_1, type_2
ORDER BY type_1, type_2;
