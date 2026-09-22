-- ============================================================
-- MIGRATION IP 2/2 — bascule vers la nouvelle ip_search
-- ============================================================
-- À lancer seulement après 10_migrate_ip_copy.sql et le contrôle des comptes
-- (« distinctes_approx » proches entre les deux tables).
-- Échange instantané : l'ancienne table est gardée sous ip_search_old.
--
-- Retour arrière tant que ip_search_old existe :
--   EXCHANGE TABLES ip_search AND ip_search_old;
-- Libérer le disque une fois satisfait :
--   DROP TABLE ip_search_old SETTINGS max_table_size_to_drop = 0;

EXCHANGE TABLES ip_search AND ip_search_new;
RENAME TABLE ip_search_new TO ip_search_old;

SELECT create_table_query FROM system.tables
WHERE database = currentDatabase() AND name = 'ip_search' FORMAT Vertical;
