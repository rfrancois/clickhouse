-- ============================================================
-- MIGRATION 2/2 — bascule vers la table triée par nom inversé
-- ============================================================
-- À lancer seulement après 07_migrate_copy.sql et le contrôle des comptes.
-- Échange instantané et atomique des deux tables : fqdn_search devient la
-- nouvelle table, l'ancienne est conservée sous le nom fqdn_search_old.
--
-- Retour arrière possible tant que fqdn_search_old existe :
--   EXCHANGE TABLES fqdn_search AND fqdn_search_old;
--
-- Une fois satisfait, libérer le disque (l'ancienne table dépasse la limite
-- de sécurité de 50 Gio, d'où le réglage) :
--   DROP TABLE fqdn_search_old SETTINGS max_table_size_to_drop = 0;

EXCHANGE TABLES fqdn_search AND fqdn_search_new;
RENAME TABLE fqdn_search_new TO fqdn_search_old;

SELECT create_table_query FROM system.tables
WHERE database = currentDatabase() AND name = 'fqdn_search' FORMAT Vertical;
