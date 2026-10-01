-- ============================================================
-- MIGRATION tables de valeurs 2/2 — bascule vers les nouvelles tables
-- ============================================================
-- À lancer seulement après 08_migrate_nodes_copy.sql et le contrôle
-- (lignes_new = nombre de valeurs distinctes de chaque ancienne table).
-- Échange instantané des 9 tables : les anciennes sont gardées sous
-- <type>_old_replacing.
--
-- Retour arrière tant que les <type>_old_replacing existent, par table :
--   EXCHANGE TABLES fqdn AND fqdn_old_replacing;   (idem pour les 8 autres)
-- Libérer le disque une fois satisfait :
--   DROP TABLE fqdn_old_replacing SETTINGS max_table_size_to_drop = 0;
--   (idem pour les 8 autres)

-- garde-fous AVANT l'échange : sinon un RENAME en échec laisserait les
-- tables à moitié basculées
SELECT throwIf(count() != 9, 'tables <type>_new incomplètes : lancer d''abord make migration-nodes')
FROM system.tables
WHERE database = currentDatabase()
  AND name IN ('application_new', 'capture_new', 'fqdn_new', 'ip_new', 'plugin_new',
               'organization_name_new', 'organization_id_new', 'phone_new', 'social_id_new')
FORMAT Null;
SELECT throwIf(count() > 0, 'des tables <type>_old_replacing existent déjà : les supprimer ou les renommer avant')
FROM system.tables
WHERE database = currentDatabase()
  AND name IN ('application_old_replacing', 'capture_old_replacing', 'fqdn_old_replacing',
               'ip_old_replacing', 'plugin_old_replacing', 'organization_name_old_replacing',
               'organization_id_old_replacing', 'phone_old_replacing', 'social_id_old_replacing')
FORMAT Null;

EXCHANGE TABLES application AND application_new;
EXCHANGE TABLES capture AND capture_new;
EXCHANGE TABLES fqdn AND fqdn_new;
EXCHANGE TABLES ip AND ip_new;
EXCHANGE TABLES plugin AND plugin_new;
EXCHANGE TABLES organization_name AND organization_name_new;
EXCHANGE TABLES organization_id AND organization_id_new;
EXCHANGE TABLES phone AND phone_new;
EXCHANGE TABLES social_id AND social_id_new;

RENAME TABLE application_new TO application_old_replacing,
             capture_new TO capture_old_replacing,
             fqdn_new TO fqdn_old_replacing,
             ip_new TO ip_old_replacing,
             plugin_new TO plugin_old_replacing,
             organization_name_new TO organization_name_old_replacing,
             organization_id_new TO organization_id_old_replacing,
             phone_new TO phone_old_replacing,
             social_id_new TO social_id_old_replacing;

SELECT name, engine, total_rows FROM system.tables
WHERE database = currentDatabase()
  AND replaceRegexpOne(name, '_old_replacing$', '') IN ('application', 'capture', 'fqdn', 'ip', 'plugin',
                                                        'organization_name', 'organization_id', 'phone', 'social_id')
ORDER BY name
FORMAT PrettyCompactMonoBlock;
