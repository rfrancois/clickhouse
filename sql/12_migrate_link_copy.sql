-- ============================================================
-- MIGRATION LINK 1/2 — copie de link_opt vers la table typée link
-- ============================================================
-- link_opt ne sait pas de quel type sont ses extrémités (un id_fqdn et un
-- id_ip peuvent avoir la même valeur). La nouvelle table link porte le type
-- de chaque extrémité (type_1 / type_2) dans sa clé de tri.
--
-- HYPOTHÈSE : tout le contenu actuel de link_opt est fqdn ↔ fqdn. Les liens
-- qui étaient en réalité fqdn ↔ ip (domains.json « cn ─ ip », données
-- générées par make generate) seront donc mal typés : les réimporter après
-- la bascule pour les obtenir avec le bon type.
--
-- La table link_opt n'est PAS modifiée : on remplit link à côté, puis
-- 13_migrate_link_swap.sql met link_opt de côté (link_opt_old).
-- Prérequis : aucun import pendant la copie ; espace disque ≈ taille actuelle
-- de link_opt (projection inverse comprise).
-- Relançable : relancer le fichier ENTIER ; les lignes copiées deux fois sont
-- identiques et fusionnées par ReplacingMergeTree (même identité de ligne
-- qu'avant : (id_node_1, id_node_2), le type étant constant).

-- Schéma sans projection inverse (cf. 02_optimized.sql) : chaque lien est
-- inséré dans les deux sens ci-dessous, un filtre sur (type_1, id_1) suffit
-- à retrouver les voisins des deux côtés.
CREATE TABLE IF NOT EXISTS link
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

-- Copie en 16 tranches, dans les deux sens. positiveModulo et non % : les ids
-- peuvent être négatifs.
SET max_threads = 8;
SET max_memory_usage = 11000000000;
SET max_bytes_before_external_sort = 536870912;
INSERT INTO link SELECT 'fqdn', id_node_1, 'fqdn', id_node_2, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 0;
INSERT INTO link SELECT 'fqdn', id_node_1, 'fqdn', id_node_2, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 1;
INSERT INTO link SELECT 'fqdn', id_node_1, 'fqdn', id_node_2, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 2;
INSERT INTO link SELECT 'fqdn', id_node_1, 'fqdn', id_node_2, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 3;
INSERT INTO link SELECT 'fqdn', id_node_1, 'fqdn', id_node_2, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 4;
INSERT INTO link SELECT 'fqdn', id_node_1, 'fqdn', id_node_2, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 5;
INSERT INTO link SELECT 'fqdn', id_node_1, 'fqdn', id_node_2, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 6;
INSERT INTO link SELECT 'fqdn', id_node_1, 'fqdn', id_node_2, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 7;
INSERT INTO link SELECT 'fqdn', id_node_1, 'fqdn', id_node_2, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 8;
INSERT INTO link SELECT 'fqdn', id_node_1, 'fqdn', id_node_2, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 9;
INSERT INTO link SELECT 'fqdn', id_node_1, 'fqdn', id_node_2, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 10;
INSERT INTO link SELECT 'fqdn', id_node_1, 'fqdn', id_node_2, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 11;
INSERT INTO link SELECT 'fqdn', id_node_1, 'fqdn', id_node_2, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 12;
INSERT INTO link SELECT 'fqdn', id_node_1, 'fqdn', id_node_2, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 13;
INSERT INTO link SELECT 'fqdn', id_node_1, 'fqdn', id_node_2, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 14;
INSERT INTO link SELECT 'fqdn', id_node_1, 'fqdn', id_node_2, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 15;
INSERT INTO link SELECT 'fqdn', id_node_2, 'fqdn', id_node_1, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 0;
INSERT INTO link SELECT 'fqdn', id_node_2, 'fqdn', id_node_1, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 1;
INSERT INTO link SELECT 'fqdn', id_node_2, 'fqdn', id_node_1, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 2;
INSERT INTO link SELECT 'fqdn', id_node_2, 'fqdn', id_node_1, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 3;
INSERT INTO link SELECT 'fqdn', id_node_2, 'fqdn', id_node_1, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 4;
INSERT INTO link SELECT 'fqdn', id_node_2, 'fqdn', id_node_1, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 5;
INSERT INTO link SELECT 'fqdn', id_node_2, 'fqdn', id_node_1, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 6;
INSERT INTO link SELECT 'fqdn', id_node_2, 'fqdn', id_node_1, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 7;
INSERT INTO link SELECT 'fqdn', id_node_2, 'fqdn', id_node_1, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 8;
INSERT INTO link SELECT 'fqdn', id_node_2, 'fqdn', id_node_1, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 9;
INSERT INTO link SELECT 'fqdn', id_node_2, 'fqdn', id_node_1, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 10;
INSERT INTO link SELECT 'fqdn', id_node_2, 'fqdn', id_node_1, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 11;
INSERT INTO link SELECT 'fqdn', id_node_2, 'fqdn', id_node_1, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 12;
INSERT INTO link SELECT 'fqdn', id_node_2, 'fqdn', id_node_1, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 13;
INSERT INTO link SELECT 'fqdn', id_node_2, 'fqdn', id_node_1, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 14;
INSERT INTO link SELECT 'fqdn', id_node_2, 'fqdn', id_node_1, source_id, detection_date, version FROM link_opt WHERE positiveModulo(id_node_1, 16) = 15;

-- Contrôle : « link » a ~2x les lignes de link_opt (les deux sens), et son
-- compte distinct (id_1, id_2, id_2, id_1 réunis) est égal ou plus petit
-- (doublons fusionnés) que 2x celui de link_opt.
SELECT 'link_opt' AS tbl, count() AS lignes, uniq(id_node_1, id_node_2) AS distinctes_approx FROM link_opt
UNION ALL
SELECT 'link', count(), uniq(id_1, id_2) FROM link;

SELECT table, formatReadableSize(sum(bytes_on_disk)) AS disque
FROM system.parts
WHERE active AND database = currentDatabase() AND table IN ('link_opt', 'link')
GROUP BY table;
