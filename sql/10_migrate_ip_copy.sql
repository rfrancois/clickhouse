-- ============================================================
-- MIGRATION IP 1/2 — copie de ip_search triée par valeur
-- ============================================================
-- Même principe que 07_migrate_copy.sql (fqdn_search), avec une différence :
-- les IP sont triées dans l'ordre NORMAL de la valeur, pas inversé. Ce qui se
-- ressemble pour une IP, c'est le préfixe (192.168.1.x = même sous-réseau) :
--  * LIKE '192.168.%'   → clé primaire, ne lit que la plage concernée ;
--  * id_ip IN (...)     → projection légère p_id (jointures liens → IP).
-- AUCUN index ngram/texte : une IP ne contient que des chiffres et des points,
-- les trigrammes (« 8.8 », « .1. »...) sont présents dans presque tous les
-- blocs → testé : l'index ne filtre rien et ralentit même LIKE '%8.8.8%'
-- (112 ms avec, 45 ms sans). L'ancien idx_ngram n'est donc pas repris.
-- rank = 0 (pas de rank) devient 1 000 000, comme pour fqdn_search.
--
-- La table actuelle n'est PAS modifiée : on remplit ip_search_new à côté,
-- puis 11_migrate_ip_swap.sql échange les deux tables (instantané).
-- Prérequis : aucun import pendant la copie ; espace disque ≈ taille actuelle
-- de ip_search.
-- Relançable : relancer le fichier ENTIER ; les lignes copiées deux fois sont
-- identiques et fusionnées par ReplacingMergeTree.

CREATE TABLE IF NOT EXISTS ip_search_new
(
    value    String,
    id_ip    Int32,
    rank     UInt32,
    version  UInt64,
    PROJECTION p_id (SELECT _part_offset ORDER BY id_ip)
)
ENGINE = ReplacingMergeTree(version)
-- même identité de ligne qu'avant (id_ip, value) : la déduplication ne change pas
ORDER BY (value, id_ip)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

-- Copie en 16 tranches. positiveModulo et non % : id_ip peut être négatif.
SET max_threads = 8;
SET max_memory_usage = 11000000000;
SET use_skip_indexes = 0;
INSERT INTO ip_search_new SELECT value, id_ip, if(rank = 0, 1000000, rank), version FROM ip_search WHERE positiveModulo(id_ip, 16) = 0;
INSERT INTO ip_search_new SELECT value, id_ip, if(rank = 0, 1000000, rank), version FROM ip_search WHERE positiveModulo(id_ip, 16) = 1;
INSERT INTO ip_search_new SELECT value, id_ip, if(rank = 0, 1000000, rank), version FROM ip_search WHERE positiveModulo(id_ip, 16) = 2;
INSERT INTO ip_search_new SELECT value, id_ip, if(rank = 0, 1000000, rank), version FROM ip_search WHERE positiveModulo(id_ip, 16) = 3;
INSERT INTO ip_search_new SELECT value, id_ip, if(rank = 0, 1000000, rank), version FROM ip_search WHERE positiveModulo(id_ip, 16) = 4;
INSERT INTO ip_search_new SELECT value, id_ip, if(rank = 0, 1000000, rank), version FROM ip_search WHERE positiveModulo(id_ip, 16) = 5;
INSERT INTO ip_search_new SELECT value, id_ip, if(rank = 0, 1000000, rank), version FROM ip_search WHERE positiveModulo(id_ip, 16) = 6;
INSERT INTO ip_search_new SELECT value, id_ip, if(rank = 0, 1000000, rank), version FROM ip_search WHERE positiveModulo(id_ip, 16) = 7;
INSERT INTO ip_search_new SELECT value, id_ip, if(rank = 0, 1000000, rank), version FROM ip_search WHERE positiveModulo(id_ip, 16) = 8;
INSERT INTO ip_search_new SELECT value, id_ip, if(rank = 0, 1000000, rank), version FROM ip_search WHERE positiveModulo(id_ip, 16) = 9;
INSERT INTO ip_search_new SELECT value, id_ip, if(rank = 0, 1000000, rank), version FROM ip_search WHERE positiveModulo(id_ip, 16) = 10;
INSERT INTO ip_search_new SELECT value, id_ip, if(rank = 0, 1000000, rank), version FROM ip_search WHERE positiveModulo(id_ip, 16) = 11;
INSERT INTO ip_search_new SELECT value, id_ip, if(rank = 0, 1000000, rank), version FROM ip_search WHERE positiveModulo(id_ip, 16) = 12;
INSERT INTO ip_search_new SELECT value, id_ip, if(rank = 0, 1000000, rank), version FROM ip_search WHERE positiveModulo(id_ip, 16) = 13;
INSERT INTO ip_search_new SELECT value, id_ip, if(rank = 0, 1000000, rank), version FROM ip_search WHERE positiveModulo(id_ip, 16) = 14;
INSERT INTO ip_search_new SELECT value, id_ip, if(rank = 0, 1000000, rank), version FROM ip_search WHERE positiveModulo(id_ip, 16) = 15;

-- Contrôle : « new » égal ou plus petit (doublons fusionnés) que l'ancienne.
SELECT 'ip_search' AS tbl, count() AS lignes, uniq(id_ip, value) AS distinctes_approx FROM ip_search
UNION ALL
SELECT 'ip_search_new', count(), uniq(id_ip, value) FROM ip_search_new;

SELECT table, formatReadableSize(sum(bytes_on_disk)) AS disque
FROM system.parts
WHERE active AND database = currentDatabase() AND table IN ('ip_search', 'ip_search_new')
GROUP BY table;
