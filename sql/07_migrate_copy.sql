-- ============================================================
-- MIGRATION 1/2 — copie de fqdn_search triée par nom de domaine inversé
-- ============================================================
-- Pourquoi : triée par id_fqdn, les lignes "google.com" sont éparpillées dans
-- toute la table (~1 par bloc de 8192 lignes) : aucun index ne peut éviter de
-- lire presque tout. Triée par reverse(value), tous les *.google.com,
-- google.com.br, ... sont côte à côte → l'index ngram n'en garde que quelques
-- blocs, et ORDER BY rank (même sans LIMIT) ne trie plus que ces lignes.
--
-- La table actuelle n'est PAS modifiée : on remplit fqdn_search_new à côté,
-- puis 08_migrate_swap.sql échange les deux tables (instantané).
--
-- Prérequis :
--  * aucun import en cours pendant la copie ;
--  * espace disque libre ≈ taille actuelle de fqdn_search hors projection
--    (voir la requête « disque » en bas) ;
--  * durée : de l'ordre d'une à quelques heures pour 2,5 milliards de lignes.
-- Relançable : si la copie est interrompue, relancer le fichier ENTIER ; les
-- lignes copiées deux fois sont identiques et fusionnées par
-- ReplacingMergeTree (même clé, même version).

CREATE TABLE IF NOT EXISTS fqdn_search_new
(
    value    String,
    id_fqdn  Int32,
    rank     UInt32,
    version  UInt64,
    INDEX idx_ngram value TYPE ngrambf_v1(3, 16384, 4, 0) GRANULARITY 1,
    -- recherche par id (jointure FQDN → liens → IP) : projection légère qui ne
    -- stocke que la position des lignes, triée par id_fqdn
    PROJECTION p_id (SELECT _part_offset ORDER BY id_fqdn)
)
ENGINE = ReplacingMergeTree(version)
-- même identité de ligne qu'avant (id_fqdn, value) : la déduplication ne change pas
ORDER BY (reverse(value), id_fqdn)
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

-- Copie en 16 tranches (mémoire bornée, progression visible).
-- rank = 0 (pas de rank, ex. domains.json) devient 1 000 000 : ces lignes
-- passent en fin de ORDER BY rank au lieu d'arriver en premier.
SET max_threads = 8;
SET max_memory_usage = 11000000000;
SET use_skip_indexes = 0;
INSERT INTO fqdn_search_new SELECT value, id_fqdn, if(rank = 0, 1000000, rank), version FROM fqdn_search WHERE id_fqdn % 16 = 0;
INSERT INTO fqdn_search_new SELECT value, id_fqdn, if(rank = 0, 1000000, rank), version FROM fqdn_search WHERE id_fqdn % 16 = 1;
INSERT INTO fqdn_search_new SELECT value, id_fqdn, if(rank = 0, 1000000, rank), version FROM fqdn_search WHERE id_fqdn % 16 = 2;
INSERT INTO fqdn_search_new SELECT value, id_fqdn, if(rank = 0, 1000000, rank), version FROM fqdn_search WHERE id_fqdn % 16 = 3;
INSERT INTO fqdn_search_new SELECT value, id_fqdn, if(rank = 0, 1000000, rank), version FROM fqdn_search WHERE id_fqdn % 16 = 4;
INSERT INTO fqdn_search_new SELECT value, id_fqdn, if(rank = 0, 1000000, rank), version FROM fqdn_search WHERE id_fqdn % 16 = 5;
INSERT INTO fqdn_search_new SELECT value, id_fqdn, if(rank = 0, 1000000, rank), version FROM fqdn_search WHERE id_fqdn % 16 = 6;
INSERT INTO fqdn_search_new SELECT value, id_fqdn, if(rank = 0, 1000000, rank), version FROM fqdn_search WHERE id_fqdn % 16 = 7;
INSERT INTO fqdn_search_new SELECT value, id_fqdn, if(rank = 0, 1000000, rank), version FROM fqdn_search WHERE id_fqdn % 16 = 8;
INSERT INTO fqdn_search_new SELECT value, id_fqdn, if(rank = 0, 1000000, rank), version FROM fqdn_search WHERE id_fqdn % 16 = 9;
INSERT INTO fqdn_search_new SELECT value, id_fqdn, if(rank = 0, 1000000, rank), version FROM fqdn_search WHERE id_fqdn % 16 = 10;
INSERT INTO fqdn_search_new SELECT value, id_fqdn, if(rank = 0, 1000000, rank), version FROM fqdn_search WHERE id_fqdn % 16 = 11;
INSERT INTO fqdn_search_new SELECT value, id_fqdn, if(rank = 0, 1000000, rank), version FROM fqdn_search WHERE id_fqdn % 16 = 12;
INSERT INTO fqdn_search_new SELECT value, id_fqdn, if(rank = 0, 1000000, rank), version FROM fqdn_search WHERE id_fqdn % 16 = 13;
INSERT INTO fqdn_search_new SELECT value, id_fqdn, if(rank = 0, 1000000, rank), version FROM fqdn_search WHERE id_fqdn % 16 = 14;
INSERT INTO fqdn_search_new SELECT value, id_fqdn, if(rank = 0, 1000000, rank), version FROM fqdn_search WHERE id_fqdn % 16 = 15;

-- Contrôle : les deux nombres de lignes doivent être égaux (ou « new » un peu
-- plus petit si des doublons ont déjà été fusionnés). Si « new » est plus
-- GRAND, une copie a été relancée : attendre les merges, ce n'est pas grave.
SELECT 'fqdn_search' AS tbl, count() AS lignes FROM fqdn_search
UNION ALL
SELECT 'fqdn_search_new', count() FROM fqdn_search_new;

SELECT table, formatReadableSize(sum(bytes_on_disk)) AS disque
FROM system.parts
WHERE active AND database = currentDatabase() AND table IN ('fqdn_search', 'fqdn_search_new')
GROUP BY table;
