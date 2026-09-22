-- ============================================================
-- MIGRATION — tri par rank rapide sur fqdn_search (base existante)
-- ============================================================
-- Problème : SELECT * FROM fqdn_search WHERE value LIKE '%google.com%'
--            ORDER BY rank LIMIT N
-- La table est triée sur (id_fqdn, value) : sans ORDER BY, ClickHouse s'arrête
-- dès qu'il a N lignes ; avec ORDER BY rank il doit lire TOUTES les lignes qui
-- matchent (un terme fréquent comme google.com est présent dans quasiment tous
-- les granules, l'index ngram n'élague rien) puis trier → scan complet.
--
-- Solution : une projection triée par rank. ClickHouse la choisit tout seul
-- pour les requêtes ORDER BY rank [DESC] LIMIT N : il lit dans l'ordre de rank
-- et s'arrête dès qu'il a N lignes qui matchent le LIKE.
--
-- Sans perte de données, sans recréer la table. Relançable sans risque.
--
-- ⚠️ VERSION : ClickHouse >= 25.x requis pour que la projection serve au tri.
--    Vérifié : 26.7 → utilisée ; 24.8 → ignorée (elle n'y sert qu'aux filtres
--    sur sa clé), aucun gain. Mettre à jour : make upgrade
--    Contrôle : EXPLAIN <requête> doit afficher « ReadFromMergeTree (p_rank) ».
-- Coût : ~ la taille de fqdn_search en disque en plus, inserts un peu plus lents.

-- 1) OBLIGATOIRE avant d'ajouter la projection sur un ReplacingMergeTree :
--    'rebuild' = la projection est reconstruite à chaque merge de déduplication
--    ('drop' la supprimerait silencieusement au premier merge).
ALTER TABLE fqdn_search
    MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';

-- 2) Déclaration de la projection (les nouveaux inserts l'alimentent aussitôt)
ALTER TABLE fqdn_search
    ADD PROJECTION IF NOT EXISTS p_rank
    (
        SELECT *
        ORDER BY rank
    );

-- 3) Construction pour les données déjà présentes (mutation en arrière-plan,
--    peut prendre plusieurs minutes sur des centaines de millions de lignes).
--    Suivi :
--      SELECT command, parts_to_do, is_done, latest_fail_reason
--      FROM system.mutations WHERE table = 'fqdn_search' AND NOT is_done;
ALTER TABLE fqdn_search MATERIALIZE PROJECTION p_rank;
