-- ============================================================
-- AJOUT — index texte exact sur fqdn_search.value (base existante)
-- ============================================================
-- Pourquoi : l'index ngram (filtre de Bloom) garde ~73 % des blocs pour
-- LIKE '%google.com%', alors que ~7 % contiennent vraiment le terme. L'index
-- texte est exact : essai sur 1/16 des données (sql/test_text_index.sql) :
-- 9 051 → 1 656 blocs lus, 88 → 24 ms, mais ~30 Gio d'index par milliard
-- de lignes.
--
-- Sans copier la table, relançable. L'index ngram est GARDÉ pour l'instant
-- (filet de sécurité) : le supprimer une fois un import validé avec l'index
-- texte (mémoire OK) :
--   ALTER TABLE fqdn_search DROP INDEX idx_ngram;
-- Retour arrière complet :
--   ALTER TABLE fqdn_search DROP INDEX idx_text;
--
-- Pas d'import pendant la construction (plusieurs heures, en arrière-plan).
-- Suivi :
--   SELECT parts_to_do, is_done, latest_fail_reason FROM system.mutations
--   WHERE table = 'fqdn_search' AND NOT is_done;

-- 1) Déclaration : les nouvelles lignes (imports) sont indexées aussitôt
ALTER TABLE fqdn_search
    ADD INDEX IF NOT EXISTS idx_text value TYPE text(tokenizer = ngrams(3));

-- 2) Construction pour les lignes existantes (mutation en arrière-plan)
ALTER TABLE fqdn_search MATERIALIZE INDEX idx_text;
