-- ============================================================
-- MIGRATION fqdn.rank / ip.rank : Int32 → Nullable(Int32)
-- ============================================================
-- rank inconnu = NULL (cf. sql/02_optimized.sql) au lieu des valeurs
-- conventionnelles 1000000 (import_data.py) et 1000001 (send.py).
-- anyLast ignore les NULL : un insert sans rank n'efface plus un rank connu.
-- ORDER BY rank range les NULL en dernier, comme l'étaient 1000000/1000001.
-- À lancer après make migration-nodes-swap si la base en a besoin (les
-- tables <type>_new de 08_migrate_nodes_copy.sql ont encore un rank Int32).
--
-- En place, deux mutations par table, attendues (mutations_sync = 2) :
--   1. ALTER ... MODIFY COLUMN : réécrit la seule colonne rank de chaque part ;
--   2. ALTER ... UPDATE : 1000000 et 1000001 → NULL. Un rank RÉEL de
--      1000000 (dernier FQDN d'un top 1M) devient aussi NULL : le prochain
--      make ranks le rétablit.
-- Suivi depuis une autre session :
--   SELECT table, command, parts_to_do, latest_fail_reason FROM system.mutations
--   WHERE table IN ('fqdn', 'ip') AND NOT is_done;
-- Prérequis : aucun import pendant les mutations. Relançable (MODIFY vers
-- le même type ne fait rien, plus aucune ligne à 1000000/1000001).
-- Retour arrière (NULL → 1000000 par le DEFAULT, exigé par ClickHouse pour
-- quitter Nullable, puis retrait du DEFAULT) :
--   ALTER TABLE fqdn MODIFY COLUMN rank SimpleAggregateFunction(anyLast, Int32)
--       DEFAULT 1000000 SETTINGS mutations_sync = 2;
--   ALTER TABLE fqdn MODIFY COLUMN rank REMOVE DEFAULT;
--   (idem pour ip)

-- types actuels, affichés avant les garde-fous
SELECT table, type AS type_avant FROM system.columns
WHERE database = currentDatabase() AND table IN ('fqdn', 'ip') AND name = 'rank'
FORMAT PrettyCompactMonoBlock;

-- garde-fous : tables pas encore en AggregatingMergeTree, ou type inattendu
-- → arrêt ; déjà Nullable accepté (relançable)
SELECT throwIf(countIf(engine = 'AggregatingMergeTree') != 2,
               'fqdn / ip pas en AggregatingMergeTree : lancer make migration-nodes puis make migration-nodes-swap')
FROM system.tables WHERE database = currentDatabase() AND name IN ('fqdn', 'ip')
FORMAT Null;
SELECT throwIf(countIf(match(type, '^SimpleAggregateFunction\\(anyLast, (Int32|Nullable\\(Int32\\))\\)$')) != 2,
               'fqdn.rank / ip.rank : type inattendu (cf. type_avant ci-dessus), ni Int32 ni Nullable(Int32)')
FROM system.columns
WHERE database = currentDatabase() AND table IN ('fqdn', 'ip') AND name = 'rank'
FORMAT Null;

ALTER TABLE fqdn MODIFY COLUMN rank SimpleAggregateFunction(anyLast, Nullable(Int32))
SETTINGS mutations_sync = 2;
ALTER TABLE ip MODIFY COLUMN rank SimpleAggregateFunction(anyLast, Nullable(Int32))
SETTINGS mutations_sync = 2;

ALTER TABLE fqdn UPDATE rank = NULL WHERE rank IN (1000000, 1000001)
SETTINGS mutations_sync = 2;
ALTER TABLE ip UPDATE rank = NULL WHERE rank IN (1000000, 1000001)
SETTINGS mutations_sync = 2;

SELECT table, type AS type_apres FROM system.columns
WHERE database = currentDatabase() AND table IN ('fqdn', 'ip') AND name = 'rank'
FORMAT PrettyCompactMonoBlock;
-- FINAL : une ligne par valeur (les parts pas encore fusionnées comptées une fois)
SELECT 'fqdn' AS table, countIf(rank IS NULL) AS rank_null, count() AS valeurs FROM fqdn FINAL
UNION ALL
SELECT 'ip', countIf(rank IS NULL), count() FROM ip FINAL
FORMAT PrettyCompactMonoBlock;
