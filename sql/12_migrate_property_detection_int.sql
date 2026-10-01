-- ============================================================
-- MIGRATION property.detection_date : DateTime → Int32
-- ============================================================
-- Pour une base où property est DÉJÀ en AggregatingMergeTree avec
-- detection_date en SimpleAggregateFunction(min, DateTime) (make
-- migration-property-swap lancé avant ce changement). Une base pas encore
-- migrée n'en a pas besoin : 10_migrate_property_copy.sql produit
-- directement de l'Int32.
--
-- detection_date devient un timestamp Unix (secondes) en
-- SimpleAggregateFunction(min, Int32) (cf. sql/02_optimized.sql). Int32 :
-- dates jusqu'au 2038-01-19.
--
-- En place (ALTER ... MODIFY COLUMN) : mutation qui réécrit la seule colonne
-- detection_date de chaque part (payload non touché), pas de copie de la
-- table. Le fichier attend la fin de la mutation (mutations_sync = 2).
-- Suivi depuis une autre session :
--   SELECT parts_to_do, latest_fail_reason FROM system.mutations
--   WHERE table = 'property' AND NOT is_done;
-- Prérequis : aucun import pendant la mutation.
-- Retour arrière (même mécanisme, sans perte) :
--   ALTER TABLE property MODIFY COLUMN detection_date
--       SimpleAggregateFunction(min, DateTime) SETTINGS mutations_sync = 2;

-- garde-fous : property pas encore migrée, ou colonne déjà en Int32 → arrêt
SELECT throwIf(engine != 'AggregatingMergeTree',
               'property n''est pas en AggregatingMergeTree : lancer make migration-property puis make migration-property-swap (déjà en Int32)')
FROM system.tables WHERE database = currentDatabase() AND name = 'property'
FORMAT Null;
SELECT throwIf(type != 'SimpleAggregateFunction(min, DateTime)',
               'property.detection_date n''est pas en SimpleAggregateFunction(min, DateTime) : rien à faire')
FROM system.columns
WHERE database = currentDatabase() AND table = 'property' AND name = 'detection_date'
FORMAT Null;

ALTER TABLE property MODIFY COLUMN detection_date SimpleAggregateFunction(min, Int32)
SETTINGS mutations_sync = 2;

SELECT name, type FROM system.columns
WHERE database = currentDatabase() AND table = 'property' AND name = 'detection_date'
FORMAT PrettyCompactMonoBlock;
