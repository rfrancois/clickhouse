-- ============================================================
-- MIGRATION link.detection_date : UInt64 → Int32
-- ============================================================
-- Même type que property.detection_date (cf. sql/02_optimized.sql) :
-- timestamp Unix (secondes) en SimpleAggregateFunction(min, Int32), dates
-- jusqu'au 2038-01-19. version reste en UInt64 (comme dans property).
--
-- En place (ALTER ... MODIFY COLUMN) : mutation qui réécrit la seule colonne
-- detection_date de chaque part, pas de copie de la table. Le fichier attend
-- la fin de la mutation (mutations_sync = 2). Suivi depuis une autre session :
--   SELECT parts_to_do, latest_fail_reason FROM system.mutations
--   WHERE table = 'link' AND NOT is_done;
-- Prérequis : aucun import pendant la mutation.
-- Retour arrière (même mécanisme, sans perte) :
--   ALTER TABLE link MODIFY COLUMN detection_date
--       SimpleAggregateFunction(min, UInt64) SETTINGS mutations_sync = 2;

-- type actuel, affiché avant les garde-fous
SELECT name, type AS type_avant FROM system.columns
WHERE database = currentDatabase() AND table = 'link' AND name = 'detection_date'
FORMAT PrettyCompactMonoBlock;

-- garde-fous : type inattendu → arrêt ; déjà Int32 accepté (l'ALTER vers le
-- même type ne fait rien : relançable).
SELECT throwIf(NOT match(type, '^SimpleAggregateFunction\(min, (UInt64|Int32)\)$'),
               'link.detection_date : type inattendu (cf. type_avant ci-dessus), ni UInt64 ni Int32')
FROM system.columns
WHERE database = currentDatabase() AND table = 'link' AND name = 'detection_date'
FORMAT Null;
-- une date au-delà de 2038-01-19 ne tient pas en Int32 (elle deviendrait
-- négative) : arrêt plutôt que de la corrompre
SELECT throwIf(max(detection_date) > 2147483647,
               'link.detection_date : dates au-delà de 2038-01-19, non convertibles en Int32')
FROM link
FORMAT Null;

ALTER TABLE link MODIFY COLUMN detection_date SimpleAggregateFunction(min, Int32)
SETTINGS mutations_sync = 2;

SELECT name, type AS type_apres FROM system.columns
WHERE database = currentDatabase() AND table = 'link' AND name = 'detection_date'
FORMAT PrettyCompactMonoBlock;
