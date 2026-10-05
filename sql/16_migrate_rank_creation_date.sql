-- ============================================================
-- MIGRATION rank.week (Date, lundi) → rank.creation_date (Int32, samedi)
-- ============================================================
-- creation_date : timestamp Unix (secondes) du SAMEDI 00:00 UTC de la
-- semaine (fin de la semaine anglaise, cf. sql/02_optimized.sql). Une
-- semaine lundi M → dimanche M+6 de l'ancienne table devient le samedi
-- M+5 : celui de la semaine anglaise (dimanche M-1 → samedi M+5) qui
-- contient le lundi.
--
-- week est dans la clé de tri et de partition : ClickHouse refuse d'en
-- changer le type en place. Copie dans rank_new (dédupliquée, FINAL), contrôle
-- des comptes, puis échange instantané ; l'ancienne table est gardée sous
-- rank_old_week.
-- Prérequis : aucun make ranks pendant la migration (ce qui serait écrit
-- entre-temps dans rank ne serait pas copié).
-- Relançable tant que l'échange n'a pas eu lieu (rank_new est recréée) ;
-- après, le garde-fou arrête le fichier (rank n'a plus de colonne week).
--
-- Retour arrière tant que rank_old_week existe :
--   EXCHANGE TABLES rank AND rank_old_week;
-- (les ranks envoyés depuis la migration ne sont alors pas dans l'ancienne)
-- Libérer le disque une fois satisfait :
--   DROP TABLE rank_old_week;

-- garde-fous AVANT toute écriture
SELECT throwIf(count() = 0, 'rank n''a pas de colonne week : migration déjà faite, ou table absente (make migration-rank)')
FROM system.columns
WHERE database = currentDatabase() AND table = 'rank' AND name = 'week'
FORMAT Null;
SELECT throwIf(count() > 0, 'rank_old_week existe déjà : la supprimer ou la renommer avant')
FROM system.tables WHERE database = currentDatabase() AND name = 'rank_old_week'
FORMAT Null;

DROP TABLE IF EXISTS rank_new;
CREATE TABLE rank_new
(
    node_type  Enum8('application' = 1, 'capture' = 2, 'fqdn' = 3, 'ip' = 4,
                     'plugin' = 5, 'organization_name' = 6, 'organization_id' = 7,
                     'phone' = 8, 'social_id' = 9),
    id_node    Int64 CODEC(Delta, ZSTD),
    id_source  Int32,
    creation_date  Int32 CODEC(DoubleDelta, ZSTD),
    rank       Int32 CODEC(Delta, ZSTD)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(toDateTime(creation_date, 'UTC'))
ORDER BY (node_type, id_node, id_source, creation_date)
TTL toDateTime(creation_date, 'UTC') + INTERVAL 2 YEAR DELETE
SETTINGS ttl_only_drop_parts = 1;

-- toLastDayOfWeek(mode 0, semaine commençant le dimanche) → samedi
INSERT INTO rank_new (node_type, id_node, id_source, creation_date, rank)
SELECT node_type, id_node, id_source,
       toInt32(toUnixTimestamp(toDateTime(toLastDayOfWeek(week, 0), 'UTC'))),
       rank
FROM rank FINAL;

-- contrôle : autant de lignes (dédupliquées) et de semaines de chaque côté
SELECT throwIf((SELECT count() FROM rank FINAL) != (SELECT count() FROM rank_new FINAL),
               'nombre de lignes différent entre rank et rank_new : échange annulé')
FORMAT Null;
SELECT throwIf((SELECT uniqExact(week) FROM rank) != (SELECT uniqExact(creation_date) FROM rank_new),
               'nombre de semaines différent entre rank et rank_new : échange annulé')
FORMAT Null;

EXCHANGE TABLES rank AND rank_new;
RENAME TABLE rank_new TO rank_old_week;

-- lignes par semaine, avant (rank_old_week, lundi) / après (rank, samedi)
SELECT * FROM
(
    SELECT 'rank' AS table_, toDate(toDateTime(creation_date, 'UTC')) AS semaine, count() AS lignes
    FROM rank FINAL GROUP BY semaine
    UNION ALL
    SELECT 'rank_old_week', week, count() FROM rank_old_week FINAL GROUP BY week
)
ORDER BY table_, semaine
FORMAT PrettyCompactMonoBlock;
