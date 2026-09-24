-- ============================================================
-- DIAGNOSTIC — lenteur de ... LIKE '%google.com%' ORDER BY rank LIMIT 500
-- ============================================================
-- Lecture seule : ne modifie rien. Durée : quelques dizaines de secondes.
-- Lancement (Linux, depuis le dossier du projet) :
--   docker exec -i ch_container clickhouse-client --user chuser --password Royal15Raccoon --multiquery --echo --format PrettyCompactMonoBlock < sql/diag_rank.sql > diag_rank.txt 2>&1
-- Puis envoyer diag_rank.txt.

-- 1) Serveur
SELECT version() AS version,
       getSetting('max_threads') AS max_threads,
       getSetting('optimize_read_in_order') AS read_in_order;
SELECT metric, formatReadableQuantity(value) AS value
FROM system.asynchronous_metrics
WHERE metric IN ('OSMemoryTotal', 'OSMemoryAvailable', 'NumberOfCPUCores', 'NumberOfPhysicalCPUCores')
ORDER BY metric;

-- 2) Table et projection
SELECT create_table_query FROM system.tables
WHERE database = currentDatabase() AND name = 'fqdn' FORMAT Vertical;
SELECT count() AS parts, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS disk
FROM system.parts WHERE database = currentDatabase() AND table = 'fqdn' AND active;
SELECT name AS projection, count() AS parts, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS disk
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'fqdn' AND active
GROUP BY name;
SELECT command, parts_to_do, is_done, latest_fail_reason
FROM system.mutations WHERE database = currentDatabase() AND table = 'fqdn'
ORDER BY create_time DESC LIMIT 5;

-- 3) Plan choisi
EXPLAIN projections = 1, indexes = 1
SELECT * FROM fqdn WHERE value LIKE '%google.com%' ORDER BY rank LIMIT 500;

-- 4) Requêtes chronométrées (résultats jetés, temps lus dans query_log)
SELECT * FROM fqdn WHERE value LIKE '%google.com%' ORDER BY rank LIMIT 500
SETTINGS log_comment = 'diag_1_proj_run1' FORMAT Null;
SELECT * FROM fqdn WHERE value LIKE '%google.com%' ORDER BY rank LIMIT 500
SETTINGS log_comment = 'diag_2_proj_run2' FORMAT Null;
SELECT * FROM fqdn WHERE value LIKE '%google.com%' ORDER BY rank LIMIT 500
SETTINGS optimize_use_projections = 0, log_comment = 'diag_3_sans_proj' FORMAT Null;
SELECT * FROM fqdn WHERE value LIKE '%google.com%' ORDER BY rank DESC LIMIT 500
SETTINGS log_comment = 'diag_4_desc' FORMAT Null;
SELECT * FROM fqdn WHERE value LIKE '%google.com%' LIMIT 500
SETTINGS log_comment = 'diag_5_sans_tri' FORMAT Null;

-- 5) Où sont les lignes qui matchent, dans l'ordre de rank ?
SELECT count() AS matches,
       min(rank) AS rank_min,
       quantiles(0.001, 0.01, 0.5)(rank) AS rank_q_matches
FROM fqdn WHERE value LIKE '%google.com%'
SETTINGS log_comment = 'diag_6_distribution';
SELECT countIf(rank = 0) AS rank_0,
       quantiles(0.001, 0.01, 0.5)(rank) AS rank_q_table
FROM fqdn
SETTINGS log_comment = 'diag_7_distribution_table';

-- 6) Mesures
SYSTEM FLUSH LOGS;
SELECT log_comment,
       query_duration_ms AS ms,
       formatReadableQuantity(read_rows) AS lignes_lues,
       formatReadableSize(read_bytes) AS octets_lus,
       formatReadableSize(memory_usage) AS memoire,
       projections
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment LIKE 'diag_%'
  AND event_time > now() - INTERVAL 30 MINUTE
ORDER BY event_time_microseconds;
