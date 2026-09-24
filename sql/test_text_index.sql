-- ============================================================
-- ESSAI — index texte exact vs index ngram, sur 1/16 de fqdn
-- ============================================================
-- Ne modifie PAS fqdn : copie 1/16 des lignes (~90 M) dans une table
-- d'essai qui porte les DEUX index, puis compare blocs lus, temps et taille.
-- Durée : quelques minutes. Nettoyage à la fin : DROP TABLE test_text_index.
-- Lancement (Linux, depuis le dossier du projet) :
--   docker exec -i ch_container clickhouse-client --user chuser --password Royal15Raccoon --multiquery --echo --format PrettyCompactMonoBlock < sql/test_text_index.sql > test_text_index.txt 2>&1

DROP TABLE IF EXISTS test_text_index;
CREATE TABLE test_text_index
(
    value    String,
    id_fqdn  Int64,
    rank     Int32,
    version  UInt64,
    INDEX idx_ngram value TYPE ngrambf_v1(3, 16384, 4, 0) GRANULARITY 1,
    INDEX idx_text  value TYPE text(tokenizer = ngrams(3))
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (reverse(value), id_fqdn);

INSERT INTO test_text_index
SELECT value, id_fqdn, rank, version FROM fqdn
WHERE positiveModulo(id_fqdn, 16) = 0
SETTINGS max_memory_usage = 11000000000;

-- Blocs gardés par chaque index (vrais blocs utiles en dernier)
EXPLAIN indexes = 1
SELECT * FROM test_text_index WHERE value LIKE '%google.com%' ORDER BY rank LIMIT 500
SETTINGS ignore_data_skipping_indices = 'idx_text';
EXPLAIN indexes = 1
SELECT * FROM test_text_index WHERE value LIKE '%google.com%' ORDER BY rank LIMIT 500
SETTINGS ignore_data_skipping_indices = 'idx_ngram';
SELECT count() AS lignes, uniqExact(_part, intDiv(_part_offset, 8192)) AS blocs_utiles
FROM test_text_index WHERE value LIKE '%google.com%';

-- Chronométrage (2 passes chacun)
SELECT * FROM test_text_index WHERE value LIKE '%google.com%' ORDER BY rank LIMIT 500
SETTINGS ignore_data_skipping_indices = 'idx_text', log_comment = 'tti_ngram_1' FORMAT Null;
SELECT * FROM test_text_index WHERE value LIKE '%google.com%' ORDER BY rank LIMIT 500
SETTINGS ignore_data_skipping_indices = 'idx_text', log_comment = 'tti_ngram_2' FORMAT Null;
SELECT * FROM test_text_index WHERE value LIKE '%google.com%' ORDER BY rank LIMIT 500
SETTINGS ignore_data_skipping_indices = 'idx_ngram', log_comment = 'tti_text_1' FORMAT Null;
SELECT * FROM test_text_index WHERE value LIKE '%google.com%' ORDER BY rank LIMIT 500
SETTINGS ignore_data_skipping_indices = 'idx_ngram', log_comment = 'tti_text_2' FORMAT Null;
SELECT * FROM test_text_index WHERE value LIKE '%google.com%' ORDER BY rank
SETTINGS ignore_data_skipping_indices = 'idx_ngram', log_comment = 'tti_text_sans_limit' FORMAT Null;

SYSTEM FLUSH LOGS;
SELECT log_comment, query_duration_ms AS ms,
       formatReadableQuantity(read_rows) AS lignes_lues,
       formatReadableSize(read_bytes) AS octets_lus
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment LIKE 'tti_%'
  AND event_time > now() - INTERVAL 30 MINUTE
ORDER BY event_time_microseconds;

-- Taille : multiplier par ~16 pour la table complète
SELECT name, formatReadableSize(data_compressed_bytes) AS taille_index
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'test_text_index';
SELECT formatReadableSize(sum(bytes_on_disk)) AS taille_table_essai, sum(rows) AS lignes
FROM system.parts
WHERE database = currentDatabase() AND table = 'test_text_index' AND active;
