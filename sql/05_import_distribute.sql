-- ============================================================
-- DISTRIBUTION staging → tables optimisées — partie NODE / DOMAINS
-- (fqdn_search / ip_search ; les tables naïves ne sont plus alimentées)
-- ============================================================
-- La résolution des LIENS (stg_link → link_opt) n'est plus faite ici : elle
-- est pilotée en TRANCHES par scripts/import_data.py (distribute_links), sinon
-- les jointures sur les grosses tables font tuer le serveur par l'OOM killer
-- (exit 137) sur les VM Docker à faible RAM.
--
-- Hypothèses (cf. README) :
--  * node.csv fournit id/rank/date ; domains.json n'a ni id ni rank
--    → id synthétique = cityHash64(valeur) tronqué à 31 bits, rank = 0,
--      version = now()

-- ------------------------------------------------------------
-- Garde-fous mémoire : chaque requête plafonne sa RAM et déborde sur disque
-- très tôt plutôt que de pousser le serveur jusqu'à l'OOM.
-- ------------------------------------------------------------
SET max_threads = 1;
SET max_memory_usage = 11000000000;                  -- 11 Gio / requête (< cap serveur)
SET max_bytes_before_external_group_by = 536870912;  -- 512 Mio → spill disque
SET max_bytes_before_external_sort = 536870912;      -- 512 Mio → spill disque
SET use_skip_indexes = 0;                            -- l'index ngram n'aide pas ici, il coûte de la RAM

-- ---------- 1) node.csv → fqdn / ip ----------

INSERT INTO fqdn_search (value, id_fqdn, rank, version)
SELECT value,
       toInt32OrZero(id),
       toUInt32OrZero(rank),
       coalesce(toUnixTimestamp(parseDateTimeBestEffortOrNull(creation_date)),
                toUnixTimestamp(now()))
FROM stg_node
WHERE lower(node_type) = 'fqdn' AND value != '';

INSERT INTO ip_search (value, id_ip, rank, version)
SELECT value,
       toInt32OrZero(id),
       toUInt32OrZero(rank),
       coalesce(toUnixTimestamp(parseDateTimeBestEffortOrNull(creation_date)),
                toUnixTimestamp(now()))
FROM stg_node
WHERE lower(node_type) = 'ip' AND value != '';

-- ---------- 2) domains.json → fqdn (cn + chaque entrée dns) et ip ----------

INSERT INTO fqdn_search (value, id_fqdn, rank, version)
SELECT cn,
       toInt32(bitAnd(cityHash64(cn), 0x7FFFFFFF)),
       0,
       toUnixTimestamp(now())
FROM stg_domain
WHERE cn IS NOT NULL AND cn != '';

INSERT INTO fqdn_search (value, id_fqdn, rank, version)
SELECT value,
       toInt32(bitAnd(cityHash64(value), 0x7FFFFFFF)),
       0,
       toUnixTimestamp(now())
FROM (SELECT arrayJoin(dns) AS value FROM stg_domain)
WHERE value IS NOT NULL AND value != '';

INSERT INTO ip_search (value, id_ip, rank, version)
SELECT ip,
       toInt32(bitAnd(cityHash64(ip), 0x7FFFFFFF)),
       0,
       toUnixTimestamp(now())
FROM stg_domain
WHERE ip IS NOT NULL AND ip != '';

-- ---------- 3) staging node / domains nettoyé ----------
-- stg_link est conservé : consommé ensuite par distribute_links() côté Python.

DROP TABLE stg_node;
DROP TABLE stg_domain;
