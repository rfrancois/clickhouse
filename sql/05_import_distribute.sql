-- ============================================================
-- DISTRIBUTION staging → tables optimisées — partie DOMAINS
-- ============================================================
-- node.csv (qui fournit ses propres ids) est distribué par
-- scripts/import_data.py (distribute_nodes), une table par type de nœud.
-- domains.json n'a pas d'id : ses valeurs (cn, dns, ip) sont versées dans
-- stg_value et ses liens (cn ↔ dns, cn ↔ ip) dans stg_link, pour être traités
-- comme les liens CSV par scripts/import_data.py (distribute_links) :
--   * valeur déjà connue de fqdn / ip → on reprend son id ;
--   * valeur inconnue → nouvel id auto-incrémenté à partir du max(id) du type,
--     rank = 1000000, version = now().
-- Cette résolution est faite en TRANCHES côté Python, sinon les jointures sur
-- les grosses tables font tuer le serveur par l'OOM killer (exit 137) sur les
-- VM Docker à faible RAM.

-- ------------------------------------------------------------
-- Garde-fous mémoire : chaque requête plafonne sa RAM et déborde sur disque
-- très tôt plutôt que de pousser le serveur jusqu'à l'OOM.
-- ------------------------------------------------------------
SET max_threads = 1;
SET max_memory_usage = 11000000000;                  -- 11 Gio / requête (< cap serveur)
SET max_bytes_before_external_group_by = 536870912;  -- 512 Mio → spill disque
SET max_bytes_before_external_sort = 536870912;      -- 512 Mio → spill disque
SET use_skip_indexes = 0;                            -- l'index ngram n'aide pas ici, il coûte de la RAM

-- ---------- 1) domains.json → valeurs sans id (stg_value) ----------
-- Pas d'insert direct dans fqdn / ip : l'id est attribué plus
-- tard (auto-incrément), après résolution contre les valeurs existantes.

INSERT INTO stg_value (node_type, value)
SELECT 'fqdn', cn
FROM stg_domain
WHERE cn IS NOT NULL AND cn != '';

INSERT INTO stg_value (node_type, value)
SELECT 'fqdn', value
FROM (SELECT arrayJoin(dns) AS value FROM stg_domain)
WHERE value IS NOT NULL AND value != '';

INSERT INTO stg_value (node_type, value)
SELECT 'ip', ip
FROM stg_domain
WHERE ip IS NOT NULL AND ip != '';

-- ---------- 1b) domains.json → liens cn ↔ dns et cn ↔ ip (stg_link) ----------
-- Un seul sens ici : distribute_links() insère chaque lien résolu dans les
-- DEUX sens dans link. Dates vides → now(), source 0. Auto-liens (dns == cn)
-- filtrés.

-- cn ─ dns  (fqdn ↔ fqdn)
INSERT INTO stg_link (id_node_1, id_node_2, type_1, type_2, id_source,
                      creation_date, update_date)
SELECT cn, d, 'fqdn', 'fqdn', '0', '', ''
FROM (SELECT cn, arrayJoin(dns) AS d FROM stg_domain
      WHERE cn IS NOT NULL AND cn != '')
WHERE d IS NOT NULL AND d != '' AND d != cn;

-- cn ─ ip  (fqdn ↔ ip)
INSERT INTO stg_link (id_node_1, id_node_2, type_1, type_2, id_source,
                      creation_date, update_date)
SELECT cn, ip, 'fqdn', 'ip', '0', '', ''
FROM stg_domain
WHERE cn IS NOT NULL AND cn != '' AND ip IS NOT NULL AND ip != '';

-- ---------- 2) staging domains nettoyé ----------
-- stg_link et stg_value sont conservés : consommés ensuite par
-- distribute_links() côté Python.

DROP TABLE stg_domain;
