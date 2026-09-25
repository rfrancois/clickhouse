-- ============================================================
-- IMPORT domains.json — étapes 1 / 1b : valeurs et liens
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
--
-- Entrée : stg_domain_norm (05_import_normalize.sql). Rejouable : en cas
-- d'échec, `make import-resume` vide stg_value et les liens issus de
-- domains.json puis relance ce fichier.

-- ------------------------------------------------------------
-- Garde-fous mémoire : chaque requête plafonne sa RAM et déborde sur disque
-- très tôt plutôt que de pousser le serveur jusqu'à l'OOM.
-- ------------------------------------------------------------
SET max_threads = 1;
SET max_memory_usage = 11000000000;                  -- 11 Gio / requête (< cap serveur)
SET max_bytes_before_external_group_by = 536870912;  -- 512 Mio → spill disque
SET max_bytes_before_external_sort = 536870912;      -- 512 Mio → spill disque
SET use_skip_indexes = 0;                            -- l'index ngram n'aide pas ici, il coûte de la RAM
-- Gros blocs d'insertion (1 Gio) : un INSERT ... SELECT de milliards de
-- lignes crée une part par bloc (~1 M de lignes par défaut) ; avec des
-- blocs 16x plus gros, les merges suivent au lieu de TOO_MANY_PARTS.
SET min_insert_block_size_rows = 100000000;
SET min_insert_block_size_bytes = 1073741824;

-- ---------- 1) valeurs retenues → stg_value (sans id) ----------
-- Pas d'insert direct dans fqdn / ip : l'id est attribué plus
-- tard (auto-incrément), après résolution contre les valeurs existantes.

INSERT INTO stg_value (node_type, value)
SELECT t.1, t.2
FROM (SELECT arrayJoin(arrayConcat([ip, cn], dns)) AS t FROM stg_domain_norm)
WHERE t.1 IN ('fqdn', 'ip');

-- ---------- 1b) liens cn ↔ dns et cn ↔ ip (stg_link) ----------
-- Un seul sens ici : distribute_links() insère chaque lien résolu dans les
-- DEUX sens dans link. Dates vides → now(), source 0. Seules les valeurs
-- retenues sont liées ; une valeur rejetée n'a aucun lien. Auto-liens (même
-- type et même valeur normalisée des deux côtés) filtrés ici, et de nouveau
-- après résolution des ids dans distribute_links().

-- cn ─ dns  (fqdn ↔ fqdn, ip ↔ fqdn, ...)
INSERT INTO stg_link (id_node_1, id_node_2, type_1, type_2, id_source,
                      creation_date, update_date)
SELECT cn.2, d.2, cn.1, d.1, '0', '', ''
FROM (SELECT cn, arrayJoin(dns) AS d FROM stg_domain_norm
      WHERE cn.1 IN ('fqdn', 'ip'))
WHERE d.1 IN ('fqdn', 'ip') AND d != cn;

-- cn ─ ip  (fqdn ↔ ip, ou ip ↔ ip si le cn est une autre IP)
INSERT INTO stg_link (id_node_1, id_node_2, type_1, type_2, id_source,
                      creation_date, update_date)
SELECT cn.2, ip.2, cn.1, 'ip', '0', '', ''
FROM stg_domain_norm
WHERE cn.1 IN ('fqdn', 'ip') AND ip.1 = 'ip' AND ip != cn;

-- stg_link et stg_value sont consommés ensuite par distribute_links() ;
-- stg_domain_norm par report_domains() (comptage des rejets), côté Python.
