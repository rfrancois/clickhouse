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
--
-- ATTENTION : domains.json n'est PAS fiable (cn = IP, cn identique à ip, noms
-- en majuscules, wildcards, texte libre...). Chaque valeur est donc
-- normalisée et validée (étape 0) avant d'être utilisée.

-- ------------------------------------------------------------
-- Garde-fous mémoire : chaque requête plafonne sa RAM et déborde sur disque
-- très tôt plutôt que de pousser le serveur jusqu'à l'OOM.
-- ------------------------------------------------------------
SET max_threads = 1;
SET max_memory_usage = 11000000000;                  -- 11 Gio / requête (< cap serveur)
SET max_bytes_before_external_group_by = 536870912;  -- 512 Mio → spill disque
SET max_bytes_before_external_sort = 536870912;      -- 512 Mio → spill disque
SET use_skip_indexes = 0;                            -- l'index ngram n'aide pas ici, il coûte de la RAM

-- ---------- 0) nettoyage / validation → stg_domain_norm ----------
-- ip, cn et chaque entrée de dns passent par la même règle (lambda unique
-- appliquée à [ip, cn, dns...]) :
--   normalisation : espaces retirés, minuscules, point final retiré
--                   (Netflix.COM. → netflix.com) ;
--   IPv4 / IPv6   → ('ip', forme canonique : 2001:DB8:0::1 → 2001:db8::1),
--                   le type vient de la FORME de la valeur, pas du champ
--                   (un cn qui est une IP n'est jamais versé dans fqdn) ;
--                   IP jamais significative (0.0.0.0/8, 127/8, 169.254/16,
--                   224/3 = multicast + réservé + broadcast, ::, ::1,
--                   fe80::/10, ff00::/8, ::ffff:0|127.x) → 'x_nonroutable' ;
--   *.domaine     → 'x_wildcard' (pas un nœud réel) ;
--   nom d'hôte valide → ('fqdn', valeur) : ≤ 253 caractères, au moins deux
--                   labels de 1 à 63 caractères [a-z0-9_-] ou non-ASCII
--                   (IDN), dernier label pas entièrement numérique (écarte
--                   999.1.1.1) ; donc rejet de localhost, "a b.com",
--                   admin@x.com, x.com/path, CN=foo.com... → 'x_invalid' ;
--   vide / null   → ('', '').
-- Le champ ip doit être une IP : un nom d'hôte y est rejeté ('x_invalid').
-- Doublons dans dns (après normalisation) supprimés.

INSERT INTO stg_domain_norm (ip, cn, dns)
SELECT if(n[1].1 = 'fqdn', ('x_invalid', n[1].2), n[1]),
       n[2],
       arrayDistinct(arrayFilter(x -> x.1 != '', arraySlice(n, 3)))
FROM (
    SELECT arrayMap(v -> multiIf(
               v = '', ('', ''),
               isIPv4String(v),
                   if(arrayExists(r -> isIPAddressInRange(v, r),
                                  ['0.0.0.0/8', '127.0.0.0/8', '169.254.0.0/16',
                                   '224.0.0.0/3']),
                      ('x_nonroutable', v), ('ip', toString(toIPv4(v)))),
               isIPv6String(v),
                   if(arrayExists(r -> isIPAddressInRange(v, r),
                                  ['::/127', 'fe80::/10', 'ff00::/8',
                                   '::ffff:0.0.0.0/104', '::ffff:127.0.0.0/104']),
                      ('x_nonroutable', v), ('ip', toString(toIPv6(v)))),
               startsWith(v, '*.'), ('x_wildcard', v),
               length(v) <= 253
                   AND match(v, '^(?:[a-z0-9_]|[^\\x00-\\x7f])(?:[a-z0-9_-]|[^\\x00-\\x7f]){0,62}(?:\\.(?:[a-z0-9_]|[^\\x00-\\x7f])(?:[a-z0-9_-]|[^\\x00-\\x7f]){0,62})+$')
                   AND NOT match(v, '\\.[0-9]+$'),
                   ('fqdn', v),
               ('x_invalid', v)),
             arrayMap(x -> trim(TRAILING '.' FROM lowerUTF8(trimBoth(ifNull(x, '')))),
                      arrayConcat([ip, cn], dns))) AS n
    FROM stg_domain
);

DROP TABLE stg_domain;

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
