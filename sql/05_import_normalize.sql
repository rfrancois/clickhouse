-- ============================================================
-- IMPORT domains.json — étape 0 : nettoyage / validation
-- ============================================================
-- stg_domain (brut) → stg_domain_norm, puis DROP stg_domain. Les valeurs
-- retenues et les liens sont extraits ensuite par 06_import_domains.sql.
--
-- ATTENTION : domains.json n'est PAS fiable (cn = IP, cn identique à ip, noms
-- en majuscules, wildcards, texte libre...). Chaque valeur est donc
-- normalisée et validée avant d'être utilisée.

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

-- stg_domain_norm n'est plus que lue (06_import_domains.sql, report_domains) :
-- inutile de la fusionner (SYSTEM START MERGES en fin de distribution).
SYSTEM STOP MERGES stg_domain_norm;
