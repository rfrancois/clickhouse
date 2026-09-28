-- ============================================================
-- IMPORT domains.json — nettoyage / validation d'un lot
-- ============================================================
-- stg_domain (un lot brut) → stg_domain_norm. Exécutée pour chaque lot par
-- import_domains() dans scripts/import_data.py : UNE SEULE requête, sans
-- `;` final ni SET (envoyée telle quelle par HTTP).
--
-- ATTENTION : domains.json n'est PAS fiable (cn = IP, cn identique à ip, noms
-- en majuscules, wildcards, texte libre...). Chaque valeur est donc
-- normalisée et validée avant d'être utilisée.
--
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
