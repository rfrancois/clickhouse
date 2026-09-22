# Banc d'essai ClickHouse — schéma optimisé (application de SOLUTION.md)

Stack : ClickHouse 26.7 en Docker + scripts Python (venv local).

- **Données factices** (optionnelles, pour tester) : 500 000 FQDN,
  500 000 IP, 1 000 000 liens (2 % des FQDN contiennent des "hot terms" :
  youtube, shop, bank, mail…).
- **Schéma de production** : `fqdn_search` / `ip_search` / `link_opt`
  (index de saut `ngrambf_v1` sur `value`, ids typés `Int64`, projection
  inversée, `ReplacingMergeTree(version)`).

## Commandes

```bash
cd bench

make all          # docker + schéma + données factices de test
# ou étape par étape :
make up wait      # démarre ClickHouse (ports 8123/9000, user/pass bench/bench)
make init         # crée le schéma optimisé
make generate     # insère 500k FQDN / 500k IP / 1M liens factices
make test         # test rapide : LIKE + jointure sur les tables optimisées
make import FILE=archive.zip   # import de données réelles (voir ci-dessous)
make pdf          # régénère RAPPORT_OPTIMISATION.pdf
make down         # stoppe le conteneur
make clean        # tout supprime (volume, venv, résultats)
```

## Import de données réelles

```bash
make import FILE=archive.zip   # ou FILE=fichier.csv / fichier.json.gz / dossier
```

Fichiers reconnus (classification par nom) :

| Fichier | Format | Destination |
|---|---|---|
| `*node*.csv` | `id;value;type;creation_date;rank` (';', quoté) | `fqdn_search` / `ip_search` selon `type` |
| `*link*.csv` | `id_node_1;id_node_2;type_1;type_2;id_source;creation_date;update_date` | `link_opt` |
| `*.json` / `*.json.gz` | `{"cn":…, "dns":[…]\|null, "ip":…\|null}` (JSONEachRow) | `fqdn_search` (cn + dns), `ip_search` |

Pipeline : extraction du zip → staging brut (`sql/04_import_staging.sql`,
streaming `clickhouse-client`, pas de parsing Python) → distribution
(`sql/05_import_distribute.sql`) vers les tables optimisées.

`make import` est autonome : si les tables optimisées n'existent pas encore
(`make init` jamais lancé), le schéma `sql/02_optimized.sql` est créé
automatiquement avant le chargement.

Choix d'import :
- `domains.json` n'a ni id ni rank → id synthétique `cityHash64(valeur)`
  tronqué, `rank = 1000000`, `version = now()` ;
- les liens référencent des **valeurs** (ex. `netflix.com`) + le type de
  chaque extrémité (`type_1` / `type_2` = `fqdn` | `ip`) : résolution
  `(type, valeur) → id` par jointure sur `fqdn_search` / `ip_search`. Un
  lien dont une extrémité est inconnue de ces tables est ignoré (jointure
  INNER) et compté dans `liens_ignores_noeud_inconnu` pendant l'import.
  La jointure utilise `join_algorithm = 'partial_merge'` (tri-fusion avec
  débordement disque) pour tenir en mémoire à très grande volumétrie ;
- `rank` absent ou à 0 → `1000000` (ces lignes passent en fin de
  `ORDER BY rank`) ; `make migrate` applique la même règle aux données existantes ;
- la déduplication est assurée par `ReplacingMergeTree(version)`
  (asynchrone) ;
- lignes malformées tolérées (0,1 % max, 1000 erreurs).

Ré-import idempotent : on peut relancer `make import` sur un fichier déjà
importé, les doublons seront fusionnés dans les tables optimisées.

Tester une requête à la main :

```bash
docker exec -it bench_clickhouse clickhouse-client --user bench --password bench
```

```sql
-- index de saut ngram : granules éludés, pas de scan complet
EXPLAIN indexes = 1
SELECT id_fqdn, value FROM fqdn_search WHERE value LIKE '%tube%' LIMIT 100;
```

## Recherche `LIKE '%…%'` triée par rank

```sql
SELECT * FROM fqdn_search WHERE value LIKE '%google.com%' ORDER BY rank;
```

`fqdn_search` est triée par **nom de domaine inversé**
(`ORDER BY (reverse(value), id_fqdn)`) : tous les `*.google.com`,
`google.com.br`… sont stockés côte à côte, donc l'index ngram ne garde que
quelques blocs et `ORDER BY rank` — avec ou sans `LIMIT` — ne trie que les
lignes trouvées. Triée par `id_fqdn` (ancien schéma), ces lignes étaient
éparpillées (~1 par bloc) et chaque recherche triée relisait presque toute la
table. La recherche par id (jointures) passe par la projection légère `p_id`.

Migrer une base existante (copie à côté, table actuelle intacte ; prévoir
de l'espace disque ≈ taille actuelle de la table et une à quelques heures
pour des milliards de lignes ; pas d'import pendant la copie) :

```bash
make migrate        # sql/07_migrate_copy.sql : remplit fqdn_search_new, affiche les comptes
make migrate-swap   # sql/08_migrate_swap.sql : bascule (ancienne → fqdn_search_old)
```

Retour arrière : `EXCHANGE TABLES fqdn_search AND fqdn_search_old;`.
Libérer le disque une fois satisfait :
`DROP TABLE fqdn_search_old SETTINGS max_table_size_to_drop = 0;`

Diagnostic de performance : `sql/diag_rank.sql` (lecture seule).

## Résultats historiques (dans `results/`)

Les benchmarks naïf vs optimisé qui ont justifié cette architecture sont
conservés dans `results/*.json` et `results/*.png` (300M FQDN : recherche
×17, jointure ×3,9). Ils ont servi à produire `RAPPORT_OPTIMISATION.pdf`
(`make pdf`). Le schéma naïf n'existe plus : seules les tables optimisées
sont utilisées.
