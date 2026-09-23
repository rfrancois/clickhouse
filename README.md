# Banc d'essai ClickHouse — schéma optimisé (application de SOLUTION.md)

Stack : ClickHouse 26.7 en Docker + scripts Python (venv local).

- **Données factices** (optionnelles, pour tester) : 500 000 FQDN,
  500 000 IP, 1 000 000 liens = 2 000 000 lignes dans `link` (chaque lien est
  inséré dans les deux sens, 2 % des FQDN contiennent des "hot terms" :
  youtube, shop, bank, mail…).
- **Schéma de production** : `fqdn_search` / `ip_search` / `link`
  (index de saut `ngrambf_v1` sur `value`, ids typés `Int64`, liens
  dupliqués dans les deux sens, `ReplacingMergeTree(version)`).

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
| `*link*.csv` | `id_node_1;id_node_2;type_1;type_2;id_source;creation_date;update_date` | `link` |
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
  `(type, valeur) → id` par jointure sur `fqdn_search` / `ip_search`. Une
  extrémité fqdn/ip inconnue de ces tables est **créée** (même id synthétique
  `cityHash64` tronqué et `rank = 1000000` que pour `domains.json`) avant la
  jointure, plutôt que d'être ignorée. Seuls les autres types (`application`,
  `plugin`, ...), qui n'ont pas de table de valeurs, restent ignorés et
  comptés dans `liens_ignores_noeud_inconnu` pendant l'import.
  La jointure utilise `join_algorithm = 'partial_merge'` (tri-fusion avec
  débordement disque) pour tenir en mémoire à très grande volumétrie ;
  chaque lien résolu est inséré dans `link` **dans les deux sens** (voir la
  section "Migration de `link_opt` vers `link`" plus bas pour le détail) ;
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

### Index texte exact

L'index ngram (filtre de Bloom) laisse passer beaucoup de faux positifs sur
les vraies données : il garde ~73 % des blocs, alors que ~7 % contiennent le
terme. L'index texte `idx_text` est exact. Essai sur 1/16 des données
(`sql/test_text_index.sql`) : 9 051 → 1 656 blocs, 88 → 24 ms par
recherche, mais ~30 Gio d'index par milliard de lignes.

Base existante (sans copie, construction en arrière-plan, pas d'import
pendant ce temps) :

```bash
make text-index   # = sql/09_add_text_index.sql
```

L'index ngram est gardé pour l'instant. Après un import réussi avec l'index
texte (pas d'erreur mémoire), il peut être supprimé :
`ALTER TABLE fqdn_search DROP INDEX idx_ngram;`
Retour arrière : `ALTER TABLE fqdn_search DROP INDEX idx_text;`

Diagnostic de performance : `sql/diag_rank.sql` (lecture seule).

### Table `ip_search`

Même principe (projection `p_id`, rank 0 → 1 000 000), avec deux
différences :
- triée par valeur dans l'ordre **normal** (`ORDER BY (value, id_ip)`) : pour
  une IP, c'est le préfixe qui regroupe (sous-réseau), donc
  `LIKE '192.168.%'` passe par la clé primaire ;
- **aucun index ngram/texte** : une IP n'a que des chiffres et des points,
  les trigrammes sont présents dans presque tous les blocs, et l'index ne
  filtre rien (testé : `LIKE '%8.8.8%'` en 112 ms avec, 45 ms sans).

```bash
make migrate-ip        # sql/10_migrate_ip_copy.sql : remplit ip_search_new, affiche les comptes
make migrate-ip-swap   # sql/11_migrate_ip_swap.sql : bascule (ancienne → ip_search_old)
```

### Migration de `link_opt` vers `link` (liens typés)

`link_opt` ne stockait que deux ids, sans savoir si chaque extrémité était
un FQDN ou une IP (les ids ne sont uniques qu'à l'intérieur d'un type). La
table `link` porte le type de chaque extrémité :

- `(type_1, id_1, type_2, id_2)`, types en `Enum8` (`application`,
  `capture`, `fqdn`, `ip`, `plugin`, `organization_name`,
  `organization_id`, `phone`, `social_id`) ; un nouveau type s'ajoute à la
  fin des deux `Enum8` (métadonnées seules) ;
- une seule table pour tous les couples de types, triée
  `(type_1, id_1, type_2, id_2)`, `PARTITION BY type_1` ;
- **pas de projection inverse** : chaque lien est inséré physiquement dans
  les deux sens (A→B et B→A) par l'import (`sql/05_import_distribute.sql`,
  `distribute_links()` dans `scripts/import_data.py`) et par `make generate`.
  Un simple filtre `type_1 = ... AND id_1 = ...` retrouve donc les voisins
  des deux côtés, sans `UNION`. Coût disque équivalent à l'ancienne
  projection (qui dupliquait déjà les mêmes colonnes) ; en contrepartie,
  toute écriture sur `link` (update, suppression) doit traiter les deux
  lignes ensemble pour rester cohérente — rien ne les garde plus
  synchronisées automatiquement.

La migration considère **tout le contenu de `link_opt` comme fqdn ↔ fqdn**
et écrit les deux sens. Les liens qui étaient en réalité fqdn ↔ ip sont à
réimporter pour être correctement typés.

```bash
make migrate-link        # sql/12_migrate_link_copy.sql : remplit link (2 sens), affiche les comptes
make migrate-link-swap   # sql/13_migrate_link_swap.sql : link_opt → link_opt_old
```

Base dont `link` existe déjà mais à **sens unique** (schéma d'avant cette
migration, avec projection `p_reverse`) : ajoute le sens manquant puis
supprime la projection, devenue inutile. À lancer une seule fois, import et
génération arrêtés pendant ce temps.

```bash
make migrate-link-bidir   # sql/15_migrate_link_bidirectional.sql
```

### Table `property` (informations par nœud et par source)

Une ligne par `(node_type, id_node, id_source)` : `payload` (JSON
stocké en `String` compressé ZSTD, renvoyé tel quel), `detection_date`,
`version`. `ReplacingMergeTree(version)` : une nouvelle détection d'une même
source remplace l'ancienne (pas d'historique). `node_type` utilise le même
`Enum8` que `link` : `(node_type, id_node)` identifie un nœud.
Projection légère `p_source` pour « tout ce qu'a produit la source X ».

```bash
make property   # sql/14_create_property.sql : crée la table (vide) sur une base existante
```

```sql
SELECT id_source, payload, detection_date FROM property FINAL
WHERE node_type = 'fqdn' AND id_node = 123456;
```

## Résultats historiques (dans `results/`)

Les benchmarks naïf vs optimisé qui ont justifié cette architecture sont
conservés dans `results/*.json` et `results/*.png` (300M FQDN : recherche
×17, jointure ×3,9). Ils ont servi à produire `RAPPORT_OPTIMISATION.pdf`
(`make pdf`). Le schéma naïf n'existe plus : seules les tables optimisées
sont utilisées.
