# Banc d'essai ClickHouse — schéma optimisé (application de SOLUTION.md)

Stack : ClickHouse 26.7 en Docker + scripts Python (venv local).

- **Données factices** (optionnelles, pour tester) : 500 000 FQDN,
  500 000 IP, 1 000 000 liens = 2 000 000 lignes dans `link` (chaque lien est
  inséré dans les deux sens, 2 % des FQDN contiennent des "hot terms" :
  youtube, shop, bank, mail…).
- **Schéma de production** : une table de valeurs par type de nœud
  (`fqdn`, `ip`, `application`, `plugin`, ...) / `link`
  (index de saut `ngrambf_v1` sur `value`, ids typés `Int64`, liens
  dupliqués dans les deux sens, `ReplacingMergeTree(version)`).

## Commandes

```bash
cd bench

make all          # docker + schéma + données factices de test
# ou étape par étape :
make up wait      # démarre ClickHouse (ports 8123/9000, user/pass)
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
make import-resume             # reprend un import interrompu pendant la distribution
```

Fichiers reconnus (classification par nom) :

| Fichier | Format | Destination |
|---|---|---|
| `*node*.csv` | `id;value;type;creation_date;rank` (';', quoté) | table du `type` (`fqdn`, `ip`, `application`, ...) |
| `*link*.csv` | `id_node_1;id_node_2;type_1;type_2;id_source;creation_date;update_date` | `link` |
| `*propert*.csv` | `id_node;type;id_source;payload;version;detection_date` (';', quoté, `\"` dans le payload) | `property` |
| `*.json` / `*.json.gz` | `{"cn":…, "dns":[…]\|null, "ip":…\|null}` (JSONEachRow) | `fqdn` (cn + dns), `ip` (ip, et cn / dns qui sont des IP) |

Pipeline : extraction du zip → staging brut (`sql/04_import_staging.sql` ;
CSV en streaming `clickhouse-client`, JSON par lots de `IMPORT_CHUNK_LINES`
lignes via HTTP avec progression) → distribution vers les tables optimisées
(`sql/05_import_normalize.sql`, `sql/06_import_domains.sql`,
`scripts/import_data.py`).

Chaque étape de distribution consomme puis supprime sa table de staging :
si l'import échoue après le chargement (mémoire, `TOO_MANY_PARTS`...),
`make import-resume` reprend à l'étape interrompue sans recharger les
fichiers.

`make import` est autonome : si les tables optimisées n'existent pas encore
(`make init` jamais lancé), le schéma `sql/02_optimized.sql` est créé
automatiquement avant le chargement.

Choix d'import :
- aucun id n'est calculé à partir d'une valeur : `node.csv` garde ses
  propres ids ; toute autre valeur fqdn/ip (`domains.json`, extrémités de
  liens) est d'abord cherchée dans la table de son type et reprend
  l'id existant ; si elle est absente, elle reçoit un **nouvel id
  auto-incrémenté** à partir du `max(id)` du type déjà en base
  (`max + 1`, `max + 2`, ...), avec `version = now()` (et `rank = 1000000`
  pour `fqdn` / `ip`).
  Un seul import à la fois (deux imports concurrents liraient le même max) ;
- les liens référencent des **valeurs** (ex. `netflix.com`) + le type de
  chaque extrémité (`type_1` / `type_2`) : résolution `(type, valeur) → id`
  par jointure sur la table du type, après création des extrémités
  inconnues (règle ci-dessus). Les liens
  `cn ↔ dns` et `cn ↔ ip` de `domains.json` suivent le même chemin. Seul
  un type hors de l'`Enum8` (sans table) est ignoré, et compté pendant
  l'import. Les auto-liens (même type et même valeur / id des deux côtés,
  ex. `cn` identique à `ip`) sont écartés, et comptés.
  La jointure utilise `join_algorithm = 'partial_merge'` (tri-fusion avec
  débordement disque) pour tenir en mémoire à très grande volumétrie ;
  chaque lien résolu est inséré dans `link` **dans les deux sens** (voir la
  section "Table `link`" plus bas pour le détail) ;
- `domains.json` **n'est pas fiable** : chaque valeur (`ip`, `cn`, entrées
  de `dns`) est normalisée puis validée avant usage
  (`sql/05_import_normalize.sql`) :
  - normalisation : espaces, minuscules, point final retirés ; IPv6 en forme
    canonique ; doublons de `dns` supprimés ;
  - le type vient de la **forme** de la valeur : un `cn` / `dns` qui est
    une IP va dans `ip`, jamais dans `fqdn` ; le champ `ip` doit être une IP ;
  - rejetés (ni nœud ni lien, comptés par champ et raison pendant
    l'import) : wildcards (`*.x.com`), IP jamais significatives (`0.0.0.0/8`,
    `127/8`, `169.254/16`, multicast / réservé / broadcast, `::`, `::1`,
    `fe80::/10`, `ff00::/8`), noms d'hôte invalides (`localhost`, espaces,
    `@`, `/`, `CN=…`, label > 63 caractères, nom > 253, TLD numérique) ;
  - les IP privées (`10/8`, `192.168/16`...) sont **conservées** ;
- `rank` absent ou à 0 → `1000000` (ces lignes passent en fin de
  `ORDER BY rank`) ;
- `properties.csv` référence les nœuds par leur **id** (pas par valeur) :
  insertion directe dans `property`, sans résolution ni vérification que le
  nœud existe. Ses guillemets internes sont échappés par backslash (`\"`,
  export MySQL), ce que le format CSV de ClickHouse ne lit pas : le fichier
  est chargé en `CustomSeparatedWithNames` avec la règle d'échappement
  `JSON` (fins de ligne `\r\n` acceptées). `version` → timestamp Unix ;
  `detection_date` vide ou à la date zéro MySQL (`0000-00-00 00:00:00`)
  → date de `version` ;
- la déduplication est assurée par `ReplacingMergeTree(version)`
  (asynchrone) ;
- lignes malformées tolérées (0,1 % max, 1000 erreurs).

Ré-import idempotent : on peut relancer `make import` sur un fichier déjà
importé, les doublons seront fusionnés dans les tables optimisées.

Tester une requête à la main :

```bash
docker exec -it ch_container clickhouse-client --user chuser --password Royal15Raccoon
```

```sql
-- index de saut ngram : granules éludés, pas de scan complet
EXPLAIN indexes = 1
SELECT id_fqdn, value FROM fqdn WHERE value LIKE '%tube%' LIMIT 100;
```

## Recherche `LIKE '%…%'` triée par rank

```sql
SELECT * FROM fqdn WHERE value LIKE '%google.com%' ORDER BY rank;
```

`fqdn` est triée par **nom de domaine inversé**
(`ORDER BY (reverse(value), id_fqdn)`) : tous les `*.google.com`,
`google.com.br`… sont stockés côte à côte, donc l'index ngram ne garde que
quelques blocs et `ORDER BY rank` — avec ou sans `LIMIT` — ne trie que les
lignes trouvées. Triée par `id_fqdn`, ces lignes seraient éparpillées
(~1 par bloc) et chaque recherche triée relirait presque toute la table.
La recherche par id (jointures) passe par la projection légère `p_id`.

### Index texte exact

L'index ngram (filtre de Bloom) laisse passer beaucoup de faux positifs sur
les vraies données : il garde ~73 % des blocs, alors que ~7 % contiennent le
terme. L'index texte `idx_text` est exact. Essai sur 1/16 des données
(`sql/test_text_index.sql`) : 9 051 → 1 656 blocs, 88 → 24 ms par
recherche, mais ~30 Gio d'index par milliard de lignes. Les deux index
sont créés par le schéma (`make init`).

L'index ngram est gardé pour l'instant. Après un import réussi avec l'index
texte (pas d'erreur mémoire), il peut être supprimé :
`ALTER TABLE fqdn DROP INDEX idx_ngram;`
Retour arrière : `ALTER TABLE fqdn DROP INDEX idx_text;`

Diagnostic de performance : `sql/diag_rank.sql` (lecture seule).

### Table `ip`

Même principe (projection `p_id`, rank 0 → 1 000 000), avec deux
différences :
- triée par valeur dans l'ordre **normal** (`ORDER BY (value, id_ip)`) : pour
  une IP, c'est le préfixe qui regroupe (sous-réseau), donc
  `LIKE '192.168.%'` passe par la clé primaire ;
- **aucun index ngram/texte** : une IP n'a que des chiffres et des points,
  les trigrammes sont présents dans presque tous les blocs, et l'index ne
  filtre rien (testé : `LIKE '%8.8.8%'` en 112 ms avec, 45 ms sans).

### Tables des autres types de nœuds

`application`, `capture`, `plugin`, `organization_name`, `organization_id`,
`phone`, `social_id` : une table par type, même modèle que `ip`
(`value`, `id_<type>`, `version`, projection `p_id`, tri par valeur) mais
**sans `rank`**. Liste des types côté import : `NODE_TABLES` dans
`scripts/import_data.py`.

### Table `link` (liens typés)

Les ids ne sont uniques qu'à l'intérieur d'un type : `link` porte donc le
type de chaque extrémité :

- `(type_1, id_1, type_2, id_2)`, types en `Enum8` (`application`,
  `capture`, `fqdn`, `ip`, `plugin`, `organization_name`,
  `organization_id`, `phone`, `social_id`) ; un nouveau type s'ajoute à la
  fin des deux `Enum8` (métadonnées seules) ;
- une seule table pour tous les couples de types, triée
  `(type_1, id_1, type_2, id_2, id_source)`, `PARTITION BY type_1` : une
  ligne par lien orienté **et par source** (un lien vu par deux sources
  garde ses deux lignes ; une nouvelle détection d'une même source remplace
  l'ancienne). Pour des voisins distincts, `DISTINCT` / `GROUP BY` à la
  lecture ;
- **pas de projection inverse** : chaque lien est inséré physiquement dans
  les deux sens (A→B et B→A) par l'import (`sql/06_import_domains.sql`,
  `distribute_links()` dans `scripts/import_data.py`) et par `make generate`.
  Un simple filtre `type_1 = ... AND id_1 = ...` retrouve donc les voisins
  des deux côtés, sans `UNION`. Coût disque équivalent à une projection
  inverse (qui dupliquerait les mêmes colonnes) ; en contrepartie,
  toute écriture sur `link` (update, suppression) doit traiter les deux
  lignes ensemble pour rester cohérente — rien ne les garde plus
  synchronisées automatiquement.

### Table `property` (informations par nœud et par source)

Une ligne par `(node_type, id_node, id_source)` : `payload` (JSON
stocké en `String` compressé ZSTD, renvoyé tel quel), `detection_date`,
`version`. `ReplacingMergeTree(version)` : une nouvelle détection d'une même
source remplace l'ancienne (pas d'historique). `node_type` utilise le même
`Enum8` que `link` : `(node_type, id_node)` identifie un nœud.
Projection légère `p_source` pour « tout ce qu'a produit la source X ».

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
