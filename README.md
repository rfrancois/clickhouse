# Banc d'essai ClickHouse — schéma optimisé (application de SOLUTION.md)

Stack : ClickHouse 26.7 en Docker + scripts Python (venv local).

- **Données factices** (optionnelles, pour tester) : 500 000 FQDN,
  500 000 IP, 1 000 000 liens = 2 000 000 lignes dans `link` (chaque lien est
  inséré dans les deux sens, 2 % des FQDN contiennent des "hot terms" :
  youtube, shop, bank, mail…).
- **Schéma de production** : une table de valeurs par type de nœud
  (`fqdn`, `ip`, `application`, `plugin`, ...) / `link`
  (index de saut `ngrambf_v1` sur `value`, ids typés `Int64`, liens
  dupliqués dans les deux sens ; `AggregatingMergeTree` partout : tables
  de valeurs (une ligne par valeur), `link` et `property`).

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
make ranks FILE=ranks.csv SOURCE=<source_uuid> [WEEK=2026-10-05] [TYPE=fqdn|ip]
                     # ranks d'une semaine → table rank (voir plus bas)
make misclassified [HOLDOUT=20]   # FQDN mal classifiés, clients des plateformes
make graph        # nébuleuse d'un FQDN dans Streamlit (http://localhost:8501)
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
| `*node*.csv` | `id;value;type;creation_date;rank` (';', quoté) | table du `type` (`fqdn`, `ip`, `application`, ...) |
| `*link*.csv` | `id_node_1;id_node_2;type_1;type_2;id_source;creation_date;update_date` | `link` |
| `*propert*.csv` | `id_node;type;id_source;payload;version;detection_date` (';', quoté, `\"` dans le payload) | `property` |
| `*.json` / `*.json.gz` | `{"cn":…, "dns":[…]\|null, "ip":…\|null}` (JSONEachRow) | `fqdn` (cn + dns), `ip` (ip, et cn / dns qui sont des IP) |

Pipeline : extraction du zip → staging brut (`sql/04_import_staging.sql`,
streaming `clickhouse-client`, pas de parsing Python) → distribution
(`sql/05_import_distribute.sql`) vers les tables optimisées.

`make import` est autonome : si les tables optimisées n'existent pas encore
(`make init` jamais lancé), le schéma `sql/02_optimized.sql` est créé
automatiquement avant le chargement.

Choix d'import :
- aucun id n'est calculé à partir d'une valeur : `node.csv` garde ses
  propres ids ; toute autre valeur fqdn/ip (`domains.json`, extrémités de
  liens) est d'abord cherchée dans la table de son type et reprend
  l'id existant (le plus ancien, `min(id)`, si elle en a plusieurs) ; si
  elle est absente, elle reçoit un **nouvel id
  auto-incrémenté** à partir du `max(id)` du type déjà en base
  (`max + 1`, `max + 2`, ...), avec `version = now()` (et `rank = NULL`
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
  (`sql/05_import_distribute.sql`, étape 0) :
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
- `rank` absent ou à 0 → `NULL` (inconnu : en fin de `ORDER BY rank`, et
  n'efface pas un rank déjà connu, `anyLast` ignorant les `NULL`) ;
- `properties.csv` référence les nœuds par leur **id** (pas par valeur) :
  insertion directe dans `property`, sans résolution ni vérification que le
  nœud existe. Ses guillemets internes sont échappés par backslash (`\"`,
  export MySQL), ce que le format CSV de ClickHouse ne lit pas : le fichier
  est chargé en `CustomSeparatedWithNames` avec la règle d'échappement
  `JSON` (fins de ligne `\r\n` acceptées). `version` → timestamp Unix ;
  `detection_date` vide ou à la date zéro MySQL (`0000-00-00 00:00:00`)
  → date de `version` ;
- la déduplication est assurée par le moteur de chaque table
  (asynchrone), `AggregatingMergeTree` partout, fusion colonne par
  colonne : tables de valeurs (une ligne par valeur, id le plus ancien,
  voir plus bas), `link` et `property` (première date de détection
  conservée ; pour `property`, payload du dernier insert) ;
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

## Historique des ranks (table `rank`)

Un rank par nœud classé (`fqdn` ou `ip`), par source et par semaine, sur
**2 ans glissants** :

```bash
make ranks FILE=ranks.csv SOURCE=<source_uuid> WEEK=2026-10-05 [TYPE=ip]
```

- fichier : CSV à deux colonnes, rank et valeur dans n'importe quel ordre
  (`1,google.com` ou `google.com;1`), en-tête facultatif, `.gz` accepté ;
- `WEEK` : date du relevé, ramenée au **samedi** de sa semaine anglaise
  (dimanche → samedi ; défaut : aujourd'hui, UTC), stockée dans
  `creation_date` en timestamp Unix (`Int32`) du samedi 00:00 UTC.
  Renvoyer une semaine **remplace** ses ranks (`ReplacingMergeTree`, une
  ligne par `(node_type, id_node, id_source, creation_date)`) ;
- `TYPE` : `fqdn` (défaut) ou `ip`. `(node_type, id_node)` identifie le
  nœud, comme dans `link` et `property` ;
- valeurs normalisées et validées : FQDN comme ceux de `domains.json` (IP,
  wildcards, noms invalides rejetés et comptés), IP en forme canonique ;
  une valeur en double garde son meilleur rank ;
- tout est fait côté serveur (`scripts/send_ranks.py`) : chargement dans
  une table de staging, recherche des valeurs absentes de la table du type,
  création de ces nœuds (nouvel id `max(id) + 1`, ..., rank `NULL` par
  défaut), résolution valeur → `min(id)` en une seule jointure, puis
  insertion dans `rank` ;
- **rank de `fqdn` / `ip`** : chaque envoi y écrit aussi son rank
  (`anyLast`) : le rank d'un nœud est celui du **dernier** envoi, toutes
  sources confondues ;
- purge : `TTL toDateTime(creation_date) + INTERVAL 2 YEAR`, partition par mois
  (`ttl_only_drop_parts`) : un mois expiré est supprimé d'un bloc, sans
  réécriture. Un import hebdomadaire n'écrit et ne fait merger que le mois
  en cours.

Lecture (filtrer sur `creation_date` : une partition expirée n'est pas
supprimée instantanément) :

```sql
SELECT r.id_source, toDate(toDateTime(r.creation_date, 'UTC')) AS samedi, r.rank
FROM rank AS r FINAL
WHERE r.node_type = 'fqdn'
  AND r.id_node = (SELECT min(id_fqdn) FROM fqdn WHERE value = 'google.com')
  AND r.creation_date > toUnixTimestamp(now() - INTERVAL 2 YEAR)
ORDER BY r.id_source, r.creation_date;
```

Un seul `make ranks` à la fois.

## FQDN mal classifiés et clients des plateformes

```bash
make misclassified              # calcul complet → tables classification_*
make misclassified HOLDOUT=20   # évaluation : rappel sur 20 % de FQDN cachés
```

Un FQDN est **classifié P** s'il a un lien `fqdn ↔ plugin P`. Le nom du
plugin n'est **jamais** comparé au nom du FQDN (`youtube.com` peut être
`google`) : seuls comptent les FQDN déjà classifiés et le graphe `link`
(`scripts/find_misclassified.py`).

**Héritage implicite** : un sous-domaine d'un FQDN classifié P est
considéré comme P (`www.google.com` sous `google.com`) et n'est jamais
signalé. Un sous-domaine lié à un autre plugin que son ancêtre n'est pas un
conflit : une marque est souvent découpée en plusieurs plugins
(`teams.microsoft.com` classifié `teams` sous `microsoft.com` classifié
`microsoft`).

**Plateformes multi-clients** (`zendesk.com`, `slack.com`…) : leurs
sous-domaines sont des clients (`monapp.zendesk.com`). Le nom du client
(`monapp`, label juste avant la plateforme, hors labels techniques `www`,
`api`, `mail`…) est listé, et s'il est la **marque** d'un domaine classifié
(`monapp.com` → plugin `monapp`), ce plugin est proposé. Marque = premier
label du domaine enregistrable, pour les seuls FQDN classifiés qui sont un
domaine enregistrable ou son `www` ; nom de client cherché entier puis
avant le premier `-` (`monapp-support` → `monapp`).

Plateformes détectées automatiquement (aucune liste à maintenir). Ce
qui distingue `zendesk.com` d'une grande marque découpée en plusieurs
plugins (`microsoft.com` : `teams`, `azure`, `office`… sur des dizaines de
sous-domaines) est la **diversité** : presque un plugin différent par
client. Plateforme si :
- ses sous-domaines classifiés dans d'autres plugins couvrent au moins
  10 plugins distincts, soit au moins 0,5 plugin distinct par sous-domaine
  (`x.amazonaws.com`, `y.amazonaws.com`… classifiés chacun ailleurs) ;
- ou ses noms de clients qui sont une marque classifiée ailleurs
  désignent au moins 10 plugins distincts, soit au moins 0,5 plugin par
  nom, et ces noms font au moins 5 % des noms de clients distincts.

**Noms génériques** : un nom de client présent sous au moins 20 domaines
enregistrables classifiés différents (`data.microsoft.com`,
`data.google.com`… : `data`, `cloud`, `news`…) n'est jamais utilisé comme
marque, ni pour proposer un plugin (`data.gov` → `gov_us`), ni pour
détecter une plateforme. Calculé sur les données, sans liste.

Résultats (remplacés à chaque calcul complet) :

| Table | Contenu |
|---|---|
| `classification_candidate` | couples (FQDN, plugin) proposés : `non_classe` (sans plugin, voir filtre ci-dessous), `conflit` (lié à un autre plugin que celui de ses pivots), `complement` (déjà lié, ex. à `zendesk`, nom de client = marque d'un autre plugin) |
| `classification_tenant` | tous les clients des plateformes : FQDN, plateforme, nom du client, nom générique ou non, plugins proposés, domaine de la marque, plugins déjà liés |
| `classification_platform_detected` | plateformes détectées et leurs compteurs, pour revue (seuils en tête du script) |

Signaux, chacun propose des couples (FQDN, plugin) :

| Signal | Principe | Poids |
|---|---|---|
| `tenant` | nom de client d'une plateforme = marque d'un domaine classifié (hors nom générique) | 2 |
| `fqdn`, `application`, `capture` | pivot relié au FQDN et attribué au plugin | 2 |
| `ip` | idem, signal faible (IP partagées) | 1 |

- **pivot attribué à P** : lié lui-même au plugin P (lien `ip` /
  `application` / `capture ↔ plugin`, ou FQDN classifié P), ou au moins
  3 FQDN classifiés parmi ses voisins dont 90 % dans P ; écarté au-delà de
  1 000 FQDN voisins (CDN, mutualisé), ou s'il est direct mais que ses FQDN
  classifiés (au moins 3) sont à moins de 50 % dans P ; un plugin déjà
  hérité de l'ancêtre n'est pas proposé ;
- **score** = somme des poids des signaux distincts, couple gardé à partir
  de 2 (une IP seule ne suffit pas). Seuils, poids et labels techniques :
  constantes en tête du script.

**Filtre `non_classe`** : un FQDN sans plugin n'est proposé que s'il n'a
**aucune propriété**, ou si sa plus ancienne propriété
(`min(detection_date)` dans `property`, toutes sources) date de **moins
d'un an** (`MAX_PROPERTY_AGE_DAYS`). Un FQDN connu depuis longtemps et
toujours sans plugin n'est pas proposé. Colonne `premiere_detection` :
cette date (`NULL` sans propriété). Seule la date est lue, jamais le
payload ; lecture par la clé primaire de `property`, pour les seuls
candidats. Ne s'applique ni à `conflit` / `complement`, ni à
`classification_tenant`, ni au mode `HOLDOUT` (qui mesure les signaux).

Coût : une passe complète sur `fqdn` (ancêtre, nom de client, marque :
recherches dans des tables `Join` en mémoire, sans jointure ni `GROUP BY`
sur les 600M lignes) ; les sous-domaines de FQDN classifiés sont écrits
sur disque (table de travail). `link` n'est lu que par sa clé primaire
`(type_1, id_1)` ; si les ids sont dispersés, cela revient à lire la
partition `fqdn` puis la table, une fois chacune. Mémoire :
proportionnelle aux FQDN classifiés, marques, plateformes et pivots, pas
aux 600M FQDN. Essai : 20M FQDN / 50M liens, 200k classifiés → 6 s.

`property` ne sert qu'au filtre `non_classe` (date seulement). Tables de
données en lecture seule ; tables de travail `mc_*` supprimées à la fin.

```sql
SELECT fqdn, plugin, status, signals, anchor
FROM classification_candidate WHERE plugin = 'google' ORDER BY status, fqdn;

SELECT platform, tenant, fqdn, plugins_proposes, domaine_marque
FROM classification_tenant WHERE platform = 'zendesk.com' ORDER BY tenant;
```

**Évaluation** (`HOLDOUT=PCT`) : PCT % des FQDN classifiés sont cachés
(tirage déterministe sur l'id), le calcul tourne sans eux, puis le script
affiche combien sont retrouvés avec leur bon plugin : par héritage
implicite, par les candidats, au total et par signal (rappel), et la part
des propositions justes sur ces FQDN (précision). Les tables de résultats
ne sont pas modifiées. Sert à régler seuils et poids sur les données
réelles.

## Nébuleuse d'un FQDN (graphe Streamlit)

```bash
pip install streamlit pyvis clickhouse-connect pandas
make graph        # → http://localhost:8501
```

`pages/nebuleuse.py` est une **page Streamlit autonome** : la copier dans
le dossier `pages/` d'une appli multipage existante suffit (aucun autre
module du dépôt). Elle ne fixe pas `st.set_page_config` (laissé à l'appli)
et préfixe ses clés de session par `neb_`. Connexion ClickHouse, première
source trouvée : section `[clickhouse]` de `.streamlit/secrets.toml`
(`host`, `port`, `username`, `password`, `database`), puis variables
`CLICKHOUSE_HOST` / `_PORT` / `_USER` / `_PASSWORD` / `_DATABASE`, puis
les valeurs du banc d'essai. Ouvrir la page sur un nœud depuis ailleurs :
URL `?value=google.com` (`&type=ip` pour un autre type), ou
`st.session_state["neb_root"] = ("fqdn", "google.com")` puis
`st.switch_page("pages/nebuleuse.py")`.

Saisir un FQDN (ou un nœud d'un autre type) : la page parcourt `link` en largeur sur 1 à 4 sauts et affiche tout ce qui y est
relié, disposé par forces (vis.js via pyvis). Couleur = type du nœud,
taille = nombre de voisins (log), étoile = nœud de départ, losange = hub.
Survol d'un nœud : valeur, rank, degré par type de voisin.

Garde-fous (barre latérale) :
- **voisins max par nœud et par type** (30) : les plus vus (nombre de
  sources, puis première détection) ; le degré complet reste affiché ;
- **hub** (500 voisins) : nœud affiché mais pas développé (IP de CDN,
  hébergeur mutualisé), seuls ses liens vers les nœuds déjà présents
  sont gardés ; le nœud de départ est toujours développé ;
- **nœuds max** (1 500) : au-delà, le graphe est tronqué (signalé).

Les nœuds du dernier niveau ne sont pas développés, mais une dernière
requête lit leur degré et, en option, les liens entre nœuds déjà présents
(amas). « Recentrer sur » relance le parcours depuis un nœud du graphe ;
tables et CSV des nœuds et des liens en bas de page.

Coût : une requête par niveau sur la clé primaire de `link`
`(type_1, id_1)` (chaque lien existe dans les deux sens), puis une par type
pour les valeurs (projection `p_id`). Essai : profondeur 3, 365 nœuds,
0,4 s. Lecture seule ; résultats en cache 10 min.

## Recherche `LIKE '%…%'` triée par rank

```sql
SELECT * FROM fqdn WHERE value LIKE '%google.com%' ORDER BY rank;
```

`fqdn` est triée par **nom de domaine inversé**
(`ORDER BY reverse(value)`) : tous les `*.google.com`,
`google.com.br`… sont stockés côte à côte, donc l'index ngram ne garde que
quelques blocs et `ORDER BY rank` — avec ou sans `LIMIT` — ne trie que les
lignes trouvées. Triée par `id_fqdn`, ces lignes seraient éparpillées
(~1 par bloc) et chaque recherche triée relirait presque toute la table.
La recherche par id (jointures) passe par la projection légère `p_id`.

**Une ligne par valeur** (toutes les tables de valeurs : `fqdn`, `ip` et
les autres types) : la clé est la valeur seule, en `AggregatingMergeTree`.
Une valeur ré-insérée sous un autre id (ex. `node.csv` qui fournit ses
propres ids) fusionne avec la ligne existante, colonne par colonne :
- `id_<type>` : `min` — l'id le plus **ancien** est gardé (ids
  auto-incrémentés) ;
- `rank` (`fqdn` et `ip`, `Nullable`) : `anyLast` — le rank du **dernier
  insert** qui en fournit un (ordre d'insertion, pas `version` ; `NULL` =
  inconnu, ignoré à la fusion) ;
- `version` : `max` — la plus récente date de mise à jour.

Lecture dédupliquée avant fusion : `FINAL`, ou `GROUP BY value` avec
`min(id_<type>)` / `max(version)`. Les liens ou propriétés qui citent un id
écarté deviennent orphelins après la fusion : l'import résout donc toujours
une valeur vers `min(id)`.

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

Même principe (projection `p_id`, rank 0 → `NULL`), avec deux
différences :
- triée par valeur dans l'ordre **normal** (`ORDER BY value`) : pour
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
  garde ses deux lignes). Pour des voisins distincts, `DISTINCT` /
  `GROUP BY` à la lecture ;
- `AggregatingMergeTree`, fusion **colonne par colonne** : une nouvelle
  détection d'une même source fusionne avec l'ancienne en gardant la plus
  ancienne `detection_date` (`min`, date de création, jamais écrasée par un
  ré-envoi) et la plus récente `version` (`max`, date de mise à jour), quel
  que soit l'ordre d'insertion. Lecture dédupliquée : `FINAL`, ou
  `GROUP BY` avec `min(detection_date)` / `max(version)`.
  `detection_date` : timestamp Unix en `Int32`, comme dans `property` ;
- **pas de projection inverse** : chaque lien est inséré physiquement dans
  les deux sens (A→B et B→A) par l'import (`sql/05_import_distribute.sql`,
  `distribute_links()` dans `scripts/import_data.py`) et par `make generate`.
  Un simple filtre `type_1 = ... AND id_1 = ...` retrouve donc les voisins
  des deux côtés, sans `UNION`. Coût disque équivalent à une projection
  inverse (qui dupliquerait les mêmes colonnes) ; en contrepartie,
  toute écriture sur `link` (update, suppression) doit traiter les deux
  lignes ensemble pour rester cohérente — rien ne les garde plus
  synchronisées automatiquement.

### Table `property` (informations par nœud et par source)

Une ligne par `(node_type, id_node, id_source)` : `payload` (JSON
stocké en `String` compressé ZSTD, renvoyé tel quel), `version`,
`detection_date`. `AggregatingMergeTree`, fusion **colonne par colonne**
quand une même source re-détecte le nœud :
- `payload` : `anyLast` — celui du **dernier insert** (ordre d'insertion,
  pas `version` ; pas d'historique) ;
- `version` : `max` — la plus récente date de mise à jour ;
- `detection_date` : `min` — la **première** date de détection, jamais
  écrasée par une nouvelle détection. Timestamp Unix en `Int32`
  (secondes, dates jusqu'au 2038-01-19) ; `toDateTime(detection_date)`
  pour l'afficher.

`node_type` utilise le même `Enum8` que `link` : `(node_type, id_node)`
identifie un nœud. Projection légère `p_source` pour « tout ce qu'a produit
la source X ». Pas d'`UPDATE` léger sur `AggregatingMergeTree` : pour
modifier un payload, ré-insérer la ligne.

```sql
SELECT id_source, payload, version, toDateTime(detection_date) AS detection_date
FROM property FINAL
WHERE node_type = 'fqdn' AND id_node = 123456;
```

## Résultats historiques (dans `results/`)

Les benchmarks naïf vs optimisé qui ont justifié cette architecture sont
conservés dans `results/*.json` et `results/*.png` (300M FQDN : recherche
×17, jointure ×3,9). Ils ont servi à produire `RAPPORT_OPTIMISATION.pdf`
(`make pdf`). Le schéma naïf n'existe plus : seules les tables optimisées
sont utilisées.
