"""Page Streamlit « Nébuleuse » : graphe de tout ce qui est relié à un FQDN
(ou à un nœud de n'importe quel type), lu dans ClickHouse.

Intégration : copier ce fichier dans le dossier pages/ de l'appli
(fichier autonome, aucun autre module du dépôt). Seul, pour tester :

    streamlit run pages/nebuleuse.py        (make graph)

Dépendances : pip install streamlit pyvis clickhouse-connect pandas

Connexion ClickHouse, première source trouvée :
  1. .streamlit/secrets.toml de l'appli :
         [clickhouse]
         host = "localhost"
         port = 8123
         username = "chuser"
         password = "..."
         database = "default"
  2. variables d'environnement CLICKHOUSE_HOST, CLICKHOUSE_PORT,
     CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE ;
  3. valeurs du banc d'essai (DEFAULTS ci-dessous).

Ouvrir la page sur un nœud depuis une autre page :
  - URL : ?value=google.com (et &type=ip pour un autre type) ;
  - code : st.session_state["neb_root"] = ("fqdn", "google.com")
           st.switch_page("pages/nebuleuse.py")

Parcours en largeur dans link à partir du nœud saisi, PROFONDEUR sauts.
link contient chaque lien dans les deux sens : un filtre sur sa clé
primaire (type_1, id_1) donne tous les voisins d'un nœud.

Garde-fous (barre latérale) :
  - au plus VOISINS voisins par nœud et par type de voisin, les plus
    importants selon le nombre de liens du voisin lui-même (PRIORITIES) :
    « reliés » (défaut) garde ceux qui ont d'autres liens sans être des
    hubs, ce qui structure la nébuleuse ; les culs-de-sac (google.com →
    des milliers de sNNN.google.com sans autre lien) et les IP de CDN
    passent après. Candidats : au plus POOL_BUDGET par niveau, les plus vus
    (sources), à égalité tirés au hasard. Le nombre total de voisins
    (degré) est conservé et affiché ;
  - un nœud de plus de HUB voisins (IP de CDN, hébergeur mutualisé...)
    est affiché mais pas développé : seuls ses liens vers des nœuds déjà
    présents sont gardés (le nœud de départ est toujours développé) ;
  - au plus NŒUDS nœuds au total ;
  - au plus LIENS liens en plus de ceux par lesquels chaque nœud a été
    atteint (les plus vus d'abord) : sur un nœud dense, les liens entre
    nœuds déjà présents se comptent en milliers.
Les nœuds du dernier niveau (feuilles) ne sont pas développés, mais une
dernière requête lit leur degré et, en option, les liens entre nœuds déjà
présents (ce qui fait apparaître les amas de la nébuleuse).
Affichage : la physique est coupée une fois la disposition stabilisée (le
graphe ne tremble plus et ne charge plus le navigateur).

Valeurs : tables de chaque type, par la projection p_id (recherche par id).
Lecture seule, link lu uniquement par sa clé primaire.
"""
import math
import os
from collections import defaultdict

import clickhouse_connect
import pandas as pd
import streamlit as st
from pyvis.network import Network

DEFAULTS = {"host": "localhost", "port": 8123, "username": "chuser",
            "password": "Royal15Raccoon", "database": "default"}

# mêmes types (et même ordre) que l'Enum8 de link / property
NODE_TYPES = ("fqdn", "ip", "application", "capture", "plugin",
              "organization_name", "organization_id", "phone", "social_id")
RANKED = ("fqdn", "ip")
# présélection : au plus POOL_BUDGET candidats par niveau du parcours,
# répartis entre (nœud, type de voisin), au moins VOISINS chacun ; on lit le
# nombre de liens de chaque candidat avant de garder les VOISINS plus
# importants. Le nœud de départ, seul à son niveau, a le plus large
# échantillon (google.com : des milliers de candidats sur ses voisins)
POOL_BUDGET = 10_000
# importance d'un voisin v (sources : sources du lien, n : liens de v)
PRIORITIES = {
    # relié ailleurs sans être un hub : ce qui structure la nébuleuse ;
    # culs-de-sac (n = 1) et hubs (n > HUB, CDN) en dernier
    "reliés": "if(n > {hub:UInt32}, 0, sources * log2(1 + n))",
    # lien quasi exclusif : voisins qui n'ont presque que ce lien
    "exclusifs": "sources / log2(1 + greatest(n, 1))",
    # liens vus par le plus de sources, sans regarder le voisin
    "sources": "sources",
}
COLORS = {"fqdn": "#4f9dff", "ip": "#ff8c42", "application": "#3ecf8e",
          "capture": "#c77dff", "plugin": "#ff4d6d", "organization_name": "#ffd166",
          "organization_id": "#e9c46a", "phone": "#06d6a0", "social_id": "#90e0ef"}


# ------------------------------------------------------------ graphe
def normalize(node_type: str, value: str) -> str:
    """Comme à l'import : espaces, minuscules, point final retirés."""
    value = value.strip()
    if node_type in RANKED:
        value = value.lower().rstrip(".")
    return value


def _node_filter(nodes, tcol: str = "type_1", icol: str = "id_1") -> str:
    """(type, id) → filtre sur la clé primaire de link, un IN par type.
    Types et ids viennent de NODE_TYPES et de la base : pas d'injection."""
    by_type = defaultdict(list)
    for t, i in nodes:
        by_type[t].append(i)
    return " OR ".join(f"({tcol} = '{t}' AND {icol} IN ({','.join(map(str, sorted(ids)))}))"
                       for t, ids in sorted(by_type.items()))


def _type_list(types) -> str:
    return ", ".join(f"'{t}'" for t in types if t in NODE_TYPES)


def find_root(query, node_type: str, value: str):
    """Id du nœud (le plus ancien, comme l'import), None s'il est absent.
    fqdn est triée par reverse(value) : filtre sur la clé primaire."""
    cond = ("reverse(value) = reverse({v:String})" if node_type == "fqdn"
            else "value = {v:String}")
    rows = query(f"SELECT min(id_{node_type}), count() FROM {node_type} WHERE {cond}",
                 {"v": value})
    return int(rows[0][0]) if rows and rows[0][1] else None


def _neighbors(query, frontier, types, per_type: int, hub: int, priority: str):
    """Voisins de la frontière, au plus per_type par (nœud, type de voisin),
    avec le degré complet par type de voisin.

    Présélection de pool candidats par (nœud, type de voisin) : les plus
    vus (sources), à égalité tirés au hasard (hash de l'id, reproductible :
    pas les premiers ids). Leur nombre de liens (n, lu par la clé primaire
    de link) départage ensuite selon PRIORITIES[priority] ; score = 0 :
    gardé seulement s'il reste de la place."""
    score = PRIORITIES[priority]
    pool = max(int(per_type), POOL_BUDGET // (len(frontier) * max(len(types), 1)))
    return query(
        "WITH cand AS ("
        " SELECT type_1, id_1, type_2, id_2,"
        "        uniqExact(id_source) AS sources, min(detection_date) AS first_seen,"
        "        count() OVER (PARTITION BY type_1, id_1, type_2) AS degree"
        f" FROM link WHERE ({_node_filter(frontier)}) AND type_2 IN ({_type_list(types)})"
        " GROUP BY type_1, id_1, type_2, id_2"
        " ORDER BY type_1, id_1, type_2, sources DESC, cityHash64(id_2)"
        f" LIMIT {pool} BY type_1, id_1, type_2"
        "), deg AS ("
        " SELECT type_1 AS t, id_1 AS i, uniq(type_2, id_2) AS n FROM link"
        " WHERE (type_1, id_1) IN (SELECT type_2, id_2 FROM cand)"
        " GROUP BY type_1, id_1"
        ") "
        "SELECT t1, id_1, t2, id_2, sources, first_seen, degree, n FROM ("
        " SELECT toString(c.type_1) AS t1, c.id_1 AS id_1, toString(c.type_2) AS t2,"
        "        c.id_2 AS id_2, c.sources AS sources, c.first_seen AS first_seen,"
        "        c.degree AS degree, d.n AS n"
        " FROM cand AS c LEFT JOIN deg AS d ON d.t = c.type_2 AND d.i = c.id_2"
        f") ORDER BY t1, id_1, t2, {score} DESC, sources DESC, first_seen, id_2 "
        f"LIMIT {int(per_type)} BY t1, id_1, t2", {"hub": int(hub)})


def _leaves(query, leaves, inside, types):
    """Degré des feuilles par type de voisin, et leurs liens vers les nœuds
    déjà présents (inside)."""
    inside_set = ", ".join(f"('{t}', {i})" for t, i in sorted(inside))
    return query(
        "SELECT t1, id_1, t2, count() AS degree,"
        "       groupArrayIf((id_2, sources, first_seen), present) AS links FROM ("
        " SELECT toString(type_1) AS t1, id_1, toString(type_2) AS t2, id_2,"
        "        uniqExact(id_source) AS sources, min(detection_date) AS first_seen,"
        f"       (t2, id_2) IN ({inside_set}) AS present"
        f" FROM link WHERE ({_node_filter(leaves)}) AND type_2 IN ({_type_list(types)})"
        " GROUP BY type_1, id_1, type_2, id_2"
        ") GROUP BY t1, id_1, t2")


def _values(query, nodes):
    """(type, id) → (valeur, rank), une requête par type (projection p_id)."""
    by_type = defaultdict(list)
    for t, i in nodes:
        by_type[t].append(i)
    out = {}
    for t, ids in by_type.items():
        rank = "anyLast(rank)" if t in RANKED else "NULL"
        for i, value, r in query(
                f"SELECT id_{t}, any(value), {rank} FROM {t} "
                f"WHERE id_{t} IN ({','.join(map(str, sorted(ids)))}) GROUP BY id_{t}"):
            out[(t, int(i))] = (value, r)
    return out


def build_graph(query, node_type: str, value: str, depth: int = 2, per_type: int = 20,
                hub: int = 500, max_nodes: int = 800, types=NODE_TYPES,
                close_leaves: bool = True, max_edges: int = 2000, priority: str = "reliés"):
    """Graphe autour du nœud (node_type, value).

    query(sql, parameters=None) → liste de lignes.
    Renvoie (nodes, edges, info) :
      nodes  {(type, id): {value, rank, level, degree {type: n}, shown, hub}}
      edges  {((type, id), (type, id)): {sources, first_seen, tree}}, clé
             triée ; tree : lien par lequel un nœud a été atteint, toujours
             gardé ; les autres (entre nœuds déjà présents) sont limités à
             max_edges au total, les plus vus d'abord
      info   {root, truncated, hubs, dropped_edges}
    """
    root_id = find_root(query, node_type, normalize(node_type, value))
    if root_id is None:
        return {}, {}, {"root": None, "truncated": False, "hubs": 0, "dropped_edges": 0}
    root = (node_type, root_id)
    nodes = {root: {"level": 0, "degree": {}, "shown": 0, "hub": False}}
    edges = {}
    truncated = False

    def add_edge(a, b, sources, first_seen, tree=False):
        key = tuple(sorted((a, b)))
        old = edges.get(key)
        if old is None or (sources or 0) > (old["sources"] or 0):
            edges[key] = {"sources": sources, "first_seen": first_seen,
                          "tree": tree or bool(old and old["tree"])}
        elif tree:
            old["tree"] = True

    frontier = [root]
    for level in range(1, depth + 1):
        if not frontier:
            break
        rows = _neighbors(query, frontier, types, per_type, hub, priority)
        by_node = defaultdict(list)
        for t1, i1, t2, i2, sources, first_seen, degree, _ in rows:
            by_node[(t1, int(i1))].append(((t2, int(i2)), sources, first_seen))
            nodes[(t1, int(i1))]["degree"][t2] = int(degree)
        nxt = []
        for a in frontier:
            info = nodes[a]
            info["hub"] = a != root and sum(info["degree"].values()) > hub
            for b, sources, first_seen in by_node.get(a, ()):
                if b not in nodes:
                    if info["hub"]:
                        continue
                    if len(nodes) >= max_nodes:
                        truncated = True
                        continue
                    nodes[b] = {"level": level, "degree": {}, "shown": 0, "hub": False}
                    nxt.append(b)
                    add_edge(a, b, sources, first_seen, tree=True)
                else:
                    add_edge(a, b, sources, first_seen)
        frontier = nxt

    # feuilles : degré, et liens entre nœuds déjà présents
    if frontier:
        for t1, i1, t2, degree, links in _leaves(query, frontier, nodes, types):
            a = (t1, int(i1))
            nodes[a]["degree"][t2] = int(degree)
            if close_leaves:
                for i2, sources, first_seen in links:
                    add_edge(a, (t2, int(i2)), sources, first_seen)
        for a in frontier:
            nodes[a]["hub"] = sum(nodes[a]["degree"].values()) > hub

    # budget de liens : arbre du parcours + les autres liens les plus vus
    extra = sorted((k for k, e in edges.items() if not e["tree"]),
                   key=lambda k: -(edges[k]["sources"] or 0))
    budget = max(0, max_edges - (len(edges) - len(extra)))
    for k in extra[budget:]:
        del edges[k]
    dropped = max(0, len(extra) - budget)

    for a, b in edges:
        nodes[a]["shown"] += 1
        nodes[b]["shown"] += 1
    for key, (val, rank) in _values(query, nodes).items():
        nodes[key]["value"], nodes[key]["rank"] = val, rank
    for key, info in nodes.items():
        info.setdefault("value", f"<{key[0]} {key[1]} sans valeur>")
        info.setdefault("rank", None)
    return nodes, edges, {"root": root, "truncated": truncated, "dropped_edges": dropped,
                          "hubs": sum(n["hub"] for n in nodes.values())}


def to_html(nodes, edges, root, labels: str = "niveau ≤ 1", dark: bool = True,
            height: int = 800, freeze: bool = True) -> str:
    """Graphe vis.js (pyvis), disposition par forces : une nébuleuse.
    freeze : physique coupée une fois la disposition stabilisée (le graphe
    ne tremble plus, le navigateur ne recalcule plus rien)."""
    bg = "#0e1117" if dark else "#ffffff"
    net = Network(height=f"{height}px", width="100%", bgcolor=bg,
                  font_color="#e6e6e6" if dark else "#222222", cdn_resources="in_line")
    for key, n in nodes.items():
        t, i = key
        total = sum(n["degree"].values())
        tip = [f"{t} : {n['value']}", f"id {i}, niveau {n['level']}"]
        if n["rank"] is not None:
            tip.append(f"rank {n['rank']}")
        tip.append(f"{total:,} voisins, {n['shown']} affichés")
        tip += [f"  {k} : {v:,}" for k, v in sorted(n["degree"].items(), key=lambda kv: -kv[1])]
        if n["hub"]:
            tip.append("hub : non développé")
        show = labels == "toutes" or (labels == "niveau ≤ 1" and n["level"] <= 1)
        net.add_node(
            f"{t}:{i}", label=n["value"] if show or key == root else " ",
            title="\n".join(tip), color=COLORS.get(t, "#999999"),
            size=40 if key == root else 8 + 4 * math.log10(1 + total),
            shape="star" if key == root else ("diamond" if n["hub"] else "dot"),
            borderWidth=3 if key == root else 1)
    for (a, b), e in edges.items():
        title = f"{e['sources'] or '?'} source(s)"
        net.add_edge(f"{a[0]}:{a[1]}", f"{b[0]}:{b[1]}", title=title,
                     width=1 + math.log2(e["sources"] or 1),
                     color={"color": "#8a8f98" if dark else "#b0b0b0", "opacity": 0.4})
    net.set_options("""{
      "physics": {"solver": "forceAtlas2Based",
                  "forceAtlas2Based": {"gravitationalConstant": -60, "centralGravity": 0.008,
                                       "springLength": 90, "springConstant": 0.08,
                                       "avoidOverlap": 0.3},
                  "stabilization": {"iterations": 400, "updateInterval": 50}},
      "layout": {"improvedLayout": false},
      "nodes": {"font": {"size": 12}},
      "edges": {"smooth": false},
      "interaction": {"hover": true, "tooltipDelay": 100, "navigationButtons": true,
                      "hideEdgesOnDrag": true, "hideEdgesOnZoom": true}
    }""")
    html = net.generate_html()
    if freeze:
        new = "network = new vis.Network(container, data, options);"
        assert new in html, "modèle pyvis inattendu"
        html = html.replace(new, new + ' network.once("stabilizationIterationsDone", '
                            'function () { network.setOptions({physics: false}); network.fit(); });', 1)
    # sans marge ni bordure dans l'iframe Streamlit
    return html.replace(
        "</head>", f"<style>body{{margin:0;background:{bg}}} "
        "#mynetwork,.card{border:none !important}</style></head>", 1)


# ------------------------------------------------------------ connexion
def _settings() -> dict:
    """secrets [clickhouse] > variables CLICKHOUSE_* > DEFAULTS."""
    try:
        secrets = dict(st.secrets.get("clickhouse", {}))
    except Exception:  # pas de secrets.toml
        secrets = {}
    env = {"host": "CLICKHOUSE_HOST", "port": "CLICKHOUSE_PORT",
           "username": "CLICKHOUSE_USER", "password": "CLICKHOUSE_PASSWORD",
           "database": "CLICKHOUSE_DATABASE"}
    cfg = {k: secrets.get(k, os.environ.get(v, DEFAULTS[k])) for k, v in env.items()}
    cfg["port"] = int(cfg["port"])
    return cfg


@st.cache_resource(show_spinner=False)
def _client(host, port, username, password, database):
    # client partagé entre sessions : pas d'id de session ClickHouse, sinon
    # deux requêtes simultanées sont refusées
    return clickhouse_connect.get_client(host=host, port=port, username=username,
                                         password=password, database=database,
                                         autogenerate_session_id=False)


@st.cache_data(ttl=600, show_spinner=False)
def _cached_graph(cfg_key, node_type, value, depth, per_type, hub, max_nodes, types,
                  close_leaves, max_edges, priority):
    c = _client(*cfg_key)
    return build_graph(lambda sql, params=None: c.query(sql, parameters=params).result_rows,
                       node_type, value, depth, per_type, hub, max_nodes, types, close_leaves,
                       max_edges, priority)


# ------------------------------------------------------------ page
# nœud courant : ?type=...&value=... dans l'URL (partageable), sinon
# st.session_state["neb_root"] (gardé quand on change de page)
def _set_root(node_type: str, value: str):
    st.session_state["neb_root"] = (node_type, value)
    st.query_params.update({"type": node_type, "value": value})


if "value" in st.query_params:
    qt = st.query_params.get("type", "fqdn")
    qt = qt if qt in NODE_TYPES else "fqdn"
    st.session_state["neb_root"] = (qt, normalize(qt, st.query_params["value"]))
cur_type, cur_value = st.session_state.get("neb_root", ("fqdn", ""))

st.title("Nébuleuse")
with st.form("neb_search", border=False):
    c1, c2, c3 = st.columns([2, 5, 1], vertical_alignment="bottom")
    form_type = c1.selectbox("Type", NODE_TYPES, index=NODE_TYPES.index(cur_type))
    form_value = c2.text_input("Valeur", cur_value, placeholder="ex. google.com")
    if c3.form_submit_button("Afficher", width="stretch"):
        _set_root(form_type, normalize(form_type, form_value))
        st.rerun()

with st.sidebar:
    st.subheader("Nébuleuse")
    depth = st.slider("Profondeur (sauts)", 1, 4, 2, key="neb_depth")
    per_type = st.slider("Voisins max par nœud et par type", 5, 200, 20, step=5,
                         key="neb_per_type")
    priority = st.radio(
        "Voisins gardés en priorité", tuple(PRIORITIES), key="neb_priority",
        help="reliés : voisins qui ont eux-mêmes des liens, sans être des hubs "
             "(structure) ; exclusifs : voisins qui n'ont presque que ce lien ; "
             "sources : liens vus par le plus de sources")
    hub = st.number_input("Hub : non développé au-delà de (voisins)", 10, 1_000_000, 500,
                          step=100, key="neb_hub")
    max_nodes = st.number_input("Nœuds max", 50, 10_000, 800, step=100, key="neb_max_nodes")
    max_edges = st.number_input("Liens max", 100, 50_000, 2000, step=500, key="neb_max_edges",
                                help="hors liens par lesquels chaque nœud a été atteint")
    types = st.multiselect("Types de voisins", NODE_TYPES, default=list(NODE_TYPES),
                           key="neb_types")
    close_leaves = st.checkbox("Liens entre feuilles", True, key="neb_close",
                               help="liens entre nœuds du dernier niveau (amas)")
    labels = st.radio("Étiquettes", ("niveau ≤ 1", "toutes", "aucune"), horizontal=True,
                      key="neb_labels")
    dark = st.checkbox("Fond sombre", True, key="neb_dark")
    freeze = st.checkbox("Figer après stabilisation", True, key="neb_freeze",
                         help="coupe l'animation une fois la disposition calculée")

value = normalize(cur_type, cur_value)
if not value:
    st.info("Saisir un FQDN (ou un autre nœud) ci-dessus.")
    st.stop()

cfg = _settings()
try:
    with st.spinner("Parcours du graphe..."):
        nodes, edges, info = _cached_graph(
            tuple(cfg[k] for k in ("host", "port", "username", "password", "database")),
            cur_type, value, depth, per_type, int(hub), int(max_nodes), tuple(types),
            close_leaves, int(max_edges), priority)
except Exception as exc:
    st.error(f"ClickHouse {cfg['host']}:{cfg['port']} ({cfg['database']}) : {exc}")
    st.stop()
if info["root"] is None:
    st.warning(f"{cur_type} « {value} » introuvable.")
    st.stop()

by_type = defaultdict(int)
for t, _ in nodes:
    by_type[t] += 1
cols = st.columns(3 + len(by_type))
cols[0].metric("Nœuds", f"{len(nodes):,}")
cols[1].metric("Liens", f"{len(edges):,}")
cols[2].metric("Hubs", info["hubs"])
for col, (t, n) in zip(cols[3:], sorted(by_type.items(), key=lambda kv: -kv[1])):
    col.metric(t, f"{n:,}")
if info["truncated"]:
    st.caption(f"Limite de {int(max_nodes):,} nœuds atteinte : graphe tronqué.")
if info["dropped_edges"]:
    st.caption(f"{info['dropped_edges']:,} liens secondaires masqués "
               f"(limite de {int(max_edges):,} liens).")

graph_html = to_html(nodes, edges, info["root"], labels, dark, freeze=freeze)
if hasattr(st, "iframe"):
    st.iframe(graph_html, height=810)
else:  # Streamlit plus ancien
    import streamlit.components.v1 as components
    components.html(graph_html, height=810)

table = pd.DataFrame([
    {"type": t, "id": i, "valeur": n["value"], "niveau": n["level"], "rank": n["rank"],
     "voisins": sum(n["degree"].values()), "affichés": n["shown"], "hub": n["hub"]}
    for (t, i), n in nodes.items()]).sort_values(["niveau", "type", "valeur"])

c1, c2 = st.columns([4, 1], vertical_alignment="bottom")
target = c1.selectbox("Recentrer sur", [f"{r.type} : {r.valeur}" for r in table.itertuples()
                                        if not r.valeur.startswith("<")], key="neb_recenter")
if c2.button("Recentrer", width="stretch") and target:
    _set_root(*target.split(" : ", 1))
    st.rerun()

with st.expander("Nœuds"):
    st.dataframe(table, hide_index=True, width="stretch")
    st.download_button("CSV des nœuds", table.to_csv(index=False), f"{value}_noeuds.csv")
with st.expander("Liens"):
    links = pd.DataFrame([
        {"type_1": a[0], "valeur_1": nodes[a]["value"], "type_2": b[0],
         "valeur_2": nodes[b]["value"], "sources": e["sources"],
         "premiere_detection": pd.to_datetime(e["first_seen"], unit="s")}
        for (a, b), e in edges.items()])
    st.dataframe(links, hide_index=True, width="stretch")
    st.download_button("CSV des liens", links.to_csv(index=False), f"{value}_liens.csv")
