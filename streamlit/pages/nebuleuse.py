"""Page Streamlit « Nébuleuse » : graphe de tout ce qui est relié à un FQDN
(ou à un nœud de n'importe quel type), lu dans ClickHouse.

Intégration : copier ce fichier dans le dossier pages/ de l'appli
(fichier autonome, aucun autre module du dépôt). Seul, pour tester :

    make graph   (ClickHouse sur localhost, hors Docker)

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
Valeur avec % (%google%) : motif LIKE, chaque nœud trouvé est une étoile,
au plus « Étoiles max » (rank croissant pour fqdn et ip) ; au moins
MIN_PATTERN caractères hors %, au plus MATCH_CAP lignes trouvées lues.

Lecture du graphe : voisins directs des étoiles cerclés et reliés à leur
étoile en couleur d'accent, le reste estompé. Double-clic sur un nœud :
marqué vu (grisé, bord pointillé), mémorisé dans le navigateur ; le nœud
de départ d'une recherche exacte l'est d'office.

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
import json
import math
import os
from collections import defaultdict

import clickhouse_connect
import pandas as pd
import streamlit as st
from pyvis.network import Network

DEFAULTS = {"host": "clickhouse", "port": 8123, "username": "chuser",
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
# taille des nœuds : nombre de liens en base (degré, tous voisins) ou
# nombre de liens affichés dans le graphe (place dans la nébuleuse)
SIZE_BY = ("liens en base", "liens affichés")
# étiquettes « importants » : les plus gros nœuds
TOP_LABELS = 25
# recherche par motif (%google%) : plusieurs étoiles, au plus « Étoiles
# max » (barre latérale) ; au moins MIN_PATTERN caractères hors % (index
# ngram de 3) ; au plus MATCH_CAP lignes trouvées lues pour les classer
MIN_PATTERN = 3
MATCH_CAP = 100_000
MAX_ROOTS = 50
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


def is_pattern(value: str) -> bool:
    return "%" in value


def find_roots(query, node_type: str, pattern: str, limit: int):
    """Motif LIKE (seul % est un joker, _ est littéral) → (ids, total) :
    au plus limit nœuds, rank croissant (fqdn, ip) puis valeur. Seules les
    MATCH_CAP premières lignes trouvées sont lues : total = MATCH_CAP veut
    dire « au moins », et le classement ne porte alors que sur elles."""
    like = pattern.replace("\\", "\\\\").replace("_", "\\_")
    rank = "anyLast(rank)" if node_type in RANKED else "NULL"
    rows = query(
        f"SELECT min(id_{node_type}), {rank} AS r, value, count() OVER () FROM ("
        f" SELECT * FROM {node_type} WHERE value LIKE {{p:String}} LIMIT {MATCH_CAP}"
        f") GROUP BY value ORDER BY r ASC NULLS LAST, value LIMIT {int(limit)} "
        f"SETTINGS max_execution_time = 30", {"p": like})
    return [int(r[0]) for r in rows], int(rows[0][3]) if rows else 0


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
                close_leaves: bool = True, max_edges: int = 2000, priority: str = "reliés",
                max_roots: int = 10):
    """Graphe autour du nœud (node_type, value), ou des nœuds dont la
    valeur correspond au motif value (%google%) : au plus max_roots étoiles,
    parcourues ensemble (niveau = distance à l'étoile la plus proche).

    query(sql, parameters=None) → liste de lignes.
    Renvoie (nodes, edges, info) :
      nodes  {(type, id): {value, rank, level, degree {type: n}, shown, hub}}
      edges  {((type, id), (type, id)): {sources, first_seen, tree}}, clé
             triée ; tree : lien par lequel un nœud a été atteint, toujours
             gardé ; les autres (entre nœuds déjà présents) sont limités à
             max_edges au total, les plus vus d'abord
      info   {roots, matches, truncated, hubs, dropped_edges}
    """
    value = normalize(node_type, value)
    if is_pattern(value):
        ids, matches = find_roots(query, node_type, value, max_roots)
    else:
        root_id = find_root(query, node_type, value)
        ids, matches = ([] if root_id is None else [root_id]), int(root_id is not None)
    if not ids:
        return {}, {}, {"roots": [], "matches": 0, "truncated": False, "hubs": 0,
                        "dropped_edges": 0}
    roots = [(node_type, i) for i in ids]
    nodes = {r: {"level": 0, "degree": {}, "shown": 0, "hub": False} for r in roots}
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

    frontier = list(roots)
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
            info["hub"] = info["level"] > 0 and sum(info["degree"].values()) > hub
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
    return nodes, edges, {"roots": roots, "matches": matches, "truncated": truncated,
                          "dropped_edges": dropped,
                          "hubs": sum(n["hub"] for n in nodes.values())}


# nœuds vus : un double-clic marque (ou démarque) un nœud, gardé dans le
# localStorage du navigateur (même origine que l'appli) : la marque survit
# au recentrage et aux changements de réglages. Les étoiles ne sont jamais
# grisées. __X__ : remplacés par to_html
SEEN_JS = """
(function () {
  var KEY = "neb_seen", stars = new Set(__STARS__), G = __GREY__, mem = [];
  function load() {
    try { return new Set(JSON.parse(localStorage.getItem(KEY) || "[]")); }
    catch (e) { return new Set(mem); }
  }
  function save(s) {
    mem = Array.from(s);
    try { localStorage.setItem(KEY, JSON.stringify(mem)); } catch (e) {}
  }
  var seen = load(), orig = {};
  nodes.forEach(function (n) { orig[n.id] = {color: n.color, font: n.font, title: n.title}; });
  __AUTO__.forEach(function (id) { seen.add(id); });
  save(seen);
  function look(id) {
    var o = orig[id];
    if (!seen.has(id) || stars.has(id))
      return {id: id, color: o.color, font: o.font, title: o.title,
              shapeProperties: {borderDashes: false}};
    var c = {background: G.fill, border: G.border};
    return {id: id, font: {color: G.font}, title: o.title + "\\n✓ vu (double-clic : annuler)",
            color: {background: c.background, border: c.border, highlight: c, hover: c},
            shapeProperties: {borderDashes: [3, 3]}};
  }
  var bar = document.createElement("div");
  bar.style.cssText = "position:absolute;top:8px;left:8px;z-index:10;font:12px sans-serif;" +
    "color:" + G.text + ";background:" + G.panel + ";padding:4px 8px;border-radius:4px";
  document.body.appendChild(bar);
  function counter() {
    var n = 0;
    nodes.forEach(function (x) { if (seen.has(x.id)) n++; });
    bar.innerHTML = "double-clic : marquer vu · " + n + " vu(s) ici" +
      (seen.size ? ' · <a href="#" style="color:inherit">tout effacer (' + seen.size + ")</a>" : "");
    var a = bar.querySelector("a");
    if (a) a.onclick = function (ev) {
      ev.preventDefault(); seen = new Set(); save(seen); repaint();
    };
  }
  function repaint() { nodes.update(Object.keys(orig).map(look)); counter(); }
  network.on("doubleClick", function (p) {
    if (!p.nodes.length) return;
    var id = p.nodes[0];
    if (seen.has(id)) seen.delete(id); else seen.add(id);
    save(seen); nodes.update(look(id)); counter();
  });
  repaint();
})();
"""


def _rgba(hex_color: str, alpha: float) -> str:
    r, g, b = (int(hex_color[k:k + 2], 16) for k in (1, 3, 5))
    return f"rgba({r},{g},{b},{alpha})"


def to_html(nodes, edges, roots, labels: str = "importants", dark: bool = True,
            height: int = 800, freeze: bool = True, size_by: str = SIZE_BY[0],
            focus: bool = True, mark_seen=()) -> str:
    """Graphe vis.js (pyvis), disposition par forces : une nébuleuse.

    Taille d'un nœud selon size_by, en log puis RELATIVE au graphe : du
    moins relié (petit) au plus relié (gros), quels que soient les nombres
    absolus ; l'étiquette suit. labels : « importants » (TOP_LABELS plus
    gros nœuds), « niveau ≤ 1 », « toutes », « aucune » ; les étoiles
    (roots) ont toujours la leur.
    focus : voisins directs des étoiles cerclés (couleur d'accent), liens
    étoile–voisin direct en accent, le reste estompé.
    mark_seen : nœuds marqués vus d'office (SEEN_JS), en plus des
    double-clics.
    freeze : physique coupée une fois la disposition stabilisée (le graphe
    ne tremble plus, le navigateur ne recalcule plus rien)."""
    bg = "#0e1117" if dark else "#ffffff"
    accent = "#ffffff" if dark else "#111111"
    font = "#e6e6e6" if dark else "#222222"
    roots = set(roots)
    net = Network(height=f"{height}px", width="100%", bgcolor=bg,
                  font_color=font, cdn_resources="in_line")
    weight = {key: (n["shown"] if size_by == SIZE_BY[1] else sum(n["degree"].values()))
              for key, n in nodes.items()}
    top = set(sorted(weight, key=lambda k: -weight[k])[:TOP_LABELS])
    for key, n in nodes.items():
        t, i = key
        total = sum(n["degree"].values())
        tip = [f"{t} : {n['value']}", f"id {i}, niveau {n['level']}"
               + (" (étoile)" if key in roots else
                  " (voisin direct)" if n["level"] == 1 else "")]
        if n["rank"] is not None:
            tip.append(f"rank {n['rank']}")
        tip.append(f"{total:,} voisins, {n['shown']} affichés")
        tip += [f"  {k} : {v:,}" for k, v in sorted(n["degree"].items(), key=lambda kv: -kv[1])]
        if n["hub"]:
            tip.append("hub : non développé")
        show = (labels == "toutes" or (labels == "importants" and key in top)
                or (labels == "niveau ≤ 1" and n["level"] <= 1))
        base = COLORS.get(t, "#999999")
        near = focus and n["level"] <= 1
        fill = _rgba(base, 0.35) if focus and n["level"] >= 2 else base
        border = accent if near else fill
        lit = {"background": base, "border": accent}
        net.add_node(
            f"{t}:{i}", label=n["value"] if show or key in roots else " ",
            title="\n".join(tip), value=math.log1p(weight[key]),
            color={"background": fill, "border": border, "highlight": lit, "hover": lit},
            shape="star" if key in roots else ("diamond" if n["hub"] else "dot"),
            borderWidth=4 if key in roots else (3 if near else 1), borderWidthSelected=4)
    for (a, b), e in edges.items():
        title = f"{e['sources'] or '?'} source(s)"
        width = 1 + math.log2(e["sources"] or 1)
        if focus and (a in roots or b in roots) and max(nodes[a]["level"], nodes[b]["level"]) <= 1:
            color, width = {"color": accent, "opacity": 0.85}, width + 1
        else:
            color = {"color": "#8a8f98" if dark else "#b0b0b0", "opacity": 0.25 if focus else 0.4}
        net.add_edge(f"{a[0]}:{a[1]}", f"{b[0]}:{b[1]}", title=title, width=width, color=color)
    net.set_options("""{
      "physics": {"solver": "forceAtlas2Based",
                  "forceAtlas2Based": {"gravitationalConstant": -60, "centralGravity": 0.008,
                                       "springLength": 90, "springConstant": 0.08,
                                       "avoidOverlap": 0.3},
                  "stabilization": {"iterations": 400, "updateInterval": 50}},
      "layout": {"improvedLayout": false},
      "nodes": {"font": {"size": 12},
                "scaling": {"min": 5, "max": 55,
                            "label": {"enabled": true, "min": 10, "max": 32,
                                      "drawThreshold": 5, "maxVisible": 40}}},
      "edges": {"smooth": false},
      "interaction": {"hover": true, "tooltipDelay": 100, "navigationButtons": true,
                      "hideEdgesOnDrag": true, "hideEdgesOnZoom": true}
    }""")
    html = net.generate_html()
    new = "network = new vis.Network(container, data, options);"
    assert new in html, "modèle pyvis inattendu"
    grey = ({"fill": "#3a3d44", "border": "#6b7079", "font": "#6b7079",
             "text": "#9aa0a6", "panel": "rgba(14,17,23,0.8)"} if dark else
            {"fill": "#d6d6d6", "border": "#9a9a9a", "font": "#9a9a9a",
             "text": "#555555", "panel": "rgba(255,255,255,0.85)"})
    seen_js = (SEEN_JS.replace("__STARS__", json.dumps([f"{t}:{i}" for t, i in roots]))
               .replace("__GREY__", json.dumps(grey))
               .replace("__AUTO__", json.dumps([f"{t}:{i}" for t, i in mark_seen])))
    if freeze:
        seen_js = (' network.once("stabilizationIterationsDone", function () {'
                   ' network.setOptions({physics: false}); network.fit(); });' + seen_js)
    html = html.replace(new, new + seen_js, 1)
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
                  close_leaves, max_edges, priority, max_roots):
    c = _client(*cfg_key)
    return build_graph(lambda sql, params=None: c.query(sql, parameters=params).result_rows,
                       node_type, value, depth, per_type, hub, max_nodes, types, close_leaves,
                       max_edges, priority, max_roots)


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
    form_value = c2.text_input(
        "Valeur", cur_value, placeholder="ex. google.com, ou %google% (plusieurs étoiles)",
        help="% : n'importe quelle suite de caractères ; plusieurs nœuds trouvés → "
             "plusieurs étoiles (« Étoiles max » dans la barre latérale)")
    if c3.form_submit_button("Afficher", width="stretch"):
        _set_root(form_type, normalize(form_type, form_value))
        st.rerun()

with st.sidebar:
    st.subheader("Nébuleuse")
    depth = st.slider("Profondeur (sauts)", 1, 4, 2, key="neb_depth")
    max_roots = st.slider("Étoiles max (recherche %…%)", 1, MAX_ROOTS, 10, key="neb_max_roots",
                          help="nœuds trouvés par le motif, rank croissant (fqdn, ip) "
                               "puis valeur ; chacun est une étoile")
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
    size_by = st.radio("Taille des nœuds", SIZE_BY, key="neb_size_by",
                       help="liens en base : nombre total de voisins du nœud ; "
                            "liens affichés : ses liens dans ce graphe")
    labels = st.radio("Étiquettes", ("importants", "niveau ≤ 1", "toutes", "aucune"),
                      horizontal=True, key="neb_labels",
                      help=f"importants : les {TOP_LABELS} plus gros nœuds")
    focus = st.checkbox("Voisins directs en évidence", True, key="neb_focus",
                        help="voisins directs des étoiles cerclés, leurs liens vers "
                             "l'étoile en couleur d'accent, le reste estompé")
    dark = st.checkbox("Fond sombre", True, key="neb_dark")
    freeze = st.checkbox("Figer après stabilisation", True, key="neb_freeze",
                         help="coupe l'animation une fois la disposition calculée")

value = normalize(cur_type, cur_value)
if not value:
    st.info("Saisir un FQDN (ou un autre nœud, ou un motif %…%) ci-dessus.")
    st.stop()
pattern = is_pattern(value)
if pattern and len(value.replace("%", "")) < MIN_PATTERN:
    st.warning(f"Motif trop large : au moins {MIN_PATTERN} caractères en plus des %.")
    st.stop()

cfg = _settings()
try:
    with st.spinner("Parcours du graphe..."):
        nodes, edges, info = _cached_graph(
            tuple(cfg[k] for k in ("host", "port", "username", "password", "database")),
            cur_type, value, depth, per_type, int(hub), int(max_nodes), tuple(types),
            close_leaves, int(max_edges), priority, int(max_roots))
except Exception as exc:
    st.error(f"ClickHouse {cfg['host']}:{cfg['port']} ({cfg['database']}) : {exc}")
    st.stop()
if not info["roots"]:
    st.warning(f"{cur_type} « {value} » introuvable.")
    st.stop()
if pattern:
    shown = len(info["roots"])
    if info["matches"] >= MATCH_CAP:
        st.warning(f"Plus de {MATCH_CAP:,} {cur_type} correspondent à « {value} » : "
                   f"{shown} étoiles prises parmi les {MATCH_CAP:,} premiers trouvés "
                   "(pas forcément les meilleurs rank). Préciser le motif.")
    elif info["matches"] > shown:
        st.caption(f"{info['matches']:,} {cur_type} correspondent à « {value} » : "
                   f"les {shown} premiers (rank croissant) sont les étoiles "
                   "(« Étoiles max » dans la barre latérale).")
    else:
        st.caption(f"{shown} {cur_type} correspondent à « {value} » : autant d'étoiles.")

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

graph_html = to_html(nodes, edges, info["roots"], labels, dark, freeze=freeze,
                     size_by=size_by, focus=focus,
                     mark_seen=() if pattern else info["roots"])
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

fname = value.replace("%", "_")
with st.expander("Nœuds"):
    st.dataframe(table, hide_index=True, width="stretch")
    st.download_button("CSV des nœuds", table.to_csv(index=False), f"{fname}_noeuds.csv")
with st.expander("Liens"):
    links = pd.DataFrame([
        {"type_1": a[0], "valeur_1": nodes[a]["value"], "type_2": b[0],
         "valeur_2": nodes[b]["value"], "sources": e["sources"],
         "premiere_detection": pd.to_datetime(e["first_seen"], unit="s")}
        for (a, b), e in edges.items()])
    st.dataframe(links, hide_index=True, width="stretch")
    st.download_button("CSV des liens", links.to_csv(index=False), f"{fname}_liens.csv")
