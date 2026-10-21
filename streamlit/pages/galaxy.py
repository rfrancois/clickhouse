"""Streamlit "Galaxy" page: graph of everything linked to an FQDN (or to a
node of any type), read from ClickHouse.

Integration: copy this file into the app's pages/ folder (standalone file,
no other module from the repo), with galaxy_blacklist.txt next to it if
needed. Alone, for testing:

    make graph   (ClickHouse on localhost, outside Docker)

Dependencies: pip install streamlit pyvis clickhouse-connect pandas

ClickHouse connection, first source found:
  1. the app's .streamlit/secrets.toml:
         [clickhouse]
         host = "localhost"
         port = 8123
         username = "chuser"
         password = "..."
         database = "default"
  2. environment variables CLICKHOUSE_HOST, CLICKHOUSE_PORT,
     CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE;
  3. test bench values (DEFAULTS below).

Open the page on a node from another page:
  - URL: ?value=google.com (and &type=ip for another type);
  - code: st.session_state["galaxy_root"] = ("fqdn", "google.com")
          st.switch_page("pages/galaxy.py")

Breadth-first traversal of link from the entered node, DEPTH hops.
link holds each link in both directions: a filter on its primary key
(type_1, id_1) gives all the neighbors of a node.
Value with % (%google%): LIKE pattern, each node found is a star, at most
"Max stars" (ascending rank for fqdn and ip); at least MIN_PATTERN
characters besides %, at most MATCH_CAP matching rows read.

Blacklist: plugins listed in BLACKLIST_FILE (one name per line, # starts a
comment line) never appear in the graph, neither as stars nor as neighbors;
their links are not counted in the degrees. File read on every run.

Reading the graph: direct neighbors of the stars circled and linked to
their star in accent color, the rest faded. Double-click on a node:
marked seen (greyed, dashed border), remembered in the browser; the start
node of an exact search is marked automatically.

Safeguards (sidebar):
  - at most NEIGHBORS neighbors per node and per neighbor type, the most
    important according to the neighbor's own number of links
    (PRIORITIES): "linked" (default) keeps those that have other links
    without being hubs, which structures the galaxy; dead ends (google.com
    → thousands of sNNN.google.com with no other link) and CDN IPs come
    last. Candidates: at most POOL_BUDGET per level, the most seen
    (sources), ties broken at random. The total number of neighbors
    (degree) is kept and shown;
  - a node with more than HUB neighbors (CDN IP, shared hosting...) is
    shown but not expanded: only its links to nodes already present are
    kept (the start node is always expanded);
  - at most NODES nodes in total;
  - at most LINKS links on top of those through which each node was
    reached (most seen first): on a dense node, links between nodes
    already present number in the thousands.
Nodes of the last level (leaves) are not expanded, but one last query
reads their degree and, optionally, the links between nodes already
present (which reveals the galaxy's clusters).
Display: physics is turned off once the layout has stabilized (the graph
stops shaking and no longer loads the browser).

Values: tables of each type, through the p_id projection (search by id).
Read only, link read only by its primary key.
"""
import json
import math
import os
from collections import defaultdict
from pathlib import Path

import clickhouse_connect
import pandas as pd
import streamlit as st
from pyvis.network import Network

DEFAULTS = {"host": "clickhouse", "port": 8123, "username": "chuser",
            "password": "Royal15Raccoon", "database": "default"}

# same types (and same order) as the Enum8 of link / property
NODE_TYPES = ("fqdn", "ip", "application", "capture", "plugin",
              "organization_name", "organization_id", "phone", "social_id")
RANKED = ("fqdn", "ip")
# plugins never shown: one name per line, exact value, lines starting
# with # are comments
BLACKLIST_FILE = Path(__file__).with_name("galaxy_blacklist.txt")
# preselection: at most POOL_BUDGET candidates per traversal level, spread
# over (node, neighbor type), at least NEIGHBORS each; the number of links
# of each candidate is read before keeping the NEIGHBORS most important.
# The start node, alone at its level, gets the widest sample (google.com:
# thousands of candidates among its neighbors)
POOL_BUDGET = 10_000
# importance of a neighbor v (sources: sources of the link, n: links of v)
PRIORITIES = {
    # linked elsewhere without being a hub: what structures the galaxy;
    # dead ends (n = 1) and hubs (n > HUB, CDN) last
    "linked": "if(n > {hub:UInt32}, 0, sources * log2(1 + n))",
    # near-exclusive link: neighbors that have almost only this link
    "exclusive": "sources / log2(1 + greatest(n, 1))",
    # links seen by the most sources, regardless of the neighbor
    "sources": "sources",
}
# node size: number of links in the database (degree, all neighbors) or
# number of links shown in the graph (place in the galaxy)
SIZE_BY = ("links in database", "links shown")
# "important" labels: the biggest nodes
TOP_LABELS = 25
# pattern search (%google%): several stars, at most "Max stars" (sidebar);
# at least MIN_PATTERN characters besides % (ngram index of 3); at most
# MATCH_CAP matching rows read to rank them
MIN_PATTERN = 3
MATCH_CAP = 100_000
MAX_ROOTS = 50
COLORS = {"fqdn": "#4f9dff", "ip": "#ff8c42", "application": "#3ecf8e",
          "capture": "#c77dff", "plugin": "#ff4d6d", "organization_name": "#ffd166",
          "organization_id": "#e9c46a", "phone": "#06d6a0", "social_id": "#90e0ef"}


# ------------------------------------------------------------ graph
def normalize(node_type: str, value: str) -> str:
    """As at import: whitespace, lower case, trailing dot removed."""
    value = value.strip()
    if node_type in RANKED:
        value = value.lower().rstrip(".")
    return value


def load_blacklist(path: Path = BLACKLIST_FILE) -> tuple:
    """Plugin names from the blacklist file, sorted; () if it is missing."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return ()
    names = {normalize("plugin", line) for line in lines}
    return tuple(sorted(n for n in names if n and not n.startswith("#")))


def _node_filter(nodes, tcol: str = "type_1", icol: str = "id_1") -> str:
    """(type, id) → filter on the primary key of link, one IN per type.
    Types and ids come from NODE_TYPES and the database: no injection."""
    by_type = defaultdict(list)
    for t, i in nodes:
        by_type[t].append(i)
    return " OR ".join(f"({tcol} = '{t}' AND {icol} IN ({','.join(map(str, sorted(ids)))}))"
                       for t, ids in sorted(by_type.items()))


def _type_list(types) -> str:
    return ", ".join(f"'{t}'" for t in types if t in NODE_TYPES)


def _blacklist_ids(query, names) -> list:
    """Blacklisted plugin names → all their ids (plugin is sorted by value)."""
    if not names:
        return []
    return sorted({int(r[0]) for r in query(
        "SELECT id_plugin FROM plugin WHERE value IN {names:Array(String)}",
        {"names": list(names)})})


def _exclude(banned, tcol: str = "type_2", icol: str = "id_2") -> str:
    """SQL clause dropping links to blacklisted plugins ("" if none)."""
    if not banned:
        return ""
    return f" AND NOT ({tcol} = 'plugin' AND {icol} IN ({','.join(map(str, banned))}))"


def find_root(query, node_type: str, value: str):
    """Node id (the oldest, as at import), None if absent.
    fqdn is sorted by reverse(value): filter on the primary key."""
    cond = ("reverse(value) = reverse({v:String})" if node_type == "fqdn"
            else "value = {v:String}")
    rows = query(f"SELECT min(id_{node_type}), count() FROM {node_type} WHERE {cond}",
                 {"v": value})
    return int(rows[0][0]) if rows and rows[0][1] else None


def is_pattern(value: str) -> bool:
    return "%" in value


def find_roots(query, node_type: str, pattern: str, limit: int, banned=()):
    """LIKE pattern (only % is a wildcard, _ is literal) → (ids, total,
    linked): at most limit nodes, those that have links first (a node
    created by make ranks often has none: lone star), then ascending rank
    (fqdn, ip), number of links, value. Links counted through the primary
    key of link. Only the first MATCH_CAP matching rows are read: total =
    MATCH_CAP means "at least", and ranking then only covers them.
    banned: blacklisted plugin ids, excluded (plugin type only)."""
    like = pattern.replace("\\", "\\\\").replace("_", "\\_")
    rank = "anyLast(rank)" if node_type in RANKED else "NULL"
    skip = (f" AND id_plugin NOT IN ({','.join(map(str, banned))})"
            if node_type == "plugin" and banned else "")
    rows = query(
        "WITH m AS ("
        f" SELECT min(id_{node_type}) AS id, {rank} AS r, value FROM ("
        f"  SELECT * FROM {node_type} WHERE value LIKE {{p:String}}{skip} LIMIT {MATCH_CAP}"
        " ) GROUP BY value"
        "), l AS ("
        " SELECT id_1 AS id, count() AS n FROM link"
        f" WHERE type_1 = '{node_type}' AND id_1 IN (SELECT id FROM m){_exclude(banned)}"
        " GROUP BY id_1"
        ") "
        "SELECT m.id, count() OVER (), countIf(l.n > 0) OVER ()"
        " FROM m LEFT JOIN l ON l.id = m.id"
        f" ORDER BY (l.n > 0) DESC, m.r ASC NULLS LAST, l.n DESC, m.value LIMIT {int(limit)}"
        " SETTINGS max_execution_time = 30, join_use_nulls = 0", {"p": like})
    if not rows:
        return [], 0, 0
    return [int(r[0]) for r in rows], int(rows[0][1]), int(rows[0][2])


def _neighbors(query, frontier, types, per_type: int, hub: int, priority: str, banned=()):
    """Neighbors of the frontier, at most per_type per (node, neighbor
    type), with the full degree per neighbor type.

    Preselection of pool candidates per (node, neighbor type): the most
    seen (sources), ties broken at random (hash of the id, reproducible:
    not the first ids). Their number of links (n, read through the primary
    key of link) then decides according to PRIORITIES[priority]; score = 0:
    kept only if there is room left. Blacklisted plugins (banned) are
    skipped."""
    score = PRIORITIES[priority]
    pool = max(int(per_type), POOL_BUDGET // (len(frontier) * max(len(types), 1)))
    return query(
        "WITH cand AS ("
        " SELECT type_1, id_1, type_2, id_2,"
        "        uniqExact(id_source) AS sources, min(detection_date) AS first_seen,"
        "        count() OVER (PARTITION BY type_1, id_1, type_2) AS degree"
        f" FROM link WHERE ({_node_filter(frontier)}) AND type_2 IN ({_type_list(types)})"
        f"{_exclude(banned)}"
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


def _leaves(query, leaves, inside, types, banned=()):
    """Degree of the leaves per neighbor type, and their links to the nodes
    already present (inside). Blacklisted plugins (banned) are skipped."""
    inside_set = ", ".join(f"('{t}', {i})" for t, i in sorted(inside))
    return query(
        "SELECT t1, id_1, t2, count() AS degree,"
        "       groupArrayIf((id_2, sources, first_seen), present) AS links FROM ("
        " SELECT toString(type_1) AS t1, id_1, toString(type_2) AS t2, id_2,"
        "        uniqExact(id_source) AS sources, min(detection_date) AS first_seen,"
        f"       (t2, id_2) IN ({inside_set}) AS present"
        f" FROM link WHERE ({_node_filter(leaves)}) AND type_2 IN ({_type_list(types)})"
        f"{_exclude(banned)}"
        " GROUP BY type_1, id_1, type_2, id_2"
        ") GROUP BY t1, id_1, t2")


def _values(query, nodes):
    """(type, id) → (value, rank), one query per type (p_id projection)."""
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
                close_leaves: bool = True, max_edges: int = 2000, priority: str = "linked",
                max_roots: int = 10, blacklist=()):
    """Graph around the node (node_type, value), or around the nodes whose
    value matches the pattern value (%google%): at most max_roots stars,
    traversed together (level = distance to the nearest star).
    blacklist: plugin names that never appear (neither star nor neighbor).

    query(sql, parameters=None) → list of rows.
    Returns (nodes, edges, info):
      nodes  {(type, id): {value, rank, level, degree {type: n}, shown, hub}}
      edges  {((type, id), (type, id)): {sources, first_seen, tree}}, sorted
             key; tree: link through which a node was reached, always
             kept; the others (between nodes already present) are limited
             to max_edges in total, most seen first
      info   {roots, matches, linked (matches that have links),
              truncated, hubs, dropped_edges, blacklisted (root found but
              blacklisted)}
    """
    value = normalize(node_type, value)
    banned = _blacklist_ids(query, blacklist)
    blacklisted = False
    if is_pattern(value):
        ids, matches, linked = find_roots(query, node_type, value, max_roots, banned)
    else:
        root_id = find_root(query, node_type, value)
        blacklisted = node_type == "plugin" and root_id in banned
        ids = [] if root_id is None or blacklisted else [root_id]
        matches = linked = len(ids)
    if not ids:
        return {}, {}, {"roots": [], "matches": 0, "linked": 0, "truncated": False,
                        "hubs": 0, "dropped_edges": 0, "blacklisted": blacklisted}
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
        rows = _neighbors(query, frontier, types, per_type, hub, priority, banned)
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

    # leaves: degree, and links between nodes already present
    if frontier:
        for t1, i1, t2, degree, links in _leaves(query, frontier, nodes, types, banned):
            a = (t1, int(i1))
            nodes[a]["degree"][t2] = int(degree)
            if close_leaves:
                for i2, sources, first_seen in links:
                    add_edge(a, (t2, int(i2)), sources, first_seen)
        for a in frontier:
            nodes[a]["hub"] = sum(nodes[a]["degree"].values()) > hub

    # link budget: traversal tree + the other most seen links
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
        info.setdefault("value", f"<{key[0]} {key[1]} without value>")
        info.setdefault("rank", None)
    return nodes, edges, {"roots": roots, "matches": matches, "linked": linked,
                          "truncated": truncated,
                          "dropped_edges": dropped,
                          "hubs": sum(n["hub"] for n in nodes.values()),
                          "blacklisted": False}


# seen nodes: a double-click marks (or unmarks) a node, kept in the
# browser's localStorage (same origin as the app): the mark survives
# recentering and settings changes. Stars are never greyed. __X__:
# replaced by to_html
SEEN_JS = """
(function () {
  var KEY = "galaxy_seen", stars = new Set(__STARS__), G = __GREY__, mem = [];
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
    return {id: id, font: {color: G.font}, title: o.title + "\\n✓ seen (double-click: undo)",
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
    bar.innerHTML = "double-click: mark seen · " + n + " seen here" +
      (seen.size ? ' · <a href="#" style="color:inherit">clear all (' + seen.size + ")</a>" : "");
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


def to_html(nodes, edges, roots, labels: str = "important", dark: bool = True,
            height: int = 800, freeze: bool = True, size_by: str = SIZE_BY[0],
            focus: bool = True, mark_seen=()) -> str:
    """vis.js graph (pyvis), force-directed layout: a galaxy.

    Node size according to size_by, in log then RELATIVE to the graph:
    from the least linked (small) to the most linked (big), whatever the
    absolute numbers; the label follows. labels: "important" (TOP_LABELS
    biggest nodes), "level ≤ 1", "all", "none"; the stars (roots) always
    have theirs.
    focus: direct neighbors of the stars circled (accent color), star –
    direct neighbor links in accent, the rest faded.
    mark_seen: nodes marked seen automatically (SEEN_JS), on top of
    double-clicks.
    freeze: physics turned off once the layout has stabilized (the graph
    stops shaking, the browser no longer recomputes anything)."""
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
        tip = [f"{t}: {n['value']}", f"id {i}, level {n['level']}"
               + (" (star)" if key in roots else
                  " (direct neighbor)" if n["level"] == 1 else "")]
        if n["rank"] is not None:
            tip.append(f"rank {n['rank']}")
        tip.append(f"{total:,} neighbors, {n['shown']} shown")
        tip += [f"  {k}: {v:,}" for k, v in sorted(n["degree"].items(), key=lambda kv: -kv[1])]
        if n["hub"]:
            tip.append("hub: not expanded")
        show = (labels == "all" or (labels == "important" and key in top)
                or (labels == "level ≤ 1" and n["level"] <= 1))
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
    assert new in html, "unexpected pyvis template"
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
    # no margin or border inside the Streamlit iframe
    return html.replace(
        "</head>", f"<style>body{{margin:0;background:{bg}}} "
        "#mynetwork,.card{border:none !important}</style></head>", 1)


# ------------------------------------------------------------ connection
def _settings() -> dict:
    """secrets [clickhouse] > CLICKHOUSE_* variables > DEFAULTS."""
    try:
        secrets = dict(st.secrets.get("clickhouse", {}))
    except Exception:  # no secrets.toml
        secrets = {}
    env = {"host": "CLICKHOUSE_HOST", "port": "CLICKHOUSE_PORT",
           "username": "CLICKHOUSE_USER", "password": "CLICKHOUSE_PASSWORD",
           "database": "CLICKHOUSE_DATABASE"}
    cfg = {k: secrets.get(k, os.environ.get(v, DEFAULTS[k])) for k, v in env.items()}
    cfg["port"] = int(cfg["port"])
    return cfg


@st.cache_resource(show_spinner=False)
def _client(host, port, username, password, database):
    # client shared between sessions: no ClickHouse session id, otherwise
    # two simultaneous queries are refused
    return clickhouse_connect.get_client(host=host, port=port, username=username,
                                         password=password, database=database,
                                         autogenerate_session_id=False)


@st.cache_data(ttl=600, show_spinner=False)
def _cached_graph(cfg_key, node_type, value, depth, per_type, hub, max_nodes, types,
                  close_leaves, max_edges, priority, max_roots, blacklist):
    c = _client(*cfg_key)
    return build_graph(lambda sql, params=None: c.query(sql, parameters=params).result_rows,
                       node_type, value, depth, per_type, hub, max_nodes, types, close_leaves,
                       max_edges, priority, max_roots, blacklist)


# ------------------------------------------------------------ page
# current node: ?type=...&value=... in the URL (shareable), otherwise
# st.session_state["galaxy_root"] (kept when switching pages)
def _set_root(node_type: str, value: str):
    st.session_state["galaxy_root"] = (node_type, value)
    st.query_params.update({"type": node_type, "value": value})


if "value" in st.query_params:
    qt = st.query_params.get("type", "fqdn")
    qt = qt if qt in NODE_TYPES else "fqdn"
    st.session_state["galaxy_root"] = (qt, normalize(qt, st.query_params["value"]))
cur_type, cur_value = st.session_state.get("galaxy_root", ("fqdn", ""))

st.title("Galaxy")
with st.form("galaxy_search", border=False):
    c1, c2, c3 = st.columns([2, 5, 1], vertical_alignment="bottom")
    form_type = c1.selectbox("Type", NODE_TYPES, index=NODE_TYPES.index(cur_type))
    form_value = c2.text_input(
        "Value", cur_value, placeholder="e.g. google.com, or %google% (several stars)",
        help="%: any sequence of characters; several nodes found → "
             "several stars (\"Max stars\" in the sidebar)")
    if c3.form_submit_button("Show", width="stretch"):
        _set_root(form_type, normalize(form_type, form_value))
        st.rerun()

with st.sidebar:
    st.subheader("Galaxy")
    depth = st.slider("Depth (hops)", 1, 4, 2, key="galaxy_depth")
    max_roots = st.slider("Max stars (%…% search)", 1, MAX_ROOTS, 10, key="galaxy_max_roots",
                          help="nodes found by the pattern, ascending rank (fqdn, ip) "
                               "then value; each one is a star")
    per_type = st.slider("Max neighbors per node and per type", 5, 200, 20, step=5,
                         key="galaxy_per_type")
    priority = st.radio(
        "Neighbors kept first", tuple(PRIORITIES), key="galaxy_priority",
        help="linked: neighbors that have links of their own, without being hubs "
             "(structure); exclusive: neighbors that have almost only this link; "
             "sources: links seen by the most sources")
    hub = st.number_input("Hub: not expanded beyond (neighbors)", 10, 1_000_000, 500,
                          step=100, key="galaxy_hub")
    max_nodes = st.number_input("Max nodes", 50, 10_000, 800, step=100, key="galaxy_max_nodes")
    max_edges = st.number_input("Max links", 100, 50_000, 2000, step=500, key="galaxy_max_edges",
                                help="excluding links through which each node was reached")
    types = st.multiselect("Neighbor types", NODE_TYPES, default=list(NODE_TYPES),
                           key="galaxy_types")
    close_leaves = st.checkbox("Links between leaves", True, key="galaxy_close",
                               help="links between nodes of the last level (clusters)")
    size_by = st.radio("Node size", SIZE_BY, key="galaxy_size_by",
                       help="links in database: total number of neighbors of the node; "
                            "links shown: its links in this graph")
    labels = st.radio("Labels", ("important", "level ≤ 1", "all", "none"),
                      horizontal=True, key="galaxy_labels",
                      help=f"important: the {TOP_LABELS} biggest nodes")
    focus = st.checkbox("Highlight direct neighbors", True, key="galaxy_focus",
                        help="direct neighbors of the stars circled, their links to "
                             "the star in accent color, the rest faded")
    dark = st.checkbox("Dark background", True, key="galaxy_dark")
    freeze = st.checkbox("Freeze after stabilization", True, key="galaxy_freeze",
                         help="stops the animation once the layout is computed")
    blacklist = load_blacklist()
    st.caption(f"{len(blacklist)} blacklisted plugin(s) ({BLACKLIST_FILE.name})")

value = normalize(cur_type, cur_value)
if not value:
    st.info("Enter an FQDN (or another node, or a %…% pattern) above.")
    st.stop()
pattern = is_pattern(value)
if pattern and len(value.replace("%", "")) < MIN_PATTERN:
    st.warning(f"Pattern too broad: at least {MIN_PATTERN} characters besides the %.")
    st.stop()

cfg = _settings()
try:
    with st.spinner("Traversing the graph..."):
        nodes, edges, info = _cached_graph(
            tuple(cfg[k] for k in ("host", "port", "username", "password", "database")),
            cur_type, value, depth, per_type, int(hub), int(max_nodes), tuple(types),
            close_leaves, int(max_edges), priority, int(max_roots), blacklist)
except Exception as exc:
    st.error(f"ClickHouse {cfg['host']}:{cfg['port']} ({cfg['database']}): {exc}")
    st.stop()
if not info["roots"]:
    if info["blacklisted"]:
        st.warning(f"plugin \"{value}\" is blacklisted ({BLACKLIST_FILE.name}).")
    else:
        st.warning(f"{cur_type} \"{value}\" not found.")
    st.stop()
if pattern:
    shown, linked = len(info["roots"]), info["linked"]
    found = (f"{info['matches']:,} {cur_type} match \"{value}\", "
             f"of which {linked:,} linked to other nodes")
    if info["matches"] >= MATCH_CAP:
        st.warning(f"More than {MATCH_CAP:,} {cur_type} match \"{value}\": "
                   f"{shown} stars taken among the first {MATCH_CAP:,} found. "
                   "Narrow the pattern.")
    else:
        st.caption(f"{found}: {shown} stars, linked ones first, then ascending rank "
                   "(\"Max stars\" in the sidebar).")
    if linked < shown:
        st.caption(f"{shown - linked} star(s) without any link in the database: alone.")

by_type = defaultdict(int)
for t, _ in nodes:
    by_type[t] += 1
cols = st.columns(3 + len(by_type))
cols[0].metric("Nodes", f"{len(nodes):,}")
cols[1].metric("Links", f"{len(edges):,}")
cols[2].metric("Hubs", info["hubs"])
for col, (t, n) in zip(cols[3:], sorted(by_type.items(), key=lambda kv: -kv[1])):
    col.metric(t, f"{n:,}")
if info["truncated"]:
    st.caption(f"Limit of {int(max_nodes):,} nodes reached: graph truncated.")
if info["dropped_edges"]:
    st.caption(f"{info['dropped_edges']:,} secondary links hidden "
               f"(limit of {int(max_edges):,} links).")

graph_html = to_html(nodes, edges, info["roots"], labels, dark, freeze=freeze,
                     size_by=size_by, focus=focus,
                     mark_seen=() if pattern else info["roots"])
if hasattr(st, "iframe"):
    st.iframe(graph_html, height=810)
else:  # older Streamlit
    import streamlit.components.v1 as components
    components.html(graph_html, height=810)

table = pd.DataFrame([
    {"type": t, "id": i, "value": n["value"], "level": n["level"], "rank": n["rank"],
     "neighbors": sum(n["degree"].values()), "shown": n["shown"], "hub": n["hub"]}
    for (t, i), n in nodes.items()]).sort_values(["level", "type", "value"])

c1, c2 = st.columns([4, 1], vertical_alignment="bottom")
target = c1.selectbox("Recenter on", [f"{r.type} : {r.value}" for r in table.itertuples()
                                      if not r.value.startswith("<")], key="galaxy_recenter")
if c2.button("Recenter", width="stretch") and target:
    _set_root(*target.split(" : ", 1))
    st.rerun()

fname = value.replace("%", "_")
with st.expander("Nodes"):
    st.dataframe(table, hide_index=True, width="stretch")
    st.download_button("Nodes CSV", table.to_csv(index=False), f"{fname}_nodes.csv")
with st.expander("Links"):
    links = pd.DataFrame([
        {"type_1": a[0], "value_1": nodes[a]["value"], "type_2": b[0],
         "value_2": nodes[b]["value"], "sources": e["sources"],
         "first_seen": pd.to_datetime(e["first_seen"], unit="s")}
        for (a, b), e in edges.items()])
    st.dataframe(links, hide_index=True, width="stretch")
    st.download_button("Links CSV", links.to_csv(index=False), f"{fname}_links.csv")
