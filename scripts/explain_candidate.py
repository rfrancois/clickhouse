#!/usr/bin/env python3
"""Pourquoi find_misclassified.py propose un couple (FQDN, plugin), et
verdicts OK / KO sur un échantillon de candidats.

    python scripts/explain_candidate.py FQDN PLUGIN [--anonymize]
    python scripts/explain_candidate.py --sample 5 -o verdicts.csv
    python scripts/explain_candidate.py --verdicts verdicts.csv [--anonymize] -o rapport.md

Explication (lecture seule, depuis les tables de données, sans les tables
mc_* de find_misclassified.py) : plugins liés au FQDN, ancêtre classifié,
ligne de classification_candidate / classification_tenant, et chaque pivot
(ip, application, capture, fqdn) relié au FQDN avec son attribution
(direct ou vote, votes, pureté, degré) recalculée avec les mêmes paramètres
(importés de find_misclassified.py). « Concurrence » : tous les plugins que
les pivots proposeraient pour ce FQDN, avec leur score ; un FQDN proposé
pour des dizaines de plugins est un service partagé (SSO, CDN...).

--sample N : N candidats tirés au hasard par combinaison (statut, signaux)
de classification_candidate → CSV (séparateur « ; ») avec une colonne
verdict à remplir (OK / KO) et un commentaire.
--verdicts CSV : explication de chaque couple jugé (tous si aucun ne l'est)
et bilan OK / KO par signal → rapport Markdown à transmettre.
--anonymize : valeurs des pivots (IP, applications, captures, FQDN voisins)
remplacées par un pseudonyme stable (même valeur → même pseudonyme) ; le
FQDN candidat, ses ancêtres et les noms de plugins restent en clair.
"""
import argparse
import csv
import hashlib
import sys
from collections import Counter, defaultdict

import clickhouse_connect

from find_misclassified import (GUARD_PURITY, HOST, MAX_DEGREE, MIN_PURITY,
                                MIN_SCORE, MIN_VOTES, PASSWORD, PIVOT_IN, PIVOTS,
                                PORT, SETTINGS, USER, WEIGHTS)

DETAIL_MAX = 30  # pivots détaillés par couple (les plus forts d'abord)


class Explainer:
    def __init__(self, client, anonymize: bool):
        self.client = client
        self.anonymize = anonymize
        self.plugin_name = dict(self.q("SELECT id_plugin, any(value) FROM plugin "
                                       "GROUP BY id_plugin"))
        self.plugin_id = defaultdict(set)
        for i, name in self.plugin_name.items():
            self.plugin_id[name].add(i)
        self.tables = {r[0] for r in self.q(
            "SELECT name FROM system.tables WHERE database = currentDatabase()")}

    def q(self, sql, **params):
        return self.client.query(sql, parameters=params, settings=SETTINGS).result_rows

    def names(self, ids) -> str:
        return ", ".join(sorted(self.plugin_name.get(i, str(i)) for i in ids)) or "aucun"

    def label(self, kind: str, value: str) -> str:
        if not self.anonymize:
            return value
        return f"{kind}_{hashlib.sha1(value.encode()).hexdigest()[:8]}"

    def fqdn_ids(self, values) -> dict:
        """valeur → ids (plusieurs ids possibles avant fusion)"""
        out = defaultdict(set)
        for v, i in self.q("SELECT value, id_fqdn FROM fqdn "
                           "WHERE reverse(value) IN {r:Array(String)}",
                           r=[v[::-1] for v in values]):
            out[v].add(i)
        return out

    def fqdn_plugins(self, ids) -> set:
        return {r[0] for r in self.q(
            "SELECT DISTINCT id_2 FROM link WHERE type_1 = 'fqdn' "
            "AND id_1 IN {ids:Array(Int64)} AND type_2 = 'plugin'", ids=list(ids))}

    def values(self, kind: str, ids) -> dict:
        if not ids:
            return {}
        return dict(self.q(f"SELECT id_{kind}, any(value) FROM {kind} "
                           f"WHERE id_{kind} IN {{ids:Array(Int64)}} GROUP BY id_{kind}",
                           ids=list(ids)))

    def pivots(self, ids) -> list:
        """pivots du FQDN, degré, votes et plugins directs → attribution"""
        deg = {(str(t), i): d for t, i, d in self.q(
            "SELECT type_1, id_1, uniqExact(id_2) FROM link "
            "WHERE (type_1, id_1) IN (SELECT type_2, id_2 FROM link"
            "  WHERE type_1 = 'fqdn' AND id_1 IN {ids:Array(Int64)}"
            f"  AND type_2 IN ({PIVOT_IN})) "
            "AND type_2 = 'fqdn' GROUP BY type_1, id_1", ids=list(ids))}
        small = defaultdict(list)
        for (t, i), d in deg.items():
            if d <= MAX_DEGREE:
                small[t].append(i)
        votes, direct = defaultdict(Counter), defaultdict(set)
        if small:
            cond = " OR ".join(f"(type_1 = '{t}' AND id_1 IN {{{t}:Array(Int64)}})"
                               for t in small)
            for t, i, p, n in self.q(
                    "SELECT n.type_1, n.id_1, c.id_2, uniqExact(n.id_2) "
                    f"FROM (SELECT type_1, id_1, id_2 FROM link WHERE ({cond}) "
                    "      AND type_2 = 'fqdn') AS n "
                    "INNER JOIN (SELECT id_1, id_2 FROM link WHERE type_1 = 'fqdn'"
                    f"  AND id_1 IN (SELECT id_2 FROM link WHERE ({cond}) AND type_2 = 'fqdn')"
                    "  AND type_2 = 'plugin') AS c ON c.id_1 = n.id_2 "
                    "GROUP BY n.type_1, n.id_1, c.id_2", **small):
                votes[(str(t), i)][p] = n
            for t, i, p in self.q(f"SELECT DISTINCT type_1, id_1, id_2 FROM link "
                                  f"WHERE ({cond}) AND type_2 = 'plugin'", **small):
                direct[(str(t), i)].add(p)
        out = []
        for key, d in deg.items():
            v = votes.get(key, Counter())
            total = sum(v.values())
            for p in set(v) | direct.get(key, set()):
                n = v.get(p, 0)
                purity = n / total if total else 1.0
                is_direct = p in direct.get(key, set())
                kept = d <= MAX_DEGREE and (
                    (is_direct and (total < MIN_VOTES or purity >= GUARD_PURITY))
                    or (n >= MIN_VOTES and purity >= MIN_PURITY))
                out.append(dict(type=key[0], id=key[1], degree=d, plugin=p, n=n,
                                total=total, purity=purity, direct=is_direct,
                                kept=kept, top=v.most_common(3)))
        hubs = Counter(t for (t, _), d in deg.items() if d > MAX_DEGREE)
        return out, Counter(t for t, _ in deg), hubs

    def explain(self, fqdn: str, plugin: str, verdict="", comment="") -> tuple:
        lines = [f"## {fqdn} → {plugin}" + (f"  [{verdict}]" if verdict else "")
                 + (f" — {comment}" if comment else ""), ""]
        ids = self.fqdn_ids([fqdn]).get(fqdn)
        target = self.plugin_id.get(plugin)
        if not ids or not target:
            lines.append(f"introuvable : {'FQDN' if not ids else 'plugin'}\n")
            return lines, set()
        own = self.fqdn_plugins(ids)
        lines.append(f"- id_fqdn : {', '.join(map(str, sorted(ids)))} ; "
                     f"plugins liés : {self.names(own)}")

        # ancêtre classifié le plus proche (héritage implicite)
        labels = fqdn.split(".")
        parents = [".".join(labels[i:]) for i in range(1, len(labels) - 1)]
        anchor, inherited = "", set()
        if parents:
            pids = self.fqdn_ids(parents)
            for p in parents:
                if pids.get(p) and (pl := self.fqdn_plugins(pids[p])):
                    anchor, inherited = p, pl
                    break
        lines.append(f"- ancêtre classifié : {anchor or 'aucun'}"
                     + (f" ({self.names(inherited)})" if anchor else ""))
        if anchor and "classification_platform_detected" in self.tables:
            plat = self.q("SELECT nb_tenants, nb_brand, nb_brand_plugins, nb_classified, "
                          "nb_classified_other, nb_plugins_other "
                          "FROM classification_platform_detected WHERE platform = {a:String}",
                          a=anchor)
            lines.append("- plateforme détectée : " + (
                "oui (clients {}, marques {}, plugins des marques {}, classifiés {}, "
                "dont ailleurs {}, plugins ailleurs {})".format(*plat[0]) if plat else "non"))

        tenant_plugins = set()
        if "classification_tenant" in self.tables:
            for t, gen, prop, dom in self.q(
                    "SELECT tenant, nom_generique, plugins_proposes, domaine_marque "
                    "FROM classification_tenant WHERE fqdn = {f:String}", f=fqdn):
                tenant_plugins |= {i for name in prop for i in self.plugin_id.get(name, ())}
                lines.append(f"- client de plateforme : {t}"
                             + (" (nom générique)" if gen else "")
                             + (f", marque {dom} → {', '.join(prop)}" if prop else ""))
        if "classification_candidate" in self.tables:
            for st, sc, sig, nb in self.q(
                    "SELECT status, score, signals, nb_pivots FROM classification_candidate "
                    "WHERE fqdn = {f:String} AND plugin = {p:String}", f=fqdn, p=plugin):
                lines.append(f"- classification_candidate : {st}, score {sc}, "
                             f"signaux {', '.join(sig)}, {nb} pivots")

        piv, by_type, hubs = self.pivots(ids)
        lines.append("- pivots du FQDN : " + (", ".join(
            f"{t} {n}" + (f" (dont {hubs[t]} > {MAX_DEGREE} FQDN, écartés)" if hubs[t] else "")
            for t, n in sorted(by_type.items())) or "aucun"))

        # concurrence : plugins proposés par les pivots retenus, comme
        # find_misclassified (sans plugins liés ni hérités)
        sig = defaultdict(set)
        for p in piv:
            if p["kept"] and p["plugin"] not in own and p["plugin"] not in inherited:
                sig[p["plugin"]].add(p["type"])
        for p in tenant_plugins - own:
            sig[p].add("tenant")
        score = {p: sum(WEIGHTS[s] for s in s_) for p, s_ in sig.items()}
        ranked = sorted(score, key=lambda p: (-score[p], self.plugin_name.get(p, "")))
        kept = [p for p in ranked if score[p] >= MIN_SCORE]
        lines.append(f"- concurrence : proposé pour {len(kept)} plugin(s) (score >= {MIN_SCORE})"
                     + (" : " + ", ".join(f"{self.plugin_name.get(p, p)} {score[p]} "
                                          f"({'+'.join(sorted(sig[p]))})" for p in kept[:15])
                        + (" ..." if len(kept) > 15 else "") if kept else ""))
        signals = set().union(*(sig.get(i, set()) for i in target))

        mine = sorted((p for p in piv if p["plugin"] in target),
                      key=lambda p: (not p["kept"], not p["direct"], -p["n"], p["degree"]))
        lines += ["", f"Pivots attribuables à {plugin} : {sum(p['kept'] for p in mine)} retenus "
                  f"sur {len(mine)}", ""]
        if mine:
            vals = {t: self.values(t, {p["id"] for p in mine[:DETAIL_MAX] if p["type"] == t})
                    for t in PIVOTS}
            lines += ["| type | valeur | degré | direct | votes | classifiés | pureté "
                      "| retenu | plugins des voisins |",
                      "|---|---|---:|:-:|---:|---:|---:|:-:|---|"]
            for p in mine[:DETAIL_MAX]:
                v = vals[p["type"]].get(p["id"], f"id {p['id']}")
                top = ", ".join(f"{self.plugin_name.get(i, i)} {n}" for i, n in p["top"])
                lines.append(f"| {p['type']} | {self.label(p['type'], v)} | {p['degree']} "
                             f"| {'oui' if p['direct'] else ''} | {p['n']} | {p['total']} "
                             f"| {p['purity']:.0%} | {'oui' if p['kept'] else 'non'} | {top} |")
            if len(mine) > DETAIL_MAX:
                lines.append(f"| ... | {len(mine) - DETAIL_MAX} autres | | | | | | | |")
        lines.append("")
        return lines, signals


def sample(client, per_group: int, out) -> None:
    rows = client.query(
        "SELECT fqdn, plugin, status, score, arrayStringConcat(arraySort(signals), '+') AS s "
        "FROM classification_candidate ORDER BY status, s, rand() "
        "LIMIT {k:UInt32} BY status, s", parameters={"k": per_group},
        settings=SETTINGS).result_rows
    w = csv.writer(out, delimiter=";")
    w.writerow(["fqdn", "plugin", "status", "score", "signals", "verdict", "commentaire"])
    w.writerows([*r, "", ""] for r in rows)
    print(f"{len(rows)} candidats", file=sys.stderr)


def report(ex: Explainer, pairs, out) -> None:
    judged = [p for p in pairs if p[2]] or pairs
    by_signal, by_combo = defaultdict(Counter), defaultdict(Counter)
    body = []
    for fqdn, plugin, verdict, comment in judged:
        lines, signals = ex.explain(fqdn, plugin, verdict, comment)
        body += lines
        if verdict in ("OK", "KO"):
            for s in signals or ["aucun"]:
                by_signal[s][verdict] += 1
            by_combo["+".join(sorted(signals)) or "aucun"][verdict] += 1
    print("# Explication des candidats\n", file=out)
    print(f"Paramètres : MIN_SCORE {MIN_SCORE}, poids {WEIGHTS}, MIN_VOTES {MIN_VOTES}, "
          f"MIN_PURITY {MIN_PURITY}, GUARD_PURITY {GUARD_PURITY}, MAX_DEGREE {MAX_DEGREE}"
          f"{', valeurs anonymisées' if ex.anonymize else ''}\n", file=out)
    for title, tally in (("signal", by_signal), ("signaux (combinaison)", by_combo)):
        if tally:
            print(f"| {title} | OK | KO | précision |\n|---|---:|---:|---:|", file=out)
            for s, c in sorted(tally.items()):
                print(f"| {s} | {c['OK']} | {c['KO']} | {c['OK'] / (c['OK'] + c['KO']):.0%} |",
                      file=out)
            print(file=out)
    print("\n".join(body), file=out)


def read_verdicts(path: str) -> list:
    with open(path, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f, delimiter=";"))
    return [(r["fqdn"].strip(), r["plugin"].strip(),
             (r.get("verdict") or "").strip().upper(), (r.get("commentaire") or "").strip())
            for r in rows if r.get("fqdn") and r.get("plugin")]


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="explication des candidats de "
                                             "find_misclassified.py, verdicts OK / KO")
    ap.add_argument("fqdn", nargs="?")
    ap.add_argument("plugin", nargs="?")
    ap.add_argument("--sample", type=int, metavar="N",
                    help="N candidats par (statut, signaux) → CSV à juger")
    ap.add_argument("--verdicts", metavar="CSV", help="CSV jugé (fqdn;plugin;...;verdict;commentaire)")
    ap.add_argument("--anonymize", action="store_true", help="pseudonymes pour les valeurs des pivots")
    ap.add_argument("-o", "--output", help="fichier de sortie (défaut : écran)")
    ap.add_argument("--database", default="default")
    args = ap.parse_args()
    if not (args.sample or args.verdicts or (args.fqdn and args.plugin)):
        ap.error("FQDN PLUGIN, --sample N ou --verdicts CSV")
    client = clickhouse_connect.get_client(host=HOST, port=PORT, username=USER,
                                           password=PASSWORD, database=args.database)
    out = (open(args.output, "w", encoding="utf-8-sig" if args.sample else "utf-8", newline="")
           if args.output else sys.stdout)
    with out:
        if args.sample:
            sample(client, args.sample, out)
        else:
            pairs = (read_verdicts(args.verdicts) if args.verdicts
                     else [(args.fqdn, args.plugin, "", "")])
            report(Explainer(client, args.anonymize), pairs, out)
