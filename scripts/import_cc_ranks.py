#!/usr/bin/env python3
"""Sends a Common Crawl ranking (web graph, host-ranks or domain-ranks
.txt.gz) to the rank table, through send_ranks:

    python scripts/import_cc_ranks.py <cc-main-...-host-ranks.txt.gz> <source_uuid>
                                      [--week YYYY-MM-DD]

Tab-separated file, commented header (#):
    harmonicc_pos  harmonicc_val  pr_pos  pr_val  host_rev  [n_hosts]
  - rank = harmonicc_pos: harmonic centrality, Common Crawl's reference
    ranking (the file is sorted on it), less sensitive to link farms than
    PageRank (pr_pos); the _val columns (raw scores) are ignored
  - value = host_rev turned back around (com.google.www → www.google.com);
    any IPs are rejected by send_ranks like any invalid name

The whole file is sent (hundreds of millions of hosts: each one missing
from fqdn is created there).

send_ranks is called in batches of BATCH hosts, all for the same week and
the same source: each batch adds its ranks to those of the previous ones
(one row per node in rank), without replacing them.

Week: preferably the web graph's publication date (see send_ranks).
"""
import argparse
import gzip
from itertools import islice

from send_ranks import is_int, send_ranks

BATCH = 10_000      # hosts per send_ranks call


def read_common_crawl(path: str):
    """(value, rank) of each host. Comments, header and unreadable lines
    ignored."""
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as f:
        for line in f:
            cols = line.rstrip("\r\n").split("\t")
            if len(cols) < 5 or not is_int(cols[0]):
                continue
            yield ".".join(reversed(cols[4].split("."))), int(cols[0])


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Common Crawl ranking -> rank table")
    ap.add_argument("file", help="Common Crawl host-ranks or domain-ranks (.txt.gz)")
    ap.add_argument("source_uuid")
    ap.add_argument("--week", help="survey date, moved to the Saturday of its week (default: today, UTC)")
    args = ap.parse_args()
    it = read_common_crawl(args.file)
    while batch := list(islice(it, BATCH)):
        print(f"--- ranks {batch[0][1]:,} to {batch[-1][1]:,}")
        send_ranks(batch, args.source_uuid, args.week, "fqdn")
