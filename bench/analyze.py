#!/usr/bin/env python3
"""Aggregate tsdb-bench results into tables + figures for the paper.

Reads results/out/*_full.json (pulled from Atlas), writes:
  results/summary.md           — markdown tables (ingest, disk, queries)
  results/fig_ingest.png       — points/s by DB and phase
  results/fig_disk.png         — bytes/point by DB (raw + compressed)
  results/fig_queries.png      — warm p50 latency heatmap-style grouped bars

Usage: python analyze.py [--indir results/out] [--outdir results]
"""

from __future__ import annotations

import argparse
import glob
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ORDER = ["timescale", "postgres", "influx", "victoria", "clickhouse",
         "questdb", "prometheus"]
LABEL = {"timescale": "TimescaleDB", "postgres": "PostgreSQL",
         "influx": "InfluxDB 2.7", "victoria": "VictoriaMetrics",
         "clickhouse": "ClickHouse", "questdb": "QuestDB",
         "prometheus": "Prometheus*"}
QM = ["Q1_raw_1h", "Q2_downsample_12h", "Q3_groupby_1h",
      "Q4_fanout_max_full", "Q5_global_agg"]
QH = ["H1_series_raw_24h", "H2_max_by_host"]


def load(indir):
    out = {}
    for f in glob.glob(os.path.join(indir, "*_full.json")):
        d = json.load(open(f))
        if "db" in d and "phases" in d:
            out[d["db"]] = d
    return {k: out[k] for k in ORDER if k in out}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--indir", default="results/out")
    ap.add_argument("--outdir", default="results")
    a = ap.parse_args()
    res = load(a.indir)
    os.makedirs(a.outdir, exist_ok=True)
    dbs = list(res)

    lines = ["# tsdb-bench summary\n"]
    lines.append("## Ingest\n")
    lines.append("| DB | M pts/s | M wall (s) | H pts/s | H wall (s) |")
    lines.append("|---|---|---|---|---|")
    for db in dbs:
        p = res[db]["phases"]
        note = " (offline backfill)" if db == "prometheus" else ""
        lines.append(
            f"| {LABEL[db]}{note} | {p['M']['ingest']['points_per_s']:,} | "
            f"{p['M']['ingest']['wall_s']} | "
            f"{p['H']['ingest']['points_per_s']:,} | "
            f"{p['H']['ingest']['wall_s']} |")

    lines.append("\n## Disk (bytes per point)\n")
    lines.append("| DB | M raw | M compressed | H |")
    lines.append("|---|---|---|---|")
    for db in dbs:
        p = res[db]["phases"]
        mpts = p["M"]["ingest"]["points"]
        hpts = p["H"]["ingest"]["points"]
        raw = p["M"]["disk_bytes"] / mpts
        comp = p["M"].get("disk_bytes_after_compress")
        comp = f"{comp / mpts:.2f}" if comp else "—"
        hd = (p["H"]["disk_bytes"] - p["M"].get(
            "disk_bytes_after_compress", p["M"]["disk_bytes"])) / hpts
        lines.append(f"| {LABEL[db]} | {raw:.2f} | {comp} | {max(hd,0):.2f} |")

    for phase, qs in (("M", QM), ("H", QH)):
        lines.append(f"\n## Queries {phase} (warm p50 ms / p95 ms / cold ms)\n")
        lines.append("| DB | " + " | ".join(q.split('_')[0] for q in qs) + " |")
        lines.append("|---" * (len(qs) + 1) + "|")
        for db in dbs:
            row = [LABEL[db]]
            for q in qs:
                c = res[db]["phases"][phase]["queries"].get(q)
                row.append(f"{c['warm_p50_ms']} / {c['warm_p95_ms']} / "
                           f"{c['cold_ms']}" if c else "—")
            lines.append("| " + " | ".join(row) + " |")

    open(os.path.join(a.outdir, "summary.md"), "w").write("\n".join(lines))
    print(f"wrote {a.outdir}/summary.md")

    # fig: ingest
    fig, ax = plt.subplots(figsize=(7, 3.6))
    x = range(len(dbs))
    m = [res[d]["phases"]["M"]["ingest"]["points_per_s"] for d in dbs]
    h = [res[d]["phases"]["H"]["ingest"]["points_per_s"] for d in dbs]
    ax.bar([i - 0.2 for i in x], m, 0.4, label="M (200 hosts × 10 fields)")
    ax.bar([i + 0.2 for i in x], h, 0.4, label="H (20k series)")
    ax.set_yscale("log")
    ax.set_ylabel("points/s (log)")
    ax.set_xticks(list(x), [LABEL[d] for d in dbs], rotation=20, ha="right")
    ax.legend(fontsize=8)
    ax.set_title("Ingest throughput (6-stream client; *Prometheus = offline backfill)")
    fig.tight_layout()
    fig.savefig(os.path.join(a.outdir, "fig_ingest.png"), dpi=160)

    # fig: disk bytes/point
    fig, ax = plt.subplots(figsize=(7, 3.6))
    raw = [res[d]["phases"]["M"]["disk_bytes"] /
           res[d]["phases"]["M"]["ingest"]["points"] for d in dbs]
    comp = [(res[d]["phases"]["M"].get("disk_bytes_after_compress") or
             res[d]["phases"]["M"]["disk_bytes"]) /
            res[d]["phases"]["M"]["ingest"]["points"] for d in dbs]
    ax.bar([i - 0.2 for i in x], raw, 0.4, label="after ingest")
    ax.bar([i + 0.2 for i in x], comp, 0.4, label="after compression pass")
    ax.set_ylabel("bytes per point (phase M)")
    ax.set_xticks(list(x), [LABEL[d] for d in dbs], rotation=20, ha="right")
    ax.legend(fontsize=8)
    ax.set_title("On-disk footprint")
    fig.tight_layout()
    fig.savefig(os.path.join(a.outdir, "fig_disk.png"), dpi=160)

    # fig: query p50 grouped bars (phase M)
    fig, ax = plt.subplots(figsize=(8.2, 3.8))
    w = 0.8 / len(dbs)
    for i, db in enumerate(dbs):
        ys = [res[db]["phases"]["M"]["queries"].get(q, {}).get("warm_p50_ms")
              for q in QM]
        ax.bar([j + (i - len(dbs) / 2) * w + w / 2 for j in range(len(QM))],
               [y or 0 for y in ys], w, label=LABEL[db])
    ax.set_yscale("log")
    ax.set_ylabel("warm p50 (ms, log)")
    ax.set_xticks(range(len(QM)),
                  ["Q1 raw 1h", "Q2 downsample", "Q3 group-by",
                   "Q4 fanout max", "Q5 global agg"])
    ax.legend(fontsize=7, ncols=4)
    ax.set_title("Query latency, phase M (52M points)")
    fig.tight_layout()
    fig.savefig(os.path.join(a.outdir, "fig_queries.png"), dpi=160)
    print("wrote figures")


if __name__ == "__main__":
    main()
