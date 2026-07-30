# tsdb-bench protocol

One-node, out-of-box comparison of seven time-series stores, fixed before the
full run (2026-07-30). The question: for a small team's metrics workload, is
TimescaleDB/Postgres the right default, and what do the alternatives buy?

## Systems under test

| name | image | write path | query path |
|---|---|---|---|
| TimescaleDB | timescale/timescaledb:latest-pg16 | COPY (6 conns) | SQL (psycopg2) |
| PostgreSQL | postgres:16 | COPY (6 conns) | SQL (psycopg2) |
| InfluxDB | influxdb:2.7 | line protocol HTTP | Flux HTTP |
| VictoriaMetrics | victoriametrics/victoria-metrics:v1.102.1 | line protocol HTTP | PromQL HTTP |
| ClickHouse | clickhouse/clickhouse-server:24.8 | CSV INSERT HTTP | SQL HTTP |
| QuestDB | questdb/questdb:8.1.1 | ILP HTTP | SQL REST |
| Prometheus | prom/prometheus:v2.53.0 | **offline** promtool backfill | PromQL HTTP |

Prometheus is scrape-based and rejects backdated online writes; it is included
via its sanctioned offline path (`promtool tsdb create-blocks-from
openmetrics`). Its ingest number is an offline backfill rate, labeled as such
everywhere; queries and disk are fully comparable.

## Fairness rules

- Out-of-box configs; the only "tuning" is idiomatic schema setup per DB
  (hypertable + segmentby-host compression for Timescale; MergeTree ORDER BY
  (host, time) for ClickHouse; SYMBOL + PARTITION BY DAY WAL for QuestDB;
  btree (host, time DESC) for both Postgres flavors).
- One DB runs at a time. Every container pinned to NUMA node 1
  (cores 12-23 + HT siblings), 48 GB memory cap, data on /archive (RAID5 HDD).
  The bench client is pinned to the same cores (same-box reality, documented).
- Identical pre-encoded payload bytes per format family (CSV for PG/CH,
  line protocol for Influx/VM/QuestDB, OpenMetrics derived from the same
  lines for Prometheus); generation excluded from timing; 10k-row batches on
  6 parallel client streams.
- Thermal guard between phases (wait for CPU package < 80 °C) on the shared
  host; co-tenant load on NUMA node 0 documented in the paper.

## Workload

Deterministic (seeded) random-walk gauges in [0, 100].

- **Phase M** (metrics, medium cardinality): 200 hosts × 10 float fields,
  10 s interval, 3 days → 5.184 M rows / 51.84 M points.
- **Phase H** (high cardinality): 20,000 hosts × 1 field, 60 s interval,
  24 h → 28.8 M rows / 28.8 M points.

## Measurements

1. **Ingest**: wall from first batch to flush/settle; points/s; per-batch
   p50/p95 (stall detection).
2. **Disk**: `du -sb` of the bind-mounted data dir after settle; for
   Timescale/ClickHouse/VM also after an explicit compression/merge pass
   (compress_chunk all / OPTIMIZE FINAL / force_merge).
3. **Queries** (1 cold + 15 warm reps; 5 warm reps for full scans):
   - Q1 raw recent: one host, last 1 h, raw points (360 rows).
   - Q2 downsample: one host, 12 h, 1-min avg (720 rows).
   - Q3 group-by: all hosts, avg over last 1 h (200 rows).
   - Q4 fanout: max per host over full 3 d (200 rows).
   - Q5 global agg: count + avg over everything.
   - H1 series lookup: one of 20k series, 24 h raw.
   - H2 high-card fanout: max per host across 20k series.
   PromQL/Flux equivalents are semantic approximations (range queries return
   samples at step resolution, not raw rows); the mapping is in `bench.py`
   and discussed in the paper.
4. **Sanity**: row/series counts recorded per query per DB and cross-checked.

## Known limitations (stated up front)

Single node, single run per cell (ingest) and 15 reps (queries) on a shared
host; HDD-backed RAID5 rather than NVMe; out-of-box configs; PromQL/Flux
semantic mapping approximate for range queries; no concurrent
ingest+query mixed workload; no clustering/HA evaluated.
