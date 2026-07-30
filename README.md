# tsdb-bench

One-node, out-of-box benchmark of seven time-series stores (TimescaleDB,
PostgreSQL, InfluxDB 2.x, VictoriaMetrics, ClickHouse, QuestDB, Prometheus)
on the Atlas workstation, answering: *is TimescaleDB/Postgres the best
default?* See `PROTOCOL.md` for the pre-registered design and
`articles/` for the write-up.

## Layout

- `atlas/setup_dbs.sh` — create the 7 pinned Docker containers (data on
  `/archive/experiments/tsdb_bench`).
- `atlas/run_bench.sh` — venv + pinned harness launcher (`smoke` | `full`).
- `bench/bench.py` — the harness (generation, adapters, phases, thermal guard).
- `bench/analyze.py` — results → `results/summary.md` + figures.
- `results/out/` — raw per-DB JSON pulled from Atlas.
- `articles/` — the paper.

## Run

On Atlas: `bash setup_dbs.sh`, then
`screen -dmS tsdbfull bash run_bench.sh full`. Results land in
`/archive/experiments/tsdb_bench/out/`.
