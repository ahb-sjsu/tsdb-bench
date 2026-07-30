#!/usr/bin/env python3
"""tsdb-bench: one-node, out-of-box comparison of popular time-series stores.

Targets (Docker, NUMA-node-1 pinned, 48g cap, data on /archive):
  timescale (PG16 + hypertable + native compression), postgres (16, plain
  table + btree, the baseline), influx (2.7 OSS / Flux), victoria
  (VictoriaMetrics single-node / PromQL), clickhouse (24.8 / MergeTree),
  questdb (8.1 / ILP + SQL).

Workload (TSBS-inspired, deterministic seed):
  Phase M (metrics, medium cardinality): 200 hosts x 10 float fields,
    10 s interval, 3 days  -> 5.184 M rows / 51.84 M points.
  Phase H (high cardinality): 20,000 hosts x 1 field, 60 s interval,
    24 h -> 28.8 M rows / 28.8 M points.

Per DB: ingest wall time (6 parallel client streams, batches pre-encoded so
generation cost is excluded), settle/flush, on-disk bytes, query suite
(1 cold + 15 warm reps each), optional native compression pass (Timescale
compress_chunk / ClickHouse OPTIMIZE FINAL) with before/after bytes, then the
high-cardinality phase. DBs run ONE AT A TIME; a thermal guard waits for CPU
package temps to drop between heavy steps.

Results: one JSON per DB in OUT_DIR + manifest.json. Failures are contained
per-DB so one broken adapter does not sink the campaign.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import io
import json
import os
import re
import statistics
import subprocess
import time
import urllib.parse

import numpy as np
import requests

ROOT = "/archive/experiments/tsdb_bench"
OUT = os.path.join(ROOT, "out")
BASE_EPOCH = 1782864000  # 2026-07-01T00:00:00Z
BATCH_ROWS = 10_000
STREAMS = 6
WARM_REPS = 15

M = dict(hosts=200, fields=10, interval=10, span=3 * 86400)
H = dict(hosts=20_000, fields=1, interval=60, span=86400)
SMOKE_M = dict(hosts=4, fields=10, interval=10, span=1800)
SMOKE_H = dict(hosts=100, fields=1, interval=60, span=3600)

PKG_RE = re.compile(r"Package id (\d):\s+\+([\d.]+)")


def temps():
    try:
        out = subprocess.run(["sensors"], capture_output=True, text=True,
                             timeout=10).stdout
        return {int(m[0]): float(m[1]) for m in PKG_RE.findall(out)}
    except Exception:
        return {}


def thermal_wait(limit=80.0, target=76.0):
    t = temps()
    if not t or max(t.values()) < limit:
        return
    print(f"[thermal] {t} >= {limit}, cooling to {target}", flush=True)
    while t and max(t.values()) > target:
        time.sleep(20)
        t = temps()


def docker(*args, check=True):
    return subprocess.run(["docker", *args], capture_output=True, text=True,
                          check=check)


def du_bytes(path):
    r = subprocess.run(["sudo", "-n", "du", "-sb", path], capture_output=True,
                       text=True)
    try:
        return int(r.stdout.split()[0])
    except Exception:
        return -1


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))]


# ---------------------------------------------------------------- generation
def gen_phase(cfg, seed):
    ticks = cfg["span"] // cfg["interval"]
    rng = np.random.default_rng(seed)
    vals = np.clip(
        rng.uniform(10, 90, (cfg["hosts"], 1, cfg["fields"]))
        + np.cumsum(rng.normal(0, 0.8, (cfg["hosts"], ticks, cfg["fields"])),
                    axis=1),
        0, 100)
    return ticks, vals


def build_batches(cfg, seed):
    """-> (lp_batches, csv_batches, n_rows). Row order: time-major (arrival
    order), identical bytes fed to every DB of the same format family."""
    ticks, vals = gen_phase(cfg, seed)
    nh, nf = cfg["hosts"], cfg["fields"]
    hosts = [f"host_{i:05d}" for i in range(nh)]
    vs = np.char.mod("%.3f", np.round(vals, 3))  # hosts x ticks x fields
    fkeys = [f"f{i}" for i in range(nf)]
    lp_rows, csv_rows = [], []
    lp_b, csv_b = [], []
    for t in range(ticks):
        ep = BASE_EPOCH + (t + 1) * cfg["interval"]
        ns = f"{ep}000000000"
        iso = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ep))
        for hI in range(nh):
            row = vs[hI, t]
            lp_rows.append(
                f"cpu,host={hosts[hI]} "
                + ",".join(f"{k}={v}" for k, v in zip(fkeys, row))
                + f" {ns}")
            csv_rows.append(f"{iso},{hosts[hI]}," + ",".join(row))
            if len(lp_rows) == BATCH_ROWS:
                lp_b.append(("\n".join(lp_rows) + "\n").encode())
                csv_b.append(("\n".join(csv_rows) + "\n").encode())
                lp_rows, csv_rows = [], []
    if lp_rows:
        lp_b.append(("\n".join(lp_rows) + "\n").encode())
        csv_b.append(("\n".join(csv_rows) + "\n").encode())
    return lp_b, csv_b, ticks * nh


# ---------------------------------------------------------------- adapters
class Adapter:
    name = ""
    container = ""
    datadir = ""
    fmt = "lp"  # or "csv"

    def start(self):
        docker("start", self.container)
        self.wait_ready()

    def stop(self):
        docker("stop", "-t", "30", self.container, check=False)

    def wait_ready(self):
        raise NotImplementedError

    def init_schema(self, phase, nf):
        pass

    def send(self, worker_ctx, phase, batch: bytes):
        raise NotImplementedError

    def make_ctx(self):
        return requests.Session()

    def flush(self, phase):
        pass

    def compress(self, phase):
        return None  # optional; returns label

    def disk(self):
        return du_bytes(self.datadir)

    def queries(self, phase, cfg):
        raise NotImplementedError

    def run_query(self, ctx, q):
        raise NotImplementedError


class PGBase(Adapter):
    fmt = "csv"
    port = 0
    timescale = False

    def _conn(self):
        import psycopg2
        c = psycopg2.connect(host="127.0.0.1", port=self.port, user="postgres",
                             password="bench", dbname="postgres",
                             connect_timeout=5)
        c.autocommit = True
        return c

    def wait_ready(self):
        for _ in range(120):
            try:
                self._conn().close()
                return
            except Exception:
                time.sleep(2)
        raise RuntimeError(f"{self.name} not ready")

    def table(self, phase):
        return "cpu" if phase == "M" else "cpuh"

    def init_schema(self, phase, nf):
        t = self.table(phase)
        cols = ", ".join(f"f{i} float8" for i in range(nf))
        c = self._conn()
        cur = c.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {t}")
        cur.execute(f"CREATE TABLE {t} (time timestamptz NOT NULL, "
                    f"host text NOT NULL, {cols})")
        if self.timescale:
            cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
            cur.execute(f"SELECT create_hypertable('{t}','time')")
        cur.execute(f"CREATE INDEX ON {t} (host, time DESC)")
        c.close()

    def make_ctx(self):
        return self._conn()

    def send(self, ctx, phase, batch):
        cur = ctx.cursor()
        cur.copy_expert(
            f"COPY {self.table(phase)} FROM STDIN WITH (FORMAT csv)",
            io.BytesIO(batch))

    def flush(self, phase):
        c = self._conn()
        c.cursor().execute(f"ANALYZE {self.table(phase)}")
        c.close()

    def compress(self, phase):
        if not self.timescale or phase != "M":
            return None
        c = self._conn()
        cur = c.cursor()
        cur.execute("ALTER TABLE cpu SET (timescaledb.compress, "
                    "timescaledb.compress_segmentby='host')")
        cur.execute("SELECT compress_chunk(c, true) "
                    "FROM show_chunks('cpu') c")
        cur.fetchall()
        c.close()
        return "timescale compress_chunk(all)"

    def queries(self, phase, cfg):
        end = BASE_EPOCH + cfg["span"]
        e = time.strftime("%Y-%m-%d %H:%M:%S+00", time.gmtime(end))
        h = f"host_{min(100, cfg['hosts'] - 1):05d}"
        mins = "date_trunc('minute', time)"
        if self.timescale:
            mins = "time_bucket('1 minute', time)"
        if phase == "M":
            return {
                "Q1_raw_1h": (f"SELECT time, f0 FROM cpu WHERE host='{h}' "
                              f"AND time > '{e}'::timestamptz - interval '1 hour'"),
                "Q2_downsample_12h": (
                    f"SELECT {mins} tb, avg(f0) FROM cpu WHERE host='{h}' AND "
                    f"time > '{e}'::timestamptz - interval '12 hours' GROUP BY tb"),
                "Q3_groupby_1h": (
                    f"SELECT host, avg(f0) FROM cpu WHERE time > "
                    f"'{e}'::timestamptz - interval '1 hour' GROUP BY host"),
                "Q4_fanout_max_full": "SELECT host, max(f0) FROM cpu GROUP BY host",
                "Q5_global_agg": "SELECT count(*), avg(f0) FROM cpu",
            }
        return {
            "H1_series_raw_24h": (f"SELECT time, f0 FROM cpuh WHERE "
                                  f"host='host_00042'"),
            "H2_max_by_host": "SELECT host, max(f0) FROM cpuh GROUP BY host",
        }

    def run_query(self, ctx, q):
        cur = ctx.cursor()
        cur.execute(q)
        return len(cur.fetchall())


class Timescale(PGBase):
    name, container, port, timescale = "timescale", "tsdb-timescale", 5433, True
    datadir = f"{ROOT}/data/timescale"


class Postgres(PGBase):
    name, container, port = "postgres", "tsdb-postgres", 5434
    datadir = f"{ROOT}/data/postgres"


class ClickHouse(Adapter):
    name, container = "clickhouse", "tsdb-clickhouse"
    datadir = f"{ROOT}/data/clickhouse"
    fmt = "csv"
    url = "http://127.0.0.1:8123/"

    def wait_ready(self):
        for _ in range(120):
            try:
                if requests.get(self.url + "ping", timeout=3).ok:
                    return
            except Exception:
                pass
            time.sleep(2)
        raise RuntimeError("clickhouse not ready")

    def sql(self, ctx, q, body=None):
        r = ctx.post(self.url,
                     params={"query": q, "user": "bench", "password": "bench"},
                     data=body, timeout=600)
        r.raise_for_status()
        return r.text

    def table(self, phase):
        return "cpu" if phase == "M" else "cpuh"

    def init_schema(self, phase, nf):
        t = self.table(phase)
        cols = ", ".join(f"f{i} Float64" for i in range(nf))
        s = requests.Session()
        self.sql(s, f"DROP TABLE IF EXISTS {t}")
        self.sql(s, f"CREATE TABLE {t} (time DateTime('UTC'), "
                    f"host LowCardinality(String), {cols}) "
                    f"ENGINE = MergeTree ORDER BY (host, time)")

    def send(self, ctx, phase, batch):
        self.sql(ctx, f"INSERT INTO {self.table(phase)} FORMAT CSV", body=batch)

    def compress(self, phase):
        if phase != "M":
            return None
        self.sql(requests.Session(), "OPTIMIZE TABLE cpu FINAL")
        return "OPTIMIZE TABLE FINAL"

    def queries(self, phase, cfg):
        end = BASE_EPOCH + cfg["span"]
        e = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(end))
        h = f"host_{min(100, cfg['hosts'] - 1):05d}"
        if phase == "M":
            return {
                "Q1_raw_1h": (f"SELECT time, f0 FROM cpu WHERE host='{h}' AND "
                              f"time > toDateTime('{e}','UTC') - 3600"),
                "Q2_downsample_12h": (
                    f"SELECT toStartOfMinute(time) tb, avg(f0) FROM cpu WHERE "
                    f"host='{h}' AND time > toDateTime('{e}','UTC') - 43200 "
                    f"GROUP BY tb"),
                "Q3_groupby_1h": (
                    f"SELECT host, avg(f0) FROM cpu WHERE time > "
                    f"toDateTime('{e}','UTC') - 3600 GROUP BY host"),
                "Q4_fanout_max_full": "SELECT host, max(f0) FROM cpu GROUP BY host",
                "Q5_global_agg": "SELECT count(*), avg(f0) FROM cpu",
            }
        return {
            "H1_series_raw_24h": "SELECT time, f0 FROM cpuh WHERE host='host_00042'",
            "H2_max_by_host": "SELECT host, max(f0) FROM cpuh GROUP BY host",
        }

    def run_query(self, ctx, q):
        return self.sql(ctx, q).count("\n")


class QuestDB(Adapter):
    name, container = "questdb", "tsdb-questdb"
    datadir = f"{ROOT}/data/questdb"
    url = "http://127.0.0.1:9000/"

    def wait_ready(self):
        for _ in range(120):
            try:
                r = requests.get(self.url + "exec",
                                 params={"query": "select 1"}, timeout=3)
                if r.ok:
                    return
            except Exception:
                pass
            time.sleep(2)
        raise RuntimeError("questdb not ready")

    def exec(self, ctx, q):
        r = ctx.get(self.url + "exec", params={"query": q}, timeout=600)
        r.raise_for_status()
        return r.json()

    def init_schema(self, phase, nf):
        s = requests.Session()
        t = "cpu" if phase == "M" else "cpuh"
        cols = ", ".join(f"f{i} double" for i in range(nf))
        cap = 1024 if phase == "M" else 65536
        self.exec(s, f"DROP TABLE IF EXISTS {t}")
        self.exec(s, f"CREATE TABLE {t} (timestamp timestamp, "
                     f"host symbol capacity {cap}, {cols}) "
                     f"timestamp(timestamp) PARTITION BY DAY WAL")
        self._measurement = t

    def send(self, ctx, phase, batch):
        t = "cpu" if phase == "M" else "cpuh"
        if t != "cpu":
            batch = batch.replace(b"cpu,host=", b"cpuh,host=")
        r = ctx.post(self.url + "write", params={"precision": "n"},
                     data=batch, timeout=600)
        r.raise_for_status()

    def flush(self, phase):
        t = "cpu" if phase == "M" else "cpuh"
        s = requests.Session()
        for _ in range(300):
            j = self.exec(s, f"select count() from {t}")
            n = j["dataset"][0][0]
            time.sleep(2)
            j2 = self.exec(s, f"select count() from {t}")
            if j2["dataset"][0][0] == n:
                return
        raise RuntimeError("questdb WAL did not settle")

    def queries(self, phase, cfg):
        end = BASE_EPOCH + cfg["span"]
        iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(end))

        def minus(sec):
            return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(end - sec))
        h = f"host_{min(100, cfg['hosts'] - 1):05d}"
        if phase == "M":
            return {
                "Q1_raw_1h": (f"SELECT timestamp, f0 FROM cpu WHERE host='{h}' "
                              f"AND timestamp > '{minus(3600)}'"),
                "Q2_downsample_12h": (
                    f"SELECT timestamp, avg(f0) FROM cpu WHERE host='{h}' AND "
                    f"timestamp > '{minus(43200)}' SAMPLE BY 1m"),
                "Q3_groupby_1h": (f"SELECT host, avg(f0) FROM cpu WHERE "
                                  f"timestamp > '{minus(3600)}' GROUP BY host"),
                "Q4_fanout_max_full": "SELECT host, max(f0) FROM cpu GROUP BY host",
                "Q5_global_agg": "SELECT count(), avg(f0) FROM cpu",
            }
        return {
            "H1_series_raw_24h": ("SELECT timestamp, f0 FROM cpuh WHERE "
                                  "host='host_00042'"),
            "H2_max_by_host": "SELECT host, max(f0) FROM cpuh GROUP BY host",
        }

    def run_query(self, ctx, q):
        return len(self.exec(ctx, q).get("dataset", []))


class Influx(Adapter):
    name, container = "influx", "tsdb-influx"
    datadir = f"{ROOT}/data/influx"
    url = "http://127.0.0.1:8086"
    hdr = {"Authorization": "Token benchtoken"}

    def wait_ready(self):
        for _ in range(120):
            try:
                if requests.get(self.url + "/health", timeout=3).ok:
                    return
            except Exception:
                pass
            time.sleep(2)
        raise RuntimeError("influx not ready")

    def init_schema(self, phase, nf):
        if phase == "H":  # separate measurement via rewrite in send()
            pass

    def send(self, ctx, phase, batch):
        if phase == "H":
            batch = batch.replace(b"cpu,host=", b"cpuh,host=")
        r = ctx.post(self.url + "/api/v2/write",
                     params={"org": "bench", "bucket": "bench",
                             "precision": "ns"},
                     headers=self.hdr, data=batch, timeout=600)
        r.raise_for_status()

    def make_ctx(self):
        s = requests.Session()
        s.headers.update(self.hdr)
        return s

    def flux(self, ctx, q):
        r = ctx.post(self.url + "/api/v2/query", params={"org": "bench"},
                     headers={**self.hdr,
                              "Content-Type": "application/vnd.flux",
                              "Accept": "application/csv"},
                     data=q, timeout=600)
        r.raise_for_status()
        return r.text

    def queries(self, phase, cfg):
        end = BASE_EPOCH + cfg["span"]

        def iso(sec):
            return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(sec))
        e, m = iso(end + 1), "cpu" if phase == "M" else "cpuh"
        h = f"host_{min(100, cfg['hosts'] - 1):05d}"
        base = (f'from(bucket:"bench") |> range(start: {{s}}, stop: {e}) '
                f'|> filter(fn: (r) => r._measurement == "{m}" and '
                f'r._field == "f0")')
        one = base + f' |> filter(fn: (r) => r.host == "{h}")'
        if phase == "M":
            return {
                "Q1_raw_1h": one.format(s=iso(end - 3600)),
                "Q2_downsample_12h": (
                    one.format(s=iso(end - 43200)) +
                    " |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)"),
                "Q3_groupby_1h": (base.format(s=iso(end - 3600)) +
                                  ' |> group(columns: ["host"]) |> mean()'),
                "Q4_fanout_max_full": (base.format(s=iso(BASE_EPOCH)) +
                                       ' |> group(columns: ["host"]) |> max()'),
                "Q5_global_agg": (base.format(s=iso(BASE_EPOCH)) +
                                  ' |> group() |> mean()'),
            }
        return {
            "H1_series_raw_24h": (base.format(s=iso(BASE_EPOCH)) +
                                  ' |> filter(fn: (r) => r.host == "host_00042")'),
            "H2_max_by_host": (base.format(s=iso(BASE_EPOCH)) +
                               ' |> group(columns: ["host"]) |> max()'),
        }

    def run_query(self, ctx, q):
        return self.flux(ctx, q).count("\n")


class Victoria(Adapter):
    name, container = "victoria", "tsdb-victoria"
    datadir = f"{ROOT}/data/victoria"
    url = "http://127.0.0.1:8429"

    def wait_ready(self):
        for _ in range(120):
            try:
                if requests.get(self.url + "/health", timeout=3).ok:
                    return
            except Exception:
                pass
            time.sleep(2)
        raise RuntimeError("victoria not ready")

    def send(self, ctx, phase, batch):
        if phase == "H":
            batch = batch.replace(b"cpu,host=", b"cpuh,host=")
        r = ctx.post(self.url + "/write", data=batch, timeout=600)
        r.raise_for_status()

    def flush(self, phase):
        requests.get(self.url + "/internal/force_flush", timeout=60)

    def compress(self, phase):
        if phase != "M":
            return None
        requests.get(self.url + "/internal/force_merge", timeout=600)
        time.sleep(10)
        return "force_merge"

    def q(self, ctx, ep, query, start=None, end=None, step=None, at=None):
        params = {"query": query}
        if start is not None:
            params.update({"start": start, "end": end, "step": step})
        if at is not None:
            params["time"] = at
        r = ctx.get(self.url + ep, params=params, timeout=600)
        r.raise_for_status()
        return r.json()

    def queries(self, phase, cfg):
        end = BASE_EPOCH + cfg["span"]
        m = "cpu" if phase == "M" else "cpuh"
        h = f"host_{min(100, cfg['hosts'] - 1):05d}"
        full = cfg["span"]
        if phase == "M":
            return {
                "Q1_raw_1h": ("range", f'{m}_f0{{host="{h}"}}',
                              end - 3600, end, "10s"),
                "Q2_downsample_12h": ("range",
                                      f'avg_over_time({m}_f0{{host="{h}"}}[1m])',
                                      end - 43200, end, "60s"),
                "Q3_groupby_1h": ("instant",
                                  f'avg by (host) (avg_over_time({m}_f0[1h]))',
                                  None, end, None),
                "Q4_fanout_max_full": ("instant",
                                       f'max by (host) (max_over_time({m}_f0[{full}s]))',
                                       None, end, None),
                "Q5_global_agg": ("instant",
                                  f'avg(avg_over_time({m}_f0[{full}s]))',
                                  None, end, None),
            }
        return {
            "H1_series_raw_24h": ("range", f'{m}_f0{{host="host_00042"}}',
                                  BASE_EPOCH, end, "60s"),
            "H2_max_by_host": ("instant",
                               f'max by (host) (max_over_time({m}_f0[{full}s]))',
                               None, end, None),
        }

    def run_query(self, ctx, spec):
        kind, query, start, end, step = spec
        if kind == "range":
            j = self.q(ctx, "/api/v1/query_range", query,
                       start=start, end=end, step=step)
            res = j.get("data", {}).get("result", [])
            return sum(len(r.get("values", [])) for r in res)
        j = self.q(ctx, "/api/v1/query", query, at=end)
        return len(j.get("data", {}).get("result", []))


class Prometheus(Victoria):
    """Prometheus's embedded TSDB. It is scrape-based and rejects backdated
    online writes, so ingest uses the sanctioned OFFLINE path: promtool tsdb
    create-blocks-from openmetrics. Its 'ingest' numbers are an offline
    backfill rate, not comparable to online ingest; queries and disk are
    fully comparable (PromQL mapping shared with VictoriaMetrics)."""

    name, container = "prometheus", "tsdb-prometheus"
    datadir = f"{ROOT}/data/prometheus"
    url = "http://127.0.0.1:9091"
    offline_ingest = True

    def wait_ready(self):
        for _ in range(120):
            try:
                if requests.get(self.url + "/-/ready", timeout=3).ok:
                    return
            except Exception:
                pass
            time.sleep(2)
        raise RuntimeError("prometheus not ready")

    def start(self):  # server starts after blocks exist (custom_ingest)
        docker("stop", "-t", "10", self.container, check=False)

    def flush(self, phase):
        pass

    def compress(self, phase):
        return None

    def custom_ingest(self, phase, lp_batches):
        docker("stop", "-t", "10", self.container, check=False)
        m = "cpu" if phase == "M" else "cpuh"
        om = f"{ROOT}/data/om/om_{phase}.txt"
        with open(om, "w") as fh:
            for batch in lp_batches:
                for line in batch.decode().splitlines():
                    head, fields, ns = line.rsplit(" ", 2)
                    host = head.split("host=", 1)[1]
                    sec = int(ns) // 1_000_000_000
                    for kv in fields.split(","):
                        k, v = kv.split("=")
                        fh.write(f'{m}_{k}{{host="{host}"}} {v} {sec}\n')
            fh.write("# EOF\n")
        t0 = time.perf_counter()
        docker("run", "--rm", "--cpuset-cpus=12-23,36-47",
               "-v", f"{ROOT}/data/prometheus:/prometheus",
               "-v", f"{ROOT}/data/om:/om",
               "--entrypoint", "promtool", "prom/prometheus:v2.53.0",
               "tsdb", "create-blocks-from", "openmetrics",
               f"/om/om_{phase}.txt", "/prometheus")
        wall = time.perf_counter() - t0
        os.remove(om)
        docker("start", self.container)
        self.wait_ready()
        return {"wall_s": round(wall, 2), "send_s": round(wall, 2),
                "batch_p50_ms": -1, "batch_p95_ms": -1,
                "note": "offline promtool backfill, not online ingest"}


ADAPTERS = [Timescale(), Postgres(), Influx(), Victoria(), ClickHouse(),
            QuestDB(), Prometheus()]


# ---------------------------------------------------------------- runner
def ingest(ad, phase, batches):
    ctxs = [ad.make_ctx() for _ in range(STREAMS)]
    lat = []

    def work(k):
        for i in range(k, len(batches), STREAMS):
            t0 = time.perf_counter()
            ad.send(ctxs[k], phase, batches[i])
            lat.append(time.perf_counter() - t0)

    t0 = time.perf_counter()
    with cf.ThreadPoolExecutor(STREAMS) as ex:
        list(ex.map(work, range(STREAMS)))
    wall_send = time.perf_counter() - t0
    ad.flush(phase)
    wall = time.perf_counter() - t0
    for c in ctxs:
        try:
            c.close()
        except Exception:
            pass
    return {"wall_s": round(wall, 2), "send_s": round(wall_send, 2),
            "batch_p50_ms": round(pct(lat, 50) * 1e3, 1),
            "batch_p95_ms": round(pct(lat, 95) * 1e3, 1)}


HEAVY = ("Q4", "Q5", "H2")
HEAVY_REPS = 5


def run_queries(ad, phase, cfg):
    out = {}
    ctx = ad.make_ctx()
    for name, q in ad.queries(phase, cfg).items():
        reps = HEAVY_REPS if name.startswith(HEAVY) else WARM_REPS
        t0 = time.perf_counter()
        n = ad.run_query(ctx, q)
        cold = time.perf_counter() - t0
        warm = []
        for _ in range(reps):
            t0 = time.perf_counter()
            ad.run_query(ctx, q)
            warm.append(time.perf_counter() - t0)
        out[name] = {"rows": n, "cold_ms": round(cold * 1e3, 1),
                     "warm_p50_ms": round(pct(warm, 50) * 1e3, 1),
                     "warm_p95_ms": round(pct(warm, 95) * 1e3, 1)}
        print(f"    {name}: rows={n} cold={out[name]['cold_ms']}ms "
              f"p50={out[name]['warm_p50_ms']}ms", flush=True)
    return out


def bench_db(ad, mM, hH, data):
    res = {"db": ad.name, "phases": {}, "temps": {}}
    print(f"[{ad.name}] starting", flush=True)
    thermal_wait()
    ad.start()
    try:
        for phase, cfg in (("M", mM), ("H", hH)):
            lp, csv, nrows = data[phase]
            batches = csv if ad.fmt == "csv" else lp
            points = nrows * cfg["fields"]
            ad.init_schema(phase, cfg["fields"])
            if getattr(ad, "offline_ingest", False):
                r = ad.custom_ingest(phase, lp)
            else:
                r = ingest(ad, phase, batches)
            r["rows"], r["points"] = nrows, points
            r["points_per_s"] = int(points / r["wall_s"])
            r["rows_per_s"] = int(nrows / r["wall_s"])
            print(f"  [{ad.name}/{phase}] ingest {r['wall_s']}s "
                  f"({r['points_per_s']:,} pts/s)", flush=True)
            time.sleep(5)
            disk_raw = ad.disk()
            qres = run_queries(ad, phase, cfg)
            comp = None
            disk_comp = None
            if phase == "M":
                label = ad.compress(phase)
                if label:
                    time.sleep(5)
                    disk_comp = ad.disk()
                    comp = label
            res["phases"][phase] = {
                "cfg": cfg, "ingest": r, "disk_bytes": disk_raw,
                "disk_bytes_after_compress": disk_comp,
                "compression": comp, "queries": qres}
            res["temps"][phase] = temps()
            thermal_wait()
    finally:
        ad.stop()
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["smoke", "full"], default="smoke")
    ap.add_argument("--only", default="")
    a = ap.parse_args()
    mM, hH = (M, H) if a.mode == "full" else (SMOKE_M, SMOKE_H)
    os.makedirs(OUT, exist_ok=True)

    print(f"[gen] building payloads ({a.mode})", flush=True)
    t0 = time.perf_counter()
    data = {"M": build_batches(mM, seed=1), "H": build_batches(hH, seed=2)}
    print(f"[gen] done in {time.perf_counter()-t0:.0f}s: "
          f"M={data['M'][2]:,} rows, H={data['H'][2]:,} rows", flush=True)

    manifest = {"mode": a.mode, "M": mM, "H": hH, "batch_rows": BATCH_ROWS,
                "streams": STREAMS, "warm_reps": WARM_REPS,
                "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "images": docker("ps", "-a", "--filter", "name=tsdb-",
                                 "--format",
                                 "{{.Names}} {{.Image}}").stdout.split("\n")}
    json.dump(manifest, open(f"{OUT}/manifest_{a.mode}.json", "w"), indent=1)

    only = [s for s in a.only.split(",") if s]
    for ad in ADAPTERS:
        if only and ad.name not in only:
            continue
        try:
            res = bench_db(ad, mM, hH, data)
        except Exception as e:
            res = {"db": ad.name, "error": repr(e)[:500]}
            print(f"[{ad.name}] FAILED: {e}", flush=True)
            docker("stop", "-t", "10", ad.container, check=False)
        json.dump(res, open(f"{OUT}/{ad.name}_{a.mode}.json", "w"), indent=1)
    print("[bench] campaign complete", flush=True)


if __name__ == "__main__":
    main()
