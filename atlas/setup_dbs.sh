#!/usr/bin/env bash
# Create (not start) the six benchmark DB containers, pinned to NUMA node 1
# (cores 12-23 + HT 36-47) with a 48g memory cap, data dirs on /archive.
# Production services (postgres :5432, VictoriaMetrics :8428) are untouched;
# every bench container binds to 127.0.0.1 on its own port.
set -euo pipefail

ROOT=/archive/experiments/tsdb_bench
CPUS="12-23,36-47"
MEM=48g

mkdir -p "$ROOT"/data/{timescale,postgres,influx,victoria,clickhouse,questdb} "$ROOT"/out

pull() { docker image inspect "$1" >/dev/null 2>&1 || docker pull -q "$1"; }

pull timescale/timescaledb:latest-pg16
pull postgres:16
pull influxdb:2.7
pull victoriametrics/victoria-metrics:v1.102.1
pull clickhouse/clickhouse-server:24.8
pull questdb/questdb:8.1.1

mk() { docker rm -f "$1" >/dev/null 2>&1 || true; shift; docker create "$@" >/dev/null; }

mk tsdb-timescale --name tsdb-timescale --cpuset-cpus="$CPUS" --memory=$MEM \
  -e POSTGRES_PASSWORD=bench -e TZ=UTC -p 127.0.0.1:5433:5432 \
  -v "$ROOT"/data/timescale:/var/lib/postgresql/data \
  timescale/timescaledb:latest-pg16

mk tsdb-postgres --name tsdb-postgres --cpuset-cpus="$CPUS" --memory=$MEM \
  -e POSTGRES_PASSWORD=bench -e TZ=UTC -p 127.0.0.1:5434:5432 \
  -v "$ROOT"/data/postgres:/var/lib/postgresql/data \
  postgres:16

mk tsdb-influx --name tsdb-influx --cpuset-cpus="$CPUS" --memory=$MEM \
  -e DOCKER_INFLUXDB_INIT_MODE=setup -e DOCKER_INFLUXDB_INIT_USERNAME=bench \
  -e DOCKER_INFLUXDB_INIT_PASSWORD=benchbench -e DOCKER_INFLUXDB_INIT_ORG=bench \
  -e DOCKER_INFLUXDB_INIT_BUCKET=bench -e DOCKER_INFLUXDB_INIT_RETENTION=0 \
  -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=benchtoken -p 127.0.0.1:8086:8086 \
  -v "$ROOT"/data/influx:/var/lib/influxdb2 \
  influxdb:2.7

mk tsdb-victoria --name tsdb-victoria --cpuset-cpus="$CPUS" --memory=$MEM \
  -p 127.0.0.1:8429:8428 \
  -v "$ROOT"/data/victoria:/victoria-metrics-data \
  victoriametrics/victoria-metrics:v1.102.1 \
  -retentionPeriod=100y

mk tsdb-clickhouse --name tsdb-clickhouse --cpuset-cpus="$CPUS" --memory=$MEM \
  -e TZ=UTC -e CLICKHOUSE_USER=bench -e CLICKHOUSE_PASSWORD=bench \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  --ulimit nofile=262144:262144 -p 127.0.0.1:8123:8123 \
  -v "$ROOT"/data/clickhouse:/var/lib/clickhouse \
  clickhouse/clickhouse-server:24.8

mk tsdb-questdb --name tsdb-questdb --cpuset-cpus="$CPUS" --memory=$MEM \
  -p 127.0.0.1:9000:9000 \
  -v "$ROOT"/data/questdb:/var/lib/questdb \
  questdb/questdb:8.1.1

pull prom/prometheus:v2.53.0
mkdir -p "$ROOT"/data/prometheus_cfg "$ROOT"/data/prometheus "$ROOT"/data/om
sudo -n chown -R 65534:65534 "$ROOT"/data/prometheus
[ -f "$ROOT"/data/prometheus_cfg/prometheus.yml ] || \
  printf 'global: {}\n' > "$ROOT"/data/prometheus_cfg/prometheus.yml
docker rm -f tsdb-prometheus >/dev/null 2>&1 || true
docker create --name tsdb-prometheus --cpuset-cpus="$CPUS" --memory=$MEM \
  -p 127.0.0.1:9091:9090 \
  -v "$ROOT"/data/prometheus:/prometheus \
  -v "$ROOT"/data/prometheus_cfg:/etc/prometheus \
  prom/prometheus:v2.53.0 \
  --config.file=/etc/prometheus/prometheus.yml \
  --storage.tsdb.path=/prometheus \
  --storage.tsdb.retention.time=100y >/dev/null

docker ps -a --filter name=tsdb- --format '{{.Names}}\t{{.Status}}'
echo "setup done"
