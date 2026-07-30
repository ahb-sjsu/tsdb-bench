#!/usr/bin/env bash
# Prepare the bench venv and run the harness pinned to NUMA node 1.
# Usage: run_bench.sh smoke|full [only-list]
set -euo pipefail
ROOT=/archive/experiments/tsdb_bench
MODE="${1:-smoke}"
ONLY="${2:-}"

if [ ! -x "$ROOT/venv/bin/python" ]; then
  python3 -m venv "$ROOT/venv"
  "$ROOT/venv/bin/pip" install -q --upgrade pip
  "$ROOT/venv/bin/pip" install -q numpy requests psycopg2-binary matplotlib
fi

exec taskset -c 12-23,36-47 "$ROOT/venv/bin/python" -u \
  "$ROOT/bench.py" --mode "$MODE" ${ONLY:+--only "$ONLY"} \
  2>&1 | tee -a "$ROOT/out/bench_${MODE}.log"
