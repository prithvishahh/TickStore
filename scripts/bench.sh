#!/usr/bin/env bash
# Full comparison: TickStore, CSV, Parquet (zstd), DuckDB.
set -euo pipefail
cd "$(dirname "$0")/.."

TRADES="${TRADES:-1000000}"
SYMBOLS="${SYMBOLS:-8}"
RATIO="${RATIO:-2}"
SEED="${SEED:-7}"
REPEATS="${REPEATS:-5}"
WORKDIR="${WORKDIR:-bench/run}"

cargo build --release
./target/release/tickstore bench \
  --trades "$TRADES" \
  --symbols "$SYMBOLS" \
  --quote-ratio "$RATIO" \
  --seed "$SEED" \
  --repeats "$REPEATS" \
  --workdir "$WORKDIR"

if [[ ! -d .venv ]]; then
  python3 -m venv .venv
fi
.venv/bin/pip install -q -r python/requirements.txt
.venv/bin/python python/bench.py --workdir "$WORKDIR" --repeats "$REPEATS"
