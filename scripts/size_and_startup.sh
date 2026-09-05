#!/usr/bin/env bash
# Size-and-startup gate for the SQLModel Rust minimal application
# (`bd-4ttf.2`, PLAN success criteria: binary < 5 MiB, startup < 10 ms).
#
# Usage: size_and_startup.sh <binary> [runs] [max-size-mib] [max-median-ms]
#
# Prints the measured values always (the CI log doubles as a time series)
# and exits nonzero when a threshold is violated.

set -euo pipefail

BINARY="${1:?usage: size_and_startup.sh <binary> [runs] [max-size-mib] [max-median-ms]}"
RUNS="${2:-20}"
MAX_SIZE_MIB="${3:-5}"
MAX_MEDIAN_MS="${4:-10}"

fail=0

# --- Binary size -------------------------------------------------------------
size_bytes=$(stat -c %s "$BINARY")
size_mib=$(awk -v b="$size_bytes" 'BEGIN { printf "%.2f", b / 1048576 }')
echo "size: ${size_mib} MiB (${size_bytes} bytes) for $BINARY"
within=$(awk -v s="$size_mib" -v m="$MAX_SIZE_MIB" 'BEGIN { print (s <= m) ? 1 : 0 }')
if [ "$within" != 1 ]; then
    echo "FAIL: binary size ${size_mib} MiB exceeds the ${MAX_SIZE_MIB} MiB target"
    fail=1
fi

# --- Startup time ------------------------------------------------------------
# EPOCHREALTIME gives microsecond precision without extra tooling. Each
# sample is the full process lifetime (spawn + run + exit), which is the
# number PLAN's "startup < 10ms" refers to for the minimal app.
samples_file=$(mktemp)
trap 'rm -f "$samples_file"' EXIT
for _ in $(seq 1 "$RUNS"); do
    t0=${EPOCHREALTIME}
    "$BINARY" > /dev/null
    t1=${EPOCHREALTIME}
    awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.3f\n", (b - a) * 1000 }' >> "$samples_file"
done

median_ms=$(sort -n "$samples_file" | awk -v n="$RUNS" '
    { values[NR] = $1 }
    END {
        if (NR % 2 == 1) { print values[(NR + 1) / 2] }
        else { print (values[NR / 2] + values[NR / 2 + 1]) / 2 }
    }')
min_ms=$(sort -n "$samples_file" | head -1)
max_ms=$(sort -n "$samples_file" | tail -1)
echo "startup: median ${median_ms} ms, min ${min_ms} ms, max ${max_ms} ms over ${RUNS} runs"

within=$(awk -v s="$median_ms" -v m="$MAX_MEDIAN_MS" 'BEGIN { print (s <= m) ? 1 : 0 }')
if [ "$within" != 1 ]; then
    echo "FAIL: median startup ${median_ms} ms exceeds the ${MAX_MEDIAN_MS} ms target"
    echo "distribution:"
    cat "$samples_file"
    fail=1
fi

if [ "$fail" != 0 ]; then
    exit 1
fi
echo "size-and-startup gate PASSED"
