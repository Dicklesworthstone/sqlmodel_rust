#!/usr/bin/env bash
# Criterion regression guard (bd-4ttf.4).
#
# Guard mode (default): run after `cargo bench -p sqlmodel-e2e --bench
# sql_emission --bench row_conversion -- --noplot` and compare every guarded
# benchmark's criterion mean against the 2x-margin thresholds in
# crates/sqlmodel-e2e/benches/thresholds.toml. Exits non-zero with a
# measured-vs-threshold table when any guarded bench regresses.
#
# Update mode: SQLMODEL_UPDATE_THRESHOLDS=1 scripts/bench_guard.sh prints a
# fresh threshold table from the last criterion run (2x margin applied).
# Multiply stable local means by 2, note the baseline machine, and commit the
# file with justification - mirroring the golden-SQL regeneration rule.
#
# Usage: scripts/bench_guard.sh [thresholds.toml] [criterion-dir]

set -euo pipefail

thresholds_file="${1:-crates/sqlmodel-e2e/benches/thresholds.toml}"
criterion_dir="${2:-target/criterion}"

if [[ -n "${SQLMODEL_UPDATE_THRESHOLDS:-}" ]]; then
    for est in "$criterion_dir"/*/*/new/estimates.json; do
        [[ -f "$est" ]] || continue
        key="${est#"$criterion_dir"/}"
        key="${key%/new/estimates.json}"
        mean=$(jq -r '.mean.point_estimate' "$est")
        # 2x margin, rounded up to the next whole 100 ns (mean is a float).
        rounded=$(awk "BEGIN{printf \"%d\", int(($mean * 2 + 99) / 100) * 100}")
        printf '"%s" = %d\n' "$key" "$rounded"
    done | sort
    exit 0
fi
if [[ ! -f "$thresholds_file" ]]; then
    echo "bench_guard: thresholds file not found: $thresholds_file" >&2
    exit 2
fi

fail=0
printf '%-58s %14s %14s  %s\n' "BENCH" "MEAN_NS" "THRESHOLD_NS" "VERDICT"
while IFS= read -r line; do
    # Only the `"name" = value` entries under [thresholds].
    case "$line" in
        '"'*) ;;
        *) continue ;;
    esac
    name="${line#\"}"
    name="${name%%\"*}"
    threshold="${line##* = }"
    est_file="$criterion_dir/$name/new/estimates.json"
    if [[ ! -f "$est_file" ]]; then
        printf '%-58s %14s %14s  %s\n' "$name" "MISSING" "$threshold" "FAIL (no estimates)"
        fail=1
        continue
    fi
    mean=$(jq -r '.mean.point_estimate' "$est_file")
    if awk "BEGIN{exit !($mean > $threshold)}"; then
        printf '%-58s %14.0f %14s  %s\n' "$name" "$mean" "$threshold" "FAIL (regression)"
        fail=1
    else
        printf '%-58s %14.0f %14s  %s\n' "$name" "$mean" "$threshold" "ok"
    fi
done < "$thresholds_file"

if [[ "$fail" -ne 0 ]]; then
    echo "bench_guard: regression detected (mean exceeds the 2x threshold)" >&2
fi
exit "$fail"
