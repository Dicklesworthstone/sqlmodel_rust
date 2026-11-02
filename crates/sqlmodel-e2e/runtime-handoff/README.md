# PostgreSQL runtime-handoff qualification (issue #43)

`main.rs` is a production-only witness for
[SQLModel issue #43](https://github.com/Dicklesworthstone/sqlmodel_rust/issues/43)
and [Asupersync issue #85](https://github.com/Dicklesworthstone/asupersync/issues/85).
The runner `scripts/qualify_postgres_runtime_handoff.py` builds it in an isolated
Cargo workspace with only `sqlmodel-core`, `sqlmodel-pool`, `sqlmodel-postgres`,
and Asupersync. It rejects `test-internals`, checks the resolved source and
runtime version, and builds the inspected dependency graph with `--locked`.
Every PostgreSQL context comes from
`Runtime::request_cx_with_budget(Budget::INFINITE)`. The existing all-driver
`tests/pool.rs` scenarios remain in place.

| Cases | Required behavior |
|---|---|
| `current-thread-normal`, `current-thread-panic` | Join the holder thread and drop its current-thread runtime, then reuse the original PostgreSQL session from a second current-thread runtime |
| `two-worker-normal`, `two-worker-panic` | The same sequence with exactly `RuntimeBuilder::new().worker_threads(2)` on both sides |
| `stale-socket-current-thread`, `stale-socket-two-worker` | Install a pending TCP read in the holder runtime, abandon that read, join the holder, and deliver the peer's byte only after the consumer also returns `Pending` |

The PostgreSQL cases assert the original backend PID, one created connection,
default checkout validation, `SELECT 1`, consistent idle/active/pending counts,
and a complete drain. An unrelated holder panic fails the intentional-panic
case. The TCP witness forces stale registration and delayed readiness without
using sleeps to choose the interleaving. It is a runtime witness, separate from
the real-PostgreSQL proof.

## Running the matrix

Run from the repository root with Python 3.11+ and the pinned Rust toolchain:

```bash
export SQLMODEL_TEST_POSTGRES_URL=postgres://sqlmodel:sqlmodel@127.0.0.1:5432/sqlmodel_test

# Current repository sources and their directly resolved runtime.
python3 scripts/qualify_postgres_runtime_handoff.py --source workspace

# Published SQLModel 0.5.1 / Asupersync 0.5.0 negative control.
python3 scripts/qualify_postgres_runtime_handoff.py \
  --source registry --sqlmodel-version 0.5.1 --asupersync-version 0.5.0 \
  --case current-thread-normal --case current-thread-panic \
  --case two-worker-normal --case two-worker-panic

# Candidate PostgreSQL integration, with the override applied only to a copy.
python3 scripts/qualify_postgres_runtime_handoff.py \
  --source workspace --stage-asupersync-version 0.6.1
```

The earlier published control is available with `--source registry
--sqlmodel-version 0.4.3 --asupersync-version 0.4.10` and the same four PostgreSQL
case flags. Missing PostgreSQL configuration is an error for requested database
cases. Selecting only the `stale-socket-*` cases needs no database. The fixture
uses default driver features and a non-TLS database URL without query parameters.

Each case has its own **60-second process deadline, excluding compilation**.
Timeout kills and reaps only that child, records `timed_out`, and continues to
the controls. The runner returns failure for any failing or incomplete case;
it never converts an expected negative control into a passing case. CI checks
the published negative-control matrix separately. Evidence is retained in the
printed directory (or a new `--evidence-dir`): the fixture source, generated
manifest and lock, dependency/feature graph, source commit and hashes, compiler
logs, per-case logs, and `summary.json`.

## Integration and release status

[Asupersync 0.6.1 is published](https://crates.io/crates/asupersync/0.6.1).
Its crate identifies source commit
[`4d0ef0e79a6efa36023f32fcd81615122c809d3d`](https://github.com/Dicklesworthstone/asupersync/commit/4d0ef0e79a6efa36023f32fcd81615122c809d3d),
which contains the socket migration fixes from
[`fec7be6d7608b14b5448f24a69124e7bb3b4ee34`](https://github.com/Dicklesworthstone/asupersync/commit/fec7be6d7608b14b5448f24a69124e7bb3b4ee34),
[`8ff71ca3e5b3f2d8ab62fee02d9d82f87f97a131`](https://github.com/Dicklesworthstone/asupersync/commit/8ff71ca3e5b3f2d8ab62fee02d9d82f87f97a131),
and [`bde01fcfd6bf9faab55aaedd3e9678a573644234`](https://github.com/Dicklesworthstone/asupersync/commit/bde01fcfd6bf9faab55aaedd3e9678a573644234).
The published archive SHA-256 is
`20d7b105734d394600108baaf7a59d782ef7677a2e4c2c91e814dc208d979fa4`.

SQLModel's all-driver release graph still requires a coordinated port: the
published [FrankenSQLite 0.4.13 family](https://crates.io/crates/fsqlite/0.4.13)
requires Asupersync `^0.5.0`. Advancing only SQLModel's root dependency would
violate the existing single-runtime release gate. The staged mode therefore
copies the SQLModel source tree and changes only that copy's workspace runtime
requirement. Its summary explicitly labels the result **staged workspace**,
and its production PostgreSQL graph must resolve exactly one runtime. The
repository dependency and release gate remain unchanged.

These are distinct claims: bounded checkout protection in repository code;
successful execution of the staged PostgreSQL integration against published
Asupersync; and qualification of a published SQLModel release. Adding these
witnesses or observing upstream's own tests does not establish the latter two.
The runtime-handoff and PostgreSQL cases must actually execute and their
retained results must be inspected before claiming a successful integration.
