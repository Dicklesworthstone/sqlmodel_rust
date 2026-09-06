# sqlmodel-e2e

All-driver end-to-end proof crate for SQLModel Rust. **Not published** — it
exists to run the ORM stack against real drivers.

## What lives here

One scenario per file, each run against C SQLite (memory and file),
FrankenSQLite, and — when `SQLMODEL_TEST_POSTGRES_URL` /
`SQLMODEL_TEST_MYSQL_URL` / `SQLMODEL_TEST_MARIADB_URL` are set — PostgreSQL,
MySQL, and MariaDB. Absent network drivers are reported, never skipped
silently.

| File | Proves |
|---|---|
| `smoke.rs` | The ORM round-trips on every driver |
| `expressions.rs` | Expression DSL shapes |
| `operations.rs` | INSERT/UPDATE/DELETE semantics incl. MySQL RETURNING fallback |
| `types.rs` | Every external value type round-trips |
| `attributes.rs` | Every field attribute survives DDL + introspection + runtime |
| `migrations.rs` | Raw-SQL migration runner behavior |
| `schema_fixpoint.rs` | Create → evolve → rollback fixpoints per driver |
| `schema_oracle.rs` | Metamorphic laws: fixpoint, commutation, involution, cross-dialect agreement, 600 generated schemas |
| `session.rs` | Unit of work, identity map, relations, cascades |
| `pool.rs` | Fan-out, panic safety, cancelled drains |
| `concurrent_writers.rs` | Concurrent writers under `TransactionMode::Concurrent` |
| `sqlite_differential.rs` | One script on C SQLite and FrankenSQLite in lockstep |
| `cancellation_sweep.rs` | Every operation cancels at every checkpoint without partial state |
| `doc_truth.rs` | README/AGENTS.md document-truth guard |
| `golden_sql.rs` | Every builder statement against `golden/<dialect>/*.sql` (regenerate with `SQLMODEL_UPDATE_GOLDEN=1`) |
| `franken_mvcc_e2e.rs` | `BEGIN CONCURRENT` under real contention: disjoint/overlapping writers, retries, cancellation, reader snapshots, C-SQLite control |
| `benches/` | Criterion benches: SQL emission, row conversion, SQLite throughput, pool acquire, session flush |
| `benches/thresholds.toml` | 2x regression thresholds enforced by CI (see below) |

Scenarios that share table names must run sequentially inside one `#[test]`:
the network databases are shared, and two test threads racing to drop and
recreate the same table fail on PostgreSQL with a catalog conflict.

Run with:

```bash
cargo test -p sqlmodel-e2e
```

Against local Docker databases this must run on this machine (see the RCH
escape hatch in the root AGENTS.md).

## Bench regression guard

CI (the `size-and-startup` job) runs the CPU-only benches (`sql_emission`,
`row_conversion`) and fails when a criterion mean exceeds the 2x-margin
threshold in `benches/thresholds.toml`. I/O-bound benches (SQLite throughput,
pool acquire, session flush) are deliberately not guarded — the monthly
`Bench trend report` workflow records the full suite and uploads the criterion
output as a 30-day artifact.

Updating thresholds deliberately (mirrors the golden-SQL rule):

```bash
cargo bench -p sqlmodel-e2e --bench sql_emission --bench row_conversion -- --noplot
SQLMODEL_UPDATE_THRESHOLDS=1 scripts/bench_guard.sh target/criterion \
    > crates/sqlmodel-e2e/benches/thresholds.toml
```

The update prints RAW means: multiply stable values by 2, note the baseline
machine, and commit the toml with the justification. Thresholds are verified
against the committed baseline (RCH worker hz3, 2026-09-06); a slower runner
gets more headroom from the same 2x margin, a faster one less — if CI fails
without a real regression, re-record on the new baseline machine.
