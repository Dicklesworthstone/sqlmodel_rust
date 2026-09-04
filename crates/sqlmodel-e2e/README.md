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

Scenarios that share table names must run sequentially inside one `#[test]`:
the network databases are shared, and two test threads racing to drop and
recreate the same table fail on PostgreSQL with a catalog conflict.

Run with:

```bash
cargo test -p sqlmodel-e2e
```

Against local Docker databases this must run on this machine (see the RCH
escape hatch in the root AGENTS.md).
