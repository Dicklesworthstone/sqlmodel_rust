//! Session flush benchmarks (`bd-4ttf.1`): flush of 1k new objects and
//! flush+delete of 1k objects against an in-memory C-SQLite database.
//!
//! Runs with `cargo bench -p sqlmodel-e2e --bench session_flush`.

use std::hint::black_box;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use sqlmodel::Session;
use sqlmodel::prelude::*;
use sqlmodel_sqlite::SqliteConnection;

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "bench_flush")]
struct BenchRow {
    #[sqlmodel(primary_key, auto_increment)]
    id: Option<i64>,
    payload: String,
    bucket: i64,
}

fn bench_row(i: usize) -> BenchRow {
    BenchRow {
        id: None,
        payload: format!("payload-{i}"),
        bucket: (i % 16) as i64,
    }
}

fn runtime() -> asupersync::runtime::Runtime {
    asupersync::runtime::RuntimeBuilder::current_thread()
        .build()
        .expect("asupersync runtime")
}

const ROWS: usize = 1_000;

/// Flush of 1k new objects: the whole BEGIN + INSERT loop is the measured
/// operation (per-iteration setup builds a fresh session with the rows
/// already added).
fn bench_flush_new(c: &mut Criterion) {
    c.bench_function("session_flush/new_1000", |b| {
        b.iter_batched(
            || {
                let rt = runtime();
                let cx = Cx::for_testing();
                let conn = SqliteConnection::open_memory().expect(":memory:");
                let mut s = Session::new(conn);
                for i in 0..ROWS {
                    s.add(&bench_row(i));
                }
                (rt, cx, s)
            },
            |(rt, cx, mut s)| {
                rt.block_on(async {
                    if let sqlmodel::Outcome::Err(e) = s.flush(&cx).await {
                        panic!("flush failed: {e:?}");
                    }
                    black_box(s.pending_counts());
                });
            },
            BatchSize::PerIteration,
        )
    });
}

/// Flush + delete of 1k objects: flush inserts them, a second flush deletes
/// them; the second flush is the measured operation.
fn bench_flush_deletes(c: &mut Criterion) {
    c.bench_function("session_flush/deletes_1000", |b| {
        b.iter_batched(
            || {
                let rt = runtime();
                let cx = Cx::for_testing();
                let conn = SqliteConnection::open_memory().expect(":memory:");
                let mut s = Session::new(conn);
                for i in 0..ROWS {
                    s.add(&bench_row(i));
                }
                rt.block_on(async {
                    if let sqlmodel::Outcome::Err(e) = s.flush(&cx).await {
                        panic!("setup flush failed: {e:?}");
                    }
                });
                // Mark everything for deletion.
                let doomed: Vec<BenchRow> = (0..ROWS)
                    .map(|i| BenchRow {
                        id: Some(i as i64 + 1),
                        payload: format!("payload-{i}"),
                        bucket: (i % 16) as i64,
                    })
                    .collect();
                for row in &doomed {
                    s.delete(row);
                }
                (rt, cx, s)
            },
            |(rt, cx, mut s)| {
                rt.block_on(async {
                    if let sqlmodel::Outcome::Err(e) = s.flush(&cx).await {
                        panic!("delete flush failed: {e:?}");
                    }
                    black_box(s.pending_counts());
                });
            },
            BatchSize::PerIteration,
        )
    });
}

criterion_group!(benches, bench_flush_new, bench_flush_deletes);
criterion_main!(benches);
