//! Pool acquire benchmarks (`bd-4ttf.1`): acquire/release cycle cost against
//! a pool of real C-SQLite connections, sequentially (uncontended p50
//! proxy) and under 4/16 contending threads.
//!
//! Runs with `cargo bench -p sqlmodel-e2e --bench pool_acquire`.

use criterion::{Criterion, criterion_group, criterion_main};
use sqlmodel::prelude::*;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

type SqlitePool = Pool<sqlmodel_sqlite::SqliteConnection>;

fn runtime() -> asupersync::runtime::Runtime {
    asupersync::runtime::RuntimeBuilder::current_thread()
        .build()
        .expect("asupersync runtime")
}

fn new_pool(max_size: usize) -> Arc<SqlitePool> {
    Arc::new(Pool::with_timer_driver(
        PoolConfig::new(max_size)
            .test_on_checkout(false)
            .acquire_timeout(10_000),
        asupersync::time::TimerDriverHandle::with_wall_clock(),
    ))
}

async fn acquire_once(
    cx: &Cx,
    pool: &SqlitePool,
) -> Outcome<sqlmodel_pool::PooledConnection<sqlmodel_sqlite::SqliteConnection>, Error> {
    pool.acquire(cx, || async {
        match sqlmodel_sqlite::SqliteConnection::open_memory() {
            Ok(conn) => Outcome::Ok(conn),
            Err(e) => Outcome::Err(e),
        }
    })
    .await
}

fn acquire_release_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("pool_acquire");
    for max_size in [1usize, 16] {
        group.bench_function(format!("sequential/max_{max_size}"), |b| {
            let rt = runtime();
            let cx = Cx::for_testing();
            let pool = new_pool(max_size);

            // Warm the pool to `max_size` leases so the bench measures the
            // acquire/release fast path, not connection creation.
            rt.block_on(async {
                let mut warm: Vec<
                    sqlmodel_pool::PooledConnection<sqlmodel_sqlite::SqliteConnection>,
                > = Vec::new();
                for _ in 0..max_size {
                    match acquire_once(&cx, &pool).await {
                        Outcome::Ok(lease) => warm.push(lease),
                        _other => panic!("warm acquire failed with non-Ok outcome"),
                    }
                }
                warm.clear();
            });

            b.iter(|| {
                rt.block_on(async {
                    match acquire_once(&cx, &pool).await {
                        Outcome::Ok(lease) => black_box(lease),
                        _other => panic!("acquire failed with non-Ok outcome"),
                    }
                });
            });
        });
    }
    group.finish();
}

/// Contended acquire/release: `threads` threads each pull their share of
/// leases from the shared pool; wall time is reported per operation.
fn acquire_release_contended(c: &mut Criterion) {
    let mut group = c.benchmark_group("pool_acquire_contended");
    for threads in [4usize, 16] {
        group.bench_function(format!("threads_{threads}"), |b| {
            b.iter_custom(|iters| {
                let pool = new_pool(threads.max(4));
                let start = Instant::now();
                std::thread::scope(|scope| {
                    let per_thread = iters / threads as u64;
                    for _ in 0..threads {
                        let pool = Arc::clone(&pool);
                        scope.spawn(move || {
                            let rt = runtime();
                            let cx = Cx::for_testing();
                            rt.block_on(async {
                                for _ in 0..per_thread {
                                    if let Outcome::Err(e) = pool
                                        .acquire(&cx, || async {
                                            match sqlmodel_sqlite::SqliteConnection::open_memory() {
                                                Ok(conn) => Outcome::Ok(conn),
                                                Err(e) => Outcome::Err(e),
                                            }
                                        })
                                        .await
                                    {
                                        panic!("contended acquire failed: {e}");
                                    }
                                    black_box(());
                                }
                            });
                        });
                    }
                });
                start.elapsed() / u32::try_from(iters).unwrap_or(1)
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    acquire_release_sequential,
    acquire_release_contended
);
criterion_main!(benches);
