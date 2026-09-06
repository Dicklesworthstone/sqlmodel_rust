//! SQL emission benchmarks (`bd-4ttf.1`): builder construction + SQL text
//! generation for the shapes the ORM emits in practice, across dialects.
//!
//! A counting global allocator reports bytes allocated per iteration so the
//! "builders are zero-cost" claim is checked, not just wall time.
//!
//! Runs with `cargo bench -p sqlmodel-e2e --bench sql_emission`.
#![allow(unsafe_code)]
#![allow(unsafe_op_in_unsafe_fn)]

use criterion::{Criterion, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use sqlmodel::SchemaBuilder;
use sqlmodel::prelude::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};

struct CountingAlloc;

static ALLOCATED: AtomicU64 = AtomicU64::new(0);

// SAFETY: CountingAlloc delegates memory allocation to System and tracks bytes allocated.
unsafe impl GlobalAlloc for CountingAlloc {
    // SAFETY: Forwarded directly to System allocator.
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATED.fetch_add(layout.size() as u64, Ordering::Relaxed);
        System.alloc(layout)
    }
    // SAFETY: Forwarded directly to System allocator.
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

fn allocated_during<F: FnOnce()>(f: F) -> u64 {
    let before = ALLOCATED.load(Ordering::Relaxed);
    f();
    ALLOCATED.load(Ordering::Relaxed) - before
}

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "bench_heroes")]
struct Hero {
    #[sqlmodel(primary_key, auto_increment)]
    id: Option<i64>,
    #[sqlmodel(unique)]
    name: String,
    #[sqlmodel(nullable)]
    secret_identity: Option<String>,
    #[sqlmodel(foreign_key = "bench_teams.id")]
    team_id: Option<i64>,
    #[sqlmodel(default = "0")]
    power_level: i64,
    #[sqlmodel(nullable)]
    home_city: Option<String>,
    #[sqlmodel(default = "false")]
    is_active: bool,
}

fn hero(id: i64, name: &str) -> Hero {
    Hero {
        id: Some(id),
        name: name.to_owned(),
        secret_identity: Some(format!("{name} Prime")),
        team_id: Some(id % 7),
        power_level: 9000 + id,
        home_city: Some("Metroville".to_owned()),
        is_active: true,
    }
}

fn select_filtered_per_dialect(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_emission");
    for dialect in [Dialect::Sqlite, Dialect::Postgres, Dialect::Mysql] {
        group.bench_function(format!("select_filtered/{dialect:?}"), |b| {
            b.iter(|| {
                let (sql, params) = select!(Hero)
                    .filter(Expr::col("power_level").gt(9000))
                    .filter(Expr::col("home_city").eq("Metroville"))
                    .order_by(Expr::col("name").asc())
                    .limit(50)
                    .build_with_dialect(dialect);
                black_box((sql, params))
            });
        });
    }
    group.finish();
}

fn insert_many_emission(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_emission_batches");
    for batch in [10usize, 100, 500] {
        let heroes: Vec<Hero> = (0..batch as i64).map(|i| hero(i, "bench")).collect();
        group.bench_function(format!("insert_many_{batch}"), |b| {
            b.iter(|| {
                let (sql, params) =
                    insert_many!(black_box(&heroes)).build_with_dialect(Dialect::Sqlite);
                black_box((sql, params))
            });
        });
    }
    group.finish();
}

fn create_table_emission(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_emission");
    group.bench_function("create_table", |b| {
        b.iter(|| {
            let stmts = SchemaBuilder::new()
                .dialect(Dialect::Sqlite)
                .create_table::<Hero>()
                .build();
            black_box(stmts)
        });
    });
    group.finish();
}

fn select_filtered_allocation_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_emission_allocations");
    group.bench_function("select_filtered_bytes_allocated", |b| {
        b.iter_custom(|iters| {
            let mut total = 0u64;
            for _ in 0..iters {
                total += allocated_during(|| {
                    let (sql, params) = select!(Hero)
                        .filter(Expr::col("power_level").gt(9000))
                        .order_by(Expr::col("name").asc())
                        .limit(50)
                        .build_with_dialect(Dialect::Sqlite);
                    black_box((sql, params));
                });
            }
            std::time::Duration::from_nanos(total)
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    select_filtered_per_dialect,
    insert_many_emission,
    create_table_emission,
    select_filtered_allocation_count
);
criterion_main!(benches);
