//! SQLite throughput benchmarks (`bd-4ttf.1`): the ORM's driver + query
//! paths against a raw `libsqlite3-sys` (C API) baseline running the
//! equivalent statements. FrankenSQLite variants report absolute numbers
//! for the pure-Rust stack.
//!
//! Runs with `cargo bench -p sqlmodel-e2e --bench sqlite_throughput`.

#![allow(unsafe_code)]

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use libsqlite3_sys as ffi;
use serde::{Deserialize, Serialize};
use sqlmodel::prelude::*;
use std::hint::black_box;

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "bench_thru")]
struct Thru {
    #[sqlmodel(primary_key, auto_increment)]
    id: Option<i64>,
    payload: String,
    bucket: i64,
}

fn thru(i: usize) -> Thru {
    Thru {
        id: None,
        payload: format!("payload-{i}"),
        bucket: (i % 16) as i64,
    }
}

const ROWS: usize = 2_000;

const FIXTURE_DDL: &str = "CREATE TABLE bench_thru (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, bucket INTEGER NOT NULL)";

fn orm_runtime() -> asupersync::runtime::Runtime {
    asupersync::runtime::RuntimeBuilder::current_thread()
        .build()
        .expect("asupersync runtime")
}

fn orm_fixture<C: Connection>(cx: &Cx, rt: &asupersync::runtime::Runtime, conn: &C) {
    rt.block_on(async {
        if let sqlmodel::Outcome::Err(e) = conn.execute(cx, FIXTURE_DDL, &[]).await {
            panic!("fixture ddl failed: {e:?}");
        }
        for i in 0..200usize {
            let mut row = thru(i);
            row.id = Some(i as i64);
            if let sqlmodel::Outcome::Err(e) = insert!(&row).execute(cx, conn).await {
                panic!("fixture seed {i} failed: {e:?}");
            }
        }
    });
}

// ---------------------------------------------------------------------------
// ORM benches
// ---------------------------------------------------------------------------

fn orm_single_inserts(c: &mut Criterion) {
    c.bench_function("sqlite_orm/inserts_2000_single", |b| {
        b.iter_batched(
            || {
                let rt = orm_runtime();
                let cx = Cx::for_testing();
                let conn = sqlmodel_sqlite::SqliteConnection::open_memory().expect(":memory:");
                rt.block_on(async {
                    orm_fixture(&cx, &rt, &conn);
                });
                (rt, cx, conn)
            },
            |(rt, cx, conn)| {
                rt.block_on(async {
                    for i in 0..ROWS {
                        let row = thru(i);
                        if let sqlmodel::Outcome::Err(e) = insert!(&row).execute(&cx, &conn).await {
                            panic!("insert {i} failed: {e:?}");
                        }
                        black_box(());
                    }
                });
            },
            BatchSize::PerIteration,
        );
    });
}

fn orm_batched_inserts(c: &mut Criterion) {
    c.bench_function("sqlite_orm/inserts_2000_batched_x100", |b| {
        b.iter_batched(
            || {
                let rt = orm_runtime();
                let cx = Cx::for_testing();
                let conn = sqlmodel_sqlite::SqliteConnection::open_memory().expect(":memory:");
                rt.block_on(async {
                    orm_fixture(&cx, &rt, &conn);
                });
                let batches: Vec<Vec<Thru>> = (0..20usize)
                    .map(|b| (0..100usize).map(|i| thru(b * 100 + i)).collect())
                    .collect();
                (rt, cx, conn, batches)
            },
            |(rt, cx, conn, batches)| {
                rt.block_on(async {
                    for batch in &batches {
                        if let sqlmodel::Outcome::Err(e) =
                            insert_many!(batch).execute(&cx, &conn).await
                        {
                            panic!("batch insert failed: {e:?}");
                        }
                    }
                    black_box(());
                });
            },
            BatchSize::PerIteration,
        );
    });
}

fn orm_point_selects(c: &mut Criterion) {
    c.bench_function("sqlite_orm/selects_200_by_pk", |b| {
        b.iter_batched(
            || {
                let rt = orm_runtime();
                let cx = Cx::for_testing();
                let conn = sqlmodel_sqlite::SqliteConnection::open_memory().expect(":memory:");
                rt.block_on(async {
                    orm_fixture(&cx, &rt, &conn);
                });
                (rt, cx, conn)
            },
            |(rt, cx, conn)| {
                rt.block_on(async {
                    for i in 0..200usize {
                        match select!(Thru)
                            .filter(Expr::col("id").eq(i as i64))
                            .one_or_none(&cx, &conn)
                            .await
                        {
                            sqlmodel::Outcome::Ok(Some(row)) => {
                                black_box(row.payload.as_str());
                            }
                            other => panic!("select {i} failed: {other:?}"),
                        }
                    }
                });
            },
            BatchSize::PerIteration,
        );
    });
}

fn franken_single_inserts(c: &mut Criterion) {
    c.bench_function("franken_orm/inserts_500_single", |b| {
        b.iter_batched(
            || {
                let rt = orm_runtime();
                let cx = Cx::for_testing();
                let conn =
                    sqlmodel_frankensqlite::FrankenConnection::open_memory().expect(":memory:");
                rt.block_on(async {
                    orm_fixture(&cx, &rt, &conn);
                });
                (rt, cx, conn)
            },
            |(rt, cx, conn)| {
                rt.block_on(async {
                    for i in 0..500usize {
                        let row = thru(i);
                        if let sqlmodel::Outcome::Err(e) = insert!(&row).execute(&cx, &conn).await {
                            panic!("insert {i} failed: {e:?}");
                        }
                        black_box(());
                    }
                });
            },
            BatchSize::PerIteration,
        );
    });
}

// ---------------------------------------------------------------------------
// Native C-API baseline
// ---------------------------------------------------------------------------

struct RawDb {
    db: *mut ffi::sqlite3,
}

impl RawDb {
    fn open_memory() -> Self {
        let path = std::ffi::CString::new(":memory:").expect("nul");
        let mut db: *mut ffi::sqlite3 = std::ptr::null_mut();
        // SAFETY: path is null-terminated and db is a valid pointer.
        let rc = unsafe { ffi::sqlite3_open(path.as_ptr(), &raw mut db) };
        assert_eq!(rc, ffi::SQLITE_OK, "sqlite3_open failed: rc={rc}");
        Self { db }
    }

    fn exec(&self, sql: &str) {
        let csql = std::ffi::CString::new(sql).expect("sql contains nul");
        let mut err: *mut std::os::raw::c_char = std::ptr::null_mut();
        // SAFETY: self.db is an open connection and csql is null-terminated.
        let rc = unsafe {
            ffi::sqlite3_exec(
                self.db,
                csql.as_ptr(),
                None,
                std::ptr::null_mut(),
                &raw mut err,
            )
        };
        if rc != ffi::SQLITE_OK {
            // SAFETY: self.db is an open connection.
            let msg = unsafe { ffi::sqlite3_errmsg(self.db) };
            let msg = if msg.is_null() {
                "unknown".to_string()
            } else {
                // SAFETY: sqlite3_errmsg returns a valid C string.
                unsafe { std::ffi::CStr::from_ptr(msg) }
                    .to_string_lossy()
                    .into_owned()
            };
            panic!("sqlite3_exec failed: rc={rc}: {msg}");
        }
    }

    /// Prepares `sql` once and runs it `n` times binding (text, int) pairs.
    fn run_bound_inserts(&self, sql: &str, rows: usize) {
        // SAFETY: self.db is open, csql is null-terminated, and bindings/step follow sqlite contract.
        unsafe {
            let mut stmt: *mut ffi::sqlite3_stmt = std::ptr::null_mut();
            let csql = std::ffi::CString::new(sql).expect("sql contains nul");
            let rc = ffi::sqlite3_prepare_v2(
                self.db,
                csql.as_ptr(),
                -1,
                &raw mut stmt,
                std::ptr::null_mut(),
            );
            assert_eq!(rc, ffi::SQLITE_OK, "prepare failed: rc={rc}");
            let transient: Option<unsafe extern "C" fn(*mut std::os::raw::c_void)> =
                std::mem::transmute(-1isize);
            for i in 0..rows {
                let payload = format!("payload-{i}");
                let bucket = (i % 16) as i64;
                ffi::sqlite3_reset(stmt);
                ffi::sqlite3_clear_bindings(stmt);
                ffi::sqlite3_bind_text(
                    stmt,
                    1,
                    payload.as_ptr().cast(),
                    i32::try_from(payload.len()).expect("payload length fits i32"),
                    transient,
                );
                ffi::sqlite3_bind_int64(stmt, 2, bucket);
                let rc = ffi::sqlite3_step(stmt);
                assert_eq!(rc, ffi::SQLITE_DONE, "step failed: rc={rc}");
            }
            ffi::sqlite3_finalize(stmt);
        }
    }

    /// Prepares `sql` (point select by id) and steps it `n` times.
    fn run_point_selects(&self, sql: &str, rows: usize) {
        // SAFETY: self.db is open, csql is null-terminated, and bindings/step follow sqlite contract.
        unsafe {
            let mut stmt: *mut ffi::sqlite3_stmt = std::ptr::null_mut();
            let csql = std::ffi::CString::new(sql).expect("sql contains nul");
            let rc = ffi::sqlite3_prepare_v2(
                self.db,
                csql.as_ptr(),
                -1,
                &raw mut stmt,
                std::ptr::null_mut(),
            );
            assert_eq!(rc, ffi::SQLITE_OK, "prepare failed: rc={rc}");
            for i in 0..rows as i64 {
                ffi::sqlite3_reset(stmt);
                ffi::sqlite3_bind_int64(stmt, 1, i);
                let rc = ffi::sqlite3_step(stmt);
                assert!(rc == ffi::SQLITE_ROW || rc == ffi::SQLITE_DONE, "rc={rc}");
            }
            ffi::sqlite3_finalize(stmt);
        }
    }
}

impl Drop for RawDb {
    fn drop(&mut self) {
        // SAFETY: self.db was opened by sqlite3_open and is closed on drop.
        unsafe {
            ffi::sqlite3_close(self.db);
        }
    }
}

fn native_single_inserts(c: &mut Criterion) {
    c.bench_function("native_c_api/inserts_2000_single", |b| {
        b.iter_batched(
            || {
                let db = RawDb::open_memory();
                db.exec(FIXTURE_DDL);
                db
            },
            |db| {
                db.run_bound_inserts(
                    "INSERT INTO bench_thru (payload, bucket) VALUES (?1, ?2)",
                    ROWS,
                );
            },
            BatchSize::PerIteration,
        );
    });
}

fn native_point_selects(c: &mut Criterion) {
    c.bench_function("native_c_api/selects_200_by_pk", |b| {
        b.iter_batched(
            || {
                let db = RawDb::open_memory();
                db.exec(FIXTURE_DDL);
                db.exec("INSERT INTO bench_thru (id, payload, bucket) VALUES (0, 'x', 0)");
                db
            },
            |db| {
                db.run_point_selects(
                    "SELECT id, payload, bucket FROM bench_thru WHERE id = ?1",
                    200,
                );
            },
            BatchSize::PerIteration,
        );
    });
}

criterion_group!(
    benches,
    orm_single_inserts,
    orm_batched_inserts,
    orm_point_selects,
    franken_single_inserts,
    native_single_inserts,
    native_point_selects
);
criterion_main!(benches);
