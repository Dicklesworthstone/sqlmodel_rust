//! Row conversion benchmarks (`bd-4ttf.1`): derive-generated
//! `to_row`/`from_row` for a 13-column model versus a hand-written baseline
//! doing the identical conversions — the README's "as fast as hand-written"
//! claim, measured directly.
//!
//! Runs with `cargo bench -p sqlmodel-e2e --bench row_conversion`.

// `from_row` returns `sqlmodel::Error`, which is large by design (the same
// pattern `sqlmodel-core` allows internally); benching it is the point.
#![allow(clippy::result_large_err)]

use criterion::{Criterion, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use sqlmodel::prelude::*;
use std::hint::black_box;

#[derive(sqlmodel::Model, Debug, Clone, Serialize, Deserialize)]
#[sqlmodel(table = "bench_wide")]
struct Wide {
    #[sqlmodel(primary_key, auto_increment)]
    id: Option<i64>,
    c01: String,
    c02: String,
    #[sqlmodel(nullable)]
    c03: Option<String>,
    c04: i64,
    #[sqlmodel(nullable)]
    c05: Option<i64>,
    c06: f64,
    c07: bool,
    c08: String,
    #[sqlmodel(nullable)]
    c09: Option<Vec<u8>>,
    c10: i32,
    #[sqlmodel(nullable)]
    c11: Option<f64>,
    c12: bool,
}

fn wide(id: i64) -> Wide {
    Wide {
        id: Some(id),
        c01: "text01".to_owned(),
        c02: "text02".to_owned(),
        c03: Some("text03".to_owned()),
        c04: 404,
        c05: Some(-405),
        c06: 4.25,
        c07: true,
        c08: "text08".to_owned(),
        c09: Some(vec![1, 2, 3, 4]),
        c10: -410,
        c11: Some(1.5),
        c12: false,
    }
}

/// The column names the derive emits, in declaration order.
const COLS: [&str; 13] = [
    "id", "c01", "c02", "c03", "c04", "c05", "c06", "c07", "c08", "c09", "c10", "c11", "c12",
];

/// Hand-written baseline packing the same 13 values (`Option::None` becomes
/// `Value::Null`, mirroring the derive).
fn hand_to_row(w: &Wide) -> Vec<Value> {
    vec![
        Value::BigInt(w.id.unwrap_or_default()),
        Value::Text(w.c01.clone()),
        Value::Text(w.c02.clone()),
        match &w.c03 {
            Some(v) => Value::Text(v.clone()),
            None => Value::Null,
        },
        Value::BigInt(w.c04),
        match w.c05 {
            Some(v) => Value::BigInt(v),
            None => Value::Null,
        },
        Value::Double(w.c06),
        Value::Bool(w.c07),
        Value::Text(w.c08.clone()),
        match &w.c09 {
            Some(v) => Value::Bytes(v.clone()),
            None => Value::Null,
        },
        Value::Int(w.c10),
        match w.c11 {
            Some(v) => Value::Double(v),
            None => Value::Null,
        },
        Value::Bool(w.c12),
    ]
}

/// Hand-written baseline reading the same 13 columns back via the row's
/// `FromValue` path.
fn hand_from_row(row: &Row) -> Wide {
    Wide {
        id: row.get_as::<Option<i64>>(0).ok().flatten().or(Some(0)),
        c01: row.get_as::<String>(1).unwrap_or_default(),
        c02: row.get_as::<String>(2).unwrap_or_default(),
        c03: row.get_as::<Option<String>>(3).ok().flatten(),
        c04: row.get_as::<i64>(4).unwrap_or_default(),
        c05: row.get_as::<Option<i64>>(5).ok().flatten(),
        c06: row.get_as::<f64>(6).unwrap_or_default(),
        c07: row.get_as::<bool>(7).unwrap_or_default(),
        c08: row.get_as::<String>(8).unwrap_or_default(),
        c09: row.get_as::<Option<Vec<u8>>>(9).ok().flatten(),
        c10: row.get_as::<i32>(10).unwrap_or_default(),
        c11: row.get_as::<Option<f64>>(11).ok().flatten(),
        c12: row.get_as::<bool>(12).unwrap_or_default(),
    }
}

fn build_row(values: &[Value]) -> Row {
    let names: Vec<String> = COLS.iter().map(|s| s.to_string()).collect();
    Row::new(names, values.to_vec())
}

fn derive_to_row(c: &mut Criterion) {
    let w = wide(7);
    c.bench_function("row_conversion/derive_to_row", |b| {
        b.iter(|| black_box(w.to_row()));
    });
}

fn derive_from_row(c: &mut Criterion) {
    let w = wide(7);
    let values = hand_to_row(&w);
    let row = build_row(&values);
    c.bench_function("row_conversion/derive_from_row", |b| {
        b.iter(|| black_box(Wide::from_row(&row)));
    });
    black_box(row);
}

fn hand_to_row_bench(c: &mut Criterion) {
    let w = wide(7);
    c.bench_function("row_conversion/hand_to_row", |b| {
        b.iter(|| black_box(hand_to_row(&w)));
    });
}

fn hand_from_row_bench(c: &mut Criterion) {
    let w = wide(7);
    let values = hand_to_row(&w);
    let row = build_row(&values);
    c.bench_function("row_conversion/hand_from_row", |b| {
        b.iter(|| black_box(hand_from_row(&row)));
    });
    black_box(w);
}

/// Derive vs hand side by side in one group so criterion prints the delta.
fn to_row_ratio(c: &mut Criterion) {
    let w = wide(7);
    let mut group = c.benchmark_group("row_conversion_ratio_to_row");
    group.bench_function("derive", |b| b.iter(|| black_box(w.to_row())));
    group.bench_function("hand", |b| b.iter(|| black_box(hand_to_row(&w))));
    group.finish();
    black_box(&w);
}

criterion_group!(
    benches,
    derive_to_row,
    derive_from_row,
    hand_to_row_bench,
    hand_from_row_bench,
    to_row_ratio
);
criterion_main!(benches);
