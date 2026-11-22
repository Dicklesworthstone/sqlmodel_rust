//! External-consumer compile & runtime verification for `#[derive(Model)]`,
//! `#[derive(Validate)]`, and `#[derive(SqlEnum)]` (issue #37).
//!
//! Spawns isolated Cargo packages outside the repository workspace so Cargo
//! enforces each consumer's exact dependency declarations without workspace
//! inheritance masking missing direct dependencies:
//!
//! 1. `facade_only`: depends ONLY on `sqlmodel` (no `sqlmodel-core` or `sqlmodel-query`).
//! 2. `renamed_facade`: depends ONLY on `sm = { package = "sqlmodel", ... }`.
//! 3. `direct_core`: depends on `sqlmodel-core` + `sqlmodel-macros` (no `sqlmodel`).
//! 4. `renamed_core`: depends on `sm_core = { package = "sqlmodel-core", ... }` + `sqlmodel-macros`.
//! 5. `compile_fail_control`: depends ONLY on `sqlmodel` and supplies an invalid
//!    `#[sqlmodel(crate = "nonexistent_sqlmodel_crate")]` override; must fail to
//!    compile, proving the harness genuinely type-checks the macro expansion.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("repo root resolves")
}

fn fresh_temp_root() -> PathBuf {
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "sqlmodel-facade-consumer-{}-{seq}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("create temp workspace dir");
    dir
}

fn write_member(root: &Path, name: &str, cargo_toml: &str, main_rs: &str) {
    let pkg = root.join(name);
    fs::create_dir_all(pkg.join("src")).expect("create member src dir");
    fs::write(pkg.join("Cargo.toml"), cargo_toml).expect("write member Cargo.toml");
    fs::write(pkg.join("src/main.rs"), main_rs).expect("write member src/main.rs");
}

fn run_cargo(temp_root: &Path, args: &[&str]) -> std::process::Output {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    Command::new(cargo)
        .current_dir(temp_root)
        .env("CARGO_TARGET_DIR", temp_root.join("target"))
        .env("RCH_SHIM_LOCAL_IDE", "1")
        .env("CARGO_BUILD_JOBS", "4")
        .args(args)
        .output()
        .expect("invoke cargo for external consumer test")
}

#[test]
fn external_consumers_derive_without_undeclared_core_dependency() {
    let root = repo_root();
    let temp = fresh_temp_root();

    let sqlmodel_path = root.join("crates/sqlmodel");
    let core_path = root.join("crates/sqlmodel-core");
    let macros_path = root.join("crates/sqlmodel-macros");

    // Share the repo's Cargo.lock and rust-toolchain.toml so the isolated
    // workspace resolves offline against the already-cached registry crates.
    fs::copy(root.join("Cargo.lock"), temp.join("Cargo.lock")).expect("copy Cargo.lock");
    fs::copy(
        root.join("rust-toolchain.toml"),
        temp.join("rust-toolchain.toml"),
    )
    .expect("copy rust-toolchain.toml");

    fs::write(
        temp.join("Cargo.toml"),
        r#"[workspace]
resolver = "2"
members = [
    "facade_only",
    "renamed_facade",
    "direct_core",
    "renamed_core",
    "compile_fail_control",
]
"#,
    )
    .expect("write root Cargo.toml");

    // 1. Facade-only consumer: depends solely on `sqlmodel`.
    write_member(
        &temp,
        "facade_only",
        &format!(
            r#"[package]
name = "facade_only"
version = "0.0.0"
edition = "2024"

[dependencies]
sqlmodel = {{ path = {:?} }}
"#,
            sqlmodel_path
        ),
        r#"use sqlmodel::prelude::*;

#[derive(sqlmodel::SqlEnum, Debug, Clone, PartialEq)]
enum Kind {
    Draft,
    #[sqlmodel(rename = "live")]
    Published,
}

#[derive(sqlmodel::Model, sqlmodel::Validate, Debug, Clone)]
#[sqlmodel(table = "notes")]
struct Note {
    #[sqlmodel(primary_key)]
    id: i64,
    #[validate(min_length = 1)]
    title: String,
    #[sqlmodel(hybrid, sql = "UPPER(title)")]
    upper_title: String,
}

fn main() {
    let note = Note {
        id: 7,
        title: "hello".into(),
        upper_title: String::new(),
    };
    assert!(note.validate().is_ok());
    assert_eq!(<Note as Model>::TABLE_NAME, "notes");
    assert_eq!(note.primary_key_value(), vec![Value::BigInt(7)]);
    let row_pairs = note.to_row();
    let row = Row::new(
        row_pairs.iter().map(|(k, _)| (*k).to_string()).collect(),
        row_pairs.into_iter().map(|(_, v)| v).collect(),
    );
    let round_trip = Note::from_row(&row).expect("from_row");
    assert_eq!(round_trip.id, 7);
    assert_eq!(round_trip.title, "hello");
    let _expr = Note::upper_title_expr();
    let (sql, params) = select!(Note)
        .filter(Expr::col("id").eq(7))
        .build_with_dialect(Dialect::Postgres);
    assert!(sql.contains("FROM \"notes\""));
    assert_eq!(params, vec![Value::Int(7)]);
    assert_eq!(Kind::Published.to_string(), "live");
}
"#,
    );

    // 2. Renamed facade-only consumer: depends solely on `sm = { package = "sqlmodel" }`.
    write_member(
        &temp,
        "renamed_facade",
        &format!(
            r#"[package]
name = "renamed_facade"
version = "0.0.0"
edition = "2024"

[dependencies]
sm = {{ package = "sqlmodel", path = {:?} }}
"#,
            sqlmodel_path
        ),
        r#"use sm::Model as _;

#[derive(sm::SqlEnum, Debug, Clone, PartialEq)]
enum State {
    Open,
    Closed,
}

#[derive(sm::Model, sm::Validate, Debug, Clone)]
#[sqlmodel(table = "tickets")]
struct Ticket {
    #[sqlmodel(primary_key)]
    id: i64,
    #[validate(min_length = 1)]
    subject: String,
}

fn main() {
    let t = Ticket {
        id: 1,
        subject: "renamed facade".into(),
    };
    assert!(t.validate().is_ok());
    assert_eq!(Ticket::TABLE_NAME, "tickets");
    assert_eq!(State::Open.to_string(), "open");
}
"#,
    );

    // 3. Direct-core consumer: depends on `sqlmodel-core` + `sqlmodel-macros` (no facade).
    write_member(
        &temp,
        "direct_core",
        &format!(
            r#"[package]
name = "direct_core"
version = "0.0.0"
edition = "2024"

[dependencies]
sqlmodel-core = {{ path = {:?} }}
sqlmodel-macros = {{ path = {:?} }}
"#,
            core_path, macros_path
        ),
        r#"use sqlmodel_core::Model as _;

#[derive(sqlmodel_macros::Model, sqlmodel_macros::Validate, Debug)]
#[sqlmodel(table = "items")]
struct Item {
    #[sqlmodel(primary_key)]
    id: i64,
    #[validate(min_length = 1)]
    name: String,
}

fn main() {
    let item = Item {
        id: 3,
        name: "direct".into(),
    };
    assert!(item.validate().is_ok());
    assert_eq!(Item::TABLE_NAME, "items");
}
"#,
    );

    // 4. Renamed direct-core consumer: `sm_core = { package = "sqlmodel-core" }`.
    write_member(
        &temp,
        "renamed_core",
        &format!(
            r#"[package]
name = "renamed_core"
version = "0.0.0"
edition = "2024"

[dependencies]
sm_core = {{ package = "sqlmodel-core", path = {:?} }}
sqlmodel-macros = {{ path = {:?} }}
"#,
            core_path, macros_path
        ),
        r#"use sm_core::Model as _;

#[derive(sqlmodel_macros::Model, sqlmodel_macros::Validate, Debug)]
#[sqlmodel(table = "widgets")]
struct Widget {
    #[sqlmodel(primary_key)]
    id: i64,
    #[validate(min_length = 1)]
    label: String,
}

fn main() {
    let w = Widget {
        id: 9,
        label: "renamed".into(),
    };
    assert!(w.validate().is_ok());
    assert_eq!(Widget::TABLE_NAME, "widgets");
}
"#,
    );

    // 5. Compile-failure control: invalid crate override must fail to compile.
    write_member(
        &temp,
        "compile_fail_control",
        &format!(
            r#"[package]
name = "compile_fail_control"
version = "0.0.0"
edition = "2024"

[dependencies]
sqlmodel = {{ path = {:?} }}
"#,
            sqlmodel_path
        ),
        r#"#[derive(sqlmodel::Model)]
#[sqlmodel(crate = "nonexistent_sqlmodel_crate")]
struct Broken {
    #[sqlmodel(primary_key)]
    id: i64,
}

fn main() {}
"#,
    );

    // All four positive consumers must compile AND execute successfully.
    for pkg in [
        "facade_only",
        "renamed_facade",
        "direct_core",
        "renamed_core",
    ] {
        let out = run_cargo(&temp, &["run", "--offline", "-p", pkg]);
        assert!(
            out.status.success(),
            "external consumer `{pkg}` failed to compile/run:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
    }

    // The negative control must fail to compile with an unresolved crate diagnostic.
    let neg = run_cargo(&temp, &["check", "--offline", "-p", "compile_fail_control"]);
    assert!(
        !neg.status.success(),
        "compile-failure control unexpectedly succeeded; consumer test may not be checking generated paths"
    );
    let neg_stderr = String::from_utf8_lossy(&neg.stderr);
    assert!(
        neg_stderr.contains("nonexistent_sqlmodel_crate"),
        "expected unresolved crate diagnostic for negative control, got:\n{neg_stderr}"
    );

    let _ = fs::remove_dir_all(&temp);
}
