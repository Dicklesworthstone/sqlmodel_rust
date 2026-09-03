use std::time::{Duration, SystemTime, UNIX_EPOCH};

use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome};

use sqlmodel_core::error::QueryErrorKind;
use sqlmodel_core::{Connection, Error, TransactionOps, Value};

use sqlmodel_mysql::{MySqlConfig, SharedMySqlConnection};
use sqlmodel_schema::introspect::{Dialect, Introspector};

const MYSQL_URL_ENV: &str = "SQLMODEL_TEST_MYSQL_URL";

fn mysql_test_config() -> Option<MySqlConfig> {
    let raw = std::env::var(MYSQL_URL_ENV).ok()?;
    let cfg = parse_mysql_url(&raw)?;
    if cfg.database.is_none() {
        eprintln!(
            "skipping MySQL integration tests: {MYSQL_URL_ENV} must include a database name (mysql://user:pass@host:3306/db)"
        );
        return None;
    }
    Some(cfg.connect_timeout(Duration::from_secs(10)))
}

fn parse_mysql_url(url: &str) -> Option<MySqlConfig> {
    let url = url.trim();
    if url.is_empty() {
        return None;
    }

    let rest = url.strip_prefix("mysql://")?;
    let (auth, host_and_path) = rest.split_once('@')?;
    let (user, password) = match auth.split_once(':') {
        Some((u, p)) => (u, Some(p)),
        None => (auth, None),
    };

    let (host_port, db) = match host_and_path.split_once('/') {
        Some((hp, path)) => (hp, Some(path)),
        None => (host_and_path, None),
    };

    let db = db
        .map(|s| s.split_once('?').map_or(s, |(left, _)| left))
        .filter(|s| !s.is_empty());

    let (host, port) = parse_host_port(host_port)?;

    let mut cfg = MySqlConfig::new().host(host).port(port).user(user);
    if let Some(pw) = password.filter(|p| !p.is_empty()) {
        cfg = cfg.password(pw);
    }
    if let Some(db) = db {
        cfg = cfg.database(db);
    }

    Some(cfg)
}

fn parse_host_port(input: &str) -> Option<(&str, u16)> {
    if let Some(rest) = input.strip_prefix('[') {
        let end = rest.find(']')?;
        let host = &rest[..end];
        let after = &rest[end + 1..];
        let port = after
            .strip_prefix(':')
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(3306);
        return Some((host, port));
    }

    match input.rsplit_once(':') {
        Some((host, port_str)) if port_str.chars().all(|c| c.is_ascii_digit()) => {
            Some((host, port_str.parse::<u16>().ok()?))
        }
        _ => Some((input, 3306)),
    }
}

fn unwrap_outcome<T>(outcome: Outcome<T, Error>) -> T {
    match outcome {
        Outcome::Ok(v) => v,
        Outcome::Err(e) => {
            eprintln!("unexpected error: {e}");
            std::process::abort();
        }
        Outcome::Cancelled(r) => {
            eprintln!("cancelled: {r:?}");
            std::process::abort();
        }
        Outcome::Panicked(p) => {
            eprintln!("panicked: {p:?}");
            std::process::abort();
        }
    }
}

fn compact_sql_fragment(input: &str) -> String {
    input
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '"' && *c != '`')
        .collect::<String>()
        .to_ascii_lowercase()
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_nanos()
}

fn test_table_name(prefix: &str) -> String {
    format!("{prefix}_{}", unique_suffix())
}

#[test]
fn mysql_connect_select_1() {
    let Some(cfg) = mysql_test_config() else {
        eprintln!("skipping MySQL integration tests: set {MYSQL_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(SharedMySqlConnection::connect(&cx, cfg).await);
        let rows = unwrap_outcome(conn.query(&cx, "SELECT 1", &[]).await);
        assert_eq!(rows.len(), 1);
        let one: i64 = rows[0].get_as(0).expect("row[0] as i64");
        assert_eq!(one, 1);
    });
}

#[test]
fn mysql_insert_and_select_roundtrip() {
    let Some(cfg) = mysql_test_config() else {
        eprintln!("skipping MySQL integration tests: set {MYSQL_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(SharedMySqlConnection::connect(&cx, cfg).await);

        let table = test_table_name("sqlmodel_roundtrip");
        let create_sql = format!(
            "CREATE TABLE `{table}` (\
             id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,\
             name TEXT NOT NULL\
             )"
        );
        let insert_sql = format!("INSERT INTO `{table}` (name) VALUES (?)");
        let select_sql = format!("SELECT id, name FROM `{table}` WHERE id = ?");
        let drop_sql = format!("DROP TABLE IF EXISTS `{table}`");

        let _ = conn.execute(&cx, &drop_sql, &[]).await;
        unwrap_outcome(conn.execute(&cx, &create_sql, &[]).await);

        let id = unwrap_outcome(
            conn.insert(&cx, &insert_sql, &[Value::Text("Alice".into())])
                .await,
        );
        assert!(id > 0);

        let rows = unwrap_outcome(conn.query(&cx, &select_sql, &[Value::BigInt(id)]).await);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get_as::<i64>(0).expect("id"), id);
        assert_eq!(rows[0].get_as::<String>(1).expect("name"), "Alice");

        let _ = conn.execute(&cx, &drop_sql, &[]).await;
    });
}

#[test]
fn mysql_transaction_rollback_discards_changes() {
    let Some(cfg) = mysql_test_config() else {
        eprintln!("skipping MySQL integration tests: set {MYSQL_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(SharedMySqlConnection::connect(&cx, cfg).await);

        let table = test_table_name("sqlmodel_tx");
        let create_sql = format!(
            "CREATE TABLE `{table}` (\
             id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,\
             name TEXT NOT NULL\
             )"
        );
        let insert_sql = format!("INSERT INTO `{table}` (name) VALUES (?)");
        let count_sql = format!("SELECT COUNT(*) FROM `{table}` WHERE name = ?");
        let drop_sql = format!("DROP TABLE IF EXISTS `{table}`");

        let _ = conn.execute(&cx, &drop_sql, &[]).await;
        unwrap_outcome(conn.execute(&cx, &create_sql, &[]).await);

        let tx = unwrap_outcome(conn.begin(&cx).await);
        unwrap_outcome(
            tx.execute(&cx, &insert_sql, &[Value::Text("Bob".into())])
                .await,
        );
        unwrap_outcome(tx.rollback(&cx).await);

        let rows = unwrap_outcome(
            conn.query(&cx, &count_sql, &[Value::Text("Bob".into())])
                .await,
        );
        assert_eq!(rows.len(), 1);
        let count: i64 = rows[0].get_as(0).expect("COUNT(*) as i64");
        assert_eq!(count, 0);

        let _ = conn.execute(&cx, &drop_sql, &[]).await;
    });
}

#[test]
fn mysql_unique_violation_maps_to_constraint() {
    let Some(cfg) = mysql_test_config() else {
        eprintln!("skipping MySQL integration tests: set {MYSQL_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(SharedMySqlConnection::connect(&cx, cfg).await);

        let table = test_table_name("sqlmodel_unique");
        let create_sql = format!(
            "CREATE TABLE `{table}` (\
             id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,\
             name VARCHAR(255) NOT NULL,\
             UNIQUE KEY uk_name (name)\
             )"
        );
        let insert_sql = format!("INSERT INTO `{table}` (name) VALUES (?)");
        let drop_sql = format!("DROP TABLE IF EXISTS `{table}`");

        let _ = conn.execute(&cx, &drop_sql, &[]).await;
        unwrap_outcome(conn.execute(&cx, &create_sql, &[]).await);
        unwrap_outcome(
            conn.execute(&cx, &insert_sql, &[Value::Text("dup".into())])
                .await,
        );

        let outcome = conn
            .execute(&cx, &insert_sql, &[Value::Text("dup".into())])
            .await;
        assert!(
            matches!(&outcome, Outcome::Err(Error::Query(q)) if q.kind == QueryErrorKind::Constraint),
            "expected constraint violation, got outcome: {outcome:?}"
        );

        let _ = conn.execute(&cx, &drop_sql, &[]).await;
    });
}

#[test]
fn mysql_syntax_error_maps_to_syntax() {
    let Some(cfg) = mysql_test_config() else {
        eprintln!("skipping MySQL integration tests: set {MYSQL_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(SharedMySqlConnection::connect(&cx, cfg).await);
        let outcome = conn.query(&cx, "SELEKT 1", &[]).await;
        assert!(
            matches!(&outcome, Outcome::Err(Error::Query(q)) if q.kind == QueryErrorKind::Syntax),
            "expected syntax error, got outcome: {outcome:?}"
        );
    });
}

#[test]
fn mysql_introspection_reports_check_constraints_and_table_comment() {
    let Some(cfg) = mysql_test_config() else {
        eprintln!("skipping MySQL integration tests: set {MYSQL_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(SharedMySqlConnection::connect(&cx, cfg).await);

        let table = test_table_name("sqlmodel_intro");
        // CHECK constraint names are schema-global in MySQL, so a table left behind by
        // an aborted earlier run must not collide with this one.
        let chk_non_negative = format!("chk_nonneg_{}", &table["sqlmodel_intro_".len()..]);
        let chk_max = format!("chk_max_{}", &table["sqlmodel_intro_".len()..]);
        let create_sql = format!(
            "CREATE TABLE `{table}` (\
             id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,\
             age INT NOT NULL,\
             CONSTRAINT {chk_non_negative} CHECK (age >= 0),\
             CONSTRAINT {chk_max} CHECK (age <= 150)\
             ) COMMENT='hero table comment'"
        );
        let drop_sql = format!("DROP TABLE IF EXISTS `{table}`");

        let _ = conn.execute(&cx, &drop_sql, &[]).await;
        unwrap_outcome(conn.execute(&cx, &create_sql, &[]).await);

        let introspector = Introspector::new(Dialect::Mysql);
        let table_info = unwrap_outcome(introspector.table_info(&cx, &conn, &table).await);

        assert_eq!(table_info.comment.as_deref(), Some("hero table comment"));
        assert!(
            table_info.check_constraints.len() >= 2,
            "expected >=2 check constraints, got {:?}",
            table_info
                .check_constraints
                .iter()
                .map(|c| (&c.name, &c.expression))
                .collect::<Vec<_>>()
        );

        let named_check = table_info
            .check_constraints
            .iter()
            .find(|c| c.name.as_deref() == Some(chk_non_negative.as_str()));
        assert!(
            named_check.is_some(),
            "missing {chk_non_negative} check in {:?}",
            table_info
                .check_constraints
                .iter()
                .map(|c| (&c.name, &c.expression))
                .collect::<Vec<_>>()
        );
        let named_check = named_check.expect("named check should exist");
        let normalized = compact_sql_fragment(&named_check.expression);
        assert!(
            normalized.contains("age>=0"),
            "unexpected normalized expression for chk_age_non_negative: {}",
            named_check.expression
        );

        for check in &table_info.check_constraints {
            let expr = check.expression.trim_start();
            assert!(
                !expr
                    .get(..5)
                    .is_some_and(|prefix| prefix.eq_ignore_ascii_case("CHECK")),
                "expression should be normalized without CHECK prefix: {}",
                check.expression
            );
        }

        let _ = conn.execute(&cx, &drop_sql, &[]).await;
    });
}

#[test]
fn mysql_introspection_preserves_composite_index_column_order() {
    let Some(cfg) = mysql_test_config() else {
        eprintln!("skipping MySQL integration tests: set {MYSQL_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(SharedMySqlConnection::connect(&cx, cfg).await);

        let table = test_table_name("sqlmodel_idx_order");
        let index = format!("{table}_c_a_idx");
        let create_sql = format!(
            "CREATE TABLE `{table}` (\
             id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,\
             a INT NOT NULL,\
             b INT NOT NULL,\
             c INT NOT NULL\
             )"
        );
        let create_index_sql = format!("CREATE INDEX `{index}` ON `{table}` (c, a)");
        let drop_sql = format!("DROP TABLE IF EXISTS `{table}`");

        let _ = conn.execute(&cx, &drop_sql, &[]).await;
        unwrap_outcome(conn.execute(&cx, &create_sql, &[]).await);
        unwrap_outcome(conn.execute(&cx, &create_index_sql, &[]).await);

        let introspector = Introspector::new(Dialect::Mysql);
        let table_info = unwrap_outcome(introspector.table_info(&cx, &conn, &table).await);
        let index_info = table_info.indexes.iter().find(|idx| idx.name == index);
        assert!(
            index_info.is_some(),
            "missing expected index {index} in {:?}",
            table_info.indexes
        );
        let index_info = index_info.expect("checked above");

        assert_eq!(
            index_info.columns,
            vec!["c".to_string(), "a".to_string()],
            "composite index columns should preserve defined order"
        );

        let _ = conn.execute(&cx, &drop_sql, &[]).await;
    });
}

/// A table with a foreign key and a secondary index must introspect fully,
/// and neither the prepared-statement foreign-key query nor `SHOW INDEX` may
/// leave the connection desynchronized for the statement after it (found by
/// the e2e schema fixpoint scenario on MySQL: "Protocol error: Invalid column
/// count" on the query following the foreign-key lookup).
#[test]
fn mysql_introspection_reports_foreign_keys_and_indexes_without_desync() {
    let Some(cfg) = mysql_test_config() else {
        eprintln!("skipping MySQL integration tests: set {MYSQL_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(SharedMySqlConnection::connect(&cx, cfg).await);

        let parent = test_table_name("sqlmodel_fk_parent");
        let child = test_table_name("sqlmodel_fk_child");
        let _ = conn
            .execute(&cx, &format!("DROP TABLE IF EXISTS `{child}`"), &[])
            .await;
        let _ = conn
            .execute(&cx, &format!("DROP TABLE IF EXISTS `{parent}`"), &[])
            .await;
        unwrap_outcome(
            conn.execute(
                &cx,
                &format!("CREATE TABLE `{parent}` (id BIGINT NOT NULL, PRIMARY KEY (id))"),
                &[],
            )
            .await,
        );
        unwrap_outcome(
            conn.execute(
                &cx,
                &format!(
                    "CREATE TABLE `{child}` (\
                     id BIGINT NOT NULL, parent_id BIGINT NOT NULL, name VARCHAR(64) NOT NULL,\
                     PRIMARY KEY (id), FOREIGN KEY (parent_id) REFERENCES `{parent}`(id),\
                     INDEX `{child}_name_idx` (name))"
                ),
                &[],
            )
            .await,
        );

        // The foreign-key lookup is a prepared statement with one row of result.
        let fk_sql = "SELECT kcu.constraint_name, kcu.column_name, kcu.referenced_table_name, \
                      kcu.referenced_column_name, rc.delete_rule, rc.update_rule \
                      FROM information_schema.key_column_usage AS kcu \
                      JOIN information_schema.referential_constraints AS rc \
                        ON rc.constraint_name = kcu.constraint_name \
                       AND rc.constraint_schema = kcu.constraint_schema \
                      WHERE kcu.table_schema = DATABASE() AND kcu.table_name = ? \
                        AND kcu.referenced_table_name IS NOT NULL";
        let fk_rows = unwrap_outcome(conn.query(&cx, fk_sql, &[Value::Text(child.clone())]).await);
        assert_eq!(fk_rows.len(), 1, "one foreign key: {fk_rows:?}");
        // MySQL names these columns CONSTRAINT_NAME etc.; the introspector
        // must not depend on the case the server picks.
        assert!(
            fk_rows[0].get_named::<String>("CONSTRAINT_NAME").is_ok(),
            "server-cased name readable: {fk_rows:?}"
        );
        let probe = unwrap_outcome(conn.query(&cx, "SELECT 1", &[]).await);
        assert_eq!(probe.len(), 1, "connection usable after the FK query");

        let idx_rows = unwrap_outcome(
            conn.query(&cx, &format!("SHOW INDEX FROM `{child}`"), &[])
                .await,
        );
        assert!(
            idx_rows.len() >= 3,
            "PRIMARY + fk index + name index: {}",
            idx_rows.len()
        );
        let probe = unwrap_outcome(conn.query(&cx, "SELECT 1", &[]).await);
        assert_eq!(probe.len(), 1, "connection usable after SHOW INDEX");

        let introspector = Introspector::new(Dialect::Mysql);
        let info = unwrap_outcome(introspector.table_info(&cx, &conn, &child).await);
        assert_eq!(info.primary_key, vec!["id".to_string()]);
        assert_eq!(info.foreign_keys.len(), 1, "{:?}", info.foreign_keys);
        assert_eq!(info.foreign_keys[0].foreign_table, parent);
        assert!(
            info.indexes
                .iter()
                .any(|i| i.name == format!("{child}_name_idx")),
            "{:?}",
            info.indexes
        );
        let probe = unwrap_outcome(conn.query(&cx, "SELECT 1", &[]).await);
        assert_eq!(probe.len(), 1, "connection usable after table_info");

        let _ = conn
            .execute(&cx, &format!("DROP TABLE `{child}`"), &[])
            .await;
        let _ = conn
            .execute(&cx, &format!("DROP TABLE `{parent}`"), &[])
            .await;
    });
}

/// A text-protocol row whose first value is the empty string starts with the
/// byte `0x00`. Until 2026-09 the result-set reader took that for an OK packet,
/// dropped the rows, and left the terminator in the stream, so the next
/// statement failed with "Protocol error: Invalid column count".
#[test]
fn mysql_row_with_empty_first_value_does_not_desync_the_connection() {
    let Some(cfg) = mysql_test_config() else {
        eprintln!("skipping MySQL integration tests: set {MYSQL_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(SharedMySqlConnection::connect(&cx, cfg).await);
        let rows = unwrap_outcome(
            conn.query(
                &cx,
                "SELECT '' AS blank, 42 AS answer UNION ALL SELECT 'x', 43",
                &[],
            )
            .await,
        );
        assert_eq!(rows.len(), 2, "both rows survive: {rows:?}");
        assert_eq!(rows[0].get_named::<String>("blank").unwrap(), "");
        assert_eq!(rows[0].get_named::<i64>("answer").unwrap(), 42);
        assert_eq!(rows[1].get_named::<String>("blank").unwrap(), "x");
        let probe = unwrap_outcome(conn.query(&cx, "SELECT 1", &[]).await);
        assert_eq!(probe.len(), 1, "connection usable afterwards");
    });
}

/// The binary protocol (COM_STMT_PREPARE / EXECUTE) has never returned a row
/// from a live server in any test: the ORM interpolates parameters into the
/// text protocol. Every binary row starts with 0x00 (the byte bug 34 mistook
/// for an OK packet), NULLs live in a bitmap offset by two bits, and each
/// type has its own wire encoding. This drives all of that against MySQL 8.4.
#[test]
fn mysql_prepared_statements_decode_rows_nulls_and_types_without_desync() {
    let Some(cfg) = mysql_test_config() else {
        eprintln!("skipping MySQL integration tests: set {MYSQL_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(SharedMySqlConnection::connect(&cx, cfg).await);
        let table = test_table_name("sqlmodel_binary");
        let _ = conn
            .execute(&cx, &format!("DROP TABLE IF EXISTS `{table}`"), &[])
            .await;
        unwrap_outcome(
            conn.execute(
                &cx,
                &format!(
                    "CREATE TABLE `{table}` (\
                     id INT NOT NULL PRIMARY KEY, e VARCHAR(8) NULL, t TINYINT NULL, \
                     s SMALLINT NULL, m MEDIUMINT NULL, i INT NULL, b BIGINT NULL, \
                     u BIGINT UNSIGNED NULL, d DECIMAL(10,2) NULL, dt DATETIME(6) NULL, \
                     bl BLOB NULL, txt TEXT NULL)"
                ),
                &[],
            )
            .await,
        );
        // Row 1: empty first value and NULLs in the middle and at the end.
        // Row 2: every column set. Row 3: everything NULL.
        unwrap_outcome(
            conn.execute(
                &cx,
                &format!(
                    "INSERT INTO `{table}` VALUES \
                     (1, '', NULL, 7, NULL, 70000, NULL, 18446744073709551615, NULL, NULL, X'00FF', NULL), \
                     (2, 'two', -2, -300, -40000, -500000, -6000000000, 42, 12.34, '2026-09-02 12:34:56.123456', X'', 'text'), \
                     (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
                ),
                &[],
            )
            .await,
        );

        let stmt = unwrap_outcome(
            conn.prepare(
                &cx,
                &format!(
                    "SELECT e, t, s, m, i, b, u, d, dt, bl, txt FROM `{table}` WHERE id >= ? ORDER BY id"
                ),
            )
            .await,
        );
        let rows = unwrap_outcome(
            conn.query_prepared(&cx, &stmt, &[Value::BigInt(1)]).await,
        );
        assert_eq!(rows.len(), 3, "{rows:?}");

        let r1 = &rows[0];
        assert_eq!(r1.get_named::<String>("e").unwrap(), "", "empty first value");
        assert!(r1.get_by_name("t").unwrap().is_null(), "t NULL: {r1:?}");
        assert_eq!(r1.get_named::<i64>("s").unwrap(), 7);
        assert!(r1.get_by_name("m").unwrap().is_null(), "m NULL");
        assert_eq!(r1.get_named::<i64>("i").unwrap(), 70_000);
        assert!(r1.get_by_name("b").unwrap().is_null(), "b NULL");
        assert_eq!(
            r1.get_named::<u64>("u").unwrap(),
            u64::MAX,
            "unsigned bigint max: {:?}",
            r1.get_by_name("u")
        );
        assert!(r1.get_by_name("d").unwrap().is_null(), "d NULL");
        assert!(r1.get_by_name("dt").unwrap().is_null(), "dt NULL");
        assert_eq!(r1.get_named::<Vec<u8>>("bl").unwrap(), vec![0x00, 0xFF]);
        assert!(r1.get_by_name("txt").unwrap().is_null(), "txt NULL");

        let r2 = &rows[1];
        assert_eq!(r2.get_named::<String>("e").unwrap(), "two");
        assert_eq!(r2.get_named::<i64>("t").unwrap(), -2);
        assert_eq!(r2.get_named::<i64>("s").unwrap(), -300);
        assert_eq!(r2.get_named::<i64>("m").unwrap(), -40_000);
        assert_eq!(r2.get_named::<i64>("i").unwrap(), -500_000);
        assert_eq!(r2.get_named::<i64>("b").unwrap(), -6_000_000_000);
        assert_eq!(r2.get_named::<i64>("u").unwrap(), 42);
        let d = r2.get_by_name("d").unwrap().clone();
        assert!(
            matches!(&d, Value::Decimal(s) | Value::Text(s) if s.trim_end_matches('0').trim_end_matches('.') == "12.34"),
            "decimal decoded as {d:?}"
        );
        let dt = r2.get_by_name("dt").unwrap().clone();
        assert!(
            !dt.is_null(),
            "datetime decoded as {dt:?}"
        );
        assert_eq!(r2.get_named::<Vec<u8>>("bl").unwrap(), Vec::<u8>::new());
        assert_eq!(r2.get_named::<String>("txt").unwrap(), "text");

        let r3 = &rows[2];
        for col in ["e", "t", "s", "m", "i", "b", "u", "d", "dt", "bl", "txt"] {
            assert!(r3.get_by_name(col).unwrap().is_null(), "row 3 {col} must be NULL: {r3:?}");
        }

        // The connection is still in sync after a binary result set.
        let probe = unwrap_outcome(conn.query(&cx, "SELECT 1", &[]).await);
        assert_eq!(probe.len(), 1);

        // Zero rows, then in sync again.
        let none = unwrap_outcome(
            conn.query_prepared(&cx, &stmt, &[Value::BigInt(100)]).await,
        );
        assert!(none.is_empty(), "{none:?}");
        let probe = unwrap_outcome(conn.query(&cx, "SELECT 1", &[]).await);
        assert_eq!(probe.len(), 1);

        // A large result set (many packets) whose rows all begin with 0x00.
        let many = unwrap_outcome(
            conn.prepare(
                &cx,
                "WITH RECURSIVE seq (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < ?) \
                 SELECT n, '' AS blank, NULL AS nothing FROM seq",
            )
            .await,
        );
        let rows = unwrap_outcome(
            conn.query_prepared(&cx, &many, &[Value::BigInt(1000)]).await,
        );
        assert_eq!(rows.len(), 1000, "1000 rows expected");
        assert_eq!(rows[999].get_named::<i64>("n").unwrap(), 1000);
        assert_eq!(rows[0].get_named::<String>("blank").unwrap(), "");
        assert!(rows[0].get_by_name("nothing").unwrap().is_null());
        let probe = unwrap_outcome(conn.query(&cx, "SELECT 1", &[]).await);
        assert_eq!(probe.len(), 1, "connection usable after 1000 binary rows");

        // execute_prepared with a NULL parameter, read back through the same path.
        let ins = unwrap_outcome(
            conn.prepare(
                &cx,
                &format!("INSERT INTO `{table}` (id, e, i, txt) VALUES (?, ?, ?, ?)"),
            )
            .await,
        );
        let affected = unwrap_outcome(
            conn.execute_prepared(
                &cx,
                &ins,
                &[
                    Value::BigInt(4),
                    Value::Text("four".into()),
                    Value::Null,
                    Value::Text("t4".into()),
                ],
            )
            .await,
        );
        assert_eq!(affected, 1);
        let rows = unwrap_outcome(
            conn.query_prepared(&cx, &stmt, &[Value::BigInt(4)]).await,
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get_named::<String>("e").unwrap(), "four");
        assert!(rows[0].get_by_name("i").unwrap().is_null(), "NULL parameter stored as NULL");

        let _ = conn
            .execute(&cx, &format!("DROP TABLE `{table}`"), &[])
            .await;
    });
}

/// Parameterized `query`/`execute` calls go through `COM_STMT_PREPARE` /
/// `COM_STMT_EXECUTE` with a per-connection cache: the server's own counters
/// show one prepare per distinct statement, one execute per call, and a
/// close for every statement evicted from the bounded cache.
#[test]
fn mysql_parameterized_queries_are_prepared_once_and_executed_per_call() {
    let Some(cfg) = mysql_test_config() else {
        eprintln!("skipping MySQL integration tests: set {MYSQL_URL_ENV}");
        return;
    };

    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("create asupersync runtime");
    let cx = Cx::for_testing();

    rt.block_on(async {
        let conn = unwrap_outcome(SharedMySqlConnection::connect(&cx, cfg).await);
        let counter = |name: &'static str| {
            let conn = &conn;
            let cx = &cx;
            async move {
                let rows = unwrap_outcome(
                    conn.query(cx, &format!("SHOW SESSION STATUS LIKE '{name}'"), &[])
                        .await,
                );
                rows[0]
                    .get_named::<String>("Value")
                    .expect("Value column")
                    .parse::<u64>()
                    .expect("status counter")
            }
        };
        let prepares = counter("Com_stmt_prepare").await;
        let executes = counter("Com_stmt_execute").await;
        let closes = counter("Com_stmt_close").await;

        for i in 0..5 {
            let rows = unwrap_outcome(conn.query(&cx, "SELECT ? + 1", &[Value::BigInt(i)]).await);
            assert_eq!(rows[0].get_as::<i64>(0).unwrap(), i + 1);
        }
        let rows = unwrap_outcome(conn.query(&cx, "SELECT ? * 2", &[Value::Int(21)]).await);
        assert_eq!(rows[0].get_as::<i64>(0).unwrap(), 42);
        assert_eq!(
            counter("Com_stmt_prepare").await - prepares,
            2,
            "one prepare per distinct statement"
        );
        assert_eq!(
            counter("Com_stmt_execute").await - executes,
            6,
            "one execute per call"
        );

        // `$n` placeholders (and parameterless statements) stay on the text
        // protocol, where the driver interpolates the values.
        let rows = unwrap_outcome(conn.query(&cx, "SELECT $1 + 1", &[Value::BigInt(1)]).await);
        assert_eq!(rows[0].get_as::<i64>(0).unwrap(), 2);
        assert_eq!(counter("Com_stmt_prepare").await - prepares, 2);

        // Typed values arrive as their own types, not as rendered literals.
        let rows = unwrap_outcome(
            conn.query(
                &cx,
                "SELECT ?, ?, ?, ?",
                &[
                    Value::Bool(true),
                    Value::Double(2.5),
                    Value::Text("it's".into()),
                    Value::Null,
                ],
            )
            .await,
        );
        assert_eq!(rows[0].get_as::<i64>(0).unwrap(), 1);
        assert!((rows[0].get_as::<f64>(1).unwrap() - 2.5).abs() < f64::EPSILON);
        assert_eq!(rows[0].get_as::<String>(2).unwrap(), "it's");
        assert!(rows[0].get(3).unwrap().is_null());

        // The cache is bounded: more distinct statements than the capacity
        // close the least recently used ones on the server.
        for i in 0..(sqlmodel_mysql::STATEMENT_CACHE_CAPACITY + 8) {
            unwrap_outcome(
                conn.query(&cx, &format!("SELECT ? + {i}"), &[Value::BigInt(1)])
                    .await,
            );
        }
        let evicted = counter("Com_stmt_close").await - closes;
        assert!(evicted >= 8, "evicted statements are closed, got {evicted}");
    });
}
