//! All-driver end-to-end harness for SQLModel Rust.
//!
//! This crate is a workspace member with `publish = false`. It exists so the same
//! ORM-level scenario (schema generation, `insert!`/`select!`, `Session`,
//! `MigrationRunner`, concurrent transactions) can be run unchanged against
//! every driver the workspace ships:
//!
//! | driver | always available | how |
//! |--------|------------------|-----|
//! | C SQLite (`sqlmodel-sqlite`) | yes | in-memory and a temp file |
//! | FrankenSQLite (`sqlmodel-frankensqlite`) | yes | a temp file (MVCC needs a file) |
//! | PostgreSQL (`sqlmodel-postgres`) | when `SQLMODEL_TEST_POSTGRES_URL` is set | `postgres://user:pass@host:port/db` |
//! | MySQL (`sqlmodel-mysql`) | when `SQLMODEL_TEST_MYSQL_URL` is set | `mysql://user:pass@host:port/db` |
//! | MariaDB (via `sqlmodel-mysql`) | when `SQLMODEL_TEST_MARIADB_URL` is set | same URL form |
//!
//! Network drivers are never skipped silently: [`DriverUnderTest::available`]
//! prints which ones are absent and why, and CI's `integration` job fails if a
//! suite skipped while services were up.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome};
use sqlmodel_core::{Connection, Dialect, Error};
use sqlmodel_frankensqlite::FrankenConnection;
use sqlmodel_mysql::{MySqlConfig, SharedMySqlConnection};
use sqlmodel_postgres::{PgConfig, SharedPgConnection, SslMode as PgSslMode};
use sqlmodel_sqlite::SqliteConnection;

pub const POSTGRES_URL_ENV: &str = "SQLMODEL_TEST_POSTGRES_URL";
pub const MYSQL_URL_ENV: &str = "SQLMODEL_TEST_MYSQL_URL";
pub const MARIADB_URL_ENV: &str = "SQLMODEL_TEST_MARIADB_URL";

/// One database the scenarios run against.
#[derive(Debug, Clone)]
pub enum DriverUnderTest {
    /// `sqlmodel-sqlite`, `:memory:`.
    CSqliteMemory,
    /// `sqlmodel-sqlite`, file database (WAL sidecars cleaned up afterwards).
    CSqliteFile(PathBuf),
    /// `sqlmodel-frankensqlite`, file database.
    Franken(PathBuf),
    /// `sqlmodel-postgres`.
    Postgres(PgConfig),
    /// `sqlmodel-mysql` against MySQL.
    MySql(MySqlConfig),
    /// `sqlmodel-mysql` against MariaDB (same wire protocol, different server).
    MariaDb(MySqlConfig),
}

impl DriverUnderTest {
    /// Every driver that can run right now. SQLite variants are always present;
    /// network drivers are added when their URL environment variable is set.
    /// Absent network drivers are reported on stderr so a skip is never silent.
    pub fn available() -> Vec<Self> {
        let mut drivers = vec![
            Self::CSqliteMemory,
            Self::CSqliteFile(temp_db_path("csqlite")),
            Self::Franken(temp_db_path("franken")),
        ];
        match std::env::var(POSTGRES_URL_ENV)
            .ok()
            .and_then(|u| parse_postgres_url(&u))
        {
            Some(cfg) => drivers.push(Self::Postgres(cfg)),
            None => eprintln!(
                "sqlmodel-e2e: PostgreSQL not exercised ({POSTGRES_URL_ENV} unset or unparsable)"
            ),
        }
        match std::env::var(MYSQL_URL_ENV)
            .ok()
            .and_then(|u| parse_mysql_url(&u))
        {
            Some(cfg) => drivers.push(Self::MySql(cfg)),
            None => {
                eprintln!(
                    "sqlmodel-e2e: MySQL not exercised ({MYSQL_URL_ENV} unset or unparsable)"
                );
            }
        }
        match std::env::var(MARIADB_URL_ENV)
            .ok()
            .and_then(|u| parse_mysql_url(&u))
        {
            Some(cfg) => drivers.push(Self::MariaDb(cfg)),
            None => eprintln!(
                "sqlmodel-e2e: MariaDB not exercised ({MARIADB_URL_ENV} unset or unparsable)"
            ),
        }
        drivers
    }

    /// Only the drivers that can open several connections to one database.
    pub fn available_multi_connection() -> Vec<Self> {
        Self::available()
            .into_iter()
            .filter(|d| !matches!(d, Self::CSqliteMemory))
            .collect()
    }

    /// Short name for logs and assertion messages.
    pub fn name(&self) -> &'static str {
        match self {
            Self::CSqliteMemory => "c-sqlite(memory)",
            Self::CSqliteFile(_) => "c-sqlite(file)",
            Self::Franken(_) => "frankensqlite",
            Self::Postgres(_) => "postgres",
            Self::MySql(_) => "mysql",
            Self::MariaDb(_) => "mariadb",
        }
    }

    pub fn dialect(&self) -> Dialect {
        match self {
            Self::CSqliteMemory | Self::CSqliteFile(_) | Self::Franken(_) => Dialect::Sqlite,
            Self::Postgres(_) => Dialect::Postgres,
            Self::MySql(_) | Self::MariaDb(_) => Dialect::Mysql,
        }
    }

    /// Whether the server supports `RETURNING` on INSERT/UPDATE/DELETE.
    pub fn supports_returning(&self) -> bool {
        !matches!(self, Self::MySql(_))
    }

    /// Whether `TransactionMode::Concurrent` is expected to be accepted.
    pub fn supports_concurrent_transactions(&self) -> bool {
        !matches!(self, Self::CSqliteMemory | Self::CSqliteFile(_))
    }

    fn cleanup(&self) {
        match self {
            Self::CSqliteFile(p) | Self::Franken(p) => remove_db_family(p),
            _ => {}
        }
    }
}

/// A scenario that can run against any driver.
pub trait Scenario {
    fn run<C: Connection>(
        &self,
        cx: &Cx,
        conn: &C,
        driver: &DriverUnderTest,
    ) -> impl Future<Output = ()>;
}

/// A scenario that takes ownership of the connection (what `Session` and
/// `Pool` need).
pub trait OwnedScenario {
    fn run<C: Connection + 'static>(
        &self,
        cx: &Cx,
        conn: C,
        driver: &DriverUnderTest,
    ) -> impl Future<Output = ()>;
}

struct Borrowed<'a, S>(&'a S);

impl<S: Scenario> OwnedScenario for Borrowed<'_, S> {
    async fn run<C: Connection + 'static>(&self, cx: &Cx, conn: C, driver: &DriverUnderTest) {
        self.0.run(cx, &conn, driver).await;
    }
}

/// Run `scenario` once per available driver on a fresh connection each time,
/// printing which drivers ran. Panics propagate from the scenario with the
/// driver name in the message.
pub fn run_on_every_driver(cx: &Cx, scenario: &impl Scenario) -> Vec<&'static str> {
    run_on_drivers(cx, &DriverUnderTest::available(), scenario)
}

/// Run `scenario` on the given drivers.
pub fn run_on_drivers(
    cx: &Cx,
    drivers: &[DriverUnderTest],
    scenario: &impl Scenario,
) -> Vec<&'static str> {
    run_owned_on_drivers(cx, drivers, &Borrowed(scenario))
}

/// [`run_on_every_driver`] for scenarios that consume the connection.
pub fn run_owned_on_every_driver(cx: &Cx, scenario: &impl OwnedScenario) -> Vec<&'static str> {
    run_owned_on_drivers(cx, &DriverUnderTest::available(), scenario)
}

/// [`run_on_drivers`] for scenarios that consume the connection.
pub fn run_owned_on_drivers(
    cx: &Cx,
    drivers: &[DriverUnderTest],
    scenario: &impl OwnedScenario,
) -> Vec<&'static str> {
    let rt = RuntimeBuilder::current_thread()
        .build()
        .expect("asupersync runtime");
    let mut ran = Vec::new();
    for driver in drivers {
        eprintln!("sqlmodel-e2e: running on {}", driver.name());
        match driver {
            DriverUnderTest::CSqliteMemory => {
                let conn = SqliteConnection::open_memory().expect("open :memory:");
                rt.block_on(scenario.run(cx, conn, driver));
            }
            DriverUnderTest::CSqliteFile(path) => {
                let conn = SqliteConnection::open_file(path.to_string_lossy().into_owned())
                    .expect("open sqlite file");
                rt.block_on(scenario.run(cx, conn, driver));
            }
            DriverUnderTest::Franken(path) => {
                let conn = FrankenConnection::open_file(path.to_string_lossy().into_owned())
                    .expect("open frankensqlite file");
                rt.block_on(scenario.run(cx, conn, driver));
            }
            DriverUnderTest::Postgres(cfg) => {
                rt.block_on(async {
                    let conn = expect_outcome(
                        SharedPgConnection::connect(cx, cfg.clone()).await,
                        "connect to postgres",
                    );
                    scenario.run(cx, conn, driver).await;
                });
            }
            DriverUnderTest::MySql(cfg) | DriverUnderTest::MariaDb(cfg) => {
                rt.block_on(async {
                    let conn = expect_outcome(
                        SharedMySqlConnection::connect(cx, cfg.clone()).await,
                        "connect to mysql/mariadb",
                    );
                    scenario.run(cx, conn, driver).await;
                });
            }
        }
        driver.cleanup();
        ran.push(driver.name());
    }
    eprintln!("sqlmodel-e2e: ran on {}", ran.join(", "));
    ran
}

/// Open a second connection to the same database (for multi-connection
/// scenarios). Not available for `:memory:`.
pub fn open_connection_pair(
    cx: &Cx,
    rt: &asupersync::runtime::Runtime,
    driver: &DriverUnderTest,
) -> Option<ConnectionPair> {
    Some(match driver {
        DriverUnderTest::CSqliteMemory => return None,
        DriverUnderTest::CSqliteFile(p) => {
            let s = p.to_string_lossy().into_owned();
            ConnectionPair::CSqlite(
                SqliteConnection::open_file(s.clone()).expect("open a"),
                SqliteConnection::open_file(s).expect("open b"),
            )
        }
        DriverUnderTest::Franken(p) => {
            let s = p.to_string_lossy().into_owned();
            ConnectionPair::Franken(
                FrankenConnection::open_file(s.clone()).expect("open a"),
                FrankenConnection::open_file(s).expect("open b"),
            )
        }
        DriverUnderTest::Postgres(cfg) => rt.block_on(async {
            ConnectionPair::Postgres(
                expect_outcome(SharedPgConnection::connect(cx, cfg.clone()).await, "pg a"),
                expect_outcome(SharedPgConnection::connect(cx, cfg.clone()).await, "pg b"),
            )
        }),
        DriverUnderTest::MySql(cfg) | DriverUnderTest::MariaDb(cfg) => rt.block_on(async {
            ConnectionPair::MySql(
                expect_outcome(
                    SharedMySqlConnection::connect(cx, cfg.clone()).await,
                    "mysql a",
                ),
                expect_outcome(
                    SharedMySqlConnection::connect(cx, cfg.clone()).await,
                    "mysql b",
                ),
            )
        }),
    })
}

/// Two connections to one database.
pub enum ConnectionPair {
    CSqlite(SqliteConnection, SqliteConnection),
    Franken(FrankenConnection, FrankenConnection),
    Postgres(SharedPgConnection, SharedPgConnection),
    MySql(SharedMySqlConnection, SharedMySqlConnection),
}

/// Unwrap an `Outcome`, panicking with context on anything but `Ok`.
pub fn expect_outcome<T>(outcome: Outcome<T, Error>, what: &str) -> T {
    match outcome {
        Outcome::Ok(v) => v,
        Outcome::Err(e) => panic!("{what}: error: {e}"),
        Outcome::Cancelled(r) => panic!("{what}: cancelled: {r:?}"),
        Outcome::Panicked(p) => panic!("{what}: panicked: {p:?}"),
    }
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A table name unique to this process and call, safe on shared network databases.
pub fn unique_table(prefix: &str) -> String {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos()
        % 1_000_000_007;
    format!("{prefix}_{}_{nanos}_{n}", std::process::id())
}

/// A fresh database file path under the temp directory.
pub fn temp_db_path(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("sqlmodel_e2e");
    let _ = std::fs::create_dir_all(&dir);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = dir.join(format!("{prefix}_{}_{n}.db", std::process::id()));
    remove_db_family(&path);
    path
}

/// Remove a database file and every sidecar (`-wal`, `-shm`, `-journal`, and
/// FrankenSQLite's additional sidecars) that shares its stem.
pub fn remove_db_family(path: &Path) {
    let Some(dir) = path.parent() else { return };
    let Some(stem) = path.file_name().and_then(|s| s.to_str()) else {
        return;
    };
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            if name.to_string_lossy().starts_with(stem) {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
}

/// Parse `postgres://user[:pass]@host[:port]/db[?..]` into a `PgConfig` with
/// TLS disabled (the CI services do not terminate TLS).
pub fn parse_postgres_url(url: &str) -> Option<PgConfig> {
    let url = url.trim();
    let rest = url
        .strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("postgresql://"))?;
    let (auth, host_and_path) = rest.split_once('@')?;
    let (user, password) = match auth.split_once(':') {
        Some((u, p)) => (u, Some(p)),
        None => (auth, None),
    };
    let (host_port, db) = host_and_path.split_once('/')?;
    let db = db
        .split_once('?')
        .map_or(db, |(left, _)| left)
        .trim_matches('/');
    if db.is_empty() {
        return None;
    }
    let (host, port) = parse_host_port(host_port, 5432)?;
    let mut cfg = PgConfig::new(host, user, db)
        .port(port)
        .connect_timeout(Duration::from_secs(10))
        .ssl_mode(PgSslMode::Disable);
    if let Some(pw) = password.filter(|p| !p.is_empty()) {
        cfg = cfg.password(pw);
    }
    Some(cfg)
}

/// Parse `mysql://user[:pass]@host[:port]/db[?..]` into a `MySqlConfig`.
pub fn parse_mysql_url(url: &str) -> Option<MySqlConfig> {
    let url = url.trim();
    let rest = url.strip_prefix("mysql://")?;
    let (auth, host_and_path) = rest.split_once('@')?;
    let (user, password) = match auth.split_once(':') {
        Some((u, p)) => (u, Some(p)),
        None => (auth, None),
    };
    let (host_port, db) = host_and_path.split_once('/')?;
    let db = db
        .split_once('?')
        .map_or(db, |(left, _)| left)
        .trim_matches('/');
    if db.is_empty() {
        return None;
    }
    let (host, port) = parse_host_port(host_port, 3306)?;
    let mut cfg = MySqlConfig::new()
        .host(host)
        .port(port)
        .user(user)
        .database(db)
        .connect_timeout(Duration::from_secs(10));
    if let Some(pw) = password.filter(|p| !p.is_empty()) {
        cfg = cfg.password(pw);
    }
    Some(cfg)
}

fn parse_host_port(input: &str, default_port: u16) -> Option<(&str, u16)> {
    if let Some(rest) = input.strip_prefix('[') {
        let end = rest.find(']')?;
        let host = &rest[..end];
        let port = rest[end + 1..]
            .strip_prefix(':')
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(default_port);
        return Some((host, port));
    }
    match input.rsplit_once(':') {
        Some((host, port_str))
            if !port_str.is_empty() && port_str.chars().all(|c| c.is_ascii_digit()) =>
        {
            Some((host, port_str.parse::<u16>().ok()?))
        }
        _ => Some((input, default_port)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_parsers_accept_the_ci_shapes() {
        let pg =
            parse_postgres_url("postgres://sqlmodel:pw@127.0.0.1:55432/sqlmodel_test").unwrap();
        assert_eq!(pg.database, "sqlmodel_test");
        assert!(
            parse_postgres_url("postgres://u@h:1/").is_none(),
            "database required"
        );

        let my = parse_mysql_url("mysql://root:pw@127.0.0.1:53306/sqlmodel_test?x=1").unwrap();
        assert_eq!(my.database.as_deref(), Some("sqlmodel_test"));
        assert!(
            parse_mysql_url("mysql://root@h/").is_none(),
            "database required"
        );
    }

    #[test]
    fn sqlite_drivers_are_always_available() {
        let names: Vec<_> = DriverUnderTest::available()
            .iter()
            .map(DriverUnderTest::name)
            .collect();
        assert!(names.contains(&"c-sqlite(memory)"));
        assert!(names.contains(&"c-sqlite(file)"));
        assert!(names.contains(&"frankensqlite"));
    }
}
