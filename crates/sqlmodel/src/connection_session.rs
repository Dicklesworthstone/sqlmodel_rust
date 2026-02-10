//! Connection session management for SQLModel Rust.
//!
//! This module provides a small "connection + optional console" wrapper plus a builder.
//!
//! Important: this is **not** the SQLAlchemy/SQLModel ORM Session (unit of work / identity map).
//! The ORM-style session lives in `sqlmodel::session` and is re-exported from the
//! `sqlmodel-session` crate.
//!
//! # Example
//!
//! ```rust,ignore
//! use sqlmodel::prelude::*;
//!
//! // Basic connection session without console
//! let session = ConnectionSession::builder().build_with(connection);
//!
//! // Connection session with auto-detected console
//! #[cfg(feature = "console")]
//! let session = ConnectionSession::builder()
//!     .with_auto_console()
//!     .build_with(connection);
//! ```

#[cfg(feature = "console")]
use std::sync::Arc;

#[cfg(feature = "console")]
use sqlmodel_console::{ConsoleAware, SqlModelConsole};

#[cfg(feature = "console")]
use crate::global_console::global_console;

use sqlmodel_core::Connection;

/// A database session that combines connection management with optional console output.
///
/// This type is a lightweight wrapper around a `Connection` and optional console.
/// For ORM-style behavior (identity map, unit of work, lazy loading), use
/// `sqlmodel::Session` (from `sqlmodel-session`).
#[derive(Debug)]
pub struct ConnectionSession<C: Connection> {
    /// The underlying connection
    connection: C,
    /// Optional console for rich output
    #[cfg(feature = "console")]
    console: Option<Arc<SqlModelConsole>>,
}

impl<C: Connection> ConnectionSession<C> {
    /// Create a new session with a connection.
    pub fn new(connection: C) -> Self {
        Self {
            connection,
            #[cfg(feature = "console")]
            console: None,
        }
    }

    /// Create a session builder.
    #[must_use]
    pub fn builder() -> ConnectionSessionBuilder<C> {
        ConnectionSessionBuilder::new()
    }

    /// Get a reference to the underlying connection.
    #[must_use]
    pub fn connection(&self) -> &C {
        &self.connection
    }

    /// Get a mutable reference to the underlying connection.
    pub fn connection_mut(&mut self) -> &mut C {
        &mut self.connection
    }

    /// Consume the session and return the underlying connection.
    pub fn into_connection(self) -> C {
        self.connection
    }
}

#[cfg(feature = "console")]
impl<C: Connection> ConsoleAware for ConnectionSession<C> {
    fn set_console(&mut self, console: Option<Arc<SqlModelConsole>>) {
        self.console = console;
    }

    fn console(&self) -> Option<&Arc<SqlModelConsole>> {
        self.console.as_ref()
    }

    fn has_console(&self) -> bool {
        self.console.is_some()
    }
}

/// Builder for creating ConnectionSession instances with fluent API.
///
/// # Example
///
/// ```rust,ignore
/// let session = ConnectionSession::builder()
///     .with_auto_console()  // Only available with "console" feature
///     .build_with(connection);
/// ```
#[derive(Debug, Default)]
pub struct ConnectionSessionBuilder<C: Connection> {
    #[cfg(feature = "console")]
    console: Option<Arc<SqlModelConsole>>,
    #[cfg(not(feature = "console"))]
    _marker: std::marker::PhantomData<C>,
    #[cfg(feature = "console")]
    _marker: std::marker::PhantomData<C>,
}

impl<C: Connection> ConnectionSessionBuilder<C> {
    /// Create a new session builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "console")]
            console: None,
            _marker: std::marker::PhantomData,
        }
    }

    /// Attach a console for rich output.
    ///
    /// The console will be used to emit progress information, query results,
    /// and error messages in a visually rich format.
    #[cfg(feature = "console")]
    #[must_use]
    pub fn with_console(mut self, console: SqlModelConsole) -> Self {
        self.console = Some(Arc::new(console));
        self
    }

    /// Attach a shared console for rich output.
    ///
    /// Use this when multiple sessions should share the same console
    /// (e.g., for coordinated output or shared theme).
    #[cfg(feature = "console")]
    #[must_use]
    pub fn with_shared_console(mut self, console: Arc<SqlModelConsole>) -> Self {
        self.console = Some(console);
        self
    }

    /// Use auto-detected output mode for the console.
    ///
    /// This creates a console that automatically detects whether
    /// the terminal supports rich output or should fall back to plain text.
    /// Uses `SqlModelConsole::new()` which performs environment detection.
    #[cfg(feature = "console")]
    #[must_use]
    pub fn with_auto_console(mut self) -> Self {
        self.console = Some(Arc::new(SqlModelConsole::new()));
        self
    }

    /// Build the session with the provided connection.
    ///
    /// Console selection follows these priorities (highest first):
    /// 1. Explicit console set via `with_console()` or similar
    /// 2. Global console (if set via `set_global_console()` or `init_auto_console()`)
    /// 3. No console (silent operation)
    #[allow(unused_mut)] // mut only used with console feature
    pub fn build_with(self, connection: C) -> ConnectionSession<C> {
        let mut session = ConnectionSession::new(connection);

        #[cfg(feature = "console")]
        {
            // Use explicit console if set, otherwise fall back to global console
            session.console = self.console.or_else(global_console);
        }

        session
    }
}

/// Builder for creating connections with console support.
///
/// This trait extends connection factories to support console integration.
/// Implement this for driver-specific connection builders.
#[cfg(feature = "console")]
pub trait ConnectionBuilderExt {
    /// The connection type produced by this builder.
    type Connection: Connection + ConsoleAware;

    /// Attach a console for rich output.
    fn with_console(self, console: SqlModelConsole) -> Self;

    /// Attach a shared console for rich output.
    fn with_shared_console(self, console: Arc<SqlModelConsole>) -> Self;

    /// Use auto-detected output mode for the console.
    fn with_auto_console(self) -> Self;
}

#[cfg(test)]
#[allow(clippy::manual_async_fn)] // Mock trait impls must match trait signatures
mod tests {
    use super::*;

    // Mock connection for testing
    #[derive(Debug)]
    struct MockConnection;

    impl sqlmodel_core::Connection for MockConnection {
        type Tx<'conn>
            = MockTransaction
        where
            Self: 'conn;

        fn query(
            &self,
            _cx: &asupersync::Cx,
            _sql: &str,
            _params: &[sqlmodel_core::Value],
        ) -> impl std::future::Future<
            Output = asupersync::Outcome<Vec<sqlmodel_core::Row>, sqlmodel_core::Error>,
        > + Send {
            async { asupersync::Outcome::Ok(vec![]) }
        }

        fn query_one(
            &self,
            _cx: &asupersync::Cx,
            _sql: &str,
            _params: &[sqlmodel_core::Value],
        ) -> impl std::future::Future<
            Output = asupersync::Outcome<Option<sqlmodel_core::Row>, sqlmodel_core::Error>,
        > + Send {
            async { asupersync::Outcome::Ok(None) }
        }

        fn execute(
            &self,
            _cx: &asupersync::Cx,
            _sql: &str,
            _params: &[sqlmodel_core::Value],
        ) -> impl std::future::Future<Output = asupersync::Outcome<u64, sqlmodel_core::Error>> + Send
        {
            async { asupersync::Outcome::Ok(0) }
        }

        fn insert(
            &self,
            _cx: &asupersync::Cx,
            _sql: &str,
            _params: &[sqlmodel_core::Value],
        ) -> impl std::future::Future<Output = asupersync::Outcome<i64, sqlmodel_core::Error>> + Send
        {
            async { asupersync::Outcome::Ok(0) }
        }

        fn batch(
            &self,
            _cx: &asupersync::Cx,
            _statements: &[(String, Vec<sqlmodel_core::Value>)],
        ) -> impl std::future::Future<Output = asupersync::Outcome<Vec<u64>, sqlmodel_core::Error>> + Send
        {
            async { asupersync::Outcome::Ok(vec![]) }
        }

        fn begin(
            &self,
            _cx: &asupersync::Cx,
        ) -> impl std::future::Future<
            Output = asupersync::Outcome<Self::Tx<'_>, sqlmodel_core::Error>,
        > + Send {
            async { asupersync::Outcome::Ok(MockTransaction) }
        }

        fn begin_with(
            &self,
            _cx: &asupersync::Cx,
            _isolation: sqlmodel_core::connection::IsolationLevel,
        ) -> impl std::future::Future<
            Output = asupersync::Outcome<Self::Tx<'_>, sqlmodel_core::Error>,
        > + Send {
            async { asupersync::Outcome::Ok(MockTransaction) }
        }

        fn prepare(
            &self,
            _cx: &asupersync::Cx,
            _sql: &str,
        ) -> impl std::future::Future<
            Output = asupersync::Outcome<
                sqlmodel_core::connection::PreparedStatement,
                sqlmodel_core::Error,
            >,
        > + Send {
            async {
                asupersync::Outcome::Ok(sqlmodel_core::connection::PreparedStatement::new(
                    0,
                    String::new(),
                    0,
                ))
            }
        }

        fn query_prepared(
            &self,
            _cx: &asupersync::Cx,
            _stmt: &sqlmodel_core::connection::PreparedStatement,
            _params: &[sqlmodel_core::Value],
        ) -> impl std::future::Future<
            Output = asupersync::Outcome<Vec<sqlmodel_core::Row>, sqlmodel_core::Error>,
        > + Send {
            async { asupersync::Outcome::Ok(vec![]) }
        }

        fn execute_prepared(
            &self,
            _cx: &asupersync::Cx,
            _stmt: &sqlmodel_core::connection::PreparedStatement,
            _params: &[sqlmodel_core::Value],
        ) -> impl std::future::Future<Output = asupersync::Outcome<u64, sqlmodel_core::Error>> + Send
        {
            async { asupersync::Outcome::Ok(0) }
        }

        fn ping(
            &self,
            _cx: &asupersync::Cx,
        ) -> impl std::future::Future<Output = asupersync::Outcome<(), sqlmodel_core::Error>> + Send
        {
            async { asupersync::Outcome::Ok(()) }
        }

        fn close(
            self,
            _cx: &asupersync::Cx,
        ) -> impl std::future::Future<Output = sqlmodel_core::error::Result<()>> + Send {
            async { Ok(()) }
        }
    }

    struct MockTransaction;

    impl sqlmodel_core::connection::TransactionOps for MockTransaction {
        fn query(
            &self,
            _cx: &asupersync::Cx,
            _sql: &str,
            _params: &[sqlmodel_core::Value],
        ) -> impl std::future::Future<
            Output = asupersync::Outcome<Vec<sqlmodel_core::Row>, sqlmodel_core::Error>,
        > + Send {
            async { asupersync::Outcome::Ok(vec![]) }
        }

        fn query_one(
            &self,
            _cx: &asupersync::Cx,
            _sql: &str,
            _params: &[sqlmodel_core::Value],
        ) -> impl std::future::Future<
            Output = asupersync::Outcome<Option<sqlmodel_core::Row>, sqlmodel_core::Error>,
        > + Send {
            async { asupersync::Outcome::Ok(None) }
        }

        fn execute(
            &self,
            _cx: &asupersync::Cx,
            _sql: &str,
            _params: &[sqlmodel_core::Value],
        ) -> impl std::future::Future<Output = asupersync::Outcome<u64, sqlmodel_core::Error>> + Send
        {
            async { asupersync::Outcome::Ok(0) }
        }

        fn savepoint(
            &self,
            _cx: &asupersync::Cx,
            _name: &str,
        ) -> impl std::future::Future<Output = asupersync::Outcome<(), sqlmodel_core::Error>> + Send
        {
            async { asupersync::Outcome::Ok(()) }
        }

        fn rollback_to(
            &self,
            _cx: &asupersync::Cx,
            _name: &str,
        ) -> impl std::future::Future<Output = asupersync::Outcome<(), sqlmodel_core::Error>> + Send
        {
            async { asupersync::Outcome::Ok(()) }
        }

        fn release(
            &self,
            _cx: &asupersync::Cx,
            _name: &str,
        ) -> impl std::future::Future<Output = asupersync::Outcome<(), sqlmodel_core::Error>> + Send
        {
            async { asupersync::Outcome::Ok(()) }
        }

        fn commit(
            self,
            _cx: &asupersync::Cx,
        ) -> impl std::future::Future<Output = asupersync::Outcome<(), sqlmodel_core::Error>> + Send
        {
            async { asupersync::Outcome::Ok(()) }
        }

        fn rollback(
            self,
            _cx: &asupersync::Cx,
        ) -> impl std::future::Future<Output = asupersync::Outcome<(), sqlmodel_core::Error>> + Send
        {
            async { asupersync::Outcome::Ok(()) }
        }
    }

    #[test]
    fn test_connection_session_builder_basic() {
        let conn = MockConnection;
        let session = ConnectionSession::builder().build_with(conn);
        assert!(std::ptr::eq(session.connection(), session.connection()));
    }

    #[test]
    fn test_connection_session_new() {
        let conn = MockConnection;
        let session = ConnectionSession::new(conn);
        let _ = session.connection();
    }

    #[test]
    fn test_connection_session_connection_access() {
        let conn = MockConnection;
        let mut session = ConnectionSession::new(conn);
        let _ = session.connection();
        let _ = session.connection_mut();
    }

    #[test]
    fn test_connection_session_into_connection() {
        let conn = MockConnection;
        let session = ConnectionSession::new(conn);
        let _recovered: MockConnection = session.into_connection();
    }

    #[cfg(feature = "console")]
    #[test]
    fn test_connection_session_builder_with_console() {
        let console = SqlModelConsole::new();
        let conn = MockConnection;
        let session = ConnectionSession::builder()
            .with_console(console)
            .build_with(conn);
        assert!(session.has_console());
    }

    #[cfg(feature = "console")]
    #[test]
    fn test_connection_session_builder_with_shared_console() {
        let console = Arc::new(SqlModelConsole::new());
        let conn1 = MockConnection;
        let conn2 = MockConnection;

        let session1 = ConnectionSession::builder()
            .with_shared_console(console.clone())
            .build_with(conn1);
        let session2 = ConnectionSession::builder()
            .with_shared_console(console)
            .build_with(conn2);

        assert!(session1.has_console());
        assert!(session2.has_console());
    }

    #[cfg(feature = "console")]
    #[test]
    fn test_connection_session_builder_with_auto_console() {
        let conn = MockConnection;
        let session = ConnectionSession::builder()
            .with_auto_console()
            .build_with(conn);
        assert!(session.has_console());
    }

    #[cfg(feature = "console")]
    #[test]
    fn test_connection_session_console_aware() {
        let conn = MockConnection;
        let mut session = ConnectionSession::new(conn);

        assert!(!session.has_console());
        assert!(session.console().is_none());

        let console = Arc::new(SqlModelConsole::new());
        session.set_console(Some(console));
        assert!(session.has_console());
        assert!(session.console().is_some());

        session.set_console(None);
        assert!(!session.has_console());
    }

    #[test]
    fn test_builder_chain_fluent_api() {
        let conn = MockConnection;
        let builder = ConnectionSession::<MockConnection>::builder();
        let _session = builder.build_with(conn);
    }
}
