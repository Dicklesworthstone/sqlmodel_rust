#![allow(clippy::manual_async_fn)]

use sqlmodel::prelude::*;

#[derive(Debug)]
struct DummyConnection;

struct DummyTx;

impl sqlmodel_core::connection::TransactionOps for DummyTx {
    fn query(
        &self,
        _cx: &Cx,
        _sql: &str,
        _params: &[Value],
    ) -> impl std::future::Future<Output = Outcome<Vec<Row>, Error>> + Send {
        async { Outcome::Ok(vec![]) }
    }

    fn query_one(
        &self,
        _cx: &Cx,
        _sql: &str,
        _params: &[Value],
    ) -> impl std::future::Future<Output = Outcome<Option<Row>, Error>> + Send {
        async { Outcome::Ok(None) }
    }

    fn execute(
        &self,
        _cx: &Cx,
        _sql: &str,
        _params: &[Value],
    ) -> impl std::future::Future<Output = Outcome<u64, Error>> + Send {
        async { Outcome::Ok(0) }
    }

    fn savepoint(
        &self,
        _cx: &Cx,
        _name: &str,
    ) -> impl std::future::Future<Output = Outcome<(), Error>> + Send {
        async { Outcome::Ok(()) }
    }

    fn rollback_to(
        &self,
        _cx: &Cx,
        _name: &str,
    ) -> impl std::future::Future<Output = Outcome<(), Error>> + Send {
        async { Outcome::Ok(()) }
    }

    fn release(
        &self,
        _cx: &Cx,
        _name: &str,
    ) -> impl std::future::Future<Output = Outcome<(), Error>> + Send {
        async { Outcome::Ok(()) }
    }

    fn commit(self, _cx: &Cx) -> impl std::future::Future<Output = Outcome<(), Error>> + Send {
        async { Outcome::Ok(()) }
    }

    fn rollback(self, _cx: &Cx) -> impl std::future::Future<Output = Outcome<(), Error>> + Send {
        async { Outcome::Ok(()) }
    }
}

impl Connection for DummyConnection {
    type Tx<'conn>
        = DummyTx
    where
        Self: 'conn;

    fn query(
        &self,
        _cx: &Cx,
        _sql: &str,
        _params: &[Value],
    ) -> impl std::future::Future<Output = Outcome<Vec<Row>, Error>> + Send {
        async { Outcome::Ok(vec![]) }
    }

    fn query_one(
        &self,
        _cx: &Cx,
        _sql: &str,
        _params: &[Value],
    ) -> impl std::future::Future<Output = Outcome<Option<Row>, Error>> + Send {
        async { Outcome::Ok(None) }
    }

    fn execute(
        &self,
        _cx: &Cx,
        _sql: &str,
        _params: &[Value],
    ) -> impl std::future::Future<Output = Outcome<u64, Error>> + Send {
        async { Outcome::Ok(0) }
    }

    fn insert(
        &self,
        _cx: &Cx,
        _sql: &str,
        _params: &[Value],
    ) -> impl std::future::Future<Output = Outcome<i64, Error>> + Send {
        async { Outcome::Ok(0) }
    }

    fn batch(
        &self,
        _cx: &Cx,
        _statements: &[(String, Vec<Value>)],
    ) -> impl std::future::Future<Output = Outcome<Vec<u64>, Error>> + Send {
        async { Outcome::Ok(vec![]) }
    }

    fn begin(
        &self,
        _cx: &Cx,
    ) -> impl std::future::Future<Output = Outcome<Self::Tx<'_>, Error>> + Send {
        async { Outcome::Ok(DummyTx) }
    }

    fn begin_with(
        &self,
        _cx: &Cx,
        _isolation: sqlmodel_core::connection::IsolationLevel,
    ) -> impl std::future::Future<Output = Outcome<Self::Tx<'_>, Error>> + Send {
        async { Outcome::Ok(DummyTx) }
    }

    fn prepare(
        &self,
        _cx: &Cx,
        _sql: &str,
    ) -> impl std::future::Future<
        Output = Outcome<sqlmodel_core::connection::PreparedStatement, Error>,
    > + Send {
        async {
            Outcome::Ok(sqlmodel_core::connection::PreparedStatement::new(
                0,
                String::new(),
                0,
            ))
        }
    }

    fn query_prepared(
        &self,
        _cx: &Cx,
        _stmt: &sqlmodel_core::connection::PreparedStatement,
        _params: &[Value],
    ) -> impl std::future::Future<Output = Outcome<Vec<Row>, Error>> + Send {
        async { Outcome::Ok(vec![]) }
    }

    fn execute_prepared(
        &self,
        _cx: &Cx,
        _stmt: &sqlmodel_core::connection::PreparedStatement,
        _params: &[Value],
    ) -> impl std::future::Future<Output = Outcome<u64, Error>> + Send {
        async { Outcome::Ok(0) }
    }

    fn ping(&self, _cx: &Cx) -> impl std::future::Future<Output = Outcome<(), Error>> + Send {
        async { Outcome::Ok(()) }
    }

    fn close(
        self,
        _cx: &Cx,
    ) -> impl std::future::Future<Output = sqlmodel_core::Result<()>> + Send {
        async { Ok(()) }
    }
}

#[test]
fn orm_session_is_exposed_in_prelude() {
    // This is intentionally a compile-level smoke test that guards against facade drift.
    let conn = DummyConnection;
    let _session = Session::with_config(conn, SessionConfig::default());
}
