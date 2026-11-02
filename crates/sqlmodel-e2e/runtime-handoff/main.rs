//! Production-only witnesses for sqlmodel_rust#43 and asupersync#85.
//!
//! `scripts/qualify_postgres_runtime_handoff.py` builds this source in an
//! isolated Cargo workspace, without dev-dependency feature unification, and
//! runs each case in its own process with an external deadline. A runtime
//! readiness regression must fail qualification rather than hang its runner.

#[cfg(not(panic = "unwind"))]
compile_error!("runtime-handoff qualification requires panic=unwind");

use std::future::poll_fn;
use std::io::Write;
use std::net::TcpListener;
use std::pin::Pin;
use std::sync::{Arc, mpsc};
use std::task::Poll;
use std::time::Duration;

use asupersync::io::{AsyncRead, ReadBuf};
use asupersync::net::TcpStream;
use asupersync::runtime::{Runtime, RuntimeBuilder};
use asupersync::{Budget, Cx, Outcome};
use sqlmodel_core::{Connection, Error};
use sqlmodel_pool::{Pool, PoolConfig};
use sqlmodel_postgres::{PgConfig, SharedPgConnection};

#[derive(Clone, Copy)]
enum RuntimeFlavor {
    CurrentThread,
    TwoWorkers,
}

impl RuntimeFlavor {
    fn build(self) -> Runtime {
        match self {
            Self::CurrentThread => RuntimeBuilder::current_thread(),
            // This exact control configuration is part of the issue's matrix.
            Self::TwoWorkers => RuntimeBuilder::new().worker_threads(2),
        }
        .build()
        .expect("build runtime")
    }
}

#[derive(Clone, Copy)]
enum HolderExit {
    Normal,
    Panic,
}

#[derive(Debug)]
struct ExpectedHolderPanic;

fn expect_ok<T>(outcome: Outcome<T, Error>, operation: &str) -> T {
    match outcome {
        Outcome::Ok(value) => value,
        Outcome::Err(error) => panic!("{operation}: {error}"),
        Outcome::Cancelled(reason) => panic!("{operation}: cancelled: {reason:?}"),
        Outcome::Panicked(info) => panic!("{operation}: panicked: {info:?}"),
    }
}

fn postgres_config() -> PgConfig {
    // The runner parses SQLMODEL_TEST_POSTGRES_URL and passes its decoded
    // components through the environment, never through argv or the logs.
    let required = |name| std::env::var(name).unwrap_or_else(|_| panic!("missing {name}"));
    let mut config = PgConfig::new(
        required("SQLMODEL_HANDOFF_PG_HOST"),
        required("SQLMODEL_HANDOFF_PG_USER"),
        required("SQLMODEL_HANDOFF_PG_DATABASE"),
    )
    .port(
        required("SQLMODEL_HANDOFF_PG_PORT")
            .parse()
            .expect("PostgreSQL port must be a u16"),
    )
    .connect_timeout(Duration::from_secs(10));
    if let Ok(password) = std::env::var("SQLMODEL_HANDOFF_PG_PASSWORD") {
        config = config.password(password);
    }
    config
}

async fn backend_pid(cx: &Cx, connection: &SharedPgConnection) -> i64 {
    let rows = expect_ok(
        connection.query(cx, "SELECT pg_backend_pid()", &[]).await,
        "read PostgreSQL backend pid",
    );
    assert_eq!(rows.len(), 1);
    rows[0].get_as(0).expect("integer backend pid")
}

fn postgres_handoff(flavor: RuntimeFlavor, exit: HolderExit) {
    let config = postgres_config();
    let pool = Arc::new(Pool::<SharedPgConnection>::new(
        PoolConfig::new(3).min_connections(0).acquire_timeout(2_000),
    ));
    assert!(pool.config().test_on_checkout, "keep default checkout ping");
    let (pid_tx, pid_rx) = mpsc::channel();
    let holder_pool = Arc::clone(&pool);
    let holder_config = config.clone();
    let holder = std::thread::spawn(move || {
        let runtime = flavor.build();
        let cx = runtime.request_cx_with_budget(Budget::INFINITE);
        runtime.block_on(async {
            let lease = expect_ok(
                holder_pool
                    .acquire(&cx, || {
                        SharedPgConnection::connect(&cx, holder_config.clone())
                    })
                    .await,
                "holder acquire",
            );
            let rows = expect_ok(lease.query(&cx, "SELECT 1", &[]).await, "holder query");
            assert_eq!(rows.len(), 1);
            let pid = backend_pid(&cx, &lease).await;
            assert_eq!(holder_pool.stats().active_connections, 1);
            pid_tx.send(pid).expect("record original backend");
            match exit {
                HolderExit::Normal => drop(lease),
                HolderExit::Panic => {
                    eprintln!("holder: intentional panic with an active lease");
                    std::panic::panic_any(ExpectedHolderPanic);
                }
            }
        });
    });
    match (exit, holder.join()) {
        (HolderExit::Normal, Ok(())) => {}
        (HolderExit::Panic, Err(payload)) => {
            // A connect/query/assertion panic is a failure, not a successful
            // execution of the intentional-panic case.
            assert!(
                payload.is::<ExpectedHolderPanic>(),
                "holder panicked before reaching the intentional panic"
            );
        }
        (HolderExit::Normal, Err(payload)) => std::panic::resume_unwind(payload),
        (HolderExit::Panic, Ok(())) => panic!("holder did not panic"),
    }
    let original_pid = pid_rx.recv().expect("holder completed its queries");
    let stats = pool.stats();
    eprintln!("holder joined and runtime dropped: {stats:?}");
    assert_eq!(stats.total_connections, 1);
    assert_eq!(stats.idle_connections, 1);
    assert_eq!(stats.active_connections, 0);
    assert_eq!(stats.pending_requests, 0);
    assert_eq!(stats.connections_created, 1);
    assert_eq!(stats.connections_closed, 0);
    assert_eq!(stats.acquires, 1);
    assert_eq!(stats.timeouts, 0);

    // The join above establishes that no holder runtime can still drive this
    // connection. The consumer gets its own production request context.
    let runtime = flavor.build();
    let cx = runtime.request_cx_with_budget(Budget::INFINITE);
    runtime.block_on(async {
        eprintln!("consumer: before acquire (default checkout ping)");
        let lease = expect_ok(
            pool.acquire(&cx, || SharedPgConnection::connect(&cx, config.clone()))
                .await,
            "consumer acquire",
        );
        eprintln!("consumer: acquired; before query");
        let rows = expect_ok(lease.query(&cx, "SELECT 1", &[]).await, "consumer query");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            backend_pid(&cx, &lease).await,
            original_pid,
            "checkout must reuse the original PostgreSQL session"
        );
        let stats = pool.stats();
        assert_eq!(stats.total_connections, 1);
        assert_eq!(stats.active_connections, 1);
        assert_eq!(stats.idle_connections, 0);
        assert_eq!(stats.pending_requests, 0);
        assert_eq!(stats.connections_created, 1, "no reconnect masks the bug");
        assert_eq!(stats.connections_closed, 0);
        assert_eq!(stats.acquires, 2);
        assert_eq!(stats.timeouts, 0);
        drop(lease);
        eprintln!("consumer: query complete; before drain");
        expect_ok(pool.close_and_drain(&cx).await, "consumer close_and_drain");
    });
    let stats = pool.stats();
    eprintln!("consumer: drained: {stats:?}");
    assert!(pool.is_closed());
    assert_eq!(stats.total_connections, 0);
    assert_eq!(stats.idle_connections, 0);
    assert_eq!(stats.active_connections, 0);
    assert_eq!(stats.pending_requests, 0);
    assert_eq!(stats.connections_created, 1);
    assert_eq!(stats.connections_closed, 1);
}

fn stale_socket_handoff(flavor: RuntimeFlavor) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind delayed peer");
    let address = listener.local_addr().expect("peer address");
    let (release_tx, release_rx) = mpsc::channel();
    let peer = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().expect("accept witness socket");
        // Only the consumer's first Pending read releases this byte. Both
        // runtimes must therefore register readiness; no scheduling delay or
        // already-buffered response can accidentally make the witness pass.
        release_rx.recv().expect("consumer reached Pending");
        socket.write_all(&[0x43]).expect("send delayed byte");
    });
    let holder = std::thread::spawn(move || {
        let runtime = flavor.build();
        let mut stream = runtime
            .block_on(TcpStream::connect(address))
            .expect("connect witness socket");
        runtime.block_on(poll_fn(|task_cx| {
            let mut byte = [0];
            let mut buffer = ReadBuf::new(&mut byte);
            assert!(
                Pin::new(&mut stream)
                    .poll_read(task_cx, &mut buffer)
                    .is_pending(),
                "peer must withhold data while the holder installs a registration"
            );
            // Deliberately abandon this pending read, leaving the socket's
            // old registration and cached root waker intact across shutdown.
            Poll::Ready(())
        }));
        stream
    });
    let mut stream = holder.join().expect("holder exits after registering read");
    eprintln!("stale socket: holder joined; registration must migrate");
    let runtime = flavor.build();
    let mut byte = [0];
    let mut buffer = ReadBuf::new(&mut byte);
    let mut released = false;
    runtime
        .block_on(poll_fn(|task_cx| {
            let result = Pin::new(&mut stream).poll_read(task_cx, &mut buffer);
            if result.is_pending() && !released {
                released = true;
                release_tx.send(()).expect("release delayed peer");
            }
            result
        }))
        .expect("read through migrated registration");
    assert!(released, "consumer must actually await readiness");
    assert_eq!(buffer.filled(), &[0x43]);
    drop(stream);
    peer.join().expect("delayed peer completes");
}

fn main() {
    let case = std::env::args().nth(1).expect("one case name is required");
    eprintln!("START {case}");
    match case.as_str() {
        "current-thread-normal" => {
            postgres_handoff(RuntimeFlavor::CurrentThread, HolderExit::Normal);
        }
        "current-thread-panic" => {
            postgres_handoff(RuntimeFlavor::CurrentThread, HolderExit::Panic);
        }
        "two-worker-normal" => postgres_handoff(RuntimeFlavor::TwoWorkers, HolderExit::Normal),
        "two-worker-panic" => postgres_handoff(RuntimeFlavor::TwoWorkers, HolderExit::Panic),
        "stale-socket-current-thread" => stale_socket_handoff(RuntimeFlavor::CurrentThread),
        "stale-socket-two-worker" => stale_socket_handoff(RuntimeFlavor::TwoWorkers),
        _ => panic!("unknown qualification case: {case}"),
    }
    println!("PASS {case}");
}
