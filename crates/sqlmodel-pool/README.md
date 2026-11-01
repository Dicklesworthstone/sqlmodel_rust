# sqlmodel-pool

Structured-concurrency-aware connection pooling.

## Role in the SQLModel Rust System
- Budget-aware acquisition via Cx timeouts and cancellation.
- Health checks and lifecycle management for connections.
- Works with any sqlmodel-core::Connection implementation.

## Usage
Most users should depend on `sqlmodel` and import from `sqlmodel::prelude::*`.
Use this crate directly if you are extending internals or building tooling around the core APIs.

## Acquisition deadlines and cancellation

`Pool::acquire` uses one absolute deadline: the earlier of
`PoolConfig::acquire_timeout_ms` and the caller's `Cx` budget deadline. The
budget covers waiting for capacity, opening a connection, the checkout ping,
and closing rejected connections. Retrying an unhealthy idle connection does
not restart the clock. Deadline expiry returns `PoolErrorKind::Timeout` and
increments `PoolStats::timeouts` once; cancellation before that deadline
returns `Outcome::Cancelled`.

The pool registers its own timer and cancellation wakeups, so a driver whose
ping never wakes cannot turn a finite acquisition timeout into an indefinite
wait. On timeout, cancellation, or a dropped acquisition future, an in-flight
validation connection is discarded before its active/total slot is released.
It is never returned to the idle queue. Pending-request accounting is also
released on every exit. Cancellation and panic outcomes from validation are
propagated, while ordinary failed pings retire the connection and retry within
the remaining budget.

These protections bound the failure described in
[issue #43](https://github.com/Dicklesworthstone/sqlmodel_rust/issues/43). They
do not by themselves repair socket registrations owned by a departed runtime
or qualify an already-published SQLModel release. Runtime-handoff qualification
must additionally prove reuse of the original PostgreSQL connection with the
selected published Asupersync dependency.

## Links
- Repository: https://github.com/Dicklesworthstone/sqlmodel_rust
- Documentation: https://docs.rs/sqlmodel-pool
