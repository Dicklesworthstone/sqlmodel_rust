//! ORM session re-exports.
//!
//! `sqlmodel::Session` is the SQLAlchemy/SQLModel-style session:
//! identity map + unit-of-work + explicit lazy/batch loading APIs.
//!
//! The implementation lives in the separate `sqlmodel-session` crate. This module
//! exists so the `sqlmodel` facade can expose the ORM session without forcing
//! users to depend on sub-crates directly.
//!
//! For a lightweight "connection + optional console" wrapper, use
//! `sqlmodel::ConnectionSession`.

pub use sqlmodel_session::{
    GetOptions, ObjectKey, ObjectState, Session, SessionConfig, SessionDebugInfo,
};
