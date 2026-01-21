//! SQLModel-specific renderables.
//!
//! This module contains custom renderable types for SQLModel output:
//!
//! - Query results as tables
//! - Schema diagrams as trees
//! - Error messages as panels
//! - Connection pool status dashboards
//!
//! # Implementation Status
//!
//! - Phase 2: Connection pool status display ✓
//! - Phase 3: Error panels
//! - Phase 4: Query result tables
//! - Phase 5: Schema trees

pub mod pool_status;

pub use pool_status::{PoolHealth, PoolStatsProvider, PoolStatusDisplay};
