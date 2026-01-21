//! SQLModel-specific renderables.
//!
//! This module contains custom renderable types for SQLModel output:
//!
//! - Query results as tables
//! - Schema diagrams as trees
//! - Error messages as panels
//! - Connection pool status dashboards
//! - Operation progress bars
//! - Indeterminate spinners
//! - Batch operation trackers
//!
//! # Implementation Status
//!
//! - Phase 2: Connection pool status display ✓
//! - Phase 3: Error panels ✓
//! - Phase 4: Query result tables
//! - Phase 5: Schema trees
//! - Phase 6: Operation progress ✓, Indeterminate spinner ✓, Batch tracker ✓

pub mod batch_tracker;
pub mod error;
pub mod operation_progress;
pub mod pool_status;
pub mod spinner;

pub use batch_tracker::{BatchOperationTracker, BatchState};
pub use error::{ErrorPanel, ErrorSeverity};
pub use operation_progress::{OperationProgress, ProgressState};
pub use pool_status::{PoolHealth, PoolStatsProvider, PoolStatusDisplay};
pub use spinner::{IndeterminateSpinner, SpinnerStyle};
