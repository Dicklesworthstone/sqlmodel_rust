//! SQLModel-specific renderables.
//!
//! This module contains custom renderable types for SQLModel output:
//!
//! - Query results as tables
//! - Schema diagrams as trees
//! - Table info panels for single-table details
//! - Error messages as panels
//! - Connection pool status dashboards
//! - Operation progress bars
//! - Indeterminate spinners
//! - Batch operation trackers
//! - SQL syntax highlighting
//! - Query tree visualization
//! - Query timing display
//! - Migration status panels
//!
//! # Implementation Status
//!
//! - Phase 2: Connection pool status display ✓
//! - Phase 3: Error panels ✓
//! - Phase 4: Query result tables ✓, SQL syntax ✓, Query tree ✓, Query timing ✓
//! - Phase 5: Schema trees ✓, DDL syntax highlighting ✓, Table info panels ✓, Migration status ✓
//! - Phase 6: Operation progress ✓, Indeterminate spinner ✓, Batch tracker ✓

pub mod batch_tracker;
pub mod ddl_display;
pub mod error;
pub mod migration_status;
pub mod operation_progress;
pub mod pool_status;
pub mod query_results;
pub mod query_timing;
pub mod query_tree;
pub mod schema_tree;
pub mod spinner;
pub mod sql_syntax;
pub mod table_info;

pub use batch_tracker::{BatchOperationTracker, BatchState};
pub use ddl_display::{ChangeKind, ChangeRegion, DdlDisplay, SqlDialect};
pub use error::{ErrorPanel, ErrorSeverity};
pub use migration_status::{MigrationRecord, MigrationState, MigrationStatus};
pub use operation_progress::{OperationProgress, ProgressState};
pub use pool_status::{PoolHealth, PoolStatsProvider, PoolStatusDisplay};
pub use query_results::{Cell, PlainFormat, QueryResultTable, QueryResults, ValueType};
pub use query_timing::QueryTiming;
pub use query_tree::QueryTreeView;
pub use schema_tree::{
    ColumnData, ForeignKeyData, IndexData, SchemaTree, SchemaTreeConfig, TableData,
};
pub use spinner::{IndeterminateSpinner, SpinnerStyle};
pub use sql_syntax::SqlHighlighter;
pub use table_info::{TableInfo, TableStats, format_bytes, format_number};
