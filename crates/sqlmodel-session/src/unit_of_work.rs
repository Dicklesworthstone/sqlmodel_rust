//! Unit of Work pattern implementation for SQLModel Session.
//!
//! The Unit of Work pattern tracks all changes made during a session and
//! flushes them atomically in the correct dependency order.
//!
//! # Overview
//!
//! The Unit of Work:
//! - Tracks new objects to INSERT
//! - Tracks modified (dirty) objects to UPDATE
//! - Tracks deleted objects to DELETE
//! - Determines flush order based on foreign key dependencies
//! - Detects dependency cycles and reports errors
//! - Executes all changes in a single atomic transaction
//!
//! # Example
//!
//! ```ignore
//! let mut uow = UnitOfWork::new();
//!
//! // Register models (extracts FK dependencies)
//! uow.register_model::<Team>();
//! uow.register_model::<Hero>();
//!
//! // Track changes
//! uow.track_new(&team, &team_key);
//! uow.track_new(&hero, &hero_key);
//! uow.track_dirty(&existing_hero, &hero_key);
//! uow.track_deleted(&old_team, &old_team_key);
//!
//! // Compute flush plan (checks for cycles)
//! let plan = uow.compute_flush_plan()?;
//!
//! // Execute (in a transaction)
//! plan.execute(&cx, &conn).await?;
//! ```

use crate::ObjectKey;
use crate::change_tracker::ChangeTracker;
use crate::flush::{FlushOrderer, FlushPlan, PendingOp};
use serde::Serialize;
use sqlmodel_core::{Error, Model, Value};
use std::collections::{HashMap, HashSet};

/// Tracks and manages all pending changes in a session.
///
/// The Unit of Work is responsible for:
/// - Maintaining the set of new, dirty, and deleted objects
/// - Computing the correct flush order based on FK dependencies
/// - Detecting dependency cycles before flush
#[derive(Default)]
pub struct UnitOfWork {
    /// Objects to be inserted (new).
    new_objects: Vec<TrackedInsert>,

    /// Objects that have been modified (dirty).
    dirty_objects: Vec<TrackedUpdate>,

    /// Objects to be deleted.
    deleted_objects: Vec<TrackedDelete>,

    /// Change tracker for dirty detection.
    change_tracker: ChangeTracker,

    /// Flush orderer for dependency-based ordering.
    orderer: FlushOrderer,

    /// Tables we've seen (for cycle detection).
    tables: HashSet<&'static str>,

    /// Table -> tables it depends on.
    table_dependencies: HashMap<&'static str, Vec<&'static str>>,
}

/// A tracked object pending insertion.
struct TrackedInsert {
    key: ObjectKey,
    table: &'static str,
    columns: Vec<&'static str>,
    values: Vec<Value>,
}

/// A tracked object pending update.
struct TrackedUpdate {
    key: ObjectKey,
    table: &'static str,
    pk_columns: Vec<&'static str>,
    pk_values: Vec<Value>,
    set_columns: Vec<&'static str>,
    set_values: Vec<Value>,
}

/// A tracked object pending deletion.
struct TrackedDelete {
    key: ObjectKey,
    table: &'static str,
    pk_columns: Vec<&'static str>,
    pk_values: Vec<Value>,
}

/// Error type for Unit of Work operations.
#[derive(Debug, Clone)]
pub enum UowError {
    /// A dependency cycle was detected between tables.
    CycleDetected {
        /// Tables involved in the cycle.
        tables: Vec<&'static str>,
    },
    /// An object was already tracked.
    AlreadyTracked {
        /// The object key.
        key: ObjectKey,
        /// The tracking state (new, dirty, deleted).
        state: &'static str,
    },
}

impl std::fmt::Display for UowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UowError::CycleDetected { tables } => {
                write!(f, "Dependency cycle detected: {}", tables.join(" -> "))
            }
            UowError::AlreadyTracked { key, state } => {
                write!(f, "Object {:?} already tracked as {}", key, state)
            }
        }
    }
}

impl std::error::Error for UowError {}

impl From<UowError> for Error {
    fn from(e: UowError) -> Self {
        Error::Custom(e.to_string())
    }
}

impl UnitOfWork {
    /// Create a new empty Unit of Work.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a model type for dependency tracking.
    ///
    /// This extracts foreign key relationships from the model's metadata
    /// and registers them for flush ordering.
    pub fn register_model<T: Model>(&mut self) {
        self.orderer.register_model::<T>();

        let table = T::TABLE_NAME;
        self.tables.insert(table);

        // Extract FK dependencies
        let deps: Vec<&'static str> = T::fields()
            .iter()
            .filter_map(|f| f.foreign_key)
            .filter_map(|fk| fk.split('.').next())
            .collect();

        self.table_dependencies.insert(table, deps);
    }

    /// Track a new object for insertion.
    ///
    /// The object will be INSERTed during flush.
    pub fn track_new<T: Model + Serialize>(&mut self, model: &T, key: ObjectKey) {
        let row = model.to_row();
        let columns: Vec<&'static str> = row.iter().map(|(col, _)| *col).collect();
        let values: Vec<Value> = row.into_iter().map(|(_, val)| val).collect();

        self.new_objects.push(TrackedInsert {
            key,
            table: T::TABLE_NAME,
            columns,
            values,
        });
    }

    /// Track a dirty object for update.
    ///
    /// The object will be UPDATEd during flush (only changed columns).
    pub fn track_dirty<T: Model + Serialize>(
        &mut self,
        model: &T,
        key: ObjectKey,
        changed_columns: Vec<&'static str>,
    ) {
        if changed_columns.is_empty() {
            return;
        }

        let row = model.to_row();
        let row_map: HashMap<&str, Value> = row.into_iter().collect();

        let pk_columns: Vec<&'static str> = T::PRIMARY_KEY.to_vec();
        let pk_values = model.primary_key_value();

        let set_columns = changed_columns;
        let set_values: Vec<Value> = set_columns
            .iter()
            .filter_map(|col| row_map.get(*col).cloned())
            .collect();

        self.dirty_objects.push(TrackedUpdate {
            key,
            table: T::TABLE_NAME,
            pk_columns,
            pk_values,
            set_columns,
            set_values,
        });
    }

    /// Track a dirty object for update (auto-detect changed fields).
    ///
    /// Uses the change tracker to determine which fields changed.
    pub fn track_dirty_auto<T: Model + Serialize>(&mut self, model: &T, key: ObjectKey) {
        let changed = self.change_tracker.changed_fields(&key, model);
        if !changed.is_empty() {
            self.track_dirty(model, key, changed);
        }
    }

    /// Track an object for deletion.
    ///
    /// The object will be DELETEd during flush.
    pub fn track_deleted<T: Model>(&mut self, model: &T, key: ObjectKey) {
        let pk_columns: Vec<&'static str> = T::PRIMARY_KEY.to_vec();
        let pk_values = model.primary_key_value();

        self.deleted_objects.push(TrackedDelete {
            key,
            table: T::TABLE_NAME,
            pk_columns,
            pk_values,
        });
    }

    /// Take a snapshot of an object for later dirty detection.
    pub fn snapshot<T: Model + Serialize>(&mut self, key: ObjectKey, model: &T) {
        self.change_tracker.snapshot(key, model);
    }

    /// Check if an object is dirty (has changed since snapshot).
    pub fn is_dirty<T: Model + Serialize>(&self, key: &ObjectKey, model: &T) -> bool {
        self.change_tracker.is_dirty(key, model)
    }

    /// Get the changed fields for an object.
    pub fn changed_fields<T: Model + Serialize>(
        &self,
        key: &ObjectKey,
        model: &T,
    ) -> Vec<&'static str> {
        self.change_tracker.changed_fields(key, model)
    }

    /// Check for dependency cycles in the registered tables.
    ///
    /// Returns `Err(UowError::CycleDetected)` if a cycle is found.
    pub fn check_cycles(&self) -> Result<(), UowError> {
        // Use DFS to detect cycles
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
        let mut cycle_path = Vec::new();

        for table in &self.tables {
            if !visited.contains(table)
                && self.detect_cycle_dfs(table, &mut visited, &mut rec_stack, &mut cycle_path)
            {
                return Err(UowError::CycleDetected { tables: cycle_path });
            }
        }

        Ok(())
    }

    /// DFS helper for cycle detection.
    fn detect_cycle_dfs(
        &self,
        table: &'static str,
        visited: &mut HashSet<&'static str>,
        rec_stack: &mut HashSet<&'static str>,
        path: &mut Vec<&'static str>,
    ) -> bool {
        visited.insert(table);
        rec_stack.insert(table);
        path.push(table);

        if let Some(deps) = self.table_dependencies.get(table) {
            for dep in deps {
                // Only check tables we know about
                if !self.tables.contains(dep) {
                    continue;
                }

                if !visited.contains(dep) {
                    if self.detect_cycle_dfs(dep, visited, rec_stack, path) {
                        return true;
                    }
                } else if rec_stack.contains(dep) {
                    // Found cycle - add the start of cycle to complete it
                    path.push(dep);
                    return true;
                }
            }
        }

        rec_stack.remove(table);
        path.pop();
        false
    }

    /// Compute the flush plan.
    ///
    /// This checks for cycles and orders operations by dependencies.
    ///
    /// # Errors
    ///
    /// Returns `Err` if a dependency cycle is detected.
    pub fn compute_flush_plan(&self) -> Result<FlushPlan, UowError> {
        // Check for cycles first
        self.check_cycles()?;

        // Build pending ops
        let mut ops = Vec::new();

        // Add inserts
        for insert in &self.new_objects {
            ops.push(PendingOp::Insert {
                key: insert.key,
                table: insert.table,
                columns: insert.columns.clone(),
                values: insert.values.clone(),
            });
        }

        // Add updates
        for update in &self.dirty_objects {
            ops.push(PendingOp::Update {
                key: update.key,
                table: update.table,
                pk_columns: update.pk_columns.clone(),
                pk_values: update.pk_values.clone(),
                set_columns: update.set_columns.clone(),
                set_values: update.set_values.clone(),
            });
        }

        // Add deletes
        for delete in &self.deleted_objects {
            ops.push(PendingOp::Delete {
                key: delete.key,
                table: delete.table,
                pk_columns: delete.pk_columns.clone(),
                pk_values: delete.pk_values.clone(),
            });
        }

        // Order by dependencies
        Ok(self.orderer.order(ops))
    }

    /// Clear all tracked changes.
    ///
    /// Call this after a successful commit.
    pub fn clear(&mut self) {
        self.new_objects.clear();
        self.dirty_objects.clear();
        self.deleted_objects.clear();
        self.change_tracker.clear_all();
    }

    /// Check if there are any pending changes.
    #[must_use]
    pub fn has_changes(&self) -> bool {
        !self.new_objects.is_empty()
            || !self.dirty_objects.is_empty()
            || !self.deleted_objects.is_empty()
    }

    /// Get the count of pending operations.
    #[must_use]
    pub fn pending_count(&self) -> PendingCounts {
        PendingCounts {
            new: self.new_objects.len(),
            dirty: self.dirty_objects.len(),
            deleted: self.deleted_objects.len(),
        }
    }

    /// Get a reference to the change tracker.
    #[must_use]
    pub fn change_tracker(&self) -> &ChangeTracker {
        &self.change_tracker
    }

    /// Get a mutable reference to the change tracker.
    pub fn change_tracker_mut(&mut self) -> &mut ChangeTracker {
        &mut self.change_tracker
    }
}

/// Count of pending operations by type.
#[derive(Debug, Clone, Copy, Default)]
pub struct PendingCounts {
    /// Objects pending INSERT.
    pub new: usize,
    /// Objects pending UPDATE.
    pub dirty: usize,
    /// Objects pending DELETE.
    pub deleted: usize,
}

impl PendingCounts {
    /// Total number of pending operations.
    #[must_use]
    pub fn total(&self) -> usize {
        self.new + self.dirty + self.deleted
    }

    /// Check if there are no pending operations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.new == 0 && self.dirty == 0 && self.deleted == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use sqlmodel_core::{FieldInfo, Row, SqlType};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Team {
        id: Option<i64>,
        name: String,
    }

    impl Model for Team {
        const TABLE_NAME: &'static str = "teams";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", SqlType::BigInt).primary_key(true),
                FieldInfo::new("name", "name", SqlType::Text),
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![
                ("id", self.id.map_or(Value::Null, Value::BigInt)),
                ("name", Value::Text(self.name.clone())),
            ]
        }

        fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
            Ok(Self {
                id: None,
                name: String::new(),
            })
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![self.id.map_or(Value::Null, Value::BigInt)]
        }

        fn is_new(&self) -> bool {
            self.id.is_none()
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Hero {
        id: Option<i64>,
        name: String,
        team_id: Option<i64>,
    }

    impl Model for Hero {
        const TABLE_NAME: &'static str = "heroes";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: &[FieldInfo] = &[
                FieldInfo::new("id", "id", SqlType::BigInt).primary_key(true),
                FieldInfo::new("name", "name", SqlType::Text),
                FieldInfo::new("team_id", "team_id", SqlType::BigInt)
                    .nullable(true)
                    .foreign_key("teams.id"),
            ];
            FIELDS
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![
                ("id", self.id.map_or(Value::Null, Value::BigInt)),
                ("name", Value::Text(self.name.clone())),
                ("team_id", self.team_id.map_or(Value::Null, Value::BigInt)),
            ]
        }

        fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
            Ok(Self {
                id: None,
                name: String::new(),
                team_id: None,
            })
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![self.id.map_or(Value::Null, Value::BigInt)]
        }

        fn is_new(&self) -> bool {
            self.id.is_none()
        }
    }

    fn make_key<T: Model + 'static>(pk: i64) -> ObjectKey {
        ObjectKey::from_pk::<T>(&[Value::BigInt(pk)])
    }

    #[test]
    fn test_track_new_object() {
        let mut uow = UnitOfWork::new();

        let team = Team {
            id: Some(1),
            name: "Avengers".to_string(),
        };
        let key = make_key::<Team>(1);

        uow.track_new(&team, key);

        assert!(uow.has_changes());
        assert_eq!(uow.pending_count().new, 1);
        assert_eq!(uow.pending_count().dirty, 0);
        assert_eq!(uow.pending_count().deleted, 0);
    }

    #[test]
    fn test_track_dirty_object() {
        let mut uow = UnitOfWork::new();

        let hero = Hero {
            id: Some(1),
            name: "Spider-Man".to_string(),
            team_id: Some(1),
        };
        let key = make_key::<Hero>(1);

        uow.track_dirty(&hero, key, vec!["name"]);

        assert!(uow.has_changes());
        assert_eq!(uow.pending_count().dirty, 1);
    }

    #[test]
    fn test_track_deleted_object() {
        let mut uow = UnitOfWork::new();

        let team = Team {
            id: Some(1),
            name: "Avengers".to_string(),
        };
        let key = make_key::<Team>(1);

        uow.track_deleted(&team, key);

        assert!(uow.has_changes());
        assert_eq!(uow.pending_count().deleted, 1);
    }

    #[test]
    fn test_compute_flush_plan_orders_correctly() {
        let mut uow = UnitOfWork::new();
        uow.register_model::<Team>();
        uow.register_model::<Hero>();

        // Add hero first (has FK to team), then team
        let hero = Hero {
            id: Some(1),
            name: "Spider-Man".to_string(),
            team_id: Some(1),
        };
        let team = Team {
            id: Some(1),
            name: "Avengers".to_string(),
        };

        uow.track_new(&hero, make_key::<Hero>(1));
        uow.track_new(&team, make_key::<Team>(1));

        let plan = uow.compute_flush_plan().unwrap();

        // Team should be inserted first (no deps)
        assert_eq!(plan.inserts[0].table(), "teams");
        assert_eq!(plan.inserts[1].table(), "heroes");
    }

    #[test]
    fn test_clear_removes_all_tracked() {
        let mut uow = UnitOfWork::new();

        let team = Team {
            id: Some(1),
            name: "Avengers".to_string(),
        };
        uow.track_new(&team, make_key::<Team>(1));
        uow.track_deleted(&team, make_key::<Team>(2));

        assert!(uow.has_changes());

        uow.clear();

        assert!(!uow.has_changes());
        assert!(uow.pending_count().is_empty());
    }

    #[test]
    fn test_snapshot_and_dirty_detection() {
        let mut uow = UnitOfWork::new();

        let hero = Hero {
            id: Some(1),
            name: "Spider-Man".to_string(),
            team_id: Some(1),
        };
        let key = make_key::<Hero>(1);

        // Take snapshot
        uow.snapshot(key, &hero);

        // Not dirty yet
        assert!(!uow.is_dirty(&key, &hero));

        // Modify
        let modified = Hero {
            id: Some(1),
            name: "Peter Parker".to_string(),
            team_id: Some(1),
        };

        // Now dirty
        assert!(uow.is_dirty(&key, &modified));

        // Check which fields changed
        let changed = uow.changed_fields(&key, &modified);
        assert_eq!(changed, vec!["name"]);
    }

    #[test]
    fn test_track_dirty_auto() {
        let mut uow = UnitOfWork::new();

        let hero = Hero {
            id: Some(1),
            name: "Spider-Man".to_string(),
            team_id: Some(1),
        };
        let key = make_key::<Hero>(1);

        // Snapshot original
        uow.snapshot(key, &hero);

        // Modify
        let modified = Hero {
            id: Some(1),
            name: "Peter Parker".to_string(),
            team_id: Some(2),
        };

        // Auto-track dirty
        uow.track_dirty_auto(&modified, key);

        assert_eq!(uow.pending_count().dirty, 1);
    }

    #[test]
    fn test_no_cycle_in_normal_hierarchy() {
        let mut uow = UnitOfWork::new();
        uow.register_model::<Team>();
        uow.register_model::<Hero>();

        // Hero -> Team is a valid hierarchy (no cycle)
        assert!(uow.check_cycles().is_ok());
    }

    #[test]
    fn test_pending_counts() {
        let counts = PendingCounts {
            new: 3,
            dirty: 2,
            deleted: 1,
        };

        assert_eq!(counts.total(), 6);
        assert!(!counts.is_empty());

        let empty = PendingCounts::default();
        assert!(empty.is_empty());
        assert_eq!(empty.total(), 0);
    }

    #[test]
    fn test_empty_dirty_not_tracked() {
        let mut uow = UnitOfWork::new();

        let hero = Hero {
            id: Some(1),
            name: "Spider-Man".to_string(),
            team_id: Some(1),
        };
        let key = make_key::<Hero>(1);

        // Empty changed columns - should not track
        uow.track_dirty(&hero, key, vec![]);

        assert!(!uow.has_changes());
    }
}
