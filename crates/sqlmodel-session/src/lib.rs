//! Session and Unit of Work for SQLModel Rust.
//!
//! The Session is the central unit-of-work manager. It holds a database connection,
//! tracks objects, and coordinates flushing changes to the database.
//!
//! # Design Philosophy
//!
//! - **Explicit over implicit**: No autoflush by default
//! - **Ownership clarity**: Session owns the connection
//! - **Type erasure**: Identity map stores `Box<dyn Any>` for heterogeneous objects
//! - **Transaction safety**: Atomic commit/rollback semantics
//!
//! # Example
//!
//! ```ignore
//! // Create session from pool
//! let mut session = Session::new(&pool).await?;
//!
//! // Add new objects (will be INSERTed on flush)
//! session.add(&hero);
//!
//! // Get by primary key (uses identity map)
//! let hero = session.get::<Hero>(1).await?;
//!
//! // Mark for deletion
//! session.delete(&hero);
//!
//! // Flush pending changes to DB
//! session.flush().await?;
//!
//! // Commit the transaction
//! session.commit().await?;
//! ```

pub mod change_tracker;
pub mod flush;
pub mod identity_map;
pub mod n1_detection;
pub mod unit_of_work;

pub use change_tracker::{ChangeTracker, ObjectSnapshot};
pub use flush::{
    FlushOrderer, FlushPlan, FlushResult, LinkTableOp, PendingOp, execute_link_table_ops,
};
pub use identity_map::{IdentityMap, ModelReadGuard, ModelRef, ModelWriteGuard, WeakIdentityMap};
pub use n1_detection::{CallSite, N1DetectionScope, N1QueryTracker, N1Stats};
pub use unit_of_work::{PendingCounts, UnitOfWork, UowError};

use asupersync::{Cx, Outcome};
use serde::{Deserialize, Serialize};
use sqlmodel_core::{Connection, Error, Lazy, LazyLoader, Model, Value};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::future::Future;
use std::hash::{Hash, Hasher};

// ============================================================================
// Session Configuration
// ============================================================================

/// Configuration for Session behavior.
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// Whether to auto-begin a transaction on first operation.
    pub auto_begin: bool,
    /// Whether to auto-flush before queries (not recommended for performance).
    pub auto_flush: bool,
    /// Whether to expire objects after commit (reload from DB on next access).
    pub expire_on_commit: bool,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            auto_begin: true,
            auto_flush: false,
            expire_on_commit: true,
        }
    }
}

// ============================================================================
// Object Key and State
// ============================================================================

/// Unique key for an object in the identity map.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObjectKey {
    /// Type identifier for the Model type.
    type_id: TypeId,
    /// Hash of the primary key value(s).
    pk_hash: u64,
}

impl ObjectKey {
    /// Create an object key from a model instance.
    pub fn from_model<M: Model + 'static>(obj: &M) -> Self {
        let pk_values = obj.primary_key_value();
        Self {
            type_id: TypeId::of::<M>(),
            pk_hash: hash_values(&pk_values),
        }
    }

    /// Create an object key from type and primary key.
    pub fn from_pk<M: Model + 'static>(pk: &[Value]) -> Self {
        Self {
            type_id: TypeId::of::<M>(),
            pk_hash: hash_values(pk),
        }
    }

    /// Get the primary key hash.
    pub fn pk_hash(&self) -> u64 {
        self.pk_hash
    }

    /// Get the type identifier.
    pub fn type_id(&self) -> TypeId {
        self.type_id
    }
}

/// Hash a slice of values for use as a primary key hash.
fn hash_values(values: &[Value]) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    let mut hasher = DefaultHasher::new();
    for v in values {
        // Hash based on value variant and content
        match v {
            Value::Null => 0u8.hash(&mut hasher),
            Value::Bool(b) => {
                1u8.hash(&mut hasher);
                b.hash(&mut hasher);
            }
            Value::TinyInt(i) => {
                2u8.hash(&mut hasher);
                i.hash(&mut hasher);
            }
            Value::SmallInt(i) => {
                3u8.hash(&mut hasher);
                i.hash(&mut hasher);
            }
            Value::Int(i) => {
                4u8.hash(&mut hasher);
                i.hash(&mut hasher);
            }
            Value::BigInt(i) => {
                5u8.hash(&mut hasher);
                i.hash(&mut hasher);
            }
            Value::Float(f) => {
                6u8.hash(&mut hasher);
                f.to_bits().hash(&mut hasher);
            }
            Value::Double(f) => {
                7u8.hash(&mut hasher);
                f.to_bits().hash(&mut hasher);
            }
            Value::Decimal(s) => {
                8u8.hash(&mut hasher);
                s.hash(&mut hasher);
            }
            Value::Text(s) => {
                9u8.hash(&mut hasher);
                s.hash(&mut hasher);
            }
            Value::Bytes(b) => {
                10u8.hash(&mut hasher);
                b.hash(&mut hasher);
            }
            Value::Date(d) => {
                11u8.hash(&mut hasher);
                d.hash(&mut hasher);
            }
            Value::Time(t) => {
                12u8.hash(&mut hasher);
                t.hash(&mut hasher);
            }
            Value::Timestamp(ts) => {
                13u8.hash(&mut hasher);
                ts.hash(&mut hasher);
            }
            Value::TimestampTz(ts) => {
                14u8.hash(&mut hasher);
                ts.hash(&mut hasher);
            }
            Value::Uuid(u) => {
                15u8.hash(&mut hasher);
                u.hash(&mut hasher);
            }
            Value::Json(j) => {
                16u8.hash(&mut hasher);
                // Hash the JSON string representation
                j.to_string().hash(&mut hasher);
            }
            Value::Array(arr) => {
                17u8.hash(&mut hasher);
                // Recursively hash array elements
                arr.len().hash(&mut hasher);
                for item in arr {
                    hash_value(item, &mut hasher);
                }
            }
            Value::Default => {
                18u8.hash(&mut hasher);
            }
        }
    }
    hasher.finish()
}

/// Hash a single value into the hasher.
fn hash_value(v: &Value, hasher: &mut impl Hasher) {
    match v {
        Value::Null => 0u8.hash(hasher),
        Value::Bool(b) => {
            1u8.hash(hasher);
            b.hash(hasher);
        }
        Value::TinyInt(i) => {
            2u8.hash(hasher);
            i.hash(hasher);
        }
        Value::SmallInt(i) => {
            3u8.hash(hasher);
            i.hash(hasher);
        }
        Value::Int(i) => {
            4u8.hash(hasher);
            i.hash(hasher);
        }
        Value::BigInt(i) => {
            5u8.hash(hasher);
            i.hash(hasher);
        }
        Value::Float(f) => {
            6u8.hash(hasher);
            f.to_bits().hash(hasher);
        }
        Value::Double(f) => {
            7u8.hash(hasher);
            f.to_bits().hash(hasher);
        }
        Value::Decimal(s) => {
            8u8.hash(hasher);
            s.hash(hasher);
        }
        Value::Text(s) => {
            9u8.hash(hasher);
            s.hash(hasher);
        }
        Value::Bytes(b) => {
            10u8.hash(hasher);
            b.hash(hasher);
        }
        Value::Date(d) => {
            11u8.hash(hasher);
            d.hash(hasher);
        }
        Value::Time(t) => {
            12u8.hash(hasher);
            t.hash(hasher);
        }
        Value::Timestamp(ts) => {
            13u8.hash(hasher);
            ts.hash(hasher);
        }
        Value::TimestampTz(ts) => {
            14u8.hash(hasher);
            ts.hash(hasher);
        }
        Value::Uuid(u) => {
            15u8.hash(hasher);
            u.hash(hasher);
        }
        Value::Json(j) => {
            16u8.hash(hasher);
            j.to_string().hash(hasher);
        }
        Value::Array(arr) => {
            17u8.hash(hasher);
            arr.len().hash(hasher);
            for item in arr {
                hash_value(item, hasher);
            }
        }
        Value::Default => {
            18u8.hash(hasher);
        }
    }
}

/// State of a tracked object in the session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectState {
    /// New object, needs INSERT on flush.
    New,
    /// Persistent object loaded from database.
    Persistent,
    /// Object marked for deletion, needs DELETE on flush.
    Deleted,
    /// Object detached from session.
    Detached,
    /// Object expired, needs reload from database.
    Expired,
}

/// A tracked object in the session.
struct TrackedObject {
    /// The actual object (type-erased).
    object: Box<dyn Any + Send + Sync>,
    /// Original serialized state for dirty checking.
    original_state: Option<Vec<u8>>,
    /// Current object state.
    state: ObjectState,
    /// Table name for this object.
    table_name: &'static str,
    /// Column names for this object.
    column_names: Vec<&'static str>,
    /// Current values for each column (for INSERT/UPDATE).
    values: Vec<Value>,
    /// Primary key column names.
    pk_columns: Vec<&'static str>,
    /// Primary key values (for DELETE/UPDATE WHERE clause).
    pk_values: Vec<Value>,
}

// ============================================================================
// Session
// ============================================================================

/// The Session is the central unit-of-work manager.
///
/// It tracks objects loaded from or added to the database and coordinates
/// flushing changes back to the database.
pub struct Session<C: Connection> {
    /// The database connection.
    connection: C,
    /// Whether we're in a transaction.
    in_transaction: bool,
    /// Identity map: ObjectKey -> TrackedObject.
    identity_map: HashMap<ObjectKey, TrackedObject>,
    /// Objects marked as new (need INSERT).
    pending_new: Vec<ObjectKey>,
    /// Objects marked as deleted (need DELETE).
    pending_delete: Vec<ObjectKey>,
    /// Objects that are dirty (need UPDATE).
    pending_dirty: Vec<ObjectKey>,
    /// Configuration.
    config: SessionConfig,
    /// N+1 query detection tracker (optional).
    n1_tracker: Option<N1QueryTracker>,
}

impl<C: Connection> Session<C> {
    /// Create a new session from an existing connection.
    pub fn new(connection: C) -> Self {
        Self::with_config(connection, SessionConfig::default())
    }

    /// Create a new session with custom configuration.
    pub fn with_config(connection: C, config: SessionConfig) -> Self {
        Self {
            connection,
            in_transaction: false,
            identity_map: HashMap::new(),
            pending_new: Vec::new(),
            pending_delete: Vec::new(),
            pending_dirty: Vec::new(),
            config,
            n1_tracker: None,
        }
    }

    /// Get a reference to the underlying connection.
    pub fn connection(&self) -> &C {
        &self.connection
    }

    /// Get the session configuration.
    pub fn config(&self) -> &SessionConfig {
        &self.config
    }

    // ========================================================================
    // Object Tracking
    // ========================================================================

    /// Add a new object to the session.
    ///
    /// The object will be INSERTed on the next `flush()` call.
    pub fn add<M: Model + Clone + Send + Sync + Serialize + 'static>(&mut self, obj: &M) {
        let key = ObjectKey::from_model(obj);

        // If already tracked, update the object and its values
        if let Some(tracked) = self.identity_map.get_mut(&key) {
            tracked.object = Box::new(obj.clone());

            // Update stored values to match the new object state
            let row_data = obj.to_row();
            tracked.column_names = row_data.iter().map(|(name, _)| *name).collect();
            tracked.values = row_data.into_iter().map(|(_, v)| v).collect();
            tracked.pk_values = obj.primary_key_value();

            if tracked.state == ObjectState::Deleted {
                // Un-delete: remove from pending_delete and restore state
                self.pending_delete.retain(|k| k != &key);

                if tracked.original_state.is_some() {
                    // Was previously persisted - restore to Persistent (will need UPDATE if changed)
                    tracked.state = ObjectState::Persistent;
                } else {
                    // Was never persisted - restore to New and schedule for INSERT
                    tracked.state = ObjectState::New;
                    if !self.pending_new.contains(&key) {
                        self.pending_new.push(key);
                    }
                }
            }
            return;
        }

        // Extract column data from the model while we have the concrete type
        let row_data = obj.to_row();
        let column_names: Vec<&'static str> = row_data.iter().map(|(name, _)| *name).collect();
        let values: Vec<Value> = row_data.into_iter().map(|(_, v)| v).collect();

        // Extract primary key info
        let pk_columns: Vec<&'static str> = M::PRIMARY_KEY.to_vec();
        let pk_values = obj.primary_key_value();

        let tracked = TrackedObject {
            object: Box::new(obj.clone()),
            original_state: None, // New objects have no original state
            state: ObjectState::New,
            table_name: M::TABLE_NAME,
            column_names,
            values,
            pk_columns,
            pk_values,
        };

        self.identity_map.insert(key, tracked);
        self.pending_new.push(key);
    }

    /// Delete an object from the session.
    ///
    /// The object will be DELETEd on the next `flush()` call.
    pub fn delete<M: Model + 'static>(&mut self, obj: &M) {
        let key = ObjectKey::from_model(obj);

        if let Some(tracked) = self.identity_map.get_mut(&key) {
            match tracked.state {
                ObjectState::New => {
                    // If it's new, just remove it entirely
                    self.identity_map.remove(&key);
                    self.pending_new.retain(|k| k != &key);
                }
                ObjectState::Persistent | ObjectState::Expired => {
                    tracked.state = ObjectState::Deleted;
                    self.pending_delete.push(key);
                    self.pending_dirty.retain(|k| k != &key);
                }
                ObjectState::Deleted | ObjectState::Detached => {
                    // Already deleted or detached, nothing to do
                }
            }
        }
    }

    /// Mark an object as dirty (modified) so it will be UPDATEd on flush.
    ///
    /// This updates the stored values from the object and schedules an UPDATE.
    /// Only works for objects that are already tracked as Persistent.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut hero = session.get::<Hero>(1).await?.unwrap();
    /// hero.name = "New Name".to_string();
    /// session.mark_dirty(&hero);  // Schedule for UPDATE
    /// session.flush(cx).await?;   // Execute the UPDATE
    /// ```
    pub fn mark_dirty<M: Model + Clone + Send + Sync + Serialize + 'static>(&mut self, obj: &M) {
        let key = ObjectKey::from_model(obj);

        if let Some(tracked) = self.identity_map.get_mut(&key) {
            // Only mark persistent objects as dirty
            if tracked.state != ObjectState::Persistent {
                return;
            }

            // Update the stored object and values
            tracked.object = Box::new(obj.clone());
            let row_data = obj.to_row();
            tracked.column_names = row_data.iter().map(|(name, _)| *name).collect();
            tracked.values = row_data.into_iter().map(|(_, v)| v).collect();
            tracked.pk_values = obj.primary_key_value();

            // Add to pending dirty if not already there
            if !self.pending_dirty.contains(&key) {
                self.pending_dirty.push(key);
            }
        }
    }

    /// Get an object by primary key.
    ///
    /// First checks the identity map, then queries the database if not found.
    pub async fn get<
        M: Model + Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    >(
        &mut self,
        cx: &Cx,
        pk: impl Into<Value>,
    ) -> Outcome<Option<M>, Error> {
        let pk_value = pk.into();
        let pk_values = vec![pk_value.clone()];
        let key = ObjectKey::from_pk::<M>(&pk_values);

        // Check identity map first
        if let Some(tracked) = self.identity_map.get(&key) {
            if tracked.state != ObjectState::Deleted && tracked.state != ObjectState::Detached {
                if let Some(obj) = tracked.object.downcast_ref::<M>() {
                    return Outcome::Ok(Some(obj.clone()));
                }
            }
        }

        // Query from database
        let pk_col = M::PRIMARY_KEY.first().unwrap_or(&"id");
        let sql = format!(
            "SELECT * FROM \"{}\" WHERE \"{}\" = $1 LIMIT 1",
            M::TABLE_NAME,
            pk_col
        );

        let rows = match self.connection.query(cx, &sql, &[pk_value]).await {
            Outcome::Ok(rows) => rows,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        if rows.is_empty() {
            return Outcome::Ok(None);
        }

        // Convert row to model
        let obj = match M::from_row(&rows[0]) {
            Ok(obj) => obj,
            Err(e) => return Outcome::Err(e),
        };

        // Extract column data from the model while we have the concrete type
        let row_data = obj.to_row();
        let column_names: Vec<&'static str> = row_data.iter().map(|(name, _)| *name).collect();
        let values: Vec<Value> = row_data.into_iter().map(|(_, v)| v).collect();

        // Serialize values for dirty checking (must match format used in flush)
        let serialized = serde_json::to_vec(&values).ok();

        // Extract primary key info
        let pk_columns: Vec<&'static str> = M::PRIMARY_KEY.to_vec();
        let obj_pk_values = obj.primary_key_value();

        let tracked = TrackedObject {
            object: Box::new(obj.clone()),
            original_state: serialized,
            state: ObjectState::Persistent,
            table_name: M::TABLE_NAME,
            column_names,
            values,
            pk_columns,
            pk_values: obj_pk_values,
        };

        self.identity_map.insert(key, tracked);

        Outcome::Ok(Some(obj))
    }

    /// Check if an object is tracked by this session.
    pub fn contains<M: Model + 'static>(&self, obj: &M) -> bool {
        let key = ObjectKey::from_model(obj);
        self.identity_map.contains_key(&key)
    }

    /// Detach an object from the session.
    pub fn expunge<M: Model + 'static>(&mut self, obj: &M) {
        let key = ObjectKey::from_model(obj);
        if let Some(tracked) = self.identity_map.get_mut(&key) {
            tracked.state = ObjectState::Detached;
        }
        self.pending_new.retain(|k| k != &key);
        self.pending_delete.retain(|k| k != &key);
        self.pending_dirty.retain(|k| k != &key);
    }

    /// Detach all objects from the session.
    pub fn expunge_all(&mut self) {
        for tracked in self.identity_map.values_mut() {
            tracked.state = ObjectState::Detached;
        }
        self.pending_new.clear();
        self.pending_delete.clear();
        self.pending_dirty.clear();
    }

    // ========================================================================
    // Transaction Management
    // ========================================================================

    /// Begin a transaction.
    pub async fn begin(&mut self, cx: &Cx) -> Outcome<(), Error> {
        if self.in_transaction {
            return Outcome::Ok(());
        }

        match self.connection.execute(cx, "BEGIN", &[]).await {
            Outcome::Ok(_) => {
                self.in_transaction = true;
                Outcome::Ok(())
            }
            Outcome::Err(e) => Outcome::Err(e),
            Outcome::Cancelled(r) => Outcome::Cancelled(r),
            Outcome::Panicked(p) => Outcome::Panicked(p),
        }
    }

    /// Flush pending changes to the database.
    ///
    /// This executes INSERT, UPDATE, and DELETE statements but does NOT commit.
    pub async fn flush(&mut self, cx: &Cx) -> Outcome<(), Error> {
        // Auto-begin transaction if configured
        if self.config.auto_begin && !self.in_transaction {
            match self.begin(cx).await {
                Outcome::Ok(()) => {}
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            }
        }

        // 1. Execute DELETEs first (to respect FK constraints)
        let deletes: Vec<ObjectKey> = std::mem::take(&mut self.pending_delete);
        let mut actually_deleted: Vec<ObjectKey> = Vec::new();
        for key in &deletes {
            if let Some(tracked) = self.identity_map.get(key) {
                // Skip if object was un-deleted (state changed from Deleted)
                if tracked.state != ObjectState::Deleted {
                    continue;
                }

                // Skip objects without primary keys - cannot safely DELETE without WHERE clause
                if tracked.pk_columns.is_empty() || tracked.pk_values.is_empty() {
                    tracing::warn!(
                        table = tracked.table_name,
                        "Skipping DELETE for object without primary key - cannot identify row"
                    );
                    continue;
                }

                // Build WHERE clause from primary key columns and values
                let where_parts: Vec<String> = tracked
                    .pk_columns
                    .iter()
                    .enumerate()
                    .map(|(i, col)| format!("\"{}\" = ${}", col, i + 1))
                    .collect();

                let sql = format!(
                    "DELETE FROM \"{}\" WHERE {}",
                    tracked.table_name,
                    where_parts.join(" AND ")
                );

                match self.connection.execute(cx, &sql, &tracked.pk_values).await {
                    Outcome::Ok(_) => {
                        actually_deleted.push(*key);
                    }
                    Outcome::Err(e) => {
                        // Only restore deletes that weren't already executed
                        // (exclude actually_deleted items from restoration)
                        self.pending_delete = deletes
                            .into_iter()
                            .filter(|k| !actually_deleted.contains(k))
                            .collect();
                        // Remove successfully deleted objects before returning error
                        for key in &actually_deleted {
                            self.identity_map.remove(key);
                        }
                        return Outcome::Err(e);
                    }
                    Outcome::Cancelled(r) => {
                        // Same handling for cancellation
                        self.pending_delete = deletes
                            .into_iter()
                            .filter(|k| !actually_deleted.contains(k))
                            .collect();
                        for key in &actually_deleted {
                            self.identity_map.remove(key);
                        }
                        return Outcome::Cancelled(r);
                    }
                    Outcome::Panicked(p) => {
                        // Same handling for panic
                        self.pending_delete = deletes
                            .into_iter()
                            .filter(|k| !actually_deleted.contains(k))
                            .collect();
                        for key in &actually_deleted {
                            self.identity_map.remove(key);
                        }
                        return Outcome::Panicked(p);
                    }
                }
            }
        }

        // Remove only actually deleted objects from identity map
        for key in &actually_deleted {
            self.identity_map.remove(key);
        }

        // 2. Execute INSERTs
        let inserts: Vec<ObjectKey> = std::mem::take(&mut self.pending_new);
        for key in &inserts {
            if let Some(tracked) = self.identity_map.get_mut(key) {
                // Skip if already persistent (was inserted in a previous attempt before error)
                if tracked.state == ObjectState::Persistent {
                    continue;
                }

                // Build INSERT statement using stored column names and values
                let columns = &tracked.column_names;
                let placeholders: Vec<String> =
                    (1..=columns.len()).map(|i| format!("${}", i)).collect();

                let sql = format!(
                    "INSERT INTO \"{}\" ({}) VALUES ({})",
                    tracked.table_name,
                    columns
                        .iter()
                        .map(|c| format!("\"{}\"", c))
                        .collect::<Vec<_>>()
                        .join(", "),
                    placeholders.join(", ")
                );

                match self.connection.execute(cx, &sql, &tracked.values).await {
                    Outcome::Ok(_) => {
                        tracked.state = ObjectState::Persistent;
                        // Set original_state for future dirty checking (serialize current values)
                        tracked.original_state =
                            Some(serde_json::to_vec(&tracked.values).unwrap_or_default());
                    }
                    Outcome::Err(e) => {
                        // Restore pending_new for retry
                        self.pending_new = inserts;
                        return Outcome::Err(e);
                    }
                    Outcome::Cancelled(r) => {
                        // Restore pending_new for retry (same as Err handling)
                        self.pending_new = inserts;
                        return Outcome::Cancelled(r);
                    }
                    Outcome::Panicked(p) => {
                        // Restore pending_new for retry (same as Err handling)
                        self.pending_new = inserts;
                        return Outcome::Panicked(p);
                    }
                }
            }
        }

        // 3. Execute UPDATEs for dirty objects
        let dirty: Vec<ObjectKey> = std::mem::take(&mut self.pending_dirty);
        for key in &dirty {
            if let Some(tracked) = self.identity_map.get_mut(key) {
                // Only UPDATE persistent objects
                if tracked.state != ObjectState::Persistent {
                    continue;
                }

                // Skip objects without primary keys - cannot safely UPDATE without WHERE clause
                if tracked.pk_columns.is_empty() || tracked.pk_values.is_empty() {
                    tracing::warn!(
                        table = tracked.table_name,
                        "Skipping UPDATE for object without primary key - cannot identify row"
                    );
                    continue;
                }

                // Check if actually dirty by comparing serialized state
                let current_state = serde_json::to_vec(&tracked.values).unwrap_or_default();
                let is_dirty = tracked.original_state.as_ref() != Some(&current_state);

                if !is_dirty {
                    continue;
                }

                // Build UPDATE statement with all non-PK columns
                let mut set_parts = Vec::new();
                let mut params = Vec::new();
                let mut param_idx = 1;

                for (i, col) in tracked.column_names.iter().enumerate() {
                    // Skip primary key columns in SET clause
                    if !tracked.pk_columns.contains(col) {
                        set_parts.push(format!("\"{}\" = ${}", col, param_idx));
                        params.push(tracked.values[i].clone());
                        param_idx += 1;
                    }
                }

                // Add WHERE clause for primary key
                let where_parts: Vec<String> = tracked
                    .pk_columns
                    .iter()
                    .map(|col| {
                        let clause = format!("\"{}\" = ${}", col, param_idx);
                        param_idx += 1;
                        clause
                    })
                    .collect();

                // Add PK values to params
                params.extend(tracked.pk_values.clone());

                if set_parts.is_empty() {
                    continue; // No non-PK columns to update
                }

                let sql = format!(
                    "UPDATE \"{}\" SET {} WHERE {}",
                    tracked.table_name,
                    set_parts.join(", "),
                    where_parts.join(" AND ")
                );

                match self.connection.execute(cx, &sql, &params).await {
                    Outcome::Ok(_) => {
                        // Update original_state to current state
                        tracked.original_state = Some(current_state);
                    }
                    Outcome::Err(e) => {
                        // Restore pending_dirty for retry
                        self.pending_dirty = dirty;
                        return Outcome::Err(e);
                    }
                    Outcome::Cancelled(r) => {
                        // Restore pending_dirty for retry (same as Err handling)
                        self.pending_dirty = dirty;
                        return Outcome::Cancelled(r);
                    }
                    Outcome::Panicked(p) => {
                        // Restore pending_dirty for retry (same as Err handling)
                        self.pending_dirty = dirty;
                        return Outcome::Panicked(p);
                    }
                }
            }
        }

        Outcome::Ok(())
    }

    /// Commit the current transaction.
    pub async fn commit(&mut self, cx: &Cx) -> Outcome<(), Error> {
        // Flush any pending changes first
        match self.flush(cx).await {
            Outcome::Ok(()) => {}
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        }

        if self.in_transaction {
            match self.connection.execute(cx, "COMMIT", &[]).await {
                Outcome::Ok(_) => {
                    self.in_transaction = false;
                }
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            }
        }

        // Expire objects if configured
        if self.config.expire_on_commit {
            for tracked in self.identity_map.values_mut() {
                if tracked.state == ObjectState::Persistent {
                    tracked.state = ObjectState::Expired;
                }
            }
        }

        Outcome::Ok(())
    }

    /// Rollback the current transaction.
    pub async fn rollback(&mut self, cx: &Cx) -> Outcome<(), Error> {
        if self.in_transaction {
            match self.connection.execute(cx, "ROLLBACK", &[]).await {
                Outcome::Ok(_) => {
                    self.in_transaction = false;
                }
                Outcome::Err(e) => return Outcome::Err(e),
                Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                Outcome::Panicked(p) => return Outcome::Panicked(p),
            }
        }

        // Clear pending operations
        self.pending_new.clear();
        self.pending_delete.clear();
        self.pending_dirty.clear();

        // Revert objects to original state or remove new ones
        let mut to_remove = Vec::new();
        for (key, tracked) in &mut self.identity_map {
            match tracked.state {
                ObjectState::New => {
                    to_remove.push(*key);
                }
                ObjectState::Deleted => {
                    tracked.state = ObjectState::Persistent;
                }
                _ => {}
            }
        }

        for key in to_remove {
            self.identity_map.remove(&key);
        }

        Outcome::Ok(())
    }

    // ========================================================================
    // Lazy Loading
    // ========================================================================

    /// Load a single lazy relationship.
    ///
    /// Fetches the related object from the database and caches it in the Lazy wrapper.
    /// If the relationship has already been loaded, returns the cached value.
    ///
    /// # Example
    ///
    /// ```ignore
    /// session.load_lazy(&hero.team, &cx).await?;
    /// let team = hero.team.get(); // Now available
    /// ```
    #[tracing::instrument(level = "debug", skip(self, lazy, cx))]
    pub async fn load_lazy<
        T: Model + Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    >(
        &mut self,
        lazy: &Lazy<T>,
        cx: &Cx,
    ) -> Outcome<bool, Error> {
        tracing::debug!(
            model = std::any::type_name::<T>(),
            fk = ?lazy.fk(),
            already_loaded = lazy.is_loaded(),
            "Loading lazy relationship"
        );

        // If already loaded, return success
        if lazy.is_loaded() {
            tracing::trace!("Already loaded");
            return Outcome::Ok(lazy.get().is_some());
        }

        // If no FK, set as empty and return
        let Some(fk) = lazy.fk() else {
            let _ = lazy.set_loaded(None);
            return Outcome::Ok(false);
        };

        // Fetch from database using get()
        let obj = match self.get::<T>(cx, fk.clone()).await {
            Outcome::Ok(obj) => obj,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        let found = obj.is_some();

        // Cache the result
        let _ = lazy.set_loaded(obj);

        tracing::debug!(found = found, "Lazy load complete");

        Outcome::Ok(found)
    }

    /// Batch load lazy relationships for multiple objects.
    ///
    /// This method collects all FK values, executes a single query, and populates
    /// each Lazy field. This prevents the N+1 query problem when iterating over
    /// a collection and accessing lazy relationships.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Load 100 heroes
    /// let mut heroes = session.query::<Hero>().all().await?;
    ///
    /// // Without batch loading: 100 queries (N+1 problem)
    /// // With batch loading: 1 query
    /// session.load_many(&cx, &mut heroes, |h| &h.team).await?;
    ///
    /// // All teams now loaded
    /// for hero in &heroes {
    ///     if let Some(team) = hero.team.get() {
    ///         println!("{} is on {}", hero.name, team.name);
    ///     }
    /// }
    /// ```
    #[tracing::instrument(level = "debug", skip(self, cx, objects, accessor))]
    pub async fn load_many<P, T, F>(
        &mut self,
        cx: &Cx,
        objects: &[P],
        accessor: F,
    ) -> Outcome<usize, Error>
    where
        P: Model + 'static,
        T: Model + Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
        F: Fn(&P) -> &Lazy<T>,
    {
        // Collect all FK values that need loading
        let mut fk_values: Vec<Value> = Vec::new();
        let mut fk_indices: Vec<usize> = Vec::new();

        for (idx, obj) in objects.iter().enumerate() {
            let lazy = accessor(obj);
            if !lazy.is_loaded() && !lazy.is_empty() {
                if let Some(fk) = lazy.fk() {
                    fk_values.push(fk.clone());
                    fk_indices.push(idx);
                }
            }
        }

        let fk_count = fk_values.len();
        tracing::info!(
            parent_model = std::any::type_name::<P>(),
            related_model = std::any::type_name::<T>(),
            parent_count = objects.len(),
            fk_count = fk_count,
            "Batch loading lazy relationships"
        );

        if fk_values.is_empty() {
            // Nothing to load - mark all empty/loaded Lazy fields
            for obj in objects {
                let lazy = accessor(obj);
                if !lazy.is_loaded() && lazy.is_empty() {
                    let _ = lazy.set_loaded(None);
                }
            }
            return Outcome::Ok(0);
        }

        // Build query with IN clause
        let pk_col = T::PRIMARY_KEY.first().unwrap_or(&"id");
        let placeholders: Vec<String> = (1..=fk_values.len()).map(|i| format!("${}", i)).collect();
        let sql = format!(
            "SELECT * FROM \"{}\" WHERE \"{}\" IN ({})",
            T::TABLE_NAME,
            pk_col,
            placeholders.join(", ")
        );

        let rows = match self.connection.query(cx, &sql, &fk_values).await {
            Outcome::Ok(rows) => rows,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        // Convert rows to objects and build PK hash -> object lookup
        let mut lookup: HashMap<u64, T> = HashMap::new();
        for row in &rows {
            match T::from_row(row) {
                Ok(obj) => {
                    let pk_values = obj.primary_key_value();
                    let pk_hash = hash_values(&pk_values);

                    // Add to session identity map
                    let key = ObjectKey::from_pk::<T>(&pk_values);

                    // Extract column data from the model while we have the concrete type
                    let row_data = obj.to_row();
                    let column_names: Vec<&'static str> =
                        row_data.iter().map(|(name, _)| *name).collect();
                    let values: Vec<Value> = row_data.into_iter().map(|(_, v)| v).collect();

                    // Serialize values for dirty checking (must match format used in flush)
                    let serialized = serde_json::to_vec(&values).ok();

                    let tracked = TrackedObject {
                        object: Box::new(obj.clone()),
                        original_state: serialized,
                        state: ObjectState::Persistent,
                        table_name: T::TABLE_NAME,
                        column_names,
                        values,
                        pk_columns: T::PRIMARY_KEY.to_vec(),
                        pk_values: pk_values.clone(),
                    };
                    self.identity_map.insert(key, tracked);

                    // Add to lookup
                    lookup.insert(pk_hash, obj);
                }
                Err(_) => continue,
            }
        }

        // Populate each Lazy field
        let mut loaded_count = 0;
        for obj in objects {
            let lazy = accessor(obj);
            if !lazy.is_loaded() {
                if let Some(fk) = lazy.fk() {
                    let fk_hash = hash_values(std::slice::from_ref(fk));
                    let related = lookup.get(&fk_hash).cloned();
                    let found = related.is_some();
                    let _ = lazy.set_loaded(related);
                    if found {
                        loaded_count += 1;
                    }
                } else {
                    let _ = lazy.set_loaded(None);
                }
            }
        }

        tracing::debug!(
            query_count = 1,
            loaded_count = loaded_count,
            "Batch load complete"
        );

        Outcome::Ok(loaded_count)
    }

    /// Batch load many-to-many relationships for multiple parent objects.
    ///
    /// This method loads related objects via a link table in a single query,
    /// avoiding the N+1 problem for many-to-many relationships.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Load 100 heroes
    /// let mut heroes = session.query::<Hero>().all().await?;
    ///
    /// // Without batch loading: 100 queries (N+1 problem)
    /// // With batch loading: 1 query via JOIN
    /// let link_info = LinkTableInfo::new("hero_powers", "hero_id", "power_id");
    /// session.load_many_to_many(&cx, &mut heroes, |h| &mut h.powers, |h| h.id.unwrap(), &link_info).await?;
    ///
    /// // All powers now loaded
    /// for hero in &heroes {
    ///     if let Some(powers) = hero.powers.get() {
    ///         println!("{} has {} powers", hero.name, powers.len());
    ///     }
    /// }
    /// ```
    #[tracing::instrument(level = "debug", skip(self, cx, objects, accessor, parent_pk))]
    pub async fn load_many_to_many<P, Child, FA, FP>(
        &mut self,
        cx: &Cx,
        objects: &mut [P],
        accessor: FA,
        parent_pk: FP,
        link_table: &sqlmodel_core::LinkTableInfo,
    ) -> Outcome<usize, Error>
    where
        P: Model + 'static,
        Child: Model + Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
        FA: Fn(&mut P) -> &mut sqlmodel_core::RelatedMany<Child>,
        FP: Fn(&P) -> Value,
    {
        // Collect all parent PK values
        let pks: Vec<Value> = objects.iter().map(&parent_pk).collect();

        tracing::info!(
            parent_model = std::any::type_name::<P>(),
            related_model = std::any::type_name::<Child>(),
            parent_count = pks.len(),
            link_table = link_table.table_name,
            "Batch loading many-to-many relationships"
        );

        if pks.is_empty() {
            return Outcome::Ok(0);
        }

        // Build query with JOIN through link table:
        // SELECT child.*, link.local_column as __parent_pk
        // FROM child
        // JOIN link ON child.pk = link.remote_column
        // WHERE link.local_column IN (...)
        let child_pk_col = Child::PRIMARY_KEY.first().unwrap_or(&"id");
        let placeholders: Vec<String> = (1..=pks.len()).map(|i| format!("${}", i)).collect();
        let sql = format!(
            "SELECT \"{}\".*, \"{}\".\"{}\" AS __parent_pk FROM \"{}\" \
             JOIN \"{}\" ON \"{}\".\"{}\" = \"{}\".\"{}\" \
             WHERE \"{}\".\"{}\" IN ({})",
            Child::TABLE_NAME,
            link_table.table_name,
            link_table.local_column,
            Child::TABLE_NAME,
            link_table.table_name,
            Child::TABLE_NAME,
            child_pk_col,
            link_table.table_name,
            link_table.remote_column,
            link_table.table_name,
            link_table.local_column,
            placeholders.join(", ")
        );

        tracing::trace!(sql = %sql, "Many-to-many batch SQL");

        let rows = match self.connection.query(cx, &sql, &pks).await {
            Outcome::Ok(rows) => rows,
            Outcome::Err(e) => return Outcome::Err(e),
            Outcome::Cancelled(r) => return Outcome::Cancelled(r),
            Outcome::Panicked(p) => return Outcome::Panicked(p),
        };

        // Group children by parent PK
        let mut by_parent: HashMap<u64, Vec<Child>> = HashMap::new();
        for row in &rows {
            // Extract the parent PK from the __parent_pk alias
            let parent_pk_value: Value = match row.get_by_name("__parent_pk") {
                Some(v) => v.clone(),
                None => continue,
            };
            let parent_pk_hash = hash_values(std::slice::from_ref(&parent_pk_value));

            // Parse the child model
            match Child::from_row(row) {
                Ok(child) => {
                    by_parent.entry(parent_pk_hash).or_default().push(child);
                }
                Err(_) => continue,
            }
        }

        // Populate each RelatedMany field
        let mut loaded_count = 0;
        for obj in objects {
            let pk = parent_pk(obj);
            let pk_hash = hash_values(std::slice::from_ref(&pk));
            let children = by_parent.remove(&pk_hash).unwrap_or_default();
            let child_count = children.len();

            let related = accessor(obj);
            related.set_parent_pk(pk);
            let _ = related.set_loaded(children);
            loaded_count += child_count;
        }

        tracing::debug!(
            query_count = 1,
            total_children = loaded_count,
            "Many-to-many batch load complete"
        );

        Outcome::Ok(loaded_count)
    }

    /// Flush pending link/unlink operations for many-to-many relationships.
    ///
    /// This method persists pending link and unlink operations that were tracked
    /// via `RelatedMany::link()` and `RelatedMany::unlink()` calls.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Add a power to a hero
    /// hero.powers.link(&fly_power);
    ///
    /// // Remove a power from a hero
    /// hero.powers.unlink(&x_ray_vision);
    ///
    /// // Flush the link table operations
    /// let link_info = LinkTableInfo::new("hero_powers", "hero_id", "power_id");
    /// session.flush_related_many(&cx, &mut [hero], |h| &mut h.powers, |h| h.id.unwrap(), &link_info).await?;
    /// ```
    #[tracing::instrument(level = "debug", skip(self, cx, objects, accessor, parent_pk))]
    pub async fn flush_related_many<P, Child, FA, FP>(
        &mut self,
        cx: &Cx,
        objects: &mut [P],
        accessor: FA,
        parent_pk: FP,
        link_table: &sqlmodel_core::LinkTableInfo,
    ) -> Outcome<usize, Error>
    where
        P: Model + 'static,
        Child: Model + 'static,
        FA: Fn(&mut P) -> &mut sqlmodel_core::RelatedMany<Child>,
        FP: Fn(&P) -> Value,
    {
        let mut ops = Vec::new();

        // Collect pending operations from all objects
        for obj in objects.iter_mut() {
            let parent_pk_value = parent_pk(obj);
            let related = accessor(obj);

            // Collect pending links
            for child_pk_values in related.take_pending_links() {
                if let Some(child_pk) = child_pk_values.first() {
                    ops.push(LinkTableOp::link(
                        link_table.table_name.to_string(),
                        link_table.local_column.to_string(),
                        parent_pk_value.clone(),
                        link_table.remote_column.to_string(),
                        child_pk.clone(),
                    ));
                }
            }

            // Collect pending unlinks
            for child_pk_values in related.take_pending_unlinks() {
                if let Some(child_pk) = child_pk_values.first() {
                    ops.push(LinkTableOp::unlink(
                        link_table.table_name.to_string(),
                        link_table.local_column.to_string(),
                        parent_pk_value.clone(),
                        link_table.remote_column.to_string(),
                        child_pk.clone(),
                    ));
                }
            }
        }

        if ops.is_empty() {
            return Outcome::Ok(0);
        }

        tracing::info!(
            parent_model = std::any::type_name::<P>(),
            related_model = std::any::type_name::<Child>(),
            link_count = ops
                .iter()
                .filter(|o| matches!(o, LinkTableOp::Link { .. }))
                .count(),
            unlink_count = ops
                .iter()
                .filter(|o| matches!(o, LinkTableOp::Unlink { .. }))
                .count(),
            link_table = link_table.table_name,
            "Flushing many-to-many relationship changes"
        );

        flush::execute_link_table_ops(cx, &self.connection, &ops).await
    }

    // ========================================================================
    // Bidirectional Relationship Sync (back_populates)
    // ========================================================================

    /// Relate a child to a parent with bidirectional sync.
    ///
    /// Sets the parent on the child (ManyToOne side) and adds the child to the
    /// parent's collection (OneToMany side) if `back_populates` is defined.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Hero has a ManyToOne relationship to Team (hero.team)
    /// // Team has a OneToMany relationship to Hero (team.heroes) with back_populates
    ///
    /// session.relate_to_one(
    ///     &mut hero,
    ///     |h| &mut h.team,
    ///     |h| h.team_id = team.id,  // Set FK
    ///     &mut team,
    ///     |t| &mut t.heroes,
    /// );
    /// // Now hero.team is set AND team.heroes includes hero
    /// ```
    pub fn relate_to_one<Child, Parent, FC, FP, FK>(
        &self,
        child: &mut Child,
        child_accessor: FC,
        set_fk: FK,
        parent: &mut Parent,
        parent_accessor: FP,
    ) where
        Child: Model + Clone + 'static,
        Parent: Model + Clone + 'static,
        FC: FnOnce(&mut Child) -> &mut sqlmodel_core::Related<Parent>,
        FP: FnOnce(&mut Parent) -> &mut sqlmodel_core::RelatedMany<Child>,
        FK: FnOnce(&mut Child),
    {
        // Set the forward direction: child.parent = Related::loaded(parent)
        let related = child_accessor(child);
        let _ = related.set_loaded(Some(parent.clone()));

        // Set the FK value
        set_fk(child);

        // Set the reverse direction: parent.children.link(child)
        let related_many = parent_accessor(parent);
        related_many.link(child);

        tracing::debug!(
            child_model = std::any::type_name::<Child>(),
            parent_model = std::any::type_name::<Parent>(),
            "Established bidirectional ManyToOne <-> OneToMany relationship"
        );
    }

    /// Unrelate a child from a parent with bidirectional sync.
    ///
    /// Clears the parent on the child and removes the child from the parent's collection.
    ///
    /// # Example
    ///
    /// ```ignore
    /// session.unrelate_from_one(
    ///     &mut hero,
    ///     |h| &mut h.team,
    ///     |h| h.team_id = None,  // Clear FK
    ///     &mut team,
    ///     |t| &mut t.heroes,
    /// );
    /// ```
    pub fn unrelate_from_one<Child, Parent, FC, FP, FK>(
        &self,
        child: &mut Child,
        child_accessor: FC,
        clear_fk: FK,
        parent: &mut Parent,
        parent_accessor: FP,
    ) where
        Child: Model + Clone + 'static,
        Parent: Model + Clone + 'static,
        FC: FnOnce(&mut Child) -> &mut sqlmodel_core::Related<Parent>,
        FP: FnOnce(&mut Parent) -> &mut sqlmodel_core::RelatedMany<Child>,
        FK: FnOnce(&mut Child),
    {
        // Clear the forward direction by assigning an empty Related
        let related = child_accessor(child);
        *related = sqlmodel_core::Related::empty();

        // Clear the FK value
        clear_fk(child);

        // Remove from the reverse direction
        let related_many = parent_accessor(parent);
        related_many.unlink(child);

        tracing::debug!(
            child_model = std::any::type_name::<Child>(),
            parent_model = std::any::type_name::<Parent>(),
            "Removed bidirectional ManyToOne <-> OneToMany relationship"
        );
    }

    /// Relate two objects in a many-to-many relationship with bidirectional sync.
    ///
    /// Adds each object to the other's collection.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Hero has ManyToMany to Power via hero_powers link table
    /// // Power has ManyToMany to Hero via hero_powers link table (back_populates)
    ///
    /// session.relate_many_to_many(
    ///     &mut hero,
    ///     |h| &mut h.powers,
    ///     &mut power,
    ///     |p| &mut p.heroes,
    /// );
    /// // Now hero.powers includes power AND power.heroes includes hero
    /// ```
    pub fn relate_many_to_many<Left, Right, FL, FR>(
        &self,
        left: &mut Left,
        left_accessor: FL,
        right: &mut Right,
        right_accessor: FR,
    ) where
        Left: Model + Clone + 'static,
        Right: Model + Clone + 'static,
        FL: FnOnce(&mut Left) -> &mut sqlmodel_core::RelatedMany<Right>,
        FR: FnOnce(&mut Right) -> &mut sqlmodel_core::RelatedMany<Left>,
    {
        // Add right to left's collection
        let left_coll = left_accessor(left);
        left_coll.link(right);

        // Add left to right's collection (back_populates)
        let right_coll = right_accessor(right);
        right_coll.link(left);

        tracing::debug!(
            left_model = std::any::type_name::<Left>(),
            right_model = std::any::type_name::<Right>(),
            "Established bidirectional ManyToMany relationship"
        );
    }

    /// Unrelate two objects in a many-to-many relationship with bidirectional sync.
    ///
    /// Removes each object from the other's collection.
    pub fn unrelate_many_to_many<Left, Right, FL, FR>(
        &self,
        left: &mut Left,
        left_accessor: FL,
        right: &mut Right,
        right_accessor: FR,
    ) where
        Left: Model + Clone + 'static,
        Right: Model + Clone + 'static,
        FL: FnOnce(&mut Left) -> &mut sqlmodel_core::RelatedMany<Right>,
        FR: FnOnce(&mut Right) -> &mut sqlmodel_core::RelatedMany<Left>,
    {
        // Remove right from left's collection
        let left_coll = left_accessor(left);
        left_coll.unlink(right);

        // Remove left from right's collection (back_populates)
        let right_coll = right_accessor(right);
        right_coll.unlink(left);

        tracing::debug!(
            left_model = std::any::type_name::<Left>(),
            right_model = std::any::type_name::<Right>(),
            "Removed bidirectional ManyToMany relationship"
        );
    }

    // ========================================================================
    // N+1 Query Detection
    // ========================================================================

    /// Enable N+1 query detection with the specified threshold.
    ///
    /// When the number of lazy loads for a single relationship reaches the
    /// threshold, a warning is emitted suggesting batch loading.
    ///
    /// # Example
    ///
    /// ```ignore
    /// session.enable_n1_detection(3);  // Warn after 3 lazy loads
    ///
    /// // This will trigger a warning:
    /// for hero in &mut heroes {
    ///     hero.team.load(&mut session).await?;
    /// }
    ///
    /// // Check stats
    /// if let Some(stats) = session.n1_stats() {
    ///     println!("Potential N+1 issues: {}", stats.potential_n1);
    /// }
    /// ```
    pub fn enable_n1_detection(&mut self, threshold: usize) {
        self.n1_tracker = Some(N1QueryTracker::new().with_threshold(threshold));
    }

    /// Disable N+1 query detection and clear the tracker.
    pub fn disable_n1_detection(&mut self) {
        self.n1_tracker = None;
    }

    /// Check if N+1 detection is enabled.
    #[must_use]
    pub fn n1_detection_enabled(&self) -> bool {
        self.n1_tracker.is_some()
    }

    /// Get mutable access to the N+1 tracker (for recording loads).
    pub fn n1_tracker_mut(&mut self) -> Option<&mut N1QueryTracker> {
        self.n1_tracker.as_mut()
    }

    /// Get N+1 detection statistics.
    #[must_use]
    pub fn n1_stats(&self) -> Option<N1Stats> {
        self.n1_tracker.as_ref().map(|t| t.stats())
    }

    /// Reset N+1 detection counts (call at start of new request/transaction).
    pub fn reset_n1_tracking(&mut self) {
        if let Some(tracker) = &mut self.n1_tracker {
            tracker.reset();
        }
    }

    /// Record a lazy load for N+1 detection.
    ///
    /// This is called automatically by lazy loading methods.
    #[track_caller]
    pub fn record_lazy_load(&mut self, parent_type: &'static str, relationship: &'static str) {
        if let Some(tracker) = &mut self.n1_tracker {
            tracker.record_load(parent_type, relationship);
        }
    }

    // ========================================================================
    // Debug Diagnostics
    // ========================================================================

    /// Get count of objects pending INSERT.
    pub fn pending_new_count(&self) -> usize {
        self.pending_new.len()
    }

    /// Get count of objects pending DELETE.
    pub fn pending_delete_count(&self) -> usize {
        self.pending_delete.len()
    }

    /// Get count of dirty objects pending UPDATE.
    pub fn pending_dirty_count(&self) -> usize {
        self.pending_dirty.len()
    }

    /// Get total tracked object count.
    pub fn tracked_count(&self) -> usize {
        self.identity_map.len()
    }

    /// Whether we're in a transaction.
    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    /// Dump session state for debugging.
    pub fn debug_state(&self) -> SessionDebugInfo {
        SessionDebugInfo {
            tracked: self.tracked_count(),
            pending_new: self.pending_new_count(),
            pending_delete: self.pending_delete_count(),
            pending_dirty: self.pending_dirty_count(),
            in_transaction: self.in_transaction,
        }
    }
}

impl<C, M> LazyLoader<M> for Session<C>
where
    C: Connection,
    M: Model + Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
{
    fn get(
        &mut self,
        cx: &Cx,
        pk: Value,
    ) -> impl Future<Output = Outcome<Option<M>, Error>> + Send {
        Session::get(self, cx, pk)
    }
}

/// Debug information about session state.
#[derive(Debug, Clone)]
pub struct SessionDebugInfo {
    /// Total tracked objects.
    pub tracked: usize,
    /// Objects pending INSERT.
    pub pending_new: usize,
    /// Objects pending DELETE.
    pub pending_delete: usize,
    /// Objects pending UPDATE.
    pub pending_dirty: usize,
    /// Whether in a transaction.
    pub in_transaction: bool,
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
#[allow(clippy::manual_async_fn)] // Mock trait impls must match trait signatures
mod tests {
    use super::*;
    use asupersync::runtime::RuntimeBuilder;
    use sqlmodel_core::Row;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_session_config_defaults() {
        let config = SessionConfig::default();
        assert!(config.auto_begin);
        assert!(!config.auto_flush);
        assert!(config.expire_on_commit);
    }

    #[test]
    fn test_object_key_hash_consistency() {
        let values1 = vec![Value::BigInt(42)];
        let values2 = vec![Value::BigInt(42)];
        let hash1 = hash_values(&values1);
        let hash2 = hash_values(&values2);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_object_key_hash_different_values() {
        let values1 = vec![Value::BigInt(42)];
        let values2 = vec![Value::BigInt(43)];
        let hash1 = hash_values(&values1);
        let hash2 = hash_values(&values2);
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_object_key_hash_different_types() {
        let values1 = vec![Value::BigInt(42)];
        let values2 = vec![Value::Text("42".to_string())];
        let hash1 = hash_values(&values1);
        let hash2 = hash_values(&values2);
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_session_debug_info() {
        let info = SessionDebugInfo {
            tracked: 5,
            pending_new: 2,
            pending_delete: 1,
            pending_dirty: 0,
            in_transaction: true,
        };
        assert_eq!(info.tracked, 5);
        assert_eq!(info.pending_new, 2);
        assert!(info.in_transaction);
    }

    fn unwrap_outcome<T: std::fmt::Debug>(outcome: Outcome<T, Error>) -> T {
        match outcome {
            Outcome::Ok(v) => v,
            other => std::panic::panic_any(format!("unexpected outcome: {other:?}")),
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Team {
        id: Option<i64>,
        name: String,
    }

    impl Model for Team {
        const TABLE_NAME: &'static str = "teams";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [sqlmodel_core::FieldInfo] {
            &[]
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(row: &Row) -> sqlmodel_core::Result<Self> {
            let id: i64 = row.get_named("id")?;
            let name: String = row.get_named("name")?;
            Ok(Self { id: Some(id), name })
        }

        fn primary_key_value(&self) -> Vec<Value> {
            self.id
                .map_or_else(|| vec![Value::Null], |id| vec![Value::BigInt(id)])
        }

        fn is_new(&self) -> bool {
            self.id.is_none()
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Hero {
        id: Option<i64>,
        team: Lazy<Team>,
    }

    impl Model for Hero {
        const TABLE_NAME: &'static str = "heroes";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [sqlmodel_core::FieldInfo] {
            &[]
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(_row: &Row) -> sqlmodel_core::Result<Self> {
            Ok(Self {
                id: None,
                team: Lazy::empty(),
            })
        }

        fn primary_key_value(&self) -> Vec<Value> {
            self.id
                .map_or_else(|| vec![Value::Null], |id| vec![Value::BigInt(id)])
        }

        fn is_new(&self) -> bool {
            self.id.is_none()
        }
    }

    #[derive(Debug, Default)]
    struct MockState {
        query_calls: usize,
    }

    #[derive(Debug, Clone)]
    struct MockConnection {
        state: Arc<Mutex<MockState>>,
    }

    impl sqlmodel_core::Connection for MockConnection {
        type Tx<'conn>
            = MockTransaction
        where
            Self: 'conn;

        fn query(
            &self,
            _cx: &Cx,
            _sql: &str,
            params: &[Value],
        ) -> impl Future<Output = Outcome<Vec<Row>, Error>> + Send {
            let params = params.to_vec();
            let state = Arc::clone(&self.state);
            async move {
                state.lock().expect("lock poisoned").query_calls += 1;

                let mut rows = Vec::new();
                for v in params {
                    match v {
                        Value::BigInt(1) => rows.push(Row::new(
                            vec!["id".into(), "name".into()],
                            vec![Value::BigInt(1), Value::Text("Avengers".into())],
                        )),
                        Value::BigInt(2) => rows.push(Row::new(
                            vec!["id".into(), "name".into()],
                            vec![Value::BigInt(2), Value::Text("X-Men".into())],
                        )),
                        _ => {}
                    }
                }

                Outcome::Ok(rows)
            }
        }

        fn query_one(
            &self,
            _cx: &Cx,
            _sql: &str,
            _params: &[Value],
        ) -> impl Future<Output = Outcome<Option<Row>, Error>> + Send {
            async { Outcome::Ok(None) }
        }

        fn execute(
            &self,
            _cx: &Cx,
            _sql: &str,
            _params: &[Value],
        ) -> impl Future<Output = Outcome<u64, Error>> + Send {
            async { Outcome::Ok(0) }
        }

        fn insert(
            &self,
            _cx: &Cx,
            _sql: &str,
            _params: &[Value],
        ) -> impl Future<Output = Outcome<i64, Error>> + Send {
            async { Outcome::Ok(0) }
        }

        fn batch(
            &self,
            _cx: &Cx,
            _statements: &[(String, Vec<Value>)],
        ) -> impl Future<Output = Outcome<Vec<u64>, Error>> + Send {
            async { Outcome::Ok(vec![]) }
        }

        fn begin(&self, _cx: &Cx) -> impl Future<Output = Outcome<Self::Tx<'_>, Error>> + Send {
            async { Outcome::Ok(MockTransaction) }
        }

        fn begin_with(
            &self,
            _cx: &Cx,
            _isolation: sqlmodel_core::connection::IsolationLevel,
        ) -> impl Future<Output = Outcome<Self::Tx<'_>, Error>> + Send {
            async { Outcome::Ok(MockTransaction) }
        }

        fn prepare(
            &self,
            _cx: &Cx,
            _sql: &str,
        ) -> impl Future<Output = Outcome<sqlmodel_core::connection::PreparedStatement, Error>> + Send
        {
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
        ) -> impl Future<Output = Outcome<Vec<Row>, Error>> + Send {
            async { Outcome::Ok(vec![]) }
        }

        fn execute_prepared(
            &self,
            _cx: &Cx,
            _stmt: &sqlmodel_core::connection::PreparedStatement,
            _params: &[Value],
        ) -> impl Future<Output = Outcome<u64, Error>> + Send {
            async { Outcome::Ok(0) }
        }

        fn ping(&self, _cx: &Cx) -> impl Future<Output = Outcome<(), Error>> + Send {
            async { Outcome::Ok(()) }
        }

        fn close(self, _cx: &Cx) -> impl Future<Output = sqlmodel_core::Result<()>> + Send {
            async { Ok(()) }
        }
    }

    struct MockTransaction;

    impl sqlmodel_core::connection::TransactionOps for MockTransaction {
        fn query(
            &self,
            _cx: &Cx,
            _sql: &str,
            _params: &[Value],
        ) -> impl Future<Output = Outcome<Vec<Row>, Error>> + Send {
            async { Outcome::Ok(vec![]) }
        }

        fn query_one(
            &self,
            _cx: &Cx,
            _sql: &str,
            _params: &[Value],
        ) -> impl Future<Output = Outcome<Option<Row>, Error>> + Send {
            async { Outcome::Ok(None) }
        }

        fn execute(
            &self,
            _cx: &Cx,
            _sql: &str,
            _params: &[Value],
        ) -> impl Future<Output = Outcome<u64, Error>> + Send {
            async { Outcome::Ok(0) }
        }

        fn savepoint(
            &self,
            _cx: &Cx,
            _name: &str,
        ) -> impl Future<Output = Outcome<(), Error>> + Send {
            async { Outcome::Ok(()) }
        }

        fn rollback_to(
            &self,
            _cx: &Cx,
            _name: &str,
        ) -> impl Future<Output = Outcome<(), Error>> + Send {
            async { Outcome::Ok(()) }
        }

        fn release(
            &self,
            _cx: &Cx,
            _name: &str,
        ) -> impl Future<Output = Outcome<(), Error>> + Send {
            async { Outcome::Ok(()) }
        }

        fn commit(self, _cx: &Cx) -> impl Future<Output = Outcome<(), Error>> + Send {
            async { Outcome::Ok(()) }
        }

        fn rollback(self, _cx: &Cx) -> impl Future<Output = Outcome<(), Error>> + Send {
            async { Outcome::Ok(()) }
        }
    }

    #[test]
    fn test_load_many_single_query_and_populates_lazy() {
        let rt = RuntimeBuilder::current_thread()
            .build()
            .expect("create asupersync runtime");
        let cx = Cx::for_testing();

        let state = Arc::new(Mutex::new(MockState::default()));
        let conn = MockConnection {
            state: Arc::clone(&state),
        };
        let mut session = Session::new(conn);

        let heroes = vec![
            Hero {
                id: Some(1),
                team: Lazy::from_fk(1_i64),
            },
            Hero {
                id: Some(2),
                team: Lazy::from_fk(2_i64),
            },
            Hero {
                id: Some(3),
                team: Lazy::from_fk(1_i64),
            },
            Hero {
                id: Some(4),
                team: Lazy::empty(),
            },
            Hero {
                id: Some(5),
                team: Lazy::from_fk(999_i64),
            },
        ];

        rt.block_on(async {
            let loaded = unwrap_outcome(
                session
                    .load_many::<Hero, Team, _>(&cx, &heroes, |h| &h.team)
                    .await,
            );
            assert_eq!(loaded, 3);

            // Populated / cached
            assert!(heroes[0].team.is_loaded());
            assert_eq!(heroes[0].team.get().unwrap().name, "Avengers");
            assert_eq!(heroes[1].team.get().unwrap().name, "X-Men");
            assert_eq!(heroes[2].team.get().unwrap().name, "Avengers");

            // Empty FK gets cached as loaded-none
            assert!(heroes[3].team.is_loaded());
            assert!(heroes[3].team.get().is_none());

            // Missing object gets cached as loaded-none
            assert!(heroes[4].team.is_loaded());
            assert!(heroes[4].team.get().is_none());

            // Identity map populated: get() should not hit the connection again
            let team1 = unwrap_outcome(session.get::<Team>(&cx, 1_i64).await);
            assert_eq!(
                team1,
                Some(Team {
                    id: Some(1),
                    name: "Avengers".to_string()
                })
            );
        });

        assert_eq!(state.lock().expect("lock poisoned").query_calls, 1);
    }
}
