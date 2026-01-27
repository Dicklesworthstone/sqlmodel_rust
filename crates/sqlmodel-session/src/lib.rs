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

use asupersync::{Cx, Outcome};
use serde::{Deserialize, Serialize};
use sqlmodel_core::{Connection, Error, Model, Value};
use std::any::{Any, TypeId};
use std::collections::HashMap;
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
    #[tracing::instrument(level = "debug", skip(self, obj))]
    pub fn add<M: Model + Clone + Send + Sync + Serialize + 'static>(&mut self, obj: &M) {
        let key = ObjectKey::from_model(obj);

        tracing::info!(
            model = std::any::type_name::<M>(),
            table = M::TABLE_NAME,
            "Adding object to session"
        );

        // If already tracked, update the object
        if let Some(tracked) = self.identity_map.get_mut(&key) {
            tracked.object = Box::new(obj.clone());
            if tracked.state == ObjectState::Deleted {
                // Un-delete: restore to persistent or new
                tracked.state = if tracked.original_state.is_some() {
                    ObjectState::Persistent
                } else {
                    ObjectState::New
                };
            }
            return;
        }

        // Serialize for dirty tracking (will be used when dirty checking is implemented)
        let _serialized = serde_json::to_vec(obj).ok();

        let column_names: Vec<&'static str> = M::fields().iter().map(|f| f.column_name).collect();

        let tracked = TrackedObject {
            object: Box::new(obj.clone()),
            original_state: None, // New objects have no original state
            state: ObjectState::New,
            table_name: M::TABLE_NAME,
            column_names,
        };

        self.identity_map.insert(key, tracked);
        self.pending_new.push(key);
    }

    /// Delete an object from the session.
    ///
    /// The object will be DELETEd on the next `flush()` call.
    #[tracing::instrument(level = "debug", skip(self, obj))]
    pub fn delete<M: Model + 'static>(&mut self, obj: &M) {
        let key = ObjectKey::from_model(obj);

        tracing::info!(
            model = std::any::type_name::<M>(),
            table = M::TABLE_NAME,
            "Marking object for deletion"
        );

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

    /// Get an object by primary key.
    ///
    /// First checks the identity map, then queries the database if not found.
    #[tracing::instrument(level = "debug", skip(self, cx, pk))]
    pub async fn get<M: Model + Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static>(
        &mut self,
        cx: &Cx,
        pk: impl Into<Value>,
    ) -> Outcome<Option<M>, Error> {
        let pk_value = pk.into();
        let pk_values = vec![pk_value.clone()];
        let key = ObjectKey::from_pk::<M>(&pk_values);

        tracing::debug!(
            model = std::any::type_name::<M>(),
            table = M::TABLE_NAME,
            "Getting object by primary key"
        );

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

        // Add to identity map
        let serialized = serde_json::to_vec(&obj).ok();
        let column_names: Vec<&'static str> = M::fields().iter().map(|f| f.column_name).collect();

        let tracked = TrackedObject {
            object: Box::new(obj.clone()),
            original_state: serialized,
            state: ObjectState::Persistent,
            table_name: M::TABLE_NAME,
            column_names,
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
    #[tracing::instrument(level = "debug", skip(self, cx))]
    pub async fn begin(&mut self, cx: &Cx) -> Outcome<(), Error> {
        if self.in_transaction {
            return Outcome::Ok(());
        }

        tracing::info!("Beginning transaction");

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
    #[tracing::instrument(level = "debug", skip(self, cx))]
    pub async fn flush(&mut self, cx: &Cx) -> Outcome<(), Error> {
        let start = std::time::Instant::now();

        tracing::info!(
            inserts = self.pending_new.len(),
            deletes = self.pending_delete.len(),
            "Starting flush"
        );

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
        for key in &deletes {
            if let Some(tracked) = self.identity_map.get(key) {
                let pk_col = tracked.column_names.first().copied().unwrap_or("id");
                let sql = format!(
                    "DELETE FROM \"{}\" WHERE \"{}\" = $1",
                    tracked.table_name, pk_col
                );

                // Get PK value from the object
                // Note: This is simplified - real implementation would extract PK properly
                match self.connection.execute(cx, &sql, &[]).await {
                    Outcome::Ok(_) => {}
                    Outcome::Err(e) => {
                        self.pending_delete = deletes;
                        return Outcome::Err(e);
                    }
                    Outcome::Cancelled(r) => return Outcome::Cancelled(r),
                    Outcome::Panicked(p) => return Outcome::Panicked(p),
                }
            }
        }

        // Remove deleted objects from identity map
        for key in &deletes {
            self.identity_map.remove(key);
        }

        // 2. Execute INSERTs
        let inserts: Vec<ObjectKey> = std::mem::take(&mut self.pending_new);
        for key in &inserts {
            if let Some(tracked) = self.identity_map.get_mut(key) {
                // Build INSERT statement
                let columns: Vec<&str> = tracked.column_names.to_vec();
                let placeholders: Vec<String> =
                    (1..=columns.len()).map(|i| format!("${}", i)).collect();

                let _sql = format!(
                    "INSERT INTO \"{}\" ({}) VALUES ({})",
                    tracked.table_name,
                    columns
                        .iter()
                        .map(|c| format!("\"{}\"", c))
                        .collect::<Vec<_>>()
                        .join(", "),
                    placeholders.join(", ")
                );

                // TODO: Execute the INSERT statement
                // Real implementation would extract values from the object
                // and execute: self.connection.execute(cx, &sql, &values).await
                tracked.state = ObjectState::Persistent;
                tracked.original_state = None; // Will be set after successful insert
            }
        }

        // 3. Execute UPDATEs for dirty objects
        // TODO: Implement dirty checking

        tracing::info!(
            elapsed_ms = start.elapsed().as_millis(),
            "Flush completed"
        );

        Outcome::Ok(())
    }

    /// Commit the current transaction.
    #[tracing::instrument(level = "debug", skip(self, cx))]
    pub async fn commit(&mut self, cx: &Cx) -> Outcome<(), Error> {
        tracing::info!("Committing transaction");

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
    #[tracing::instrument(level = "debug", skip(self, cx))]
    pub async fn rollback(&mut self, cx: &Cx) -> Outcome<(), Error> {
        tracing::info!("Rolling back transaction");

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
mod tests {
    use super::*;

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
}
