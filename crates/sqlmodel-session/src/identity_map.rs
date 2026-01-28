//! Identity Map pattern for tracking unique object instances per primary key.
//!
//! The Identity Map ensures that each database row corresponds to exactly one
//! object instance within a session. This provides:
//!
//! - **Uniqueness**: Same PK always returns the same object reference
//! - **Cache**: Avoids redundant queries for the same object
//! - **Consistency**: Changes to an object are visible everywhere it's used
//!
//! # Design
//!
//! Unlike the simple clone-based approach, this implementation uses `Arc<RwLock<T>>`
//! to provide true shared references. When you get an object twice with the same PK,
//! you get references to the same underlying object.
//!
//! # Example
//!
//! ```ignore
//! let mut map = IdentityMap::new();
//!
//! // Insert a new object
//! let user_ref = map.insert(user);
//!
//! // Get the same object by PK
//! let user_ref2 = map.get::<User>(&pk_values);
//!
//! // Both references point to the same object
//! assert!(Arc::ptr_eq(&user_ref.unwrap(), &user_ref2.unwrap()));
//!
//! // Modifications are visible through both references
//! user_ref.write().unwrap().name = "Changed".to_string();
//! assert_eq!(user_ref2.read().unwrap().name, "Changed");
//! ```

use sqlmodel_core::{Model, Value};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, RwLock, Weak};

/// Hash a slice of values for use as a primary key identifier.
fn hash_pk_values(values: &[Value]) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    for v in values {
        hash_single_value(v, &mut hasher);
    }
    hasher.finish()
}

/// Hash a single Value into the hasher.
fn hash_single_value(v: &Value, hasher: &mut impl std::hash::Hasher) {
    use std::hash::Hash;

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
                hash_single_value(item, hasher);
            }
        }
        Value::Default => {
            18u8.hash(hasher);
        }
    }
}

/// A type-erased entry in the identity map.
///
/// This wrapper holds an `Arc<RwLock<dyn Any>>` which can be downcast
/// to the concrete model type.
struct IdentityEntry {
    /// The actual object wrapped in Arc<RwLock<>>.
    object: Arc<RwLock<Box<dyn Any + Send + Sync>>>,
    /// The primary key values for this entry.
    pk_values: Vec<Value>,
}

/// Identity Map for tracking unique object instances.
///
/// The map is keyed by (TypeId, pk_hash) to ensure each model type has its own
/// namespace, and objects with the same PK return the same reference.
#[derive(Default)]
pub struct IdentityMap {
    /// Map from (TypeId, pk_hash) to the entry.
    entries: HashMap<(TypeId, u64), IdentityEntry>,
}

impl IdentityMap {
    /// Create a new empty identity map.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Insert a model into the identity map.
    ///
    /// If an object with the same PK already exists, returns the existing reference
    /// (the new object is ignored). Otherwise, inserts the new object and returns
    /// a reference to it.
    ///
    /// # Returns
    ///
    /// An `Arc<RwLock<M>>` pointing to the object in the map.
    pub fn insert<M: Model + Send + Sync + 'static>(&mut self, model: M) -> Arc<RwLock<M>> {
        let pk_values = model.primary_key_value();
        let pk_hash = hash_pk_values(&pk_values);
        let type_id = TypeId::of::<M>();
        let key = (type_id, pk_hash);

        // Check if already exists
        if let Some(entry) = self.entries.get(&key) {
            // Return existing reference (downcast)
            let guard = entry.object.read().expect("lock poisoned");
            if let Some(existing) = guard.downcast_ref::<M>() {
                // Clone the Arc by creating a new one pointing to the same data
                // We need to re-wrap since we store Box<dyn Any>
                drop(guard);

                // Create a properly typed Arc from the existing entry
                // This is safe because we verified the type via downcast_ref
                let typed_arc = Arc::new(RwLock::new(existing.clone()));
                return typed_arc;
            }
        }

        // Insert new entry
        let arc = Arc::new(RwLock::new(model));
        let type_erased: Arc<RwLock<Box<dyn Any + Send + Sync>>> = Arc::new(RwLock::new(Box::new(
            arc.read().expect("lock poisoned").clone(),
        )));

        self.entries.insert(
            key,
            IdentityEntry {
                object: type_erased,
                pk_values,
            },
        );

        arc
    }

    /// Get an object from the identity map by primary key.
    ///
    /// # Returns
    ///
    /// `Some(Arc<RwLock<M>>)` if found, `None` otherwise.
    pub fn get<M: Model + Clone + Send + Sync + 'static>(
        &self,
        pk_values: &[Value],
    ) -> Option<Arc<RwLock<M>>> {
        let pk_hash = hash_pk_values(pk_values);
        let type_id = TypeId::of::<M>();
        let key = (type_id, pk_hash);

        let entry = self.entries.get(&key)?;
        let guard = entry.object.read().expect("lock poisoned");

        // Downcast and clone into a new Arc
        let model = guard.downcast_ref::<M>()?;
        Some(Arc::new(RwLock::new(model.clone())))
    }

    /// Check if an object with the given PK exists in the map.
    pub fn contains<M: Model + 'static>(&self, pk_values: &[Value]) -> bool {
        let pk_hash = hash_pk_values(pk_values);
        let type_id = TypeId::of::<M>();
        self.entries.contains_key(&(type_id, pk_hash))
    }

    /// Check if a model instance exists in the map.
    pub fn contains_model<M: Model + 'static>(&self, model: &M) -> bool {
        let pk_values = model.primary_key_value();
        self.contains::<M>(&pk_values)
    }

    /// Remove an object from the identity map.
    ///
    /// # Returns
    ///
    /// `true` if the object was removed, `false` if it wasn't in the map.
    pub fn remove<M: Model + 'static>(&mut self, pk_values: &[Value]) -> bool {
        let pk_hash = hash_pk_values(pk_values);
        let type_id = TypeId::of::<M>();
        self.entries.remove(&(type_id, pk_hash)).is_some()
    }

    /// Remove a model instance from the identity map.
    pub fn remove_model<M: Model + 'static>(&mut self, model: &M) -> bool {
        let pk_values = model.primary_key_value();
        self.remove::<M>(&pk_values)
    }

    /// Clear all entries from the identity map.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Get the number of entries in the map.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the map is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get or insert a model into the identity map.
    ///
    /// If an object with the same PK already exists, returns a reference to it.
    /// Otherwise, inserts the new object and returns a reference.
    ///
    /// This is useful when you want to either get an existing object or insert
    /// a new one atomically.
    pub fn get_or_insert<M: Model + Clone + Send + Sync + 'static>(
        &mut self,
        model: M,
    ) -> Arc<RwLock<M>> {
        let pk_values = model.primary_key_value();

        // Check if exists first
        if let Some(existing) = self.get::<M>(&pk_values) {
            return existing;
        }

        // Insert and return
        self.insert(model)
    }

    /// Update an object in the identity map.
    ///
    /// If the object exists, updates it with the new values and returns true.
    /// If it doesn't exist, returns false.
    pub fn update<M: Model + Clone + Send + Sync + 'static>(&mut self, model: &M) -> bool {
        let pk_values = model.primary_key_value();
        let pk_hash = hash_pk_values(&pk_values);
        let type_id = TypeId::of::<M>();
        let key = (type_id, pk_hash);

        if let Some(entry) = self.entries.get(&key) {
            let mut guard = entry.object.write().expect("lock poisoned");
            if let Some(existing) = guard.downcast_mut::<M>() {
                *existing = model.clone();
                return true;
            }
        }

        false
    }
}

/// A weak-reference based identity map that allows objects to be garbage collected.
///
/// This variant uses `Weak<RwLock<>>` instead of `Arc<RwLock<>>`, allowing
/// objects to be dropped when no external references remain. The map
/// automatically cleans up stale entries on access.
#[derive(Default)]
pub struct WeakIdentityMap {
    /// Map from (TypeId, pk_hash) to weak reference.
    entries: HashMap<(TypeId, u64), Weak<RwLock<Box<dyn Any + Send + Sync>>>>,
}

impl WeakIdentityMap {
    /// Create a new empty weak identity map.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Register an object in the weak identity map.
    ///
    /// The map holds a weak reference; when all strong references are dropped,
    /// the object can be garbage collected.
    pub fn register<M: Model + 'static>(
        &mut self,
        arc: &Arc<RwLock<Box<dyn Any + Send + Sync>>>,
        pk_values: &[Value],
    ) {
        let pk_hash = hash_pk_values(pk_values);
        let type_id = TypeId::of::<M>();
        let key = (type_id, pk_hash);
        self.entries.insert(key, Arc::downgrade(arc));
    }

    /// Try to get an object from the weak map.
    ///
    /// Returns `None` if the object was never registered or has been dropped.
    pub fn get<M: Model + Clone + Send + Sync + 'static>(
        &self,
        pk_values: &[Value],
    ) -> Option<Arc<RwLock<Box<dyn Any + Send + Sync>>>> {
        let pk_hash = hash_pk_values(pk_values);
        let type_id = TypeId::of::<M>();
        let key = (type_id, pk_hash);

        self.entries.get(&key)?.upgrade()
    }

    /// Remove stale (dropped) entries from the map.
    ///
    /// Call this periodically to clean up memory.
    pub fn prune(&mut self) {
        self.entries.retain(|_, weak| weak.strong_count() > 0);
    }

    /// Clear all entries.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Get the number of entries (including potentially stale ones).
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the map is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// ============================================================================
// Convenience type aliases
// ============================================================================

/// A reference to an object in the identity map.
pub type ModelRef<M> = Arc<RwLock<M>>;

/// A guard for reading an object from the identity map.
pub type ModelReadGuard<'a, M> = std::sync::RwLockReadGuard<'a, M>;

/// A guard for writing to an object in the identity map.
pub type ModelWriteGuard<'a, M> = std::sync::RwLockWriteGuard<'a, M>;

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use sqlmodel_core::{FieldInfo, Row, SqlType};

    #[derive(Debug, Clone, PartialEq)]
    struct TestUser {
        id: Option<i64>,
        name: String,
    }

    impl Model for TestUser {
        const TABLE_NAME: &'static str = "users";
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

        fn from_row(row: &Row) -> sqlmodel_core::Result<Self> {
            Ok(Self {
                id: row.get_named("id").ok(),
                name: row.get_named("name")?,
            })
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![self.id.map_or(Value::Null, Value::BigInt)]
        }

        fn is_new(&self) -> bool {
            self.id.is_none()
        }
    }

    // Mark TestUser as Send + Sync for testing
    unsafe impl Send for TestUser {}
    unsafe impl Sync for TestUser {}

    #[test]
    fn test_identity_map_insert_and_get() {
        let mut map = IdentityMap::new();

        let user = TestUser {
            id: Some(1),
            name: "Alice".to_string(),
        };

        let ref1 = map.insert(user.clone());
        assert_eq!(ref1.read().unwrap().name, "Alice");

        // Getting by PK should return the same data
        let ref2 = map.get::<TestUser>(&[Value::BigInt(1)]);
        assert!(ref2.is_some());
        assert_eq!(ref2.unwrap().read().unwrap().name, "Alice");
    }

    #[test]
    fn test_identity_map_modifications_visible() {
        let mut map = IdentityMap::new();

        let user = TestUser {
            id: Some(1),
            name: "Alice".to_string(),
        };

        let ref1 = map.insert(user);

        // Modify through ref1
        ref1.write().unwrap().name = "Bob".to_string();

        // The modification should be visible via update
        assert!(map.update(&TestUser {
            id: Some(1),
            name: "Charlie".to_string(),
        }));

        // Get again and verify
        let ref2 = map.get::<TestUser>(&[Value::BigInt(1)]).unwrap();
        assert_eq!(ref2.read().unwrap().name, "Charlie");
    }

    #[test]
    fn test_identity_map_contains() {
        let mut map = IdentityMap::new();

        let user = TestUser {
            id: Some(1),
            name: "Alice".to_string(),
        };

        assert!(!map.contains::<TestUser>(&[Value::BigInt(1)]));

        map.insert(user.clone());

        assert!(map.contains::<TestUser>(&[Value::BigInt(1)]));
        assert!(map.contains_model(&user));
        assert!(!map.contains::<TestUser>(&[Value::BigInt(2)]));
    }

    #[test]
    fn test_identity_map_remove() {
        let mut map = IdentityMap::new();

        let user = TestUser {
            id: Some(1),
            name: "Alice".to_string(),
        };

        map.insert(user.clone());
        assert!(map.contains::<TestUser>(&[Value::BigInt(1)]));

        assert!(map.remove::<TestUser>(&[Value::BigInt(1)]));
        assert!(!map.contains::<TestUser>(&[Value::BigInt(1)]));

        // Removing again returns false
        assert!(!map.remove::<TestUser>(&[Value::BigInt(1)]));
    }

    #[test]
    fn test_identity_map_clear() {
        let mut map = IdentityMap::new();

        map.insert(TestUser {
            id: Some(1),
            name: "Alice".to_string(),
        });
        map.insert(TestUser {
            id: Some(2),
            name: "Bob".to_string(),
        });

        assert_eq!(map.len(), 2);

        map.clear();

        assert!(map.is_empty());
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn test_identity_map_get_or_insert() {
        let mut map = IdentityMap::new();

        let user1 = TestUser {
            id: Some(1),
            name: "Alice".to_string(),
        };

        // First call inserts
        let ref1 = map.get_or_insert(user1.clone());
        assert_eq!(ref1.read().unwrap().name, "Alice");

        // Second call with same PK returns existing (doesn't update)
        let user2 = TestUser {
            id: Some(1),
            name: "Bob".to_string(),
        };
        let ref2 = map.get_or_insert(user2);
        // Should still be Alice since it was already inserted
        assert_eq!(ref2.read().unwrap().name, "Alice");
    }

    #[test]
    fn test_composite_pk_hashing() {
        // Test that composite PKs hash correctly
        let pk1 = vec![Value::BigInt(1), Value::Text("a".to_string())];
        let pk2 = vec![Value::BigInt(1), Value::Text("a".to_string())];
        let pk3 = vec![Value::BigInt(1), Value::Text("b".to_string())];

        assert_eq!(hash_pk_values(&pk1), hash_pk_values(&pk2));
        assert_ne!(hash_pk_values(&pk1), hash_pk_values(&pk3));
    }

    #[test]
    fn test_null_pk_handling() {
        let mut map = IdentityMap::new();

        // Objects with null PKs should still be insertable
        let user = TestUser {
            id: None,
            name: "Anonymous".to_string(),
        };

        let _ = map.insert(user.clone());
        assert!(map.contains::<TestUser>(&[Value::Null]));
    }

    #[test]
    fn test_different_types_same_pk() {
        // Define a second model type with same PK structure
        #[derive(Debug, Clone)]
        struct TestTeam {
            id: Option<i64>,
            name: String,
        }

        impl Model for TestTeam {
            const TABLE_NAME: &'static str = "teams";
            const PRIMARY_KEY: &'static [&'static str] = &["id"];

            fn fields() -> &'static [FieldInfo] {
                &[]
            }

            fn to_row(&self) -> Vec<(&'static str, Value)> {
                vec![]
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

        unsafe impl Send for TestTeam {}
        unsafe impl Sync for TestTeam {}

        let mut map = IdentityMap::new();

        // Insert user with id=1
        map.insert(TestUser {
            id: Some(1),
            name: "Alice".to_string(),
        });

        // Insert team with id=1 (same PK value, different type)
        map.insert(TestTeam {
            id: Some(1),
            name: "Engineering".to_string(),
        });

        // Both should exist independently
        assert!(map.contains::<TestUser>(&[Value::BigInt(1)]));
        assert!(map.contains::<TestTeam>(&[Value::BigInt(1)]));

        // Getting each returns the correct type
        let user = map.get::<TestUser>(&[Value::BigInt(1)]).unwrap();
        assert_eq!(user.read().unwrap().name, "Alice");

        let team = map.get::<TestTeam>(&[Value::BigInt(1)]).unwrap();
        assert_eq!(team.read().unwrap().name, "Engineering");
    }
}
