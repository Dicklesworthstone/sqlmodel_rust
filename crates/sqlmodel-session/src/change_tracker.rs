//! Change tracking and dirty detection for SQLModel Session.
//!
//! This module provides snapshot-based change tracking to detect when objects
//! have been modified since they were loaded from the database.

use crate::ObjectKey;
use serde::Serialize;
use sqlmodel_core::Model;
use std::collections::HashMap;
use std::time::Instant;

/// Snapshot of an object's state at a point in time.
#[derive(Debug)]
pub struct ObjectSnapshot {
    /// Serialized original state (JSON bytes).
    data: Vec<u8>,
    /// Timestamp when snapshot was taken.
    taken_at: Instant,
}

impl ObjectSnapshot {
    /// Create a new snapshot from serialized data.
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            taken_at: Instant::now(),
        }
    }

    /// Get the snapshot data.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Get the timestamp when the snapshot was taken.
    pub fn taken_at(&self) -> Instant {
        self.taken_at
    }
}

/// Tracks changes to objects in the session.
///
/// Uses snapshot comparison to detect when objects have been modified.
pub struct ChangeTracker {
    /// Original snapshots by object key.
    snapshots: HashMap<ObjectKey, ObjectSnapshot>,
}

impl ChangeTracker {
    /// Create a new empty change tracker.
    pub fn new() -> Self {
        Self {
            snapshots: HashMap::new(),
        }
    }

    /// Take a snapshot of an object.
    ///
    /// This stores the serialized state of the object for later comparison.
    #[tracing::instrument(level = "trace", skip(self, obj))]
    pub fn snapshot<T: Model + Serialize>(&mut self, key: ObjectKey, obj: &T) {
        let data = match serde_json::to_vec(obj) {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(
                    model = std::any::type_name::<T>(),
                    error = %e,
                    "Snapshot serialization failed, storing empty snapshot"
                );
                Vec::new()
            }
        };
        tracing::trace!(
            model = std::any::type_name::<T>(),
            pk_hash = key.pk_hash(),
            snapshot_bytes = data.len(),
            "Taking object snapshot"
        );
        self.snapshots.insert(key, ObjectSnapshot::new(data));
    }

    /// Take a snapshot from raw bytes.
    pub fn snapshot_raw(&mut self, key: ObjectKey, data: Vec<u8>) {
        self.snapshots.insert(key, ObjectSnapshot::new(data));
    }

    /// Check if an object has changed since its snapshot.
    ///
    /// Returns `true` if:
    /// - The object has no snapshot (treated as dirty)
    /// - The current state differs from the snapshot
    #[tracing::instrument(level = "trace", skip(self, obj))]
    pub fn is_dirty<T: Model + Serialize>(&self, key: &ObjectKey, obj: &T) -> bool {
        let Some(snapshot) = self.snapshots.get(key) else {
            tracing::trace!(
                pk_hash = key.pk_hash(),
                dirty = true,
                "No snapshot - treating as dirty"
            );
            return true;
        };

        let current = match serde_json::to_vec(obj) {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(
                    model = std::any::type_name::<T>(),
                    error = %e,
                    "Dirty check serialization failed, treating as dirty"
                );
                return true;
            }
        };
        let dirty = current != snapshot.data;
        tracing::trace!(pk_hash = key.pk_hash(), dirty = dirty, "Dirty check result");
        dirty
    }

    /// Check if raw bytes match the snapshot.
    pub fn is_dirty_raw(&self, key: &ObjectKey, current: &[u8]) -> bool {
        let Some(snapshot) = self.snapshots.get(key) else {
            return true;
        };
        current != snapshot.data
    }

    /// Get changed fields between snapshot and current state.
    ///
    /// Returns a list of field names that have different values.
    #[tracing::instrument(level = "debug", skip(self, obj))]
    pub fn changed_fields<T: Model + Serialize>(
        &self,
        key: &ObjectKey,
        obj: &T,
    ) -> Vec<&'static str> {
        let Some(snapshot) = self.snapshots.get(key) else {
            // No snapshot = all fields are "changed"
            let fields: Vec<&'static str> = T::fields().iter().map(|f| f.name).collect();
            tracing::debug!(
                model = std::any::type_name::<T>(),
                changed_count = fields.len(),
                "No snapshot - all fields considered changed"
            );
            return fields;
        };

        // Parse both as JSON objects and compare fields
        let original: serde_json::Value = match serde_json::from_slice(&snapshot.data) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    model = std::any::type_name::<T>(),
                    error = %e,
                    "Snapshot deserialization failed in changed_fields, treating all as changed"
                );
                serde_json::Value::Null
            }
        };
        let current: serde_json::Value = match serde_json::to_value(obj) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    model = std::any::type_name::<T>(),
                    error = %e,
                    "Current serialization failed in changed_fields, treating all as changed"
                );
                serde_json::Value::Null
            }
        };

        let mut changed = Vec::new();
        for field in T::fields() {
            let orig_val = original.get(field.name);
            let curr_val = current.get(field.name);
            if orig_val != curr_val {
                changed.push(field.name);
            }
        }

        tracing::debug!(
            model = std::any::type_name::<T>(),
            changed_count = changed.len(),
            fields = ?changed,
            "Detected changed fields"
        );
        changed
    }

    /// Get changed fields from raw JSON bytes.
    pub fn changed_fields_raw(
        &self,
        key: &ObjectKey,
        current_bytes: &[u8],
        field_names: &[&'static str],
    ) -> Vec<&'static str> {
        let Some(snapshot) = self.snapshots.get(key) else {
            return field_names.to_vec();
        };

        let original: serde_json::Value = match serde_json::from_slice(&snapshot.data) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Snapshot deserialization failed in changed_fields_raw, treating all as changed"
                );
                serde_json::Value::Null
            }
        };
        let current: serde_json::Value = match serde_json::from_slice(current_bytes) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Current deserialization failed in changed_fields_raw, treating all as changed"
                );
                serde_json::Value::Null
            }
        };

        let mut changed = Vec::new();
        for name in field_names {
            let orig_val = original.get(*name);
            let curr_val = current.get(*name);
            if orig_val != curr_val {
                changed.push(*name);
            }
        }
        changed
    }

    /// Get detailed attribute changes between snapshot and current state.
    ///
    /// Returns `AttributeChange` structs with field name, old value, and new value.
    pub fn attribute_changes<T: Model + Serialize>(
        &self,
        key: &ObjectKey,
        obj: &T,
    ) -> Vec<sqlmodel_core::AttributeChange> {
        let Some(snapshot) = self.snapshots.get(key) else {
            return Vec::new();
        };

        let original: serde_json::Value = match serde_json::from_slice(&snapshot.data) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    model = std::any::type_name::<T>(),
                    error = %e,
                    "Snapshot deserialization failed in attribute_changes, treating as empty"
                );
                serde_json::Value::Null
            }
        };
        let current: serde_json::Value = match serde_json::to_value(obj) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    model = std::any::type_name::<T>(),
                    error = %e,
                    "Current serialization failed in attribute_changes, treating as empty"
                );
                serde_json::Value::Null
            }
        };

        let mut changes = Vec::new();
        for field in T::fields() {
            let orig_val = original
                .get(field.name)
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let curr_val = current
                .get(field.name)
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            if orig_val != curr_val {
                changes.push(sqlmodel_core::AttributeChange {
                    field_name: field.name,
                    old_value: orig_val,
                    new_value: curr_val,
                });
            }
        }
        changes
    }

    /// Check if a snapshot exists for the given key.
    pub fn has_snapshot(&self, key: &ObjectKey) -> bool {
        self.snapshots.contains_key(key)
    }

    /// Get the snapshot for a key.
    pub fn get_snapshot(&self, key: &ObjectKey) -> Option<&ObjectSnapshot> {
        self.snapshots.get(key)
    }

    /// Clear snapshot for a specific object.
    ///
    /// Call this after commit or when discarding changes.
    pub fn clear(&mut self, key: &ObjectKey) {
        self.snapshots.remove(key);
    }

    /// Clear all snapshots.
    ///
    /// Call this after commit or rollback to reset tracking state.
    pub fn clear_all(&mut self) {
        self.snapshots.clear();
    }

    /// Update snapshot after flush (new baseline).
    ///
    /// Call this after a successful flush to set the current state as the new baseline.
    #[tracing::instrument(level = "trace", skip(self, obj))]
    pub fn refresh<T: Model + Serialize>(&mut self, key: ObjectKey, obj: &T) {
        tracing::trace!(pk_hash = key.pk_hash(), "Refreshing snapshot");
        self.snapshot(key, obj);
    }

    /// Number of tracked snapshots.
    pub fn len(&self) -> usize {
        self.snapshots.len()
    }

    /// Check if there are no snapshots.
    pub fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }
}

impl Default for ChangeTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use sqlmodel_core::{FieldInfo, Row, Value};

    // Mock model for testing
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestHero {
        id: i64,
        name: String,
        age: Option<i32>,
    }

    impl Model for TestHero {
        const TABLE_NAME: &'static str = "hero";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            static FIELDS: [FieldInfo; 3] = [
                FieldInfo::new("id", "id", sqlmodel_core::SqlType::BigInt)
                    .primary_key(true)
                    .auto_increment(true),
                FieldInfo::new("name", "name", sqlmodel_core::SqlType::Text),
                FieldInfo::new("age", "age", sqlmodel_core::SqlType::Integer).nullable(true),
            ];
            &FIELDS
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![Value::BigInt(self.id)]
        }

        fn from_row(_row: &Row) -> Result<Self, sqlmodel_core::Error> {
            unimplemented!("Not needed for these tests")
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![
                ("id", Value::BigInt(self.id)),
                ("name", Value::Text(self.name.clone())),
                ("age", self.age.map_or(Value::Null, Value::Int)),
            ]
        }

        fn is_new(&self) -> bool {
            false
        }
    }

    fn make_key(id: i64) -> ObjectKey {
        ObjectKey::from_pk::<TestHero>(&[Value::BigInt(id)])
    }

    #[test]
    fn test_snapshot_captures_current_state() {
        let mut tracker = ChangeTracker::new();
        let hero = TestHero {
            id: 1,
            name: "Spider-Man".to_string(),
            age: Some(25),
        };
        let key = make_key(1);

        tracker.snapshot(key, &hero);

        assert!(tracker.has_snapshot(&key));
        let snapshot = tracker.get_snapshot(&key).unwrap();
        assert!(!snapshot.data().is_empty());
    }

    #[test]
    fn test_snapshot_overwrites_previous() {
        let mut tracker = ChangeTracker::new();
        let key = make_key(1);

        let hero1 = TestHero {
            id: 1,
            name: "Spider-Man".to_string(),
            age: Some(25),
        };
        tracker.snapshot(key, &hero1);
        let first_data = tracker.get_snapshot(&key).unwrap().data().to_vec();

        let hero2 = TestHero {
            id: 1,
            name: "Peter Parker".to_string(),
            age: Some(26),
        };
        tracker.snapshot(key, &hero2);
        let second_data = tracker.get_snapshot(&key).unwrap().data().to_vec();

        assert_ne!(first_data, second_data);
    }

    #[test]
    fn test_is_dirty_false_if_unchanged() {
        let mut tracker = ChangeTracker::new();
        let hero = TestHero {
            id: 1,
            name: "Spider-Man".to_string(),
            age: Some(25),
        };
        let key = make_key(1);

        tracker.snapshot(key, &hero);

        // Same object = not dirty
        assert!(!tracker.is_dirty(&key, &hero));
    }

    #[test]
    fn test_is_dirty_true_if_field_changed() {
        let mut tracker = ChangeTracker::new();
        let hero = TestHero {
            id: 1,
            name: "Spider-Man".to_string(),
            age: Some(25),
        };
        let key = make_key(1);

        tracker.snapshot(key, &hero);

        // Modify the hero
        let modified_hero = TestHero {
            id: 1,
            name: "Peter Parker".to_string(),
            age: Some(25),
        };

        assert!(tracker.is_dirty(&key, &modified_hero));
    }

    #[test]
    fn test_is_dirty_true_if_no_snapshot() {
        let tracker = ChangeTracker::new();
        let hero = TestHero {
            id: 1,
            name: "Spider-Man".to_string(),
            age: Some(25),
        };
        let key = make_key(1);

        // No snapshot = dirty
        assert!(tracker.is_dirty(&key, &hero));
    }

    #[test]
    fn test_changed_fields_empty_if_unchanged() {
        let mut tracker = ChangeTracker::new();
        let hero = TestHero {
            id: 1,
            name: "Spider-Man".to_string(),
            age: Some(25),
        };
        let key = make_key(1);

        tracker.snapshot(key, &hero);

        let changed = tracker.changed_fields(&key, &hero);
        assert!(changed.is_empty());
    }

    #[test]
    fn test_changed_fields_lists_modified() {
        let mut tracker = ChangeTracker::new();
        let hero = TestHero {
            id: 1,
            name: "Spider-Man".to_string(),
            age: Some(25),
        };
        let key = make_key(1);

        tracker.snapshot(key, &hero);

        let modified_hero = TestHero {
            id: 1,
            name: "Peter Parker".to_string(),
            age: Some(25),
        };

        let changed = tracker.changed_fields(&key, &modified_hero);
        assert_eq!(changed, vec!["name"]);
    }

    #[test]
    fn test_changed_fields_multiple_changes() {
        let mut tracker = ChangeTracker::new();
        let hero = TestHero {
            id: 1,
            name: "Spider-Man".to_string(),
            age: Some(25),
        };
        let key = make_key(1);

        tracker.snapshot(key, &hero);

        let modified_hero = TestHero {
            id: 1,
            name: "Peter Parker".to_string(),
            age: Some(30),
        };

        let changed = tracker.changed_fields(&key, &modified_hero);
        assert!(changed.contains(&"name"));
        assert!(changed.contains(&"age"));
        assert!(!changed.contains(&"id"));
    }

    #[test]
    fn test_clear_removes_snapshot() {
        let mut tracker = ChangeTracker::new();
        let hero = TestHero {
            id: 1,
            name: "Spider-Man".to_string(),
            age: Some(25),
        };
        let key = make_key(1);

        tracker.snapshot(key, &hero);
        assert!(tracker.has_snapshot(&key));

        tracker.clear(&key);
        assert!(!tracker.has_snapshot(&key));
    }

    #[test]
    fn test_clear_all_removes_all() {
        let mut tracker = ChangeTracker::new();

        let hero1 = TestHero {
            id: 1,
            name: "Spider-Man".to_string(),
            age: Some(25),
        };
        let hero2 = TestHero {
            id: 2,
            name: "Iron Man".to_string(),
            age: Some(40),
        };

        tracker.snapshot(make_key(1), &hero1);
        tracker.snapshot(make_key(2), &hero2);

        assert_eq!(tracker.len(), 2);

        tracker.clear_all();

        assert!(tracker.is_empty());
    }

    #[test]
    fn test_refresh_updates_baseline() {
        let mut tracker = ChangeTracker::new();
        let hero = TestHero {
            id: 1,
            name: "Spider-Man".to_string(),
            age: Some(25),
        };
        let key = make_key(1);

        tracker.snapshot(key, &hero);

        let modified_hero = TestHero {
            id: 1,
            name: "Peter Parker".to_string(),
            age: Some(25),
        };

        // Should be dirty before refresh
        assert!(tracker.is_dirty(&key, &modified_hero));

        // Refresh the baseline
        tracker.refresh(key, &modified_hero);

        // No longer dirty
        assert!(!tracker.is_dirty(&key, &modified_hero));
    }

    #[test]
    fn test_attribute_changes_empty_when_unchanged() {
        let mut tracker = ChangeTracker::new();
        let hero = TestHero {
            id: 1,
            name: "Spider-Man".to_string(),
            age: Some(25),
        };
        let key = ObjectKey::from_model(&hero);
        tracker.snapshot(key, &hero);

        let changes = tracker.attribute_changes(&key, &hero);
        assert!(changes.is_empty());
    }

    #[test]
    fn test_attribute_changes_detects_field_change() {
        let mut tracker = ChangeTracker::new();
        let hero = TestHero {
            id: 1,
            name: "Spider-Man".to_string(),
            age: Some(25),
        };
        let key = ObjectKey::from_model(&hero);
        tracker.snapshot(key, &hero);

        let modified = TestHero {
            id: 1,
            name: "Peter Parker".to_string(),
            age: Some(26),
        };

        let changes = tracker.attribute_changes(&key, &modified);
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].field_name, "name");
        assert_eq!(changes[0].old_value, serde_json::json!("Spider-Man"));
        assert_eq!(changes[0].new_value, serde_json::json!("Peter Parker"));
        assert_eq!(changes[1].field_name, "age");
        assert_eq!(changes[1].old_value, serde_json::json!(25));
        assert_eq!(changes[1].new_value, serde_json::json!(26));
    }

    #[test]
    fn test_attribute_changes_empty_without_snapshot() {
        let tracker = ChangeTracker::new();
        let hero = TestHero {
            id: 1,
            name: "Spider-Man".to_string(),
            age: Some(25),
        };
        let key = ObjectKey::from_model(&hero);

        // No snapshot → empty changes (not all fields)
        let changes = tracker.attribute_changes(&key, &hero);
        assert!(changes.is_empty());
    }
}
