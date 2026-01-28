//! Relationship metadata for SQLModel Rust.
//!
//! Relationships are defined at compile-time (via derive macros) and represented
//! as static metadata on each `Model`. This allows higher-level layers (query
//! builder, session/UoW, eager/lazy loaders) to generate correct SQL and load
//! related objects without runtime reflection.

use crate::{Error, Model, Value};
use asupersync::{Cx, Outcome};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::future::Future;
use std::sync::OnceLock;

/// The type of relationship between two models.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RelationshipKind {
    /// One-to-one: `Hero` has one `Profile`.
    OneToOne,
    /// Many-to-one: many `Hero`s belong to one `Team`.
    #[default]
    ManyToOne,
    /// One-to-many: one `Team` has many `Hero`s.
    OneToMany,
    /// Many-to-many: `Hero`s have many `Power`s via a link table.
    ManyToMany,
}

/// Information about a link/join table for many-to-many relationships.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinkTableInfo {
    /// The link table name (e.g., `"hero_powers"`).
    pub table_name: &'static str,

    /// Column in link table pointing to the local model (e.g., `"hero_id"`).
    pub local_column: &'static str,

    /// Column in link table pointing to the remote model (e.g., `"power_id"`).
    pub remote_column: &'static str,
}

impl LinkTableInfo {
    /// Create a new link-table definition.
    #[must_use]
    pub const fn new(
        table_name: &'static str,
        local_column: &'static str,
        remote_column: &'static str,
    ) -> Self {
        Self {
            table_name,
            local_column,
            remote_column,
        }
    }
}

/// Metadata about a relationship between models.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationshipInfo {
    /// Name of the relationship field.
    pub name: &'static str,

    /// The related model's table name.
    pub related_table: &'static str,

    /// Kind of relationship.
    pub kind: RelationshipKind,

    /// Local foreign key column (for ManyToOne).
    /// e.g., `"team_id"` on `Hero`.
    pub local_key: Option<&'static str>,

    /// Remote foreign key column (for OneToMany).
    /// e.g., `"team_id"` on `Hero` when accessed from `Team`.
    pub remote_key: Option<&'static str>,

    /// Link table for ManyToMany relationships.
    pub link_table: Option<LinkTableInfo>,

    /// The field on the related model that points back.
    pub back_populates: Option<&'static str>,

    /// Whether to use lazy loading.
    pub lazy: bool,

    /// Cascade delete behavior.
    pub cascade_delete: bool,
}

impl RelationshipInfo {
    /// Create a new relationship with required fields.
    #[must_use]
    pub const fn new(
        name: &'static str,
        related_table: &'static str,
        kind: RelationshipKind,
    ) -> Self {
        Self {
            name,
            related_table,
            kind,
            local_key: None,
            remote_key: None,
            link_table: None,
            back_populates: None,
            lazy: false,
            cascade_delete: false,
        }
    }

    /// Set the local foreign key column (ManyToOne).
    #[must_use]
    pub const fn local_key(mut self, key: &'static str) -> Self {
        self.local_key = Some(key);
        self
    }

    /// Set the remote foreign key column (OneToMany).
    #[must_use]
    pub const fn remote_key(mut self, key: &'static str) -> Self {
        self.remote_key = Some(key);
        self
    }

    /// Set the link table metadata (ManyToMany).
    #[must_use]
    pub const fn link_table(mut self, info: LinkTableInfo) -> Self {
        self.link_table = Some(info);
        self
    }

    /// Set the back-populates field name (bidirectional relationships).
    #[must_use]
    pub const fn back_populates(mut self, field: &'static str) -> Self {
        self.back_populates = Some(field);
        self
    }

    /// Enable/disable lazy loading.
    #[must_use]
    pub const fn lazy(mut self, value: bool) -> Self {
        self.lazy = value;
        self
    }

    /// Enable/disable cascade delete behavior.
    #[must_use]
    pub const fn cascade_delete(mut self, value: bool) -> Self {
        self.cascade_delete = value;
        self
    }
}

impl Default for RelationshipInfo {
    fn default() -> Self {
        Self::new("", "", RelationshipKind::default())
    }
}

// ============================================================================
// Relationship Lookup Helpers
// ============================================================================

/// Find a relationship by field name in a model's RELATIONSHIPS.
///
/// # Example
///
/// ```ignore
/// let rel = find_relationship::<Hero>("team");
/// assert_eq!(rel.unwrap().related_table, "teams");
/// ```
pub fn find_relationship<M: crate::Model>(field_name: &str) -> Option<&'static RelationshipInfo> {
    M::RELATIONSHIPS.iter().find(|r| r.name == field_name)
}

/// Find the back-relationship from a target model back to the source.
///
/// Given `Hero::team` with `back_populates = "heroes"`, this finds
/// `Team::heroes` which should have `back_populates = "team"`.
///
/// # Arguments
///
/// * `source_rel` - The relationship on the source model
/// * `target_relationships` - The RELATIONSHIPS slice from the target model
pub fn find_back_relationship(
    source_rel: &RelationshipInfo,
    target_relationships: &'static [RelationshipInfo],
) -> Option<&'static RelationshipInfo> {
    let back_field = source_rel.back_populates?;
    target_relationships.iter().find(|r| r.name == back_field)
}

/// Validate that back_populates is symmetric between two models.
///
/// If `Hero::team` has `back_populates = "heroes"`, then `Team::heroes`
/// must exist and have `back_populates = "team"`.
///
/// Returns Ok(()) if valid, Err with message if invalid.
pub fn validate_back_populates<Source: crate::Model, Target: crate::Model>(
    source_field: &str,
) -> Result<(), String> {
    let source_rel = find_relationship::<Source>(source_field).ok_or_else(|| {
        format!(
            "No relationship '{}' on {}",
            source_field,
            Source::TABLE_NAME
        )
    })?;

    let Some(back_field) = source_rel.back_populates else {
        // No back_populates, nothing to validate
        return Ok(());
    };

    let target_rel = find_relationship::<Target>(back_field).ok_or_else(|| {
        format!(
            "{}.{} has back_populates='{}' but {}.{} does not exist",
            Source::TABLE_NAME,
            source_field,
            back_field,
            Target::TABLE_NAME,
            back_field
        )
    })?;

    // Validate that target points back to source
    if let Some(target_back) = target_rel.back_populates {
        if target_back != source_field {
            return Err(format!(
                "{}.{} has back_populates='{}' but {}.{} has back_populates='{}' (expected '{}')",
                Source::TABLE_NAME,
                source_field,
                back_field,
                Target::TABLE_NAME,
                back_field,
                target_back,
                source_field
            ));
        }
    }

    Ok(())
}

/// Minimal session interface needed to load lazy relationships.
///
/// This trait lives in `sqlmodel-core` to avoid circular dependencies: the
/// concrete `Session` type is defined in `sqlmodel-session` (which depends on
/// `sqlmodel-core`). `sqlmodel-session` provides the blanket impl.
pub trait LazyLoader<M: Model> {
    /// Load an object by primary key.
    fn get(&mut self, cx: &Cx, pk: Value)
    -> impl Future<Output = Outcome<Option<M>, Error>> + Send;
}

/// A related single object (many-to-one or one-to-one).
///
/// This wrapper can be in one of three states:
/// - **Empty**: no relationship (`fk_value` is None)
/// - **Unloaded**: has FK value but not fetched yet (`fk_value` is Some, `loaded` unset)
/// - **Loaded**: the object has been fetched and cached (`loaded` set)
pub struct Related<T: Model> {
    fk_value: Option<Value>,
    loaded: OnceLock<Option<T>>,
}

impl<T: Model> Related<T> {
    /// Create an empty relationship (null FK, not loaded).
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            fk_value: None,
            loaded: OnceLock::new(),
        }
    }

    /// Create from a foreign key value (not yet loaded).
    #[must_use]
    pub fn from_fk(fk: impl Into<Value>) -> Self {
        Self {
            fk_value: Some(fk.into()),
            loaded: OnceLock::new(),
        }
    }

    /// Create with an already-loaded object.
    #[must_use]
    pub fn loaded(obj: T) -> Self {
        let cell = OnceLock::new();
        let _ = cell.set(Some(obj));
        Self {
            fk_value: None,
            loaded: cell,
        }
    }

    /// Get the loaded object (None if not loaded or loaded as null).
    #[must_use]
    pub fn get(&self) -> Option<&T> {
        self.loaded.get().and_then(|o| o.as_ref())
    }

    /// Check if the relationship has been loaded (including loaded-null).
    #[must_use]
    pub fn is_loaded(&self) -> bool {
        self.loaded.get().is_some()
    }

    /// Check if the relationship is empty (null FK).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fk_value.is_none()
    }

    /// Get the foreign key value (if present).
    #[must_use]
    pub fn fk(&self) -> Option<&Value> {
        self.fk_value.as_ref()
    }

    /// Set the loaded object (internal use by query system).
    pub fn set_loaded(&self, obj: Option<T>) -> Result<(), Option<T>> {
        self.loaded.set(obj)
    }
}

impl<T: Model> Default for Related<T> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<T: Model + Clone> Clone for Related<T> {
    fn clone(&self) -> Self {
        let cloned = Self {
            fk_value: self.fk_value.clone(),
            loaded: OnceLock::new(),
        };

        if let Some(value) = self.loaded.get() {
            let _ = cloned.loaded.set(value.clone());
        }

        cloned
    }
}

impl<T: Model + fmt::Debug> fmt::Debug for Related<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = if self.is_loaded() {
            "loaded"
        } else if self.is_empty() {
            "empty"
        } else {
            "unloaded"
        };

        f.debug_struct("Related")
            .field("state", &state)
            .field("fk_value", &self.fk_value)
            .field("loaded", &self.get())
            .finish()
    }
}

impl<T> Serialize for Related<T>
where
    T: Model + Serialize,
{
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.loaded.get() {
            Some(Some(obj)) => obj.serialize(serializer),
            Some(None) | None => serializer.serialize_none(),
        }
    }
}

impl<'de, T> Deserialize<'de> for Related<T>
where
    T: Model + Deserialize<'de>,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let opt = Option::<T>::deserialize(deserializer)?;
        Ok(match opt {
            Some(obj) => Self::loaded(obj),
            None => Self::empty(),
        })
    }
}

/// A collection of related objects (one-to-many or many-to-many).
///
/// This wrapper can be in one of two states:
/// - **Unloaded**: the collection has not been fetched yet
/// - **Loaded**: the objects have been fetched and cached
///
/// For many-to-many relationships, use `link()` and `unlink()` to track
/// changes that will be flushed to the link table.
pub struct RelatedMany<T: Model> {
    /// The loaded objects (if fetched).
    loaded: OnceLock<Vec<T>>,
    /// Foreign key column on the related model (for one-to-many).
    fk_column: &'static str,
    /// Parent's primary key value.
    parent_pk: Option<Value>,
    /// Link table info for many-to-many relationships.
    link_table: Option<LinkTableInfo>,
    /// Pending link operations (PK values to INSERT into link table).
    pending_links: std::sync::Mutex<Vec<Vec<Value>>>,
    /// Pending unlink operations (PK values to DELETE from link table).
    pending_unlinks: std::sync::Mutex<Vec<Vec<Value>>>,
}

impl<T: Model> RelatedMany<T> {
    /// Create a new unloaded RelatedMany with the FK column name.
    ///
    /// Use this for one-to-many relationships where the related model
    /// has a foreign key column pointing back to this model.
    #[must_use]
    pub fn new(fk_column: &'static str) -> Self {
        Self {
            loaded: OnceLock::new(),
            fk_column,
            parent_pk: None,
            link_table: None,
            pending_links: std::sync::Mutex::new(Vec::new()),
            pending_unlinks: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Create for a many-to-many relationship with a link table.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let link = LinkTableInfo::new("hero_powers", "hero_id", "power_id");
    /// let powers: RelatedMany<Power> = RelatedMany::with_link_table(link);
    /// ```
    #[must_use]
    pub fn with_link_table(link_table: LinkTableInfo) -> Self {
        Self {
            loaded: OnceLock::new(),
            fk_column: "",
            parent_pk: None,
            link_table: Some(link_table),
            pending_links: std::sync::Mutex::new(Vec::new()),
            pending_unlinks: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Create with a parent primary key for loading.
    #[must_use]
    pub fn with_parent_pk(fk_column: &'static str, pk: impl Into<Value>) -> Self {
        Self {
            loaded: OnceLock::new(),
            fk_column,
            parent_pk: Some(pk.into()),
            link_table: None,
            pending_links: std::sync::Mutex::new(Vec::new()),
            pending_unlinks: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Check if the collection has been loaded.
    #[must_use]
    pub fn is_loaded(&self) -> bool {
        self.loaded.get().is_some()
    }

    /// Get the loaded objects as a slice (None if not loaded).
    #[must_use]
    pub fn get(&self) -> Option<&[T]> {
        self.loaded.get().map(Vec::as_slice)
    }

    /// Get the number of loaded items (0 if not loaded).
    #[must_use]
    pub fn len(&self) -> usize {
        self.loaded.get().map_or(0, Vec::len)
    }

    /// Check if the collection is empty (true if not loaded or loaded empty).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.loaded.get().is_none_or(Vec::is_empty)
    }

    /// Set the loaded objects (internal use by query system).
    pub fn set_loaded(&self, objects: Vec<T>) -> Result<(), Vec<T>> {
        self.loaded.set(objects)
    }

    /// Iterate over the loaded items.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.loaded.get().map_or([].iter(), |v| v.iter())
    }

    /// Get the FK column name.
    #[must_use]
    pub fn fk_column(&self) -> &'static str {
        self.fk_column
    }

    /// Get the parent PK value (if set).
    #[must_use]
    pub fn parent_pk(&self) -> Option<&Value> {
        self.parent_pk.as_ref()
    }

    /// Set the parent PK value.
    pub fn set_parent_pk(&mut self, pk: impl Into<Value>) {
        self.parent_pk = Some(pk.into());
    }

    /// Get the link table info (if this is a many-to-many relationship).
    #[must_use]
    pub fn link_table(&self) -> Option<&LinkTableInfo> {
        self.link_table.as_ref()
    }

    /// Check if this is a many-to-many relationship (has link table).
    #[must_use]
    pub fn is_many_to_many(&self) -> bool {
        self.link_table.is_some()
    }

    /// Track a link operation (will INSERT into link table on flush).
    ///
    /// The object should already exist in the database. This method
    /// records the relationship to be persisted when flush() is called.
    ///
    /// Duplicate links to the same object are ignored (only one INSERT will occur).
    ///
    /// # Example
    ///
    /// ```ignore
    /// hero.powers.link(&fireball);
    /// session.flush().await?; // Inserts into hero_powers table
    /// ```
    pub fn link(&self, obj: &T) {
        let pk = obj.primary_key_value();
        match self.pending_links.lock() {
            Ok(mut pending) => {
                // Prevent duplicates - only add if not already pending
                if !pending.contains(&pk) {
                    pending.push(pk);
                }
            }
            Err(poisoned) => {
                // Mutex was poisoned - recover by taking the lock anyway
                // This is safe because we're just adding to a Vec
                let mut pending = poisoned.into_inner();
                if !pending.contains(&pk) {
                    pending.push(pk);
                }
            }
        }
    }

    /// Track an unlink operation (will DELETE from link table on flush).
    ///
    /// This method records the relationship removal to be persisted
    /// when flush() is called.
    ///
    /// Duplicate unlinks to the same object are ignored (only one DELETE will occur).
    ///
    /// # Example
    ///
    /// ```ignore
    /// hero.powers.unlink(&fireball);
    /// session.flush().await?; // Deletes from hero_powers table
    /// ```
    pub fn unlink(&self, obj: &T) {
        let pk = obj.primary_key_value();
        match self.pending_unlinks.lock() {
            Ok(mut pending) => {
                // Prevent duplicates - only add if not already pending
                if !pending.contains(&pk) {
                    pending.push(pk);
                }
            }
            Err(poisoned) => {
                // Mutex was poisoned - recover by taking the lock anyway
                // This is safe because we're just adding to a Vec
                let mut pending = poisoned.into_inner();
                if !pending.contains(&pk) {
                    pending.push(pk);
                }
            }
        }
    }

    /// Get and clear pending link operations.
    ///
    /// Returns the PK values that should be INSERTed into the link table.
    /// This is used by the flush system.
    pub fn take_pending_links(&self) -> Vec<Vec<Value>> {
        match self.pending_links.lock() {
            Ok(mut v) => std::mem::take(&mut *v),
            Err(poisoned) => {
                // Recover data from poisoned mutex - consistent with link()/unlink()
                std::mem::take(&mut *poisoned.into_inner())
            }
        }
    }

    /// Get and clear pending unlink operations.
    ///
    /// Returns the PK values that should be DELETEd from the link table.
    /// This is used by the flush system.
    pub fn take_pending_unlinks(&self) -> Vec<Vec<Value>> {
        match self.pending_unlinks.lock() {
            Ok(mut v) => std::mem::take(&mut *v),
            Err(poisoned) => {
                // Recover data from poisoned mutex - consistent with link()/unlink()
                std::mem::take(&mut *poisoned.into_inner())
            }
        }
    }

    /// Check if there are pending link/unlink operations.
    #[must_use]
    pub fn has_pending_ops(&self) -> bool {
        let has_links = match self.pending_links.lock() {
            Ok(v) => !v.is_empty(),
            Err(poisoned) => !poisoned.into_inner().is_empty(),
        };
        let has_unlinks = match self.pending_unlinks.lock() {
            Ok(v) => !v.is_empty(),
            Err(poisoned) => !poisoned.into_inner().is_empty(),
        };
        has_links || has_unlinks
    }
}

impl<T: Model> Default for RelatedMany<T> {
    fn default() -> Self {
        Self::new("")
    }
}

impl<T: Model + Clone> Clone for RelatedMany<T> {
    fn clone(&self) -> Self {
        // Clone pending_links, recovering from poisoned mutex
        let cloned_links = match self.pending_links.lock() {
            Ok(v) => v.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };

        // Clone pending_unlinks, recovering from poisoned mutex
        let cloned_unlinks = match self.pending_unlinks.lock() {
            Ok(v) => v.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };

        let cloned = Self {
            loaded: OnceLock::new(),
            fk_column: self.fk_column,
            parent_pk: self.parent_pk.clone(),
            link_table: self.link_table,
            pending_links: std::sync::Mutex::new(cloned_links),
            pending_unlinks: std::sync::Mutex::new(cloned_unlinks),
        };

        if let Some(vec) = self.loaded.get() {
            let _ = cloned.loaded.set(vec.clone());
        }

        cloned
    }
}

impl<T: Model + fmt::Debug> fmt::Debug for RelatedMany<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pending_links_count = self.pending_links.lock().map_or(0, |v| v.len());
        let pending_unlinks_count = self.pending_unlinks.lock().map_or(0, |v| v.len());

        f.debug_struct("RelatedMany")
            .field("loaded", &self.loaded.get())
            .field("fk_column", &self.fk_column)
            .field("parent_pk", &self.parent_pk)
            .field("link_table", &self.link_table)
            .field("pending_links_count", &pending_links_count)
            .field("pending_unlinks_count", &pending_unlinks_count)
            .finish()
    }
}

impl<T> Serialize for RelatedMany<T>
where
    T: Model + Serialize,
{
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.loaded.get() {
            Some(vec) => vec.serialize(serializer),
            None => Vec::<T>::new().serialize(serializer),
        }
    }
}

impl<'de, T> Deserialize<'de> for RelatedMany<T>
where
    T: Model + Deserialize<'de>,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let vec = Vec::<T>::deserialize(deserializer)?;
        let rel = Self::new("");
        let _ = rel.loaded.set(vec);
        Ok(rel)
    }
}

impl<'a, T: Model> IntoIterator for &'a RelatedMany<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.loaded.get().map_or([].iter(), |v| v.iter())
    }
}

// ============================================================================
// Lazy<T> - Deferred Loading
// ============================================================================

/// A lazily-loaded related object that requires explicit load() call.
///
/// Unlike `Related<T>` which is loaded during the query via JOIN, `Lazy<T>`
/// defers loading until explicitly requested with a Session reference.
///
/// # States
///
/// - **Empty**: No FK value (null relationship)
/// - **Unloaded**: Has FK but not fetched yet
/// - **Loaded**: Object fetched and cached
///
/// # Example
///
/// ```ignore
/// // Field definition
/// struct Hero {
///     team: Lazy<Team>,
/// }
///
/// // Loading (requires Session)
/// let team = hero.team.load(&mut session, &cx).await?;
///
/// // After loading, access is fast
/// if let Some(team) = hero.team.get() {
///     println!("Team: {}", team.name);
/// }
/// ```
///
/// # N+1 Prevention
///
/// Use `Session::load_many()` to batch-load lazy relationships:
///
/// ```ignore
/// // Load all teams in one query
/// session.load_many(&mut heroes, |h| &mut h.team).await?;
/// ```
pub struct Lazy<T: Model> {
    /// Foreign key value (if any).
    fk_value: Option<Value>,
    /// Loaded object (cached after first load).
    loaded: OnceLock<Option<T>>,
    /// Whether load() has been called.
    load_attempted: std::sync::atomic::AtomicBool,
}

impl<T: Model> Lazy<T> {
    /// Create an empty lazy relationship (null FK).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            fk_value: None,
            loaded: OnceLock::new(),
            load_attempted: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Create from a foreign key value (not yet loaded).
    #[must_use]
    pub fn from_fk(fk: impl Into<Value>) -> Self {
        Self {
            fk_value: Some(fk.into()),
            loaded: OnceLock::new(),
            load_attempted: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Create with an already-loaded object.
    #[must_use]
    pub fn loaded(obj: T) -> Self {
        let cell = OnceLock::new();
        let _ = cell.set(Some(obj));
        Self {
            fk_value: None,
            loaded: cell,
            load_attempted: std::sync::atomic::AtomicBool::new(true),
        }
    }

    /// Load the related object via the provided loader (cached after first success).
    ///
    /// - If the FK is NULL, this caches `None` and returns `Ok(None)`.
    /// - If the loader errors/cancels/panics, this does **not** mark the
    ///   relationship as loaded, allowing retries.
    pub async fn load<L>(&mut self, cx: &Cx, loader: &mut L) -> Outcome<Option<&T>, Error>
    where
        L: LazyLoader<T> + ?Sized,
    {
        if self.is_loaded() {
            return Outcome::Ok(self.get());
        }

        let Some(fk) = self.fk_value.clone() else {
            let _ = self.set_loaded(None);
            return Outcome::Ok(None);
        };

        match loader.get(cx, fk).await {
            Outcome::Ok(obj) => {
                let _ = self.set_loaded(obj);
                Outcome::Ok(self.get())
            }
            Outcome::Err(e) => Outcome::Err(e),
            Outcome::Cancelled(r) => Outcome::Cancelled(r),
            Outcome::Panicked(p) => Outcome::Panicked(p),
        }
    }

    /// Get the loaded object (None if not loaded or FK is null).
    #[must_use]
    pub fn get(&self) -> Option<&T> {
        self.loaded.get().and_then(|o| o.as_ref())
    }

    /// Check if load() has been called.
    #[must_use]
    pub fn is_loaded(&self) -> bool {
        self.load_attempted
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Check if the relationship is empty (null FK).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fk_value.is_none()
    }

    /// Get the foreign key value.
    #[must_use]
    pub fn fk(&self) -> Option<&Value> {
        self.fk_value.as_ref()
    }

    /// Set the loaded object (internal use by Session::load_many).
    ///
    /// Returns `Ok(())` if successfully set, `Err` if already loaded.
    pub fn set_loaded(&self, obj: Option<T>) -> Result<(), Option<T>> {
        match self.loaded.set(obj) {
            Ok(()) => {
                self.load_attempted
                    .store(true, std::sync::atomic::Ordering::Release);
                Ok(())
            }
            Err(v) => Err(v),
        }
    }

    /// Reset the lazy relationship to unloaded state.
    ///
    /// This is useful when refreshing an object after commit.
    pub fn reset(&mut self) {
        self.loaded = OnceLock::new();
        self.load_attempted = std::sync::atomic::AtomicBool::new(false);
    }
}

impl<T: Model> Default for Lazy<T> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<T: Model + Clone> Clone for Lazy<T> {
    fn clone(&self) -> Self {
        let cloned = Self {
            fk_value: self.fk_value.clone(),
            loaded: OnceLock::new(),
            load_attempted: std::sync::atomic::AtomicBool::new(
                self.load_attempted
                    .load(std::sync::atomic::Ordering::Acquire),
            ),
        };

        if let Some(value) = self.loaded.get() {
            let _ = cloned.loaded.set(value.clone());
        }

        cloned
    }
}

impl<T: Model + fmt::Debug> fmt::Debug for Lazy<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = if self.is_loaded() {
            "loaded"
        } else if self.is_empty() {
            "empty"
        } else {
            "unloaded"
        };

        f.debug_struct("Lazy")
            .field("state", &state)
            .field("fk_value", &self.fk_value)
            .field("loaded", &self.get())
            .field(
                "load_attempted",
                &self
                    .load_attempted
                    .load(std::sync::atomic::Ordering::Acquire),
            )
            .finish()
    }
}

impl<T> Serialize for Lazy<T>
where
    T: Model + Serialize,
{
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.loaded.get() {
            Some(Some(obj)) => obj.serialize(serializer),
            Some(None) | None => serializer.serialize_none(),
        }
    }
}

impl<'de, T> Deserialize<'de> for Lazy<T>
where
    T: Model + Deserialize<'de>,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let opt = Option::<T>::deserialize(deserializer)?;
        Ok(match opt {
            Some(obj) => Self::loaded(obj),
            None => Self::empty(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FieldInfo, Result, Row};
    use asupersync::runtime::RuntimeBuilder;
    use serde::{Deserialize, Serialize};

    #[test]
    fn test_relationship_kind_default() {
        assert_eq!(RelationshipKind::default(), RelationshipKind::ManyToOne);
    }

    #[test]
    fn test_relationship_info_builder_chain() {
        let info = RelationshipInfo::new("team", "teams", RelationshipKind::ManyToOne)
            .local_key("team_id")
            .back_populates("heroes")
            .lazy(true)
            .cascade_delete(true);

        assert_eq!(info.name, "team");
        assert_eq!(info.related_table, "teams");
        assert_eq!(info.kind, RelationshipKind::ManyToOne);
        assert_eq!(info.local_key, Some("team_id"));
        assert_eq!(info.remote_key, None);
        assert_eq!(info.link_table, None);
        assert_eq!(info.back_populates, Some("heroes"));
        assert!(info.lazy);
        assert!(info.cascade_delete);
    }

    #[test]
    fn test_link_table_info_new() {
        let link = LinkTableInfo::new("hero_powers", "hero_id", "power_id");
        assert_eq!(link.table_name, "hero_powers");
        assert_eq!(link.local_column, "hero_id");
        assert_eq!(link.remote_column, "power_id");
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Team {
        id: Option<i64>,
        name: String,
    }

    impl Model for Team {
        const TABLE_NAME: &'static str = "teams";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];

        fn fields() -> &'static [FieldInfo] {
            &[]
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(_row: &Row) -> Result<Self> {
            Ok(Self {
                id: None,
                name: String::new(),
            })
        }

        fn primary_key_value(&self) -> Vec<Value> {
            match self.id {
                Some(id) => vec![Value::from(id)],
                None => vec![],
            }
        }

        fn is_new(&self) -> bool {
            self.id.is_none()
        }
    }

    #[test]
    fn test_related_empty_creates_unloaded_state() {
        let rel = Related::<Team>::empty();
        assert!(rel.is_empty());
        assert!(!rel.is_loaded());
        assert!(rel.get().is_none());
        assert!(rel.fk().is_none());
    }

    #[test]
    fn test_related_from_fk_stores_value() {
        let rel = Related::<Team>::from_fk(42_i64);
        assert!(!rel.is_empty());
        assert_eq!(rel.fk(), Some(&Value::from(42_i64)));
        assert!(!rel.is_loaded());
        assert!(rel.get().is_none());
    }

    #[test]
    fn test_related_loaded_sets_object() {
        let team = Team {
            id: Some(1),
            name: "Avengers".to_string(),
        };
        let rel = Related::loaded(team.clone());
        assert!(rel.is_loaded());
        assert!(rel.fk().is_none());
        assert_eq!(rel.get(), Some(&team));
    }

    #[test]
    fn test_related_set_loaded_succeeds_first_time() {
        let rel = Related::<Team>::from_fk(1_i64);
        let team = Team {
            id: Some(1),
            name: "Avengers".to_string(),
        };
        assert!(rel.set_loaded(Some(team.clone())).is_ok());
        assert!(rel.is_loaded());
        assert_eq!(rel.get(), Some(&team));
    }

    #[test]
    fn test_related_set_loaded_fails_second_time() {
        let rel = Related::<Team>::empty();
        assert!(rel.set_loaded(None).is_ok());
        assert!(rel.is_loaded());
        assert!(rel.set_loaded(None).is_err());
    }

    #[test]
    fn test_related_default_is_empty() {
        let rel: Related<Team> = Related::default();
        assert!(rel.is_empty());
    }

    #[test]
    fn test_related_clone_unloaded_is_unloaded() {
        let rel = Related::<Team>::from_fk(7_i64);
        let cloned = rel.clone();
        assert!(!cloned.is_loaded());
        assert_eq!(cloned.fk(), rel.fk());
    }

    #[test]
    fn test_related_clone_loaded_preserves_object() {
        let team = Team {
            id: Some(1),
            name: "Avengers".to_string(),
        };
        let rel = Related::loaded(team.clone());
        let cloned = rel.clone();
        assert!(cloned.is_loaded());
        assert_eq!(cloned.get(), Some(&team));
    }

    #[test]
    fn test_related_debug_output_shows_state() {
        let rel = Related::<Team>::from_fk(1_i64);
        let s = format!("{rel:?}");
        assert!(s.contains("state"));
        assert!(s.contains("unloaded"));
    }

    #[test]
    fn test_related_serde_serialize_loaded_outputs_object() {
        let rel = Related::loaded(Team {
            id: Some(1),
            name: "Avengers".to_string(),
        });
        let json = serde_json::to_value(&rel).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "id": 1,
                "name": "Avengers"
            })
        );
    }

    #[test]
    fn test_related_serde_serialize_unloaded_outputs_null() {
        let rel = Related::<Team>::from_fk(1_i64);
        let json = serde_json::to_value(&rel).unwrap();
        assert_eq!(json, serde_json::Value::Null);
    }

    #[test]
    fn test_related_serde_deserialize_object_creates_loaded() {
        let rel: Related<Team> = serde_json::from_value(serde_json::json!({
            "id": 1,
            "name": "Avengers"
        }))
        .unwrap();

        let expected = Team {
            id: Some(1),
            name: "Avengers".to_string(),
        };
        assert!(rel.is_loaded());
        assert_eq!(rel.get(), Some(&expected));
    }

    #[test]
    fn test_related_serde_deserialize_null_creates_empty() {
        let rel: Related<Team> = serde_json::from_value(serde_json::Value::Null).unwrap();
        assert!(rel.is_empty());
        assert!(!rel.is_loaded());
        assert!(rel.get().is_none());
    }

    #[test]
    fn test_related_serde_roundtrip_preserves_data() {
        let rel = Related::loaded(Team {
            id: Some(1),
            name: "Avengers".to_string(),
        });
        let json = serde_json::to_string(&rel).unwrap();
        let decoded: Related<Team> = serde_json::from_str(&json).unwrap();
        assert!(decoded.is_loaded());
        assert_eq!(decoded.get(), rel.get());
    }

    // ========================================================================
    // RelatedMany<T> Tests
    // ========================================================================

    #[test]
    fn test_related_many_new_is_unloaded() {
        let rel: RelatedMany<Team> = RelatedMany::new("team_id");
        assert!(!rel.is_loaded());
        assert!(rel.get().is_none());
        assert_eq!(rel.len(), 0);
        assert!(rel.is_empty());
    }

    #[test]
    fn test_related_many_set_loaded() {
        let rel: RelatedMany<Team> = RelatedMany::new("team_id");
        let teams = vec![
            Team {
                id: Some(1),
                name: "Avengers".to_string(),
            },
            Team {
                id: Some(2),
                name: "X-Men".to_string(),
            },
        ];
        assert!(rel.set_loaded(teams.clone()).is_ok());
        assert!(rel.is_loaded());
        assert_eq!(rel.len(), 2);
        assert!(!rel.is_empty());
    }

    #[test]
    fn test_related_many_get_returns_slice() {
        let rel: RelatedMany<Team> = RelatedMany::new("team_id");
        let teams = vec![Team {
            id: Some(1),
            name: "Avengers".to_string(),
        }];
        rel.set_loaded(teams.clone()).unwrap();
        let slice = rel.get().unwrap();
        assert_eq!(slice.len(), 1);
        assert_eq!(slice[0].name, "Avengers");
    }

    #[test]
    fn test_related_many_iter() {
        let rel: RelatedMany<Team> = RelatedMany::new("team_id");
        let teams = vec![
            Team {
                id: Some(1),
                name: "A".to_string(),
            },
            Team {
                id: Some(2),
                name: "B".to_string(),
            },
        ];
        rel.set_loaded(teams).unwrap();
        let names: Vec<_> = rel.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, vec!["A", "B"]);
    }

    #[test]
    fn test_related_many_default() {
        let rel: RelatedMany<Team> = RelatedMany::default();
        assert!(!rel.is_loaded());
        assert!(rel.is_empty());
    }

    #[test]
    fn test_related_many_clone() {
        let rel: RelatedMany<Team> = RelatedMany::new("team_id");
        rel.set_loaded(vec![Team {
            id: Some(1),
            name: "Test".to_string(),
        }])
        .unwrap();
        let cloned = rel.clone();
        assert!(cloned.is_loaded());
        assert_eq!(cloned.len(), 1);
    }

    #[test]
    fn test_related_many_debug() {
        let rel: RelatedMany<Team> = RelatedMany::new("team_id");
        let debug_str = format!("{:?}", rel);
        assert!(debug_str.contains("RelatedMany"));
        assert!(debug_str.contains("fk_column"));
    }

    #[test]
    fn test_related_many_serde_serialize_loaded() {
        let rel: RelatedMany<Team> = RelatedMany::new("team_id");
        rel.set_loaded(vec![Team {
            id: Some(1),
            name: "A".to_string(),
        }])
        .unwrap();
        let json = serde_json::to_value(&rel).unwrap();
        assert!(json.is_array());
        assert_eq!(json.as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_related_many_serde_serialize_unloaded() {
        let rel: RelatedMany<Team> = RelatedMany::new("team_id");
        let json = serde_json::to_value(&rel).unwrap();
        assert!(json.is_array());
        assert!(json.as_array().unwrap().is_empty());
    }

    #[test]
    fn test_related_many_serde_deserialize() {
        let rel: RelatedMany<Team> = serde_json::from_value(serde_json::json!([
            {"id": 1, "name": "A"},
            {"id": 2, "name": "B"}
        ]))
        .unwrap();
        assert!(rel.is_loaded());
        assert_eq!(rel.len(), 2);
    }

    // ========================================================================
    // RelatedMany Many-to-Many Tests
    // ========================================================================

    #[test]
    fn test_related_many_with_link_table() {
        let link = LinkTableInfo::new("hero_powers", "hero_id", "power_id");
        let rel: RelatedMany<Team> = RelatedMany::with_link_table(link);

        assert!(rel.is_many_to_many());
        assert_eq!(rel.link_table().unwrap().table_name, "hero_powers");
        assert_eq!(rel.link_table().unwrap().local_column, "hero_id");
        assert_eq!(rel.link_table().unwrap().remote_column, "power_id");
    }

    #[test]
    fn test_related_many_link_tracks_pending() {
        let rel: RelatedMany<Team> = RelatedMany::new("");
        let team = Team {
            id: Some(1),
            name: "A".to_string(),
        };

        assert!(!rel.has_pending_ops());
        rel.link(&team);
        assert!(rel.has_pending_ops());

        let pending = rel.take_pending_links();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0], vec![Value::from(1_i64)]);

        // Should be cleared
        assert!(!rel.has_pending_ops());
    }

    #[test]
    fn test_related_many_unlink_tracks_pending() {
        let rel: RelatedMany<Team> = RelatedMany::new("");
        let team = Team {
            id: Some(2),
            name: "B".to_string(),
        };

        rel.unlink(&team);
        assert!(rel.has_pending_ops());

        let pending = rel.take_pending_unlinks();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0], vec![Value::from(2_i64)]);
    }

    #[test]
    fn test_related_many_multiple_links() {
        let rel: RelatedMany<Team> = RelatedMany::new("");
        let team1 = Team {
            id: Some(1),
            name: "A".to_string(),
        };
        let team2 = Team {
            id: Some(2),
            name: "B".to_string(),
        };

        rel.link(&team1);
        rel.link(&team2);

        let pending = rel.take_pending_links();
        assert_eq!(pending.len(), 2);
    }

    #[test]
    fn test_related_many_link_and_unlink_together() {
        let rel: RelatedMany<Team> = RelatedMany::new("");
        let team1 = Team {
            id: Some(1),
            name: "A".to_string(),
        };
        let team2 = Team {
            id: Some(2),
            name: "B".to_string(),
        };

        rel.link(&team1);
        rel.unlink(&team2);
        assert!(rel.has_pending_ops());

        let links = rel.take_pending_links();
        let unlinks = rel.take_pending_unlinks();

        assert_eq!(links.len(), 1);
        assert_eq!(unlinks.len(), 1);
        assert!(!rel.has_pending_ops());
    }

    #[test]
    fn test_related_many_clone_preserves_pending() {
        let rel: RelatedMany<Team> = RelatedMany::new("");
        let team = Team {
            id: Some(1),
            name: "A".to_string(),
        };

        rel.link(&team);
        let cloned = rel.clone();

        assert!(cloned.has_pending_ops());
        let pending = cloned.take_pending_links();
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn test_related_many_set_parent_pk() {
        let mut rel: RelatedMany<Team> = RelatedMany::new("team_id");
        assert!(rel.parent_pk().is_none());

        rel.set_parent_pk(42_i64);
        assert_eq!(rel.parent_pk(), Some(&Value::from(42_i64)));
    }

    // ========================================================================
    // Lazy<T> Tests
    // ========================================================================

    #[test]
    fn test_lazy_empty_has_no_fk() {
        let lazy = Lazy::<Team>::empty();
        assert!(lazy.fk().is_none());
        assert!(lazy.is_empty());
        assert!(!lazy.is_loaded());
        assert!(lazy.get().is_none());
    }

    #[test]
    fn test_lazy_from_fk_stores_value() {
        let lazy = Lazy::<Team>::from_fk(42_i64);
        assert!(!lazy.is_empty());
        assert_eq!(lazy.fk(), Some(&Value::from(42_i64)));
        assert!(!lazy.is_loaded());
        assert!(lazy.get().is_none());
    }

    #[test]
    fn test_lazy_not_loaded_initially() {
        let lazy = Lazy::<Team>::from_fk(1_i64);
        assert!(!lazy.is_loaded());
    }

    #[test]
    fn test_lazy_loaded_creates_loaded_state() {
        let team = Team {
            id: Some(1),
            name: "Avengers".to_string(),
        };
        let lazy = Lazy::loaded(team.clone());
        assert!(lazy.is_loaded());
        assert!(lazy.fk().is_none()); // No FK needed when pre-loaded
        assert_eq!(lazy.get(), Some(&team));
    }

    #[test]
    fn test_lazy_set_loaded_succeeds_first_time() {
        let lazy = Lazy::<Team>::from_fk(1_i64);
        let team = Team {
            id: Some(1),
            name: "Avengers".to_string(),
        };
        assert!(lazy.set_loaded(Some(team.clone())).is_ok());
        assert!(lazy.is_loaded());
        assert_eq!(lazy.get(), Some(&team));
    }

    #[test]
    fn test_lazy_set_loaded_fails_second_time() {
        let lazy = Lazy::<Team>::empty();
        assert!(lazy.set_loaded(None).is_ok());
        assert!(lazy.is_loaded());
        assert!(lazy.set_loaded(None).is_err());
    }

    #[test]
    fn test_lazy_load_fetches_from_loader_and_caches() {
        #[derive(Default)]
        struct Loader {
            calls: usize,
        }

        impl LazyLoader<Team> for Loader {
            fn get(
                &mut self,
                _cx: &Cx,
                pk: Value,
            ) -> impl Future<Output = Outcome<Option<Team>, Error>> + Send {
                self.calls += 1;
                let team = match pk {
                    Value::BigInt(1) => Some(Team {
                        id: Some(1),
                        name: "Avengers".to_string(),
                    }),
                    _ => None,
                };
                async move { Outcome::Ok(team) }
            }
        }

        let rt = RuntimeBuilder::current_thread()
            .build()
            .expect("create asupersync runtime");
        let cx = Cx::for_testing();

        rt.block_on(async {
            let mut lazy = Lazy::<Team>::from_fk(1_i64);
            let mut loader = Loader::default();

            let outcome = lazy.load(&cx, &mut loader).await;
            assert!(matches!(outcome, Outcome::Ok(Some(_))));
            assert!(lazy.is_loaded());
            assert_eq!(loader.calls, 1);

            // Cached: no second call to the loader
            let outcome2 = lazy.load(&cx, &mut loader).await;
            assert!(matches!(outcome2, Outcome::Ok(Some(_))));
            assert_eq!(loader.calls, 1);
        });
    }

    #[test]
    fn test_lazy_load_empty_returns_none_without_calling_loader() {
        #[derive(Default)]
        struct Loader {
            calls: usize,
        }

        impl LazyLoader<Team> for Loader {
            fn get(
                &mut self,
                _cx: &Cx,
                _pk: Value,
            ) -> impl Future<Output = Outcome<Option<Team>, Error>> + Send {
                self.calls += 1;
                async { Outcome::Ok(None) }
            }
        }

        let rt = RuntimeBuilder::current_thread()
            .build()
            .expect("create asupersync runtime");
        let cx = Cx::for_testing();

        rt.block_on(async {
            let mut lazy = Lazy::<Team>::empty();
            let mut loader = Loader::default();

            let outcome = lazy.load(&cx, &mut loader).await;
            assert!(matches!(outcome, Outcome::Ok(None)));
            assert!(lazy.is_loaded());
            assert_eq!(loader.calls, 0);
        });
    }

    #[test]
    fn test_lazy_load_error_does_not_mark_loaded() {
        #[derive(Default)]
        struct Loader {
            calls: usize,
        }

        impl LazyLoader<Team> for Loader {
            fn get(
                &mut self,
                _cx: &Cx,
                _pk: Value,
            ) -> impl Future<Output = Outcome<Option<Team>, Error>> + Send {
                self.calls += 1;
                async { Outcome::Err(Error::Custom("boom".to_string())) }
            }
        }

        let rt = RuntimeBuilder::current_thread()
            .build()
            .expect("create asupersync runtime");
        let cx = Cx::for_testing();

        rt.block_on(async {
            let mut lazy = Lazy::<Team>::from_fk(1_i64);
            let mut loader = Loader::default();

            let outcome = lazy.load(&cx, &mut loader).await;
            assert!(matches!(outcome, Outcome::Err(_)));
            assert!(!lazy.is_loaded());
            assert_eq!(loader.calls, 1);
        });
    }

    #[test]
    fn test_lazy_get_before_load_returns_none() {
        let lazy = Lazy::<Team>::from_fk(1_i64);
        assert!(lazy.get().is_none());
    }

    #[test]
    fn test_lazy_default_is_empty() {
        let lazy: Lazy<Team> = Lazy::default();
        assert!(lazy.is_empty());
        assert!(!lazy.is_loaded());
    }

    #[test]
    fn test_lazy_clone_unloaded_is_unloaded() {
        let lazy = Lazy::<Team>::from_fk(7_i64);
        let cloned = lazy.clone();
        assert!(!cloned.is_loaded());
        assert_eq!(cloned.fk(), lazy.fk());
    }

    #[test]
    fn test_lazy_clone_loaded_preserves_object() {
        let team = Team {
            id: Some(1),
            name: "Avengers".to_string(),
        };
        let lazy = Lazy::loaded(team.clone());
        let cloned = lazy.clone();
        assert!(cloned.is_loaded());
        assert_eq!(cloned.get(), Some(&team));
    }

    #[test]
    fn test_lazy_debug_output_shows_state() {
        let lazy = Lazy::<Team>::from_fk(1_i64);
        let s = format!("{lazy:?}");
        assert!(s.contains("state"));
        assert!(s.contains("unloaded"));
    }

    #[test]
    fn test_lazy_serde_serialize_loaded_outputs_object() {
        let lazy = Lazy::loaded(Team {
            id: Some(1),
            name: "Avengers".to_string(),
        });
        let json = serde_json::to_value(&lazy).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "id": 1,
                "name": "Avengers"
            })
        );
    }

    #[test]
    fn test_lazy_serde_serialize_unloaded_outputs_null() {
        let lazy = Lazy::<Team>::from_fk(1_i64);
        let json = serde_json::to_value(&lazy).unwrap();
        assert_eq!(json, serde_json::Value::Null);
    }

    #[test]
    fn test_lazy_serde_deserialize_object_creates_loaded() {
        let lazy: Lazy<Team> = serde_json::from_value(serde_json::json!({
            "id": 1,
            "name": "Avengers"
        }))
        .unwrap();

        let expected = Team {
            id: Some(1),
            name: "Avengers".to_string(),
        };
        assert!(lazy.is_loaded());
        assert_eq!(lazy.get(), Some(&expected));
    }

    #[test]
    fn test_lazy_serde_deserialize_null_creates_empty() {
        let lazy: Lazy<Team> = serde_json::from_value(serde_json::Value::Null).unwrap();
        assert!(lazy.is_empty());
        assert!(!lazy.is_loaded());
        assert!(lazy.get().is_none());
    }

    #[test]
    fn test_lazy_serde_roundtrip_preserves_data() {
        let lazy = Lazy::loaded(Team {
            id: Some(1),
            name: "Avengers".to_string(),
        });
        let json = serde_json::to_string(&lazy).unwrap();
        let decoded: Lazy<Team> = serde_json::from_str(&json).unwrap();
        assert!(decoded.is_loaded());
        assert_eq!(decoded.get(), lazy.get());
    }

    #[test]
    fn test_lazy_reset_clears_loaded_state() {
        let mut lazy = Lazy::loaded(Team {
            id: Some(1),
            name: "Test".to_string(),
        });
        assert!(lazy.is_loaded());

        lazy.reset();
        assert!(!lazy.is_loaded());
        assert!(lazy.get().is_none());
    }

    #[test]
    fn test_lazy_is_empty_accurate() {
        let empty = Lazy::<Team>::empty();
        assert!(empty.is_empty());

        let with_fk = Lazy::<Team>::from_fk(1_i64);
        assert!(!with_fk.is_empty());

        let loaded = Lazy::loaded(Team {
            id: Some(1),
            name: "Test".to_string(),
        });
        assert!(loaded.is_empty()); // loaded() doesn't set FK value
    }

    #[test]
    fn test_lazy_load_missing_object_caches_none() {
        let lazy = Lazy::<Team>::from_fk(999_i64);
        // Simulate what Session::load_many does when object not found
        assert!(lazy.set_loaded(None).is_ok());
        assert!(lazy.is_loaded());
        assert!(lazy.get().is_none());

        // Second attempt should fail (already set)
        assert!(lazy.set_loaded(None).is_err());
    }

    // ========================================================================
    // Relationship Lookup Helper Tests
    // ========================================================================

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Hero {
        id: Option<i64>,
        name: String,
    }

    impl Model for Hero {
        const TABLE_NAME: &'static str = "heroes";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];
        const RELATIONSHIPS: &'static [RelationshipInfo] = &[RelationshipInfo {
            name: "team",
            related_table: "teams",
            kind: RelationshipKind::ManyToOne,
            local_key: Some("team_id"),
            remote_key: None,
            link_table: None,
            back_populates: Some("heroes"),
            lazy: false,
            cascade_delete: false,
        }];

        fn fields() -> &'static [FieldInfo] {
            &[]
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(_row: &Row) -> Result<Self> {
            Ok(Self {
                id: None,
                name: String::new(),
            })
        }

        fn primary_key_value(&self) -> Vec<Value> {
            match self.id {
                Some(id) => vec![Value::from(id)],
                None => vec![],
            }
        }

        fn is_new(&self) -> bool {
            self.id.is_none()
        }
    }

    // TeamWithRelationships has back_populates pointing to Hero
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TeamWithRelationships {
        id: Option<i64>,
        name: String,
    }

    impl Model for TeamWithRelationships {
        const TABLE_NAME: &'static str = "teams";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];
        const RELATIONSHIPS: &'static [RelationshipInfo] = &[RelationshipInfo {
            name: "heroes",
            related_table: "heroes",
            kind: RelationshipKind::OneToMany,
            local_key: None,
            remote_key: Some("team_id"),
            link_table: None,
            back_populates: Some("team"),
            lazy: false,
            cascade_delete: false,
        }];

        fn fields() -> &'static [FieldInfo] {
            &[]
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(_row: &Row) -> Result<Self> {
            Ok(Self {
                id: None,
                name: String::new(),
            })
        }

        fn primary_key_value(&self) -> Vec<Value> {
            match self.id {
                Some(id) => vec![Value::from(id)],
                None => vec![],
            }
        }

        fn is_new(&self) -> bool {
            self.id.is_none()
        }
    }

    #[test]
    fn test_find_relationship_found() {
        let rel = find_relationship::<Hero>("team");
        assert!(rel.is_some());
        let rel = rel.unwrap();
        assert_eq!(rel.name, "team");
        assert_eq!(rel.related_table, "teams");
        assert_eq!(rel.back_populates, Some("heroes"));
    }

    #[test]
    fn test_find_relationship_not_found() {
        let rel = find_relationship::<Hero>("powers");
        assert!(rel.is_none());
    }

    #[test]
    fn test_find_relationship_empty_relationships() {
        // Team has no relationships defined
        let rel = find_relationship::<Team>("heroes");
        assert!(rel.is_none());
    }

    #[test]
    fn test_find_back_relationship_found() {
        let hero_team_rel = find_relationship::<Hero>("team").unwrap();
        let back = find_back_relationship(hero_team_rel, TeamWithRelationships::RELATIONSHIPS);
        assert!(back.is_some());
        let back = back.unwrap();
        assert_eq!(back.name, "heroes");
        assert_eq!(back.back_populates, Some("team"));
    }

    #[test]
    fn test_find_back_relationship_no_back_populates() {
        let rel = RelationshipInfo::new("team", "teams", RelationshipKind::ManyToOne);
        let back = find_back_relationship(&rel, TeamWithRelationships::RELATIONSHIPS);
        assert!(back.is_none());
    }

    #[test]
    fn test_validate_back_populates_valid() {
        let result = validate_back_populates::<Hero, TeamWithRelationships>("team");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_back_populates_no_source_relationship() {
        let result = validate_back_populates::<Hero, TeamWithRelationships>("nonexistent");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("No relationship"));
    }

    #[test]
    fn test_validate_back_populates_no_target_relationship() {
        // Team has no RELATIONSHIPS, so validation will fail
        let result = validate_back_populates::<Hero, Team>("team");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("does not exist"));
    }
}
