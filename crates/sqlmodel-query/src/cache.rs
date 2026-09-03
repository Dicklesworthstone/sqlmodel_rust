//! Statement caching for compiled SQL queries.
//!
//! Caches compiled SQL strings keyed by a hash so repeated queries
//! avoid redundant string building.
//!
//! This is a cache of SQL *text* for callers that build the same query
//! repeatedly; it is not connected to any driver and is not a server-side
//! prepared-statement cache. Server-side reuse lives in the drivers: the
//! MySQL driver prepares each distinct parameterized statement once per
//! connection, the PostgreSQL driver parses every query as the unnamed
//! statement (named statements only through `Connection::prepare`), and
//! the SQLite drivers keep none (see the README's "Prepared statements" row).

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::Instant;

/// A cached compiled SQL statement.
#[derive(Debug, Clone)]
pub struct CachedStatement {
    /// The compiled SQL string.
    pub sql: String,
    /// When this entry was last accessed.
    pub last_used: Instant,
    /// Number of times this statement has been reused.
    pub hit_count: u64,
}

/// LRU-style cache for compiled SQL statements.
///
/// Keyed by a `u64` hash that callers compute from their query structure.
/// When the cache exceeds `max_size`, the least-recently-used entry is evicted.
///
/// # Example
///
/// ```
/// use sqlmodel_query::cache::StatementCache;
///
/// let mut cache = StatementCache::new(100);
///
/// // Cache a compiled query
/// let sql = cache.get_or_insert(12345, || "SELECT * FROM users WHERE id = $1".to_string());
/// assert_eq!(sql, "SELECT * FROM users WHERE id = $1");
///
/// // Second call returns cached version
/// let called = std::cell::Cell::new(false);
/// let sql2 = cache.get_or_insert(12345, || {
///     called.set(true);
///     "SELECT * FROM users WHERE id = $1".to_string()
/// });
/// assert_eq!(sql2, "SELECT * FROM users WHERE id = $1");
/// assert!(!called.get());
/// ```
#[derive(Debug)]
pub struct StatementCache {
    cache: HashMap<u64, CachedStatement>,
    max_size: usize,
}

impl StatementCache {
    /// Create a new cache with the given maximum number of entries.
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: HashMap::with_capacity(max_size.min(256)),
            max_size,
        }
    }

    /// Get a cached statement or compile and insert it.
    ///
    /// The `builder` closure is only called on cache miss.
    pub fn get_or_insert(&mut self, key: u64, builder: impl FnOnce() -> String) -> &str {
        // Check if we need to evict before inserting
        if !self.cache.contains_key(&key) && self.cache.len() >= self.max_size {
            self.evict_lru();
        }

        let entry = self.cache.entry(key).or_insert_with(|| CachedStatement {
            sql: builder(),
            last_used: Instant::now(),
            hit_count: 0,
        });
        entry.last_used = Instant::now();
        entry.hit_count += 1;
        &entry.sql
    }

    /// Check if a statement is cached.
    pub fn contains(&self, key: u64) -> bool {
        self.cache.contains_key(&key)
    }

    /// Get cache statistics.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if cache is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Clear all cached statements.
    pub fn clear(&mut self) {
        self.cache.clear();
    }

    /// Evict the least-recently-used entry.
    fn evict_lru(&mut self) {
        if let Some((&lru_key, _)) = self.cache.iter().min_by_key(|(_, entry)| entry.last_used) {
            self.cache.remove(&lru_key);
        }
    }
}

/// Compute a hash key for caching from any hashable value.
///
/// Useful for creating cache keys from query components.
pub fn cache_key(value: &impl Hash) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

impl Default for StatementCache {
    fn default() -> Self {
        Self::new(1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_hit() {
        let mut cache = StatementCache::new(10);
        let sql = cache
            .get_or_insert(1, || "SELECT 1".to_string())
            .to_string();
        assert_eq!(sql, "SELECT 1");

        // Should return cached value
        let called = std::cell::Cell::new(false);
        let sql2 = cache
            .get_or_insert(1, || {
                called.set(true);
                "SELECT 1".to_string()
            })
            .to_string();
        assert_eq!(sql2, "SELECT 1");
        assert!(!called.get());
    }

    #[test]
    fn test_cache_miss() {
        let mut cache = StatementCache::new(10);
        let sql1 = cache
            .get_or_insert(1, || "SELECT 1".to_string())
            .to_string();
        let sql2 = cache
            .get_or_insert(2, || "SELECT 2".to_string())
            .to_string();
        assert_eq!(sql1, "SELECT 1");
        assert_eq!(sql2, "SELECT 2");
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_eviction() {
        let mut cache = StatementCache::new(2);
        cache.get_or_insert(1, || "SELECT 1".to_string());
        cache.get_or_insert(2, || "SELECT 2".to_string());
        // This should evict key 1 (LRU)
        cache.get_or_insert(3, || "SELECT 3".to_string());

        assert_eq!(cache.len(), 2);
        assert!(!cache.contains(1));
        assert!(cache.contains(2));
        assert!(cache.contains(3));
    }

    #[test]
    fn test_lru_ordering() {
        let mut cache = StatementCache::new(2);
        cache.get_or_insert(1, || "SELECT 1".to_string());
        cache.get_or_insert(2, || "SELECT 2".to_string());

        // Access key 1 to make it recently used
        let called = std::cell::Cell::new(false);
        cache.get_or_insert(1, || {
            called.set(true);
            "SELECT 1".to_string()
        });
        assert!(!called.get());

        // Eviction should remove key 2 (now LRU)
        cache.get_or_insert(3, || "SELECT 3".to_string());

        assert!(cache.contains(1));
        assert!(!cache.contains(2));
        assert!(cache.contains(3));
    }

    #[test]
    fn test_cache_key_function() {
        let key1 = cache_key(&"SELECT * FROM users");
        let key2 = cache_key(&"SELECT * FROM users");
        let key3 = cache_key(&"SELECT * FROM orders");

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_clear() {
        let mut cache = StatementCache::new(10);
        cache.get_or_insert(1, || "SELECT 1".to_string());
        cache.get_or_insert(2, || "SELECT 2".to_string());
        assert_eq!(cache.len(), 2);

        cache.clear();
        assert!(cache.is_empty());
    }
}
