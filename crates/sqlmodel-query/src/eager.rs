//! Eager loading infrastructure for relationships.
//!
//! This module provides the `EagerLoader` builder for configuring which
//! relationships to load with a query. Eager loading fetches related
//! objects in the same query using SQL JOINs.

use sqlmodel_core::{Model, RelationshipInfo, RelationshipKind, Value};
use std::marker::PhantomData;

/// Builder for eager loading configuration.
///
/// # Example
///
/// ```ignore
/// let heroes = select!(Hero)
///     .eager(EagerLoader::new().include("team"))
///     .all_eager(&conn)
///     .await?;
/// ```
#[derive(Debug, Clone)]
pub struct EagerLoader<T: Model> {
    /// Relationships to eager-load.
    includes: Vec<IncludePath>,
    /// Model type marker.
    _marker: PhantomData<T>,
}

/// A path to a relationship to include.
#[derive(Debug, Clone)]
pub struct IncludePath {
    /// Relationship name on parent.
    pub relationship: &'static str,
    /// Nested relationships to load.
    pub nested: Vec<IncludePath>,
}

impl IncludePath {
    /// Create a new include path for a single relationship.
    #[must_use]
    pub fn new(relationship: &'static str) -> Self {
        Self {
            relationship,
            nested: Vec::new(),
        }
    }

    /// Add a nested relationship to load.
    #[must_use]
    pub fn nest(mut self, path: IncludePath) -> Self {
        self.nested.push(path);
        self
    }
}

impl<T: Model> EagerLoader<T> {
    /// Create a new empty eager loader.
    #[must_use]
    pub fn new() -> Self {
        Self {
            includes: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// Include a relationship in eager loading.
    ///
    /// # Example
    ///
    /// ```ignore
    /// EagerLoader::<Hero>::new().include("team")
    /// ```
    #[must_use]
    pub fn include(mut self, relationship: &'static str) -> Self {
        self.includes.push(IncludePath::new(relationship));
        self
    }

    /// Include a nested relationship (e.g., "team.headquarters").
    ///
    /// # Example
    ///
    /// ```ignore
    /// EagerLoader::<Hero>::new().include_nested("team.headquarters")
    /// ```
    #[must_use]
    pub fn include_nested(mut self, path: &'static str) -> Self {
        // Handle empty or whitespace-only paths
        let path = path.trim();
        if path.is_empty() {
            return self;
        }

        let parts: Vec<&'static str> = path.split('.').collect();
        // split('.') on non-empty string always returns at least one element
        // but we should still guard against [""] from paths like "."
        if parts.iter().all(|p| p.is_empty()) {
            return self;
        }

        // Filter out empty parts (handles cases like "team..headquarters")
        let parts: Vec<&'static str> = parts.into_iter().filter(|p| !p.is_empty()).collect();
        if parts.is_empty() {
            return self;
        }

        // Build nested IncludePath structure
        let include = Self::build_nested_path(&parts);
        self.includes.push(include);
        self
    }

    /// Build a nested IncludePath from path parts.
    fn build_nested_path(parts: &[&'static str]) -> IncludePath {
        if parts.len() == 1 {
            IncludePath::new(parts[0])
        } else {
            let mut path = IncludePath::new(parts[0]);
            path.nested.push(Self::build_nested_path(&parts[1..]));
            path
        }
    }

    /// Get the include paths.
    #[must_use]
    pub fn includes(&self) -> &[IncludePath] {
        &self.includes
    }

    /// Check if any relationships are included.
    #[must_use]
    pub fn has_includes(&self) -> bool {
        !self.includes.is_empty()
    }
}

impl<T: Model> Default for EagerLoader<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Find a relationship by name in a model's RELATIONSHIPS.
#[must_use]
pub fn find_relationship<M: Model>(name: &str) -> Option<&'static RelationshipInfo> {
    M::RELATIONSHIPS.iter().find(|r| r.name == name)
}

/// Generate a JOIN clause for a relationship.
#[must_use]
pub fn build_join_clause(
    parent_table: &str,
    rel: &RelationshipInfo,
    _param_offset: usize,
) -> (String, Vec<Value>) {
    let params = Vec::new();

    // Get the primary key column name from the relationship, defaulting to "id"
    let remote_pk = rel.remote_key.unwrap_or("id");

    let sql = match rel.kind {
        RelationshipKind::ManyToOne | RelationshipKind::OneToOne => {
            // LEFT JOIN related_table ON parent.fk = related.pk
            let local_key = rel.local_key.unwrap_or("id");
            format!(
                " LEFT JOIN {} ON {}.{} = {}.{}",
                rel.related_table, parent_table, local_key, rel.related_table, remote_pk
            )
        }
        RelationshipKind::OneToMany => {
            // LEFT JOIN related_table ON related.fk = parent.pk
            // For OneToMany, remote_key is the FK on the related table pointing to us
            let fk_on_related = rel.remote_key.unwrap_or("id");
            // And we need local_key as our PK (default "id")
            let local_pk = rel.local_key.unwrap_or("id");
            format!(
                " LEFT JOIN {} ON {}.{} = {}.{}",
                rel.related_table, rel.related_table, fk_on_related, parent_table, local_pk
            )
        }
        RelationshipKind::ManyToMany => {
            // LEFT JOIN link_table ON parent.pk = link.local_col
            // LEFT JOIN related_table ON link.remote_col = related.pk
            if let Some(link) = &rel.link_table {
                let local_pk = rel.local_key.unwrap_or("id");
                format!(
                    " LEFT JOIN {} ON {}.{} = {}.{} LEFT JOIN {} ON {}.{} = {}.{}",
                    link.table_name,
                    parent_table,
                    local_pk,
                    link.table_name,
                    link.local_column,
                    rel.related_table,
                    link.table_name,
                    link.remote_column,
                    rel.related_table,
                    remote_pk
                )
            } else {
                String::new()
            }
        }
    };

    (sql, params)
}

/// Generate aliased column names for eager loading.
///
/// Prefixes each column with the table name to avoid conflicts.
#[must_use]
pub fn build_aliased_columns(table_name: &str, columns: &[&str]) -> String {
    columns
        .iter()
        .map(|col| format!("{}.{} AS {}__{}", table_name, col, table_name, col))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlmodel_core::{Error, FieldInfo, Model, Result, Row, Value};

    #[derive(Debug, Clone)]
    struct TestHero;

    impl Model for TestHero {
        const TABLE_NAME: &'static str = "heroes";
        const PRIMARY_KEY: &'static [&'static str] = &["id"];
        const RELATIONSHIPS: &'static [RelationshipInfo] =
            &[
                RelationshipInfo::new("team", "teams", RelationshipKind::ManyToOne)
                    .local_key("team_id"),
            ];

        fn fields() -> &'static [FieldInfo] {
            &[]
        }

        fn to_row(&self) -> Vec<(&'static str, Value)> {
            vec![]
        }

        fn from_row(_row: &Row) -> Result<Self> {
            Err(Error::Custom("not used".to_string()))
        }

        fn primary_key_value(&self) -> Vec<Value> {
            vec![]
        }

        fn is_new(&self) -> bool {
            true
        }
    }

    #[test]
    fn test_eager_loader_new() {
        let loader = EagerLoader::<TestHero>::new();
        assert!(!loader.has_includes());
        assert!(loader.includes().is_empty());
    }

    #[test]
    fn test_eager_loader_include() {
        let loader = EagerLoader::<TestHero>::new().include("team");
        assert!(loader.has_includes());
        assert_eq!(loader.includes().len(), 1);
        assert_eq!(loader.includes()[0].relationship, "team");
    }

    #[test]
    fn test_eager_loader_multiple_includes() {
        let loader = EagerLoader::<TestHero>::new()
            .include("team")
            .include("powers");
        assert_eq!(loader.includes().len(), 2);
    }

    #[test]
    fn test_eager_loader_include_nested() {
        let loader = EagerLoader::<TestHero>::new().include_nested("team.headquarters");
        assert_eq!(loader.includes().len(), 1);
        assert_eq!(loader.includes()[0].relationship, "team");
        assert_eq!(loader.includes()[0].nested.len(), 1);
        assert_eq!(loader.includes()[0].nested[0].relationship, "headquarters");
    }

    #[test]
    fn test_eager_loader_include_deeply_nested() {
        let loader =
            EagerLoader::<TestHero>::new().include_nested("team.headquarters.city.country");
        assert_eq!(loader.includes().len(), 1);
        assert_eq!(loader.includes()[0].relationship, "team");
        assert_eq!(loader.includes()[0].nested[0].relationship, "headquarters");
        assert_eq!(
            loader.includes()[0].nested[0].nested[0].relationship,
            "city"
        );
        assert_eq!(
            loader.includes()[0].nested[0].nested[0].nested[0].relationship,
            "country"
        );
    }

    #[test]
    fn test_find_relationship() {
        let rel = find_relationship::<TestHero>("team");
        assert!(rel.is_some());
        assert_eq!(rel.unwrap().name, "team");
        assert_eq!(rel.unwrap().related_table, "teams");
    }

    #[test]
    fn test_find_relationship_not_found() {
        let rel = find_relationship::<TestHero>("nonexistent");
        assert!(rel.is_none());
    }

    #[test]
    fn test_build_join_many_to_one() {
        let rel = RelationshipInfo::new("team", "teams", RelationshipKind::ManyToOne)
            .local_key("team_id");

        let (sql, params) = build_join_clause("heroes", &rel, 0);

        assert_eq!(sql, " LEFT JOIN teams ON heroes.team_id = teams.id");
        assert!(params.is_empty());
    }

    #[test]
    fn test_build_join_one_to_many() {
        let rel = RelationshipInfo::new("heroes", "heroes", RelationshipKind::OneToMany)
            .remote_key("team_id");

        let (sql, params) = build_join_clause("teams", &rel, 0);

        assert_eq!(sql, " LEFT JOIN heroes ON heroes.team_id = teams.id");
        assert!(params.is_empty());
    }

    #[test]
    fn test_build_join_many_to_many() {
        let rel =
            RelationshipInfo::new("powers", "powers", RelationshipKind::ManyToMany).link_table(
                sqlmodel_core::LinkTableInfo::new("hero_powers", "hero_id", "power_id"),
            );

        let (sql, params) = build_join_clause("heroes", &rel, 0);

        assert!(sql.contains("LEFT JOIN hero_powers"));
        assert!(sql.contains("LEFT JOIN powers"));
        assert!(params.is_empty());
    }

    #[test]
    fn test_build_aliased_columns() {
        let result = build_aliased_columns("heroes", &["id", "name", "team_id"]);
        assert!(result.contains("heroes.id AS heroes__id"));
        assert!(result.contains("heroes.name AS heroes__name"));
        assert!(result.contains("heroes.team_id AS heroes__team_id"));
    }

    #[test]
    fn test_eager_loader_default() {
        let loader: EagerLoader<TestHero> = EagerLoader::default();
        assert!(!loader.has_includes());
    }

    #[test]
    fn test_include_path_new() {
        let path = IncludePath::new("team");
        assert_eq!(path.relationship, "team");
        assert!(path.nested.is_empty());
    }

    #[test]
    fn test_include_path_nest() {
        let path = IncludePath::new("team").nest(IncludePath::new("headquarters"));
        assert_eq!(path.nested.len(), 1);
        assert_eq!(path.nested[0].relationship, "headquarters");
    }
}
