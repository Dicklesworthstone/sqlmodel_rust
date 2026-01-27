//! Relationship metadata for SQLModel Rust.
//!
//! Relationships are defined at compile-time (via derive macros) and represented
//! as static metadata on each `Model`. This allows higher-level layers (query
//! builder, session/UoW, eager/lazy loaders) to generate correct SQL and load
//! related objects without runtime reflection.

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
