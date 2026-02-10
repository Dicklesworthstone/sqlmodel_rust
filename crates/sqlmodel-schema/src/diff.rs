//! Schema diff engine for comparing database schemas.
//!
//! This module provides utilities to compare a current database schema
//! against an expected schema and generate operations to bring them
//! into alignment.

use crate::introspect::{
    ColumnInfo, DatabaseSchema, Dialect, ForeignKeyInfo, IndexInfo, TableInfo, UniqueConstraintInfo,
};
use std::collections::{HashMap, HashSet};

fn fk_effective_name(table: &str, fk: &ForeignKeyInfo) -> String {
    fk.name
        .clone()
        .unwrap_or_else(|| format!("fk_{}_{}", table, fk.column))
}

fn unique_effective_name(table: &str, constraint: &UniqueConstraintInfo) -> String {
    constraint
        .name
        .clone()
        .unwrap_or_else(|| format!("uk_{}_{}", table, constraint.columns.join("_")))
}

// ============================================================================
// Schema Operations
// ============================================================================

/// A single schema modification operation.
#[derive(Debug, Clone)]
pub enum SchemaOperation {
    // Tables
    /// Create a new table.
    CreateTable(TableInfo),
    /// Drop an existing table.
    DropTable(String),
    /// Rename a table.
    RenameTable { from: String, to: String },

    // Columns
    /// Add a column to a table.
    AddColumn { table: String, column: ColumnInfo },
    /// Drop a column from a table.
    ///
    /// For SQLite, safely dropping a column on versions < 3.35 requires table
    /// recreation. When `table_info` is present, the SQLite DDL generator can
    /// emit a correct recreate-copy-drop sequence (preserving indexes we track).
    DropColumn {
        table: String,
        column: String,
        table_info: Option<TableInfo>,
    },
    /// Change a column's type.
    AlterColumnType {
        table: String,
        column: String,
        from_type: String,
        to_type: String,
        table_info: Option<TableInfo>,
    },
    /// Change a column's nullability.
    AlterColumnNullable {
        table: String,
        column: ColumnInfo,
        from_nullable: bool,
        to_nullable: bool,
        table_info: Option<TableInfo>,
    },
    /// Change a column's default value.
    AlterColumnDefault {
        table: String,
        column: String,
        from_default: Option<String>,
        to_default: Option<String>,
        table_info: Option<TableInfo>,
    },
    /// Rename a column.
    RenameColumn {
        table: String,
        from: String,
        to: String,
    },

    // Primary Keys
    /// Add a primary key constraint.
    AddPrimaryKey {
        table: String,
        columns: Vec<String>,
        table_info: Option<TableInfo>,
    },
    /// Drop a primary key constraint.
    DropPrimaryKey {
        table: String,
        table_info: Option<TableInfo>,
    },

    // Foreign Keys
    /// Add a foreign key constraint.
    AddForeignKey {
        table: String,
        fk: ForeignKeyInfo,
        table_info: Option<TableInfo>,
    },
    /// Drop a foreign key constraint.
    DropForeignKey {
        table: String,
        name: String,
        table_info: Option<TableInfo>,
    },

    // Unique Constraints
    /// Add a unique constraint.
    AddUnique {
        table: String,
        constraint: UniqueConstraintInfo,
        table_info: Option<TableInfo>,
    },
    /// Drop a unique constraint.
    DropUnique {
        table: String,
        name: String,
        table_info: Option<TableInfo>,
    },

    // Indexes
    /// Create an index.
    CreateIndex { table: String, index: IndexInfo },
    /// Drop an index.
    DropIndex { table: String, name: String },
}

impl SchemaOperation {
    /// Check if this operation potentially loses data.
    pub fn is_destructive(&self) -> bool {
        matches!(
            self,
            SchemaOperation::DropTable(_)
                | SchemaOperation::DropColumn { .. }
                | SchemaOperation::AlterColumnType { .. }
        )
    }

    /// Get the inverse operation for rollback, if possible.
    ///
    /// Some operations are not reversible (e.g., dropping a table/column) because the
    /// original schema and data cannot be reconstructed from the operation alone.
    pub fn inverse(&self) -> Option<Self> {
        match self {
            SchemaOperation::CreateTable(table) => {
                Some(SchemaOperation::DropTable(table.name.clone()))
            }
            SchemaOperation::DropTable(_) => None,
            SchemaOperation::RenameTable { from, to } => Some(SchemaOperation::RenameTable {
                from: to.clone(),
                to: from.clone(),
            }),
            SchemaOperation::AddColumn { table, column } => Some(SchemaOperation::DropColumn {
                table: table.clone(),
                column: column.name.clone(),
                table_info: None,
            }),
            SchemaOperation::DropColumn { .. } => None,
            SchemaOperation::AlterColumnType {
                table,
                column,
                from_type,
                to_type,
                ..
            } => Some(SchemaOperation::AlterColumnType {
                table: table.clone(),
                column: column.clone(),
                from_type: to_type.clone(),
                to_type: from_type.clone(),
                table_info: None,
            }),
            SchemaOperation::AlterColumnNullable {
                table,
                column,
                from_nullable,
                to_nullable,
                ..
            } => Some(SchemaOperation::AlterColumnNullable {
                table: table.clone(),
                column: {
                    let mut col = column.clone();
                    col.nullable = *from_nullable;
                    col
                },
                from_nullable: *to_nullable,
                to_nullable: *from_nullable,
                table_info: None,
            }),
            SchemaOperation::AlterColumnDefault {
                table,
                column,
                from_default,
                to_default,
                ..
            } => Some(SchemaOperation::AlterColumnDefault {
                table: table.clone(),
                column: column.clone(),
                from_default: to_default.clone(),
                to_default: from_default.clone(),
                table_info: None,
            }),
            SchemaOperation::RenameColumn { table, from, to } => {
                Some(SchemaOperation::RenameColumn {
                    table: table.clone(),
                    from: to.clone(),
                    to: from.clone(),
                })
            }
            SchemaOperation::AddPrimaryKey { table, .. } => Some(SchemaOperation::DropPrimaryKey {
                table: table.clone(),
                table_info: None,
            }),
            SchemaOperation::DropPrimaryKey { .. } => None,
            SchemaOperation::AddForeignKey { table, fk, .. } => {
                Some(SchemaOperation::DropForeignKey {
                    table: table.clone(),
                    name: fk_effective_name(table, fk),
                    table_info: None,
                })
            }
            SchemaOperation::DropForeignKey { .. } => None,
            SchemaOperation::AddUnique {
                table, constraint, ..
            } => Some(SchemaOperation::DropUnique {
                table: table.clone(),
                name: unique_effective_name(table, constraint),
                table_info: None,
            }),
            SchemaOperation::DropUnique { .. } => None,
            SchemaOperation::CreateIndex { table, index } => Some(SchemaOperation::DropIndex {
                table: table.clone(),
                name: index.name.clone(),
            }),
            SchemaOperation::DropIndex { .. } => None,
        }
    }

    /// Get the table this operation affects.
    pub fn table(&self) -> Option<&str> {
        match self {
            SchemaOperation::CreateTable(t) => Some(&t.name),
            SchemaOperation::DropTable(name) => Some(name),
            SchemaOperation::RenameTable { from, .. } => Some(from),
            SchemaOperation::AddColumn { table, .. }
            | SchemaOperation::DropColumn { table, .. }
            | SchemaOperation::AlterColumnType { table, .. }
            | SchemaOperation::AlterColumnNullable { table, .. }
            | SchemaOperation::AlterColumnDefault { table, .. }
            | SchemaOperation::RenameColumn { table, .. }
            | SchemaOperation::AddPrimaryKey { table, .. }
            | SchemaOperation::DropPrimaryKey { table, .. }
            | SchemaOperation::AddForeignKey { table, .. }
            | SchemaOperation::DropForeignKey { table, .. }
            | SchemaOperation::AddUnique { table, .. }
            | SchemaOperation::DropUnique { table, .. }
            | SchemaOperation::CreateIndex { table, .. }
            | SchemaOperation::DropIndex { table, .. } => Some(table),
        }
    }

    /// Get a priority value for ordering operations.
    fn priority(&self) -> u8 {
        // Order:
        // 1. Drop foreign keys (remove constraints before modifying)
        // 2. Drop indexes
        // 3. Drop unique constraints
        // 4. Drop primary keys
        // 5. Drop columns
        // 6. Alter columns
        // 7. Add columns
        // 8. Create tables (in FK order)
        // 9. Add primary keys
        // 10. Add unique constraints
        // 11. Add indexes
        // 12. Add foreign keys
        // 13. Drop tables (last, after FK removal)
        match self {
            SchemaOperation::DropForeignKey { .. } => 1,
            SchemaOperation::DropIndex { .. } => 2,
            SchemaOperation::DropUnique { .. } => 3,
            SchemaOperation::DropPrimaryKey { .. } => 4,
            SchemaOperation::DropColumn { .. } => 5,
            SchemaOperation::AlterColumnType { .. } => 6,
            SchemaOperation::AlterColumnNullable { .. } => 7,
            SchemaOperation::AlterColumnDefault { .. } => 8,
            SchemaOperation::AddColumn { .. } => 9,
            SchemaOperation::CreateTable(_) => 10,
            SchemaOperation::RenameTable { .. } => 11,
            SchemaOperation::RenameColumn { .. } => 12,
            SchemaOperation::AddPrimaryKey { .. } => 13,
            SchemaOperation::AddUnique { .. } => 14,
            SchemaOperation::CreateIndex { .. } => 15,
            SchemaOperation::AddForeignKey { .. } => 16,
            SchemaOperation::DropTable(_) => 17,
        }
    }
}

// ============================================================================
// Diff Result
// ============================================================================

/// Warning severity levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarningSeverity {
    /// Informational message.
    Info,
    /// Warning that should be reviewed.
    Warning,
    /// Potential data loss.
    DataLoss,
}

/// A warning about a schema operation.
#[derive(Debug, Clone)]
pub struct DiffWarning {
    /// Severity of the warning.
    pub severity: WarningSeverity,
    /// Warning message.
    pub message: String,
    /// Index into operations that caused this warning.
    pub operation_index: Option<usize>,
}

/// How to handle destructive schema operations (drops, type changes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DestructivePolicy {
    /// Skip destructive operations entirely.
    Skip,
    /// Include destructive operations, but require explicit confirmation.
    #[default]
    Warn,
    /// Include destructive operations without additional confirmation gating.
    Allow,
}

/// The result of comparing two schemas.
#[derive(Debug)]
pub struct SchemaDiff {
    /// Policy used when generating this diff.
    pub destructive_policy: DestructivePolicy,
    /// Operations to transform current schema to expected schema.
    pub operations: Vec<SchemaOperation>,
    /// Warnings about potential issues.
    pub warnings: Vec<DiffWarning>,
}

impl SchemaDiff {
    /// Create an empty diff with the provided destructive policy.
    pub fn new(destructive_policy: DestructivePolicy) -> Self {
        Self {
            destructive_policy,
            operations: Vec::new(),
            warnings: Vec::new(),
        }
    }

    /// Check if there are any changes.
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Count of all operations.
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    /// Check if there are any destructive operations.
    pub fn has_destructive(&self) -> bool {
        self.operations.iter().any(|op| op.is_destructive())
    }

    /// Get only destructive operations.
    pub fn destructive_operations(&self) -> Vec<&SchemaOperation> {
        self.operations
            .iter()
            .filter(|op| op.is_destructive())
            .collect()
    }

    /// Whether this diff requires explicit confirmation before applying.
    pub fn requires_confirmation(&self) -> bool {
        self.destructive_policy == DestructivePolicy::Warn && self.has_destructive()
    }

    /// Reorder operations for safe execution.
    pub fn order_operations(&mut self) {
        self.operations.sort_by_key(|op| op.priority());
    }

    /// Add an operation.
    fn add_op(&mut self, op: SchemaOperation) -> usize {
        let index = self.operations.len();
        self.operations.push(op);
        index
    }

    /// Add a warning.
    fn warn(
        &mut self,
        severity: WarningSeverity,
        message: impl Into<String>,
        operation_index: Option<usize>,
    ) {
        self.warnings.push(DiffWarning {
            severity,
            message: message.into(),
            operation_index,
        });
    }

    fn add_destructive_op(
        &mut self,
        op: SchemaOperation,
        warn_severity: WarningSeverity,
        warn_message: impl Into<String>,
    ) {
        let warn_message = warn_message.into();
        match self.destructive_policy {
            DestructivePolicy::Skip => {
                self.warn(
                    WarningSeverity::Warning,
                    format!("Skipped destructive operation: {}", warn_message),
                    None,
                );
            }
            DestructivePolicy::Warn => {
                let op_index = self.add_op(op);
                self.warn(warn_severity, warn_message, Some(op_index));
            }
            DestructivePolicy::Allow => {
                self.add_op(op);
            }
        }
    }
}

impl Default for SchemaDiff {
    fn default() -> Self {
        Self::new(DestructivePolicy::Warn)
    }
}

// ============================================================================
// Diff Algorithm
// ============================================================================

/// Compare two schemas and generate operations to transform current to expected.
///
/// # Example
///
/// ```ignore
/// use sqlmodel_schema::{expected_schema, Dialect};
/// use sqlmodel_schema::diff::schema_diff;
///
/// let current = introspector.introspect_all(&cx, &conn).await?;
/// let expected = expected_schema::<(Hero, Team)>(Dialect::Sqlite);
///
/// let diff = schema_diff(&current, &expected);
/// for op in &diff.operations {
///     println!("  {:?}", op);
/// }
/// ```
pub fn schema_diff(current: &DatabaseSchema, expected: &DatabaseSchema) -> SchemaDiff {
    schema_diff_with_policy(current, expected, DestructivePolicy::Warn)
}

/// Compare two schemas and generate operations to transform current to expected.
pub fn schema_diff_with_policy(
    current: &DatabaseSchema,
    expected: &DatabaseSchema,
    destructive_policy: DestructivePolicy,
) -> SchemaDiff {
    SchemaDiffer::new(destructive_policy).diff(current, expected)
}

/// Schema diff engine.
#[derive(Debug, Clone, Copy)]
pub struct SchemaDiffer {
    destructive_policy: DestructivePolicy,
}

impl SchemaDiffer {
    pub const fn new(destructive_policy: DestructivePolicy) -> Self {
        Self { destructive_policy }
    }

    pub fn diff(&self, current: &DatabaseSchema, expected: &DatabaseSchema) -> SchemaDiff {
        let mut diff = SchemaDiff::new(self.destructive_policy);

        // Detect table renames (identical structure, different name).
        let renames = detect_table_renames(current, expected, expected.dialect);
        let mut renamed_from: HashSet<&str> = HashSet::new();
        let mut renamed_to: HashSet<&str> = HashSet::new();
        for (from, to) in &renames {
            renamed_from.insert(from.as_str());
            renamed_to.insert(to.as_str());
            diff.add_op(SchemaOperation::RenameTable {
                from: from.clone(),
                to: to.clone(),
            });
        }

        // Find new tables (in expected but not current)
        for (name, table) in &expected.tables {
            if renamed_to.contains(name.as_str()) {
                continue;
            }
            if !current.tables.contains_key(name) {
                diff.add_op(SchemaOperation::CreateTable(table.clone()));
            }
        }

        // Find dropped tables (in current but not expected)
        for name in current.tables.keys() {
            if renamed_from.contains(name.as_str()) {
                continue;
            }
            if !expected.tables.contains_key(name) {
                diff.add_destructive_op(
                    SchemaOperation::DropTable(name.clone()),
                    WarningSeverity::DataLoss,
                    format!("Dropping table '{}' will delete all data", name),
                );
            }
        }

        // Compare existing tables
        for (name, expected_table) in &expected.tables {
            if let Some(current_table) = current.tables.get(name) {
                diff_table(current_table, expected_table, expected.dialect, &mut diff);
            }
        }

        // Order operations for safe execution
        diff.order_operations();

        diff
    }
}

/// Compare two tables.
fn diff_table(current: &TableInfo, expected: &TableInfo, dialect: Dialect, diff: &mut SchemaDiff) {
    let table = &current.name;

    // Diff columns
    diff_columns(current, expected, dialect, diff);

    // Diff primary key
    diff_primary_key(current, &expected.primary_key, diff);

    // Diff foreign keys
    diff_foreign_keys(current, &expected.foreign_keys, diff);

    // Diff unique constraints
    diff_unique_constraints(current, &expected.unique_constraints, diff);

    // Diff indexes
    diff_indexes(table, &current.indexes, &expected.indexes, diff);
}

/// Compare columns between tables.
fn diff_columns(
    current_table: &TableInfo,
    expected_table: &TableInfo,
    dialect: Dialect,
    diff: &mut SchemaDiff,
) {
    let table = current_table.name.as_str();
    let current = current_table.columns.as_slice();
    let expected = expected_table.columns.as_slice();
    let current_map: HashMap<&str, &ColumnInfo> =
        current.iter().map(|c| (c.name.as_str(), c)).collect();
    let expected_map: HashMap<&str, &ColumnInfo> =
        expected.iter().map(|c| (c.name.as_str(), c)).collect();

    // Detect column renames (identical definition, different name) within the table.
    let removed: Vec<&ColumnInfo> = current
        .iter()
        .filter(|c| !expected_map.contains_key(c.name.as_str()))
        .collect();
    let added: Vec<&ColumnInfo> = expected
        .iter()
        .filter(|c| !current_map.contains_key(c.name.as_str()))
        .collect();

    let col_renames = detect_column_renames(&removed, &added, dialect);
    let mut renamed_from: HashSet<&str> = HashSet::new();
    let mut renamed_to: HashSet<&str> = HashSet::new();
    for (from, to) in &col_renames {
        renamed_from.insert(from.as_str());
        renamed_to.insert(to.as_str());
        diff.add_op(SchemaOperation::RenameColumn {
            table: table.to_string(),
            from: from.clone(),
            to: to.clone(),
        });
    }

    // New columns
    for (name, col) in &expected_map {
        if renamed_to.contains(*name) {
            continue;
        }
        if !current_map.contains_key(name) {
            diff.add_op(SchemaOperation::AddColumn {
                table: table.to_string(),
                column: (*col).clone(),
            });
        }
    }

    // Dropped columns
    for name in current_map.keys() {
        if renamed_from.contains(*name) {
            continue;
        }
        if !expected_map.contains_key(name) {
            diff.add_destructive_op(
                SchemaOperation::DropColumn {
                    table: table.to_string(),
                    column: (*name).to_string(),
                    table_info: Some(current_table.clone()),
                },
                WarningSeverity::DataLoss,
                format!("Dropping column '{}.{}' will delete data", table, name),
            );
        }
    }

    // Changed columns
    for (name, expected_col) in &expected_map {
        if let Some(current_col) = current_map.get(name) {
            diff_column_details(current_table, current_col, expected_col, dialect, diff);
        }
    }
}

/// Compare column details.
fn diff_column_details(
    current_table: &TableInfo,
    current: &ColumnInfo,
    expected: &ColumnInfo,
    dialect: Dialect,
    diff: &mut SchemaDiff,
) {
    let table = current_table.name.as_str();
    let col = &current.name;

    // Type change (normalize for comparison)
    let current_type = normalize_type(&current.sql_type, dialect);
    let expected_type = normalize_type(&expected.sql_type, dialect);

    if current_type != expected_type {
        diff.add_destructive_op(
            SchemaOperation::AlterColumnType {
                table: table.to_string(),
                column: col.clone(),
                from_type: current.sql_type.clone(),
                to_type: expected.sql_type.clone(),
                table_info: Some(current_table.clone()),
            },
            WarningSeverity::Warning,
            format!(
                "Changing type of '{}.{}' from {} to {} may cause data conversion issues",
                table, col, current.sql_type, expected.sql_type
            ),
        );
    }

    // Nullable change
    if current.nullable != expected.nullable {
        let op_index = diff.add_op(SchemaOperation::AlterColumnNullable {
            table: table.to_string(),
            column: (*expected).clone(),
            from_nullable: current.nullable,
            to_nullable: expected.nullable,
            table_info: Some(current_table.clone()),
        });

        if !expected.nullable {
            diff.warn(
                WarningSeverity::Warning,
                format!(
                    "Making '{}.{}' NOT NULL may fail if column contains NULL values",
                    table, col
                ),
                Some(op_index),
            );
        }
    }

    // Default change
    if current.default != expected.default {
        diff.add_op(SchemaOperation::AlterColumnDefault {
            table: table.to_string(),
            column: col.clone(),
            from_default: current.default.clone(),
            to_default: expected.default.clone(),
            table_info: Some(current_table.clone()),
        });
    }
}

/// Compare primary keys.
fn diff_primary_key(current_table: &TableInfo, expected_pk: &[String], diff: &mut SchemaDiff) {
    let table = current_table.name.as_str();
    let current = current_table.primary_key.as_slice();
    let expected = expected_pk;
    let current_set: HashSet<&str> = current.iter().map(|s| s.as_str()).collect();
    let expected_set: HashSet<&str> = expected.iter().map(|s| s.as_str()).collect();

    if current_set != expected_set {
        // If current has a PK, drop it first
        if !current.is_empty() {
            diff.add_op(SchemaOperation::DropPrimaryKey {
                table: table.to_string(),
                table_info: Some(current_table.clone()),
            });
        }

        // Add the new PK if expected has one
        if !expected.is_empty() {
            diff.add_op(SchemaOperation::AddPrimaryKey {
                table: table.to_string(),
                columns: expected.to_vec(),
                table_info: Some(current_table.clone()),
            });
        }
    }
}

/// Compare foreign keys.
fn diff_foreign_keys(
    current_table: &TableInfo,
    expected: &[ForeignKeyInfo],
    diff: &mut SchemaDiff,
) {
    let table = current_table.name.as_str();
    let current = current_table.foreign_keys.as_slice();
    // Build maps by column (since names may differ or be auto-generated)
    let current_map: HashMap<&str, &ForeignKeyInfo> =
        current.iter().map(|fk| (fk.column.as_str(), fk)).collect();
    let expected_map: HashMap<&str, &ForeignKeyInfo> =
        expected.iter().map(|fk| (fk.column.as_str(), fk)).collect();

    // New foreign keys
    for (col, fk) in &expected_map {
        if !current_map.contains_key(col) {
            diff.add_op(SchemaOperation::AddForeignKey {
                table: table.to_string(),
                fk: (*fk).clone(),
                table_info: Some(current_table.clone()),
            });
        }
    }

    // Dropped foreign keys
    for (col, fk) in &current_map {
        if !expected_map.contains_key(col) {
            let name = fk_effective_name(table, fk);
            diff.add_op(SchemaOperation::DropForeignKey {
                table: table.to_string(),
                name,
                table_info: Some(current_table.clone()),
            });
        }
    }

    // Changed foreign keys (compare references)
    for (col, expected_fk) in &expected_map {
        if let Some(current_fk) = current_map.get(col) {
            if !fk_matches(current_fk, expected_fk) {
                // Drop and recreate
                let name = fk_effective_name(table, current_fk);
                diff.add_op(SchemaOperation::DropForeignKey {
                    table: table.to_string(),
                    name,
                    table_info: Some(current_table.clone()),
                });
                diff.add_op(SchemaOperation::AddForeignKey {
                    table: table.to_string(),
                    fk: (*expected_fk).clone(),
                    table_info: Some(current_table.clone()),
                });
            }
        }
    }
}

/// Check if two foreign keys match.
fn fk_matches(current: &ForeignKeyInfo, expected: &ForeignKeyInfo) -> bool {
    current.foreign_table == expected.foreign_table
        && current.foreign_column == expected.foreign_column
        && current.on_delete == expected.on_delete
        && current.on_update == expected.on_update
}

/// Compare unique constraints.
fn diff_unique_constraints(
    current_table: &TableInfo,
    expected: &[UniqueConstraintInfo],
    diff: &mut SchemaDiff,
) {
    let table = current_table.name.as_str();
    let current = current_table.unique_constraints.as_slice();
    // Build sets of column combinations
    let current_set: HashSet<Vec<&str>> = current
        .iter()
        .map(|u| u.columns.iter().map(|s| s.as_str()).collect())
        .collect();
    let expected_set: HashSet<Vec<&str>> = expected
        .iter()
        .map(|u| u.columns.iter().map(|s| s.as_str()).collect())
        .collect();

    // Find constraints to add
    for constraint in expected {
        let cols: Vec<&str> = constraint.columns.iter().map(|s| s.as_str()).collect();
        if !current_set.contains(&cols) {
            diff.add_op(SchemaOperation::AddUnique {
                table: table.to_string(),
                constraint: constraint.clone(),
                table_info: Some(current_table.clone()),
            });
        }
    }

    // Find constraints to drop
    for constraint in current {
        let cols: Vec<&str> = constraint.columns.iter().map(|s| s.as_str()).collect();
        if !expected_set.contains(&cols) {
            let name = unique_effective_name(table, constraint);
            diff.add_op(SchemaOperation::DropUnique {
                table: table.to_string(),
                name,
                table_info: Some(current_table.clone()),
            });
        }
    }
}

/// Compare indexes.
fn diff_indexes(table: &str, current: &[IndexInfo], expected: &[IndexInfo], diff: &mut SchemaDiff) {
    // Skip primary key indexes as they're handled separately
    let current_filtered: Vec<_> = current.iter().filter(|i| !i.primary).collect();
    let expected_filtered: Vec<_> = expected.iter().filter(|i| !i.primary).collect();

    // Build maps by name
    let current_map: HashMap<&str, &&IndexInfo> = current_filtered
        .iter()
        .map(|i| (i.name.as_str(), i))
        .collect();
    let expected_map: HashMap<&str, &&IndexInfo> = expected_filtered
        .iter()
        .map(|i| (i.name.as_str(), i))
        .collect();

    // New indexes
    for (name, index) in &expected_map {
        if !current_map.contains_key(name) {
            diff.add_op(SchemaOperation::CreateIndex {
                table: table.to_string(),
                index: (**index).clone(),
            });
        }
    }

    // Dropped indexes
    for name in current_map.keys() {
        if !expected_map.contains_key(name) {
            diff.add_op(SchemaOperation::DropIndex {
                table: table.to_string(),
                name: (*name).to_string(),
            });
        }
    }

    // Changed indexes (check columns and unique flag)
    for (name, expected_idx) in &expected_map {
        if let Some(current_idx) = current_map.get(name) {
            if current_idx.columns != expected_idx.columns
                || current_idx.unique != expected_idx.unique
            {
                // Drop and recreate
                diff.add_op(SchemaOperation::DropIndex {
                    table: table.to_string(),
                    name: (*name).to_string(),
                });
                diff.add_op(SchemaOperation::CreateIndex {
                    table: table.to_string(),
                    index: (**expected_idx).clone(),
                });
            }
        }
    }
}

// ============================================================================
// Rename Detection (Best-Effort)
// ============================================================================

fn column_signature(col: &ColumnInfo, dialect: Dialect) -> String {
    let ty = normalize_type(&col.sql_type, dialect);
    let default = col.default.as_deref().unwrap_or("");
    format!(
        "type={};nullable={};default={};pk={};ai={}",
        ty, col.nullable, default, col.primary_key, col.auto_increment
    )
}

fn detect_column_renames(
    removed: &[&ColumnInfo],
    added: &[&ColumnInfo],
    dialect: Dialect,
) -> Vec<(String, String)> {
    let mut removed_by_sig: HashMap<String, Vec<&ColumnInfo>> = HashMap::new();
    let mut added_by_sig: HashMap<String, Vec<&ColumnInfo>> = HashMap::new();

    for col in removed {
        removed_by_sig
            .entry(column_signature(col, dialect))
            .or_default()
            .push(*col);
    }
    for col in added {
        added_by_sig
            .entry(column_signature(col, dialect))
            .or_default()
            .push(*col);
    }

    let mut renames = Vec::new();
    for (sig, removed_cols) in removed_by_sig {
        if removed_cols.len() != 1 {
            continue;
        }
        let Some(added_cols) = added_by_sig.get(&sig) else {
            continue;
        };
        if added_cols.len() != 1 {
            continue;
        }
        renames.push((removed_cols[0].name.clone(), added_cols[0].name.clone()));
    }

    renames.sort_by(|a, b| a.0.cmp(&b.0));
    renames
}

fn table_signature(table: &TableInfo, dialect: Dialect) -> String {
    let mut parts = Vec::new();

    let mut cols: Vec<String> = table
        .columns
        .iter()
        .map(|c| {
            let ty = normalize_type(&c.sql_type, dialect);
            let default = c.default.as_deref().unwrap_or("");
            format!(
                "{}:{}:{}:{}:{}:{}",
                c.name, ty, c.nullable, default, c.primary_key, c.auto_increment
            )
        })
        .collect();
    cols.sort();
    parts.push(format!("cols={}", cols.join(",")));

    let mut pk = table.primary_key.clone();
    pk.sort();
    parts.push(format!("pk={}", pk.join(",")));

    let mut fks: Vec<String> = table
        .foreign_keys
        .iter()
        .map(|fk| {
            let on_delete = fk.on_delete.as_deref().unwrap_or("");
            let on_update = fk.on_update.as_deref().unwrap_or("");
            format!(
                "{}->{}.{}:{}:{}",
                fk.column, fk.foreign_table, fk.foreign_column, on_delete, on_update
            )
        })
        .collect();
    fks.sort();
    parts.push(format!("fks={}", fks.join("|")));

    let mut uniques: Vec<String> = table
        .unique_constraints
        .iter()
        .map(|u| {
            let mut cols = u.columns.clone();
            cols.sort();
            cols.join(",")
        })
        .collect();
    uniques.sort();
    parts.push(format!("uniques={}", uniques.join("|")));

    let mut checks: Vec<String> = table
        .check_constraints
        .iter()
        .map(|c| c.expression.trim().to_string())
        .collect();
    checks.sort();
    parts.push(format!("checks={}", checks.join("|")));

    let mut indexes: Vec<String> = table
        .indexes
        .iter()
        .map(|i| {
            let ty = i.index_type.as_deref().unwrap_or("");
            format!("{}:{}:{}:{}", i.columns.join(","), i.unique, i.primary, ty)
        })
        .collect();
    indexes.sort();
    parts.push(format!("indexes={}", indexes.join("|")));

    parts.join(";")
}

fn detect_table_renames(
    current: &DatabaseSchema,
    expected: &DatabaseSchema,
    dialect: Dialect,
) -> Vec<(String, String)> {
    let current_only: Vec<&TableInfo> = current
        .tables
        .values()
        .filter(|t| !expected.tables.contains_key(&t.name))
        .collect();
    let expected_only: Vec<&TableInfo> = expected
        .tables
        .values()
        .filter(|t| !current.tables.contains_key(&t.name))
        .collect();

    let mut current_by_sig: HashMap<String, Vec<&TableInfo>> = HashMap::new();
    let mut expected_by_sig: HashMap<String, Vec<&TableInfo>> = HashMap::new();

    for table in current_only {
        current_by_sig
            .entry(table_signature(table, dialect))
            .or_default()
            .push(table);
    }
    for table in expected_only {
        expected_by_sig
            .entry(table_signature(table, dialect))
            .or_default()
            .push(table);
    }

    let mut renames = Vec::new();
    for (sig, current_tables) in current_by_sig {
        if current_tables.len() != 1 {
            continue;
        }
        let Some(expected_tables) = expected_by_sig.get(&sig) else {
            continue;
        };
        if expected_tables.len() != 1 {
            continue;
        }

        renames.push((
            current_tables[0].name.clone(),
            expected_tables[0].name.clone(),
        ));
    }

    renames.sort_by(|a, b| a.0.cmp(&b.0));
    renames
}

// ============================================================================
// Type Normalization
// ============================================================================

/// Normalize a SQL type for comparison.
fn normalize_type(sql_type: &str, dialect: Dialect) -> String {
    let upper = sql_type.to_uppercase();

    match dialect {
        Dialect::Sqlite => {
            // SQLite type affinity
            if upper.contains("INT") {
                "INTEGER".to_string()
            } else if upper.contains("CHAR") || upper.contains("TEXT") || upper.contains("CLOB") {
                "TEXT".to_string()
            } else if upper.contains("REAL") || upper.contains("FLOAT") || upper.contains("DOUB") {
                "REAL".to_string()
            } else if upper.contains("BLOB") || upper.is_empty() {
                "BLOB".to_string()
            } else {
                upper
            }
        }
        Dialect::Postgres => match upper.as_str() {
            "INT" | "INT4" => "INTEGER".to_string(),
            "INT8" => "BIGINT".to_string(),
            "INT2" => "SMALLINT".to_string(),
            "FLOAT4" => "REAL".to_string(),
            "FLOAT8" => "DOUBLE PRECISION".to_string(),
            "BOOL" => "BOOLEAN".to_string(),
            "SERIAL" => "INTEGER".to_string(),
            "BIGSERIAL" => "BIGINT".to_string(),
            "SMALLSERIAL" => "SMALLINT".to_string(),
            _ => upper,
        },
        Dialect::Mysql => match upper.as_str() {
            "INTEGER" => "INT".to_string(),
            "BOOL" | "BOOLEAN" => "TINYINT".to_string(),
            _ => upper,
        },
    }
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::introspect::ParsedSqlType;

    fn make_column(name: &str, sql_type: &str, nullable: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            sql_type: sql_type.to_string(),
            parsed_type: ParsedSqlType::parse(sql_type),
            nullable,
            default: None,
            primary_key: false,
            auto_increment: false,
            comment: None,
        }
    }

    fn make_table(name: &str, columns: Vec<ColumnInfo>) -> TableInfo {
        TableInfo {
            name: name.to_string(),
            columns,
            primary_key: Vec::new(),
            foreign_keys: Vec::new(),
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            indexes: Vec::new(),
            comment: None,
        }
    }

    #[test]
    fn test_schema_diff_new_table() {
        let current = DatabaseSchema::new(Dialect::Sqlite);
        let mut expected = DatabaseSchema::new(Dialect::Sqlite);
        expected.tables.insert(
            "heroes".to_string(),
            make_table("heroes", vec![make_column("id", "INTEGER", false)]),
        );

        let diff = schema_diff(&current, &expected);
        assert_eq!(diff.len(), 1);
        assert!(
            matches!(&diff.operations[0], SchemaOperation::CreateTable(t) if t.name == "heroes")
        );
    }

    #[test]
    fn test_schema_diff_rename_table() {
        let mut current = DatabaseSchema::new(Dialect::Sqlite);
        current.tables.insert(
            "heroes_old".to_string(),
            make_table("heroes_old", vec![make_column("id", "INTEGER", false)]),
        );

        let mut expected = DatabaseSchema::new(Dialect::Sqlite);
        expected.tables.insert(
            "heroes".to_string(),
            make_table("heroes", vec![make_column("id", "INTEGER", false)]),
        );

        let diff = schema_diff(&current, &expected);
        assert!(diff.operations.iter().any(|op| {
            matches!(op, SchemaOperation::RenameTable { from, to } if from == "heroes_old" && to == "heroes")
        }));
        assert!(!diff.operations.iter().any(|op| matches!(
            op,
            SchemaOperation::CreateTable(_) | SchemaOperation::DropTable(_)
        )));
    }

    #[test]
    fn test_schema_diff_drop_table() {
        let mut current = DatabaseSchema::new(Dialect::Sqlite);
        current.tables.insert(
            "heroes".to_string(),
            make_table("heroes", vec![make_column("id", "INTEGER", false)]),
        );
        let expected = DatabaseSchema::new(Dialect::Sqlite);

        let diff = schema_diff(&current, &expected);
        assert_eq!(diff.len(), 1);
        assert!(
            matches!(&diff.operations[0], SchemaOperation::DropTable(name) if name == "heroes")
        );
        assert!(diff.has_destructive());
        assert!(diff.requires_confirmation());
        assert_eq!(diff.warnings.len(), 1);
        assert_eq!(diff.warnings[0].severity, WarningSeverity::DataLoss);
    }

    #[test]
    fn test_schema_diff_drop_table_allow_policy() {
        let mut current = DatabaseSchema::new(Dialect::Sqlite);
        current.tables.insert(
            "heroes".to_string(),
            make_table("heroes", vec![make_column("id", "INTEGER", false)]),
        );
        let expected = DatabaseSchema::new(Dialect::Sqlite);

        let diff = schema_diff_with_policy(&current, &expected, DestructivePolicy::Allow);
        assert_eq!(diff.len(), 1);
        assert!(diff.has_destructive());
        assert!(!diff.requires_confirmation());
        assert!(diff.warnings.is_empty());
    }

    #[test]
    fn test_schema_diff_drop_table_skip_policy() {
        let mut current = DatabaseSchema::new(Dialect::Sqlite);
        current.tables.insert(
            "heroes".to_string(),
            make_table("heroes", vec![make_column("id", "INTEGER", false)]),
        );
        let expected = DatabaseSchema::new(Dialect::Sqlite);

        let diff = schema_diff_with_policy(&current, &expected, DestructivePolicy::Skip);
        assert!(diff.operations.is_empty());
        assert!(!diff.has_destructive());
        assert!(!diff.requires_confirmation());
        assert!(
            diff.warnings
                .iter()
                .any(|w| w.message.contains("Skipped destructive operation"))
        );
    }

    #[test]
    fn test_schema_diff_add_column() {
        let mut current = DatabaseSchema::new(Dialect::Sqlite);
        current.tables.insert(
            "heroes".to_string(),
            make_table("heroes", vec![make_column("id", "INTEGER", false)]),
        );

        let mut expected = DatabaseSchema::new(Dialect::Sqlite);
        expected.tables.insert(
            "heroes".to_string(),
            make_table(
                "heroes",
                vec![
                    make_column("id", "INTEGER", false),
                    make_column("name", "TEXT", false),
                ],
            ),
        );

        let diff = schema_diff(&current, &expected);
        assert!(diff
            .operations
            .iter()
            .any(|op| matches!(op, SchemaOperation::AddColumn { table, column } if table == "heroes" && column.name == "name")));
    }

    #[test]
    fn test_schema_diff_drop_column() {
        let mut current = DatabaseSchema::new(Dialect::Sqlite);
        current.tables.insert(
            "heroes".to_string(),
            make_table(
                "heroes",
                vec![
                    make_column("id", "INTEGER", false),
                    make_column("old_field", "TEXT", true),
                ],
            ),
        );

        let mut expected = DatabaseSchema::new(Dialect::Sqlite);
        expected.tables.insert(
            "heroes".to_string(),
            make_table("heroes", vec![make_column("id", "INTEGER", false)]),
        );

        let diff = schema_diff(&current, &expected);
        assert!(diff.has_destructive());
        assert!(diff.operations.iter().any(
            |op| matches!(op, SchemaOperation::DropColumn { table, column, table_info: Some(_), .. } if table == "heroes" && column == "old_field")
        ));
    }

    #[test]
    fn test_schema_diff_rename_column() {
        let mut current = DatabaseSchema::new(Dialect::Sqlite);
        current.tables.insert(
            "heroes".to_string(),
            make_table("heroes", vec![make_column("old_name", "TEXT", false)]),
        );

        let mut expected = DatabaseSchema::new(Dialect::Sqlite);
        expected.tables.insert(
            "heroes".to_string(),
            make_table("heroes", vec![make_column("name", "TEXT", false)]),
        );

        let diff = schema_diff(&current, &expected);
        assert!(diff.operations.iter().any(|op| {
            matches!(op, SchemaOperation::RenameColumn { table, from, to } if table == "heroes" && from == "old_name" && to == "name")
        }));
        assert!(!diff.operations.iter().any(|op| matches!(
            op,
            SchemaOperation::AddColumn { .. } | SchemaOperation::DropColumn { .. }
        )));
        assert!(!diff.has_destructive());
    }

    #[test]
    fn test_schema_diff_alter_column_type() {
        let mut current = DatabaseSchema::new(Dialect::Sqlite);
        current.tables.insert(
            "heroes".to_string(),
            make_table("heroes", vec![make_column("age", "INTEGER", false)]),
        );

        let mut expected = DatabaseSchema::new(Dialect::Sqlite);
        expected.tables.insert(
            "heroes".to_string(),
            make_table("heroes", vec![make_column("age", "REAL", false)]),
        );

        let diff = schema_diff(&current, &expected);
        assert!(diff.operations.iter().any(
            |op| matches!(op, SchemaOperation::AlterColumnType { table, column, .. } if table == "heroes" && column == "age")
        ));
    }

    #[test]
    fn test_schema_diff_alter_nullable() {
        let mut current = DatabaseSchema::new(Dialect::Sqlite);
        current.tables.insert(
            "heroes".to_string(),
            make_table("heroes", vec![make_column("name", "TEXT", true)]),
        );

        let mut expected = DatabaseSchema::new(Dialect::Sqlite);
        expected.tables.insert(
            "heroes".to_string(),
            make_table("heroes", vec![make_column("name", "TEXT", false)]),
        );

        let diff = schema_diff(&current, &expected);
        assert!(diff.operations.iter().any(
            |op| matches!(op, SchemaOperation::AlterColumnNullable { table, column, to_nullable: false, .. } if table == "heroes" && column.name == "name")
        ));
    }

    #[test]
    fn test_schema_diff_empty() {
        let mut current = DatabaseSchema::new(Dialect::Sqlite);
        current.tables.insert(
            "heroes".to_string(),
            make_table("heroes", vec![make_column("id", "INTEGER", false)]),
        );

        let expected = current.clone();

        let diff = schema_diff(&current, &expected);
        assert!(diff.is_empty());
    }

    #[test]
    fn test_schema_diff_foreign_key_add() {
        let mut current = DatabaseSchema::new(Dialect::Sqlite);
        current.tables.insert(
            "heroes".to_string(),
            make_table("heroes", vec![make_column("team_id", "INTEGER", true)]),
        );

        let mut expected = DatabaseSchema::new(Dialect::Sqlite);
        let mut heroes = make_table("heroes", vec![make_column("team_id", "INTEGER", true)]);
        heroes.foreign_keys.push(ForeignKeyInfo {
            name: Some("fk_heroes_team".to_string()),
            column: "team_id".to_string(),
            foreign_table: "teams".to_string(),
            foreign_column: "id".to_string(),
            on_delete: Some("CASCADE".to_string()),
            on_update: None,
        });
        expected.tables.insert("heroes".to_string(), heroes);

        let diff = schema_diff(&current, &expected);
        let op = diff.operations.iter().find_map(|op| match op {
            SchemaOperation::AddForeignKey {
                table,
                fk,
                table_info,
            } if table == "heroes" && fk.column == "team_id" => Some(table_info),
            _ => None,
        });
        assert!(op.is_some(), "Expected AddForeignKey op for heroes.team_id");
        assert!(
            op.unwrap().is_some(),
            "Expected table_info on AddForeignKey op"
        );
    }

    #[test]
    fn test_schema_diff_primary_key_add_attaches_table_info() {
        let mut current = DatabaseSchema::new(Dialect::Sqlite);
        let mut current_table = make_table("heroes", vec![make_column("id", "INTEGER", false)]);
        current_table.primary_key.clear();
        current.tables.insert("heroes".to_string(), current_table);

        let mut expected = DatabaseSchema::new(Dialect::Sqlite);
        let mut expected_table = make_table("heroes", vec![make_column("id", "INTEGER", false)]);
        expected_table.primary_key = vec!["id".to_string()];
        expected.tables.insert("heroes".to_string(), expected_table);

        let diff = schema_diff(&current, &expected);
        let op = diff.operations.iter().find_map(|op| match op {
            SchemaOperation::AddPrimaryKey {
                table,
                columns,
                table_info,
            } if table == "heroes" && columns == &vec!["id".to_string()] => Some(table_info),
            _ => None,
        });
        assert!(op.is_some(), "Expected AddPrimaryKey op for heroes(id)");
        assert!(
            op.unwrap().is_some(),
            "Expected table_info on AddPrimaryKey op"
        );
    }

    #[test]
    fn test_schema_diff_unique_add_attaches_table_info() {
        let mut current = DatabaseSchema::new(Dialect::Sqlite);
        current.tables.insert(
            "heroes".to_string(),
            make_table("heroes", vec![make_column("name", "TEXT", false)]),
        );

        let mut expected = DatabaseSchema::new(Dialect::Sqlite);
        let mut expected_table = make_table("heroes", vec![make_column("name", "TEXT", false)]);
        expected_table
            .unique_constraints
            .push(UniqueConstraintInfo {
                name: Some("uk_heroes_name".to_string()),
                columns: vec!["name".to_string()],
            });
        expected.tables.insert("heroes".to_string(), expected_table);

        let diff = schema_diff(&current, &expected);
        let op = diff.operations.iter().find_map(|op| match op {
            SchemaOperation::AddUnique {
                table,
                constraint,
                table_info,
            } if table == "heroes" && constraint.columns == vec!["name".to_string()] => {
                Some(table_info)
            }
            _ => None,
        });
        assert!(op.is_some(), "Expected AddUnique op for heroes(name)");
        assert!(op.unwrap().is_some(), "Expected table_info on AddUnique op");
    }

    #[test]
    fn test_schema_diff_index_add() {
        let mut current = DatabaseSchema::new(Dialect::Sqlite);
        current.tables.insert(
            "heroes".to_string(),
            make_table("heroes", vec![make_column("name", "TEXT", false)]),
        );

        let mut expected = DatabaseSchema::new(Dialect::Sqlite);
        let mut heroes = make_table("heroes", vec![make_column("name", "TEXT", false)]);
        heroes.indexes.push(IndexInfo {
            name: "idx_heroes_name".to_string(),
            columns: vec!["name".to_string()],
            unique: false,
            index_type: None,
            primary: false,
        });
        expected.tables.insert("heroes".to_string(), heroes);

        let diff = schema_diff(&current, &expected);
        assert!(diff.operations.iter().any(
            |op| matches!(op, SchemaOperation::CreateIndex { table, index } if table == "heroes" && index.name == "idx_heroes_name")
        ));
    }

    #[test]
    fn test_operation_ordering() {
        let mut diff = SchemaDiff::new(DestructivePolicy::Warn);

        // Add in wrong order
        diff.add_op(SchemaOperation::AddForeignKey {
            table: "heroes".to_string(),
            fk: ForeignKeyInfo {
                name: None,
                column: "team_id".to_string(),
                foreign_table: "teams".to_string(),
                foreign_column: "id".to_string(),
                on_delete: None,
                on_update: None,
            },
            table_info: None,
        });
        diff.add_op(SchemaOperation::DropForeignKey {
            table: "old".to_string(),
            name: "fk_old".to_string(),
            table_info: None,
        });
        diff.add_op(SchemaOperation::AddColumn {
            table: "heroes".to_string(),
            column: make_column("age", "INTEGER", true),
        });

        diff.order_operations();

        // DropForeignKey should come first
        assert!(matches!(
            &diff.operations[0],
            SchemaOperation::DropForeignKey { .. }
        ));
        // AddColumn should come before AddForeignKey
        assert!(matches!(
            &diff.operations[1],
            SchemaOperation::AddColumn { .. }
        ));
        assert!(matches!(
            &diff.operations[2],
            SchemaOperation::AddForeignKey { .. }
        ));
    }

    #[test]
    fn test_type_normalization_sqlite() {
        assert_eq!(normalize_type("INT", Dialect::Sqlite), "INTEGER");
        assert_eq!(normalize_type("BIGINT", Dialect::Sqlite), "INTEGER");
        assert_eq!(normalize_type("VARCHAR(100)", Dialect::Sqlite), "TEXT");
        assert_eq!(normalize_type("FLOAT", Dialect::Sqlite), "REAL");
    }

    #[test]
    fn test_type_normalization_postgres() {
        assert_eq!(normalize_type("INT", Dialect::Postgres), "INTEGER");
        assert_eq!(normalize_type("INT4", Dialect::Postgres), "INTEGER");
        assert_eq!(normalize_type("INT8", Dialect::Postgres), "BIGINT");
        assert_eq!(normalize_type("SERIAL", Dialect::Postgres), "INTEGER");
    }

    #[test]
    fn test_type_normalization_mysql() {
        assert_eq!(normalize_type("INTEGER", Dialect::Mysql), "INT");
        assert_eq!(normalize_type("BOOLEAN", Dialect::Mysql), "TINYINT");
    }

    #[test]
    fn test_schema_operation_is_destructive() {
        assert!(SchemaOperation::DropTable("heroes".to_string()).is_destructive());
        assert!(
            SchemaOperation::DropColumn {
                table: "heroes".to_string(),
                column: "age".to_string(),
                table_info: None,
            }
            .is_destructive()
        );
        assert!(
            SchemaOperation::AlterColumnType {
                table: "heroes".to_string(),
                column: "age".to_string(),
                from_type: "TEXT".to_string(),
                to_type: "INTEGER".to_string(),
                table_info: None,
            }
            .is_destructive()
        );
        assert!(
            !SchemaOperation::AddColumn {
                table: "heroes".to_string(),
                column: make_column("name", "TEXT", false),
            }
            .is_destructive()
        );
    }

    #[test]
    fn test_schema_operation_inverse() {
        let table = make_table("heroes", vec![make_column("id", "INTEGER", false)]);
        let op = SchemaOperation::CreateTable(table);
        assert!(matches!(op.inverse(), Some(SchemaOperation::DropTable(name)) if name == "heroes"));

        let op = SchemaOperation::AlterColumnType {
            table: "heroes".to_string(),
            column: "age".to_string(),
            from_type: "TEXT".to_string(),
            to_type: "INTEGER".to_string(),
            table_info: None,
        };
        assert!(
            matches!(op.inverse(), Some(SchemaOperation::AlterColumnType { from_type, to_type, .. }) if from_type == "INTEGER" && to_type == "TEXT")
        );
    }
}
