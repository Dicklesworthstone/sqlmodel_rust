//! SQLite DDL generator.
//!
//! SQLite has limited ALTER TABLE support, requiring table recreation for some operations.

use super::{
    DdlGenerator, generate_add_column, generate_create_index, generate_create_table,
    generate_drop_index, generate_drop_table, generate_rename_column, generate_rename_table,
    quote_identifier,
};
use crate::diff::SchemaOperation;
use crate::introspect::{Dialect, TableInfo};

/// DDL generator for SQLite.
pub struct SqliteDdlGenerator;

impl DdlGenerator for SqliteDdlGenerator {
    fn dialect(&self) -> &'static str {
        "sqlite"
    }

    fn generate(&self, op: &SchemaOperation) -> Vec<String> {
        tracing::debug!(dialect = "sqlite", op = ?op, "Generating DDL");

        let statements = match op {
            // Tables
            SchemaOperation::CreateTable(table) => {
                vec![generate_create_table(table, Dialect::Sqlite)]
            }
            SchemaOperation::DropTable(name) => {
                vec![generate_drop_table(name, Dialect::Sqlite)]
            }
            SchemaOperation::RenameTable { from, to } => {
                vec![generate_rename_table(from, to, Dialect::Sqlite)]
            }

            // Columns
            SchemaOperation::AddColumn { table, column } => {
                vec![generate_add_column(table, column, Dialect::Sqlite)]
            }
            SchemaOperation::DropColumn {
                table,
                column,
                table_info,
            } => {
                if let Some(table_info) = table_info {
                    sqlite_drop_column_recreate(table_info, column)
                } else {
                    vec![
                        "-- SQLite: DROP COLUMN without table_info; using ALTER TABLE DROP COLUMN (requires SQLite >= 3.35.0)".to_string(),
                        format!(
                            "ALTER TABLE {} DROP COLUMN {}",
                            quote_identifier(table, Dialect::Sqlite),
                            quote_identifier(column, Dialect::Sqlite)
                        ),
                    ]
                }
            }
            SchemaOperation::AlterColumnType {
                table,
                column,
                to_type,
                ..
            } => {
                // SQLite doesn't support ALTER COLUMN TYPE
                // This requires table recreation
                tracing::warn!(
                    table = %table,
                    column = %column,
                    to_type = %to_type,
                    "SQLite does not support ALTER COLUMN TYPE - requires table recreation"
                );
                vec![format!(
                    "-- SQLite: Cannot change column type directly. Requires table recreation.\n\
                     -- Changing {}.{} to type {}",
                    table, column, to_type
                )]
            }
            SchemaOperation::AlterColumnNullable {
                table,
                column,
                to_nullable,
                ..
            } => {
                // SQLite doesn't support altering nullability
                tracing::warn!(
                    table = %table,
                    column = %column.name,
                    to_nullable = %to_nullable,
                    "SQLite does not support ALTER COLUMN nullability - requires table recreation"
                );
                let action = if *to_nullable {
                    "allow NULL"
                } else {
                    "NOT NULL"
                };
                vec![format!(
                    "-- SQLite: Cannot change column nullability directly. Requires table recreation.\n\
                     -- Setting {}.{} to {}",
                    table, column.name, action
                )]
            }
            SchemaOperation::AlterColumnDefault {
                table,
                column,
                to_default,
                ..
            } => {
                // SQLite doesn't support altering defaults
                tracing::warn!(
                    table = %table,
                    column = %column,
                    "SQLite does not support ALTER COLUMN DEFAULT - requires table recreation"
                );
                let default_str = to_default.as_deref().unwrap_or("NULL");
                vec![format!(
                    "-- SQLite: Cannot change column default directly. Requires table recreation.\n\
                     -- Setting {}.{} DEFAULT to {}",
                    table, column, default_str
                )]
            }
            SchemaOperation::RenameColumn { table, from, to } => {
                vec![generate_rename_column(table, from, to, Dialect::Sqlite)]
            }

            // Primary Keys
            SchemaOperation::AddPrimaryKey { table, columns } => {
                // SQLite doesn't support adding PK to existing table
                tracing::warn!(
                    table = %table,
                    columns = ?columns,
                    "SQLite does not support adding PRIMARY KEY to existing table"
                );
                vec![format!(
                    "-- SQLite: Cannot add PRIMARY KEY to existing table. Requires table recreation.\n\
                     -- Table: {}, Columns: {}",
                    table,
                    columns.join(", ")
                )]
            }
            SchemaOperation::DropPrimaryKey { table } => {
                tracing::warn!(
                    table = %table,
                    "SQLite does not support dropping PRIMARY KEY"
                );
                vec![format!(
                    "-- SQLite: Cannot drop PRIMARY KEY. Requires table recreation.\n\
                     -- Table: {}",
                    table
                )]
            }

            // Foreign Keys
            SchemaOperation::AddForeignKey { table, fk } => {
                // SQLite doesn't support adding FK to existing table
                tracing::warn!(
                    table = %table,
                    column = %fk.column,
                    "SQLite does not support adding FOREIGN KEY to existing table"
                );
                vec![format!(
                    "-- SQLite: Cannot add FOREIGN KEY to existing table. Requires table recreation.\n\
                     -- Table: {}, Column: {} -> {}.{}",
                    table, fk.column, fk.foreign_table, fk.foreign_column
                )]
            }
            SchemaOperation::DropForeignKey { table, name } => {
                tracing::warn!(
                    table = %table,
                    name = %name,
                    "SQLite does not support dropping FOREIGN KEY"
                );
                vec![format!(
                    "-- SQLite: Cannot drop FOREIGN KEY. Requires table recreation.\n\
                     -- Table: {}, Constraint: {}",
                    table, name
                )]
            }

            // Unique Constraints
            SchemaOperation::AddUnique { table, constraint } => {
                // SQLite: Create a unique index instead
                let cols: Vec<String> = constraint
                    .columns
                    .iter()
                    .map(|c| quote_identifier(c, Dialect::Sqlite))
                    .collect();
                let name = constraint
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("uk_{}_{}", table, constraint.columns.join("_")));
                vec![format!(
                    "CREATE UNIQUE INDEX {} ON {}({})",
                    quote_identifier(&name, Dialect::Sqlite),
                    quote_identifier(table, Dialect::Sqlite),
                    cols.join(", ")
                )]
            }
            SchemaOperation::DropUnique { table, name } => {
                // Drop the unique index
                vec![generate_drop_index(table, name, Dialect::Sqlite)]
            }

            // Indexes
            SchemaOperation::CreateIndex { table, index } => {
                vec![generate_create_index(table, index, Dialect::Sqlite)]
            }
            SchemaOperation::DropIndex { table, name } => {
                vec![generate_drop_index(table, name, Dialect::Sqlite)]
            }
        };

        for stmt in &statements {
            tracing::trace!(sql = %stmt, "Generated SQLite DDL statement");
        }

        statements
    }
}

fn sanitize_temp_ident(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_ascii_alphanumeric() || c == '_' {
            out.push(c);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        out.push_str("tmp");
    }
    out
}

fn sqlite_drop_column_recreate(table: &TableInfo, drop_column: &str) -> Vec<String> {
    let table_name = table.name.as_str();
    let drop_column = drop_column.to_string();

    if !table.columns.iter().any(|c| c.name == drop_column) {
        return vec![format!(
            "-- SQLite: column '{}' not found on table '{}' (noop)",
            drop_column, table_name
        )];
    }

    let mut new_table = table.clone();
    new_table.columns.retain(|c| c.name != drop_column);
    new_table.primary_key.retain(|c| c != &drop_column);
    new_table.foreign_keys.retain(|fk| fk.column != drop_column);
    new_table
        .unique_constraints
        .retain(|u| !u.columns.iter().any(|c| c == &drop_column));
    new_table
        .indexes
        .retain(|idx| !idx.columns.iter().any(|c| c == &drop_column));

    if new_table.columns.is_empty() {
        return vec![format!(
            "SELECT __sqlmodel_error__('cannot drop last column {}.{}')",
            sanitize_temp_ident(table_name),
            sanitize_temp_ident(&drop_column)
        )];
    }

    let tmp_old = format!(
        "__sqlmodel_old_{}_drop_{}",
        sanitize_temp_ident(table_name),
        sanitize_temp_ident(&drop_column)
    );

    let mut stmts = Vec::new();
    stmts.push("PRAGMA foreign_keys=OFF".to_string());
    stmts.push("BEGIN".to_string());
    stmts.push(generate_rename_table(table_name, &tmp_old, Dialect::Sqlite));
    stmts.push(super::generate_create_table_with_if_not_exists(
        &new_table,
        Dialect::Sqlite,
        false,
    ));

    let cols: Vec<String> = new_table
        .columns
        .iter()
        .map(|c| quote_identifier(&c.name, Dialect::Sqlite))
        .collect();
    let cols_joined = cols.join(", ");

    stmts.push(format!(
        "INSERT INTO {} ({}) SELECT {} FROM {}",
        quote_identifier(table_name, Dialect::Sqlite),
        cols_joined,
        cols_joined,
        quote_identifier(&tmp_old, Dialect::Sqlite)
    ));

    stmts.push(generate_drop_table(&tmp_old, Dialect::Sqlite));

    for idx in &new_table.indexes {
        if idx.primary {
            continue;
        }
        stmts.push(generate_create_index(table_name, idx, Dialect::Sqlite));
    }

    stmts.push("COMMIT".to_string());
    stmts.push("PRAGMA foreign_keys=ON".to_string());
    stmts
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::SchemaOperation;
    use crate::introspect::{
        ColumnInfo, ForeignKeyInfo, IndexInfo, ParsedSqlType, TableInfo, UniqueConstraintInfo,
    };

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

    fn make_table(name: &str, columns: Vec<ColumnInfo>, pk: Vec<&str>) -> TableInfo {
        TableInfo {
            name: name.to_string(),
            columns,
            primary_key: pk.into_iter().map(String::from).collect(),
            foreign_keys: Vec::new(),
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            indexes: Vec::new(),
            comment: None,
        }
    }

    #[test]
    fn test_create_table() {
        let ddl = SqliteDdlGenerator;
        let table = make_table(
            "heroes",
            vec![
                make_column("id", "INTEGER", false),
                make_column("name", "TEXT", false),
            ],
            vec!["id"],
        );
        let op = SchemaOperation::CreateTable(table);
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("CREATE TABLE IF NOT EXISTS"));
        assert!(stmts[0].contains("\"heroes\""));
    }

    #[test]
    fn test_drop_table() {
        let ddl = SqliteDdlGenerator;
        let op = SchemaOperation::DropTable("heroes".to_string());
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "DROP TABLE IF EXISTS \"heroes\"");
    }

    #[test]
    fn test_rename_table() {
        let ddl = SqliteDdlGenerator;
        let op = SchemaOperation::RenameTable {
            from: "old_heroes".to_string(),
            to: "heroes".to_string(),
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("ALTER TABLE"));
        assert!(stmts[0].contains("RENAME TO"));
    }

    #[test]
    fn test_add_column() {
        let ddl = SqliteDdlGenerator;
        let op = SchemaOperation::AddColumn {
            table: "heroes".to_string(),
            column: make_column("age", "INTEGER", true),
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("ALTER TABLE"));
        assert!(stmts[0].contains("ADD COLUMN"));
        assert!(stmts[0].contains("\"age\""));
    }

    #[test]
    fn test_drop_column() {
        let ddl = SqliteDdlGenerator;
        let mut table = make_table(
            "heroes",
            vec![
                make_column("id", "INTEGER", false),
                make_column("name", "TEXT", false),
                make_column("old_field", "TEXT", true),
            ],
            vec!["id"],
        );
        table.indexes = vec![
            IndexInfo {
                name: "idx_name".to_string(),
                columns: vec!["name".to_string()],
                unique: false,
                index_type: None,
                primary: false,
            },
            IndexInfo {
                name: "idx_old_field".to_string(),
                columns: vec!["old_field".to_string()],
                unique: false,
                index_type: None,
                primary: false,
            },
        ];
        let op = SchemaOperation::DropColumn {
            table: "heroes".to_string(),
            column: "old_field".to_string(),
            table_info: Some(table),
        };
        let stmts = ddl.generate(&op);

        // Table recreation path emits multiple statements.
        assert!(stmts.len() >= 6);
        assert!(stmts.iter().any(|s| s.contains("ALTER TABLE") && s.contains("RENAME TO")));
        assert!(stmts.iter().any(|s| s.contains("CREATE TABLE") && s.contains("\"heroes\"")));
        assert!(stmts.iter().any(|s| s.contains("INSERT INTO") && s.contains("SELECT")));
        // Index on dropped column should be omitted; remaining index should be recreated.
        assert!(stmts.iter().any(|s| s.contains("CREATE INDEX") && s.contains("idx_name")));
        assert!(!stmts
            .iter()
            .any(|s| s.contains("CREATE INDEX") && s.contains("idx_old_field")));
    }

    #[test]
    fn test_alter_column_type_unsupported() {
        let ddl = SqliteDdlGenerator;
        let op = SchemaOperation::AlterColumnType {
            table: "heroes".to_string(),
            column: "age".to_string(),
            from_type: "INTEGER".to_string(),
            to_type: "TEXT".to_string(),
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("--")); // Comment indicating unsupported
        assert!(stmts[0].contains("table recreation"));
    }

    #[test]
    fn test_rename_column() {
        let ddl = SqliteDdlGenerator;
        let op = SchemaOperation::RenameColumn {
            table: "heroes".to_string(),
            from: "old_name".to_string(),
            to: "name".to_string(),
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("RENAME COLUMN"));
    }

    #[test]
    fn test_create_index() {
        let ddl = SqliteDdlGenerator;
        let op = SchemaOperation::CreateIndex {
            table: "heroes".to_string(),
            index: IndexInfo {
                name: "idx_heroes_name".to_string(),
                columns: vec!["name".to_string()],
                unique: false,
                index_type: None,
                primary: false,
            },
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("CREATE INDEX"));
        assert!(stmts[0].contains("\"idx_heroes_name\""));
    }

    #[test]
    fn test_create_unique_index() {
        let ddl = SqliteDdlGenerator;
        let op = SchemaOperation::CreateIndex {
            table: "heroes".to_string(),
            index: IndexInfo {
                name: "idx_heroes_name_unique".to_string(),
                columns: vec!["name".to_string()],
                unique: true,
                index_type: None,
                primary: false,
            },
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("CREATE UNIQUE INDEX"));
    }

    #[test]
    fn test_drop_index() {
        let ddl = SqliteDdlGenerator;
        let op = SchemaOperation::DropIndex {
            table: "heroes".to_string(),
            name: "idx_heroes_name".to_string(),
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("DROP INDEX IF EXISTS"));
    }

    #[test]
    fn test_add_unique_creates_index() {
        let ddl = SqliteDdlGenerator;
        let op = SchemaOperation::AddUnique {
            table: "heroes".to_string(),
            constraint: UniqueConstraintInfo {
                name: Some("uk_heroes_name".to_string()),
                columns: vec!["name".to_string()],
            },
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("CREATE UNIQUE INDEX"));
    }

    #[test]
    fn test_add_fk_unsupported() {
        let ddl = SqliteDdlGenerator;
        let op = SchemaOperation::AddForeignKey {
            table: "heroes".to_string(),
            fk: ForeignKeyInfo {
                name: Some("fk_heroes_team".to_string()),
                column: "team_id".to_string(),
                foreign_table: "teams".to_string(),
                foreign_column: "id".to_string(),
                on_delete: None,
                on_update: None,
            },
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("--")); // Comment
        assert!(stmts[0].contains("table recreation"));
    }

    #[test]
    fn test_dialect() {
        let ddl = SqliteDdlGenerator;
        assert_eq!(ddl.dialect(), "sqlite");
    }

    #[test]
    fn test_generate_all() {
        let ddl = SqliteDdlGenerator;
        let ops = vec![
            SchemaOperation::CreateTable(make_table(
                "heroes",
                vec![make_column("id", "INTEGER", false)],
                vec!["id"],
            )),
            SchemaOperation::CreateIndex {
                table: "heroes".to_string(),
                index: IndexInfo {
                    name: "idx_heroes_name".to_string(),
                    columns: vec!["name".to_string()],
                    unique: false,
                    index_type: None,
                    primary: false,
                },
            },
        ];

        let stmts = ddl.generate_all(&ops);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_generate_rollback() {
        let ddl = SqliteDdlGenerator;
        let ops = vec![
            SchemaOperation::CreateTable(make_table(
                "heroes",
                vec![make_column("id", "INTEGER", false)],
                vec!["id"],
            )),
            SchemaOperation::AddColumn {
                table: "heroes".to_string(),
                column: make_column("name", "TEXT", false),
            },
        ];

        let rollback = ddl.generate_rollback(&ops);
        // Should have DROP COLUMN first (reverse of AddColumn), then DROP TABLE
        assert_eq!(rollback.len(), 2);
        assert!(rollback[0].contains("DROP COLUMN"));
        assert!(rollback[1].contains("DROP TABLE"));
    }
}
