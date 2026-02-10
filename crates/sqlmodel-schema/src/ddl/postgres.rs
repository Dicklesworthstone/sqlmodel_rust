//! PostgreSQL DDL generator.
//!
//! PostgreSQL has excellent ALTER TABLE support with fine-grained control over schema changes.

use super::{
    DdlGenerator, format_fk_constraint, generate_add_column, generate_create_index,
    generate_create_table, generate_drop_index, generate_drop_table, generate_rename_column,
    generate_rename_table, quote_identifier,
};
use crate::diff::SchemaOperation;
use crate::introspect::Dialect;

/// DDL generator for PostgreSQL.
pub struct PostgresDdlGenerator;

impl DdlGenerator for PostgresDdlGenerator {
    fn dialect(&self) -> &'static str {
        "postgres"
    }

    fn generate(&self, op: &SchemaOperation) -> Vec<String> {
        tracing::debug!(dialect = "postgres", op = ?op, "Generating DDL");

        let statements = match op {
            // Tables
            SchemaOperation::CreateTable(table) => {
                vec![generate_create_table(table, Dialect::Postgres)]
            }
            SchemaOperation::DropTable(name) => {
                vec![generate_drop_table(name, Dialect::Postgres)]
            }
            SchemaOperation::RenameTable { from, to } => {
                vec![generate_rename_table(from, to, Dialect::Postgres)]
            }

            // Columns
            SchemaOperation::AddColumn { table, column } => {
                vec![generate_add_column(table, column, Dialect::Postgres)]
            }
            SchemaOperation::DropColumn { table, column, .. } => {
                vec![format!(
                    "ALTER TABLE {} DROP COLUMN {}",
                    quote_identifier(table, Dialect::Postgres),
                    quote_identifier(column, Dialect::Postgres)
                )]
            }
            SchemaOperation::AlterColumnType {
                table,
                column,
                to_type,
                ..
            } => {
                // PostgreSQL uses ALTER COLUMN ... TYPE
                // USING clause may be needed for type conversion
                vec![format!(
                    "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
                    quote_identifier(table, Dialect::Postgres),
                    quote_identifier(column, Dialect::Postgres),
                    to_type
                )]
            }
            SchemaOperation::AlterColumnNullable {
                table,
                column,
                to_nullable,
                ..
            } => {
                // PostgreSQL uses SET NOT NULL / DROP NOT NULL
                let action = if *to_nullable {
                    "DROP NOT NULL"
                } else {
                    "SET NOT NULL"
                };
                vec![format!(
                    "ALTER TABLE {} ALTER COLUMN {} {}",
                    quote_identifier(table, Dialect::Postgres),
                    quote_identifier(&column.name, Dialect::Postgres),
                    action
                )]
            }
            SchemaOperation::AlterColumnDefault {
                table,
                column,
                to_default,
                ..
            } => {
                // PostgreSQL uses SET DEFAULT / DROP DEFAULT
                if let Some(default) = to_default {
                    vec![format!(
                        "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {}",
                        quote_identifier(table, Dialect::Postgres),
                        quote_identifier(column, Dialect::Postgres),
                        default
                    )]
                } else {
                    vec![format!(
                        "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT",
                        quote_identifier(table, Dialect::Postgres),
                        quote_identifier(column, Dialect::Postgres)
                    )]
                }
            }
            SchemaOperation::RenameColumn { table, from, to } => {
                vec![generate_rename_column(table, from, to, Dialect::Postgres)]
            }

            // Primary Keys
            SchemaOperation::AddPrimaryKey { table, columns } => {
                let cols: Vec<String> = columns
                    .iter()
                    .map(|c| quote_identifier(c, Dialect::Postgres))
                    .collect();
                // PostgreSQL auto-generates constraint name as {table}_pkey
                vec![format!(
                    "ALTER TABLE {} ADD PRIMARY KEY ({})",
                    quote_identifier(table, Dialect::Postgres),
                    cols.join(", ")
                )]
            }
            SchemaOperation::DropPrimaryKey { table } => {
                // PostgreSQL requires the constraint name, which is typically {table}_pkey
                let constraint_name = format!("{}_pkey", table);
                vec![format!(
                    "ALTER TABLE {} DROP CONSTRAINT {}",
                    quote_identifier(table, Dialect::Postgres),
                    quote_identifier(&constraint_name, Dialect::Postgres)
                )]
            }

            // Foreign Keys
            SchemaOperation::AddForeignKey { table, fk } => {
                let constraint_name = fk
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("fk_{}_{}", table, fk.column));
                vec![format!(
                    "ALTER TABLE {} ADD CONSTRAINT {} {}",
                    quote_identifier(table, Dialect::Postgres),
                    quote_identifier(&constraint_name, Dialect::Postgres),
                    format_fk_constraint(fk, Dialect::Postgres)
                )]
            }
            SchemaOperation::DropForeignKey { table, name } => {
                vec![format!(
                    "ALTER TABLE {} DROP CONSTRAINT {}",
                    quote_identifier(table, Dialect::Postgres),
                    quote_identifier(name, Dialect::Postgres)
                )]
            }

            // Unique Constraints
            SchemaOperation::AddUnique { table, constraint } => {
                let cols: Vec<String> = constraint
                    .columns
                    .iter()
                    .map(|c| quote_identifier(c, Dialect::Postgres))
                    .collect();
                let name = constraint
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("uk_{}_{}", table, constraint.columns.join("_")));
                vec![format!(
                    "ALTER TABLE {} ADD CONSTRAINT {} UNIQUE ({})",
                    quote_identifier(table, Dialect::Postgres),
                    quote_identifier(&name, Dialect::Postgres),
                    cols.join(", ")
                )]
            }
            SchemaOperation::DropUnique { table, name } => {
                vec![format!(
                    "ALTER TABLE {} DROP CONSTRAINT {}",
                    quote_identifier(table, Dialect::Postgres),
                    quote_identifier(name, Dialect::Postgres)
                )]
            }

            // Indexes
            SchemaOperation::CreateIndex { table, index } => {
                vec![generate_create_index(table, index, Dialect::Postgres)]
            }
            SchemaOperation::DropIndex { table, name } => {
                vec![generate_drop_index(table, name, Dialect::Postgres)]
            }
        };

        for stmt in &statements {
            tracing::trace!(sql = %stmt, "Generated PostgreSQL DDL statement");
        }

        statements
    }
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
        let ddl = PostgresDdlGenerator;
        let table = make_table(
            "heroes",
            vec![
                make_column("id", "SERIAL", false),
                make_column("name", "VARCHAR(100)", false),
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
        let ddl = PostgresDdlGenerator;
        let op = SchemaOperation::DropTable("heroes".to_string());
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "DROP TABLE IF EXISTS \"heroes\"");
    }

    #[test]
    fn test_rename_table() {
        let ddl = PostgresDdlGenerator;
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
        let ddl = PostgresDdlGenerator;
        let op = SchemaOperation::AddColumn {
            table: "heroes".to_string(),
            column: make_column("age", "INTEGER", true),
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("ALTER TABLE"));
        assert!(stmts[0].contains("ADD COLUMN"));
    }

    #[test]
    fn test_drop_column() {
        let ddl = PostgresDdlGenerator;
        let op = SchemaOperation::DropColumn {
            table: "heroes".to_string(),
            column: "old_field".to_string(),
            table_info: None,
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("ALTER TABLE"));
        assert!(stmts[0].contains("DROP COLUMN"));
    }

    #[test]
    fn test_alter_column_type() {
        let ddl = PostgresDdlGenerator;
        let op = SchemaOperation::AlterColumnType {
            table: "heroes".to_string(),
            column: "age".to_string(),
            from_type: "INTEGER".to_string(),
            to_type: "BIGINT".to_string(),
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("ALTER COLUMN"));
        assert!(stmts[0].contains("TYPE BIGINT"));
    }

    #[test]
    fn test_alter_column_set_not_null() {
        let ddl = PostgresDdlGenerator;
        let op = SchemaOperation::AlterColumnNullable {
            table: "heroes".to_string(),
            column: make_column("name", "TEXT", false),
            from_nullable: true,
            to_nullable: false,
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("SET NOT NULL"));
    }

    #[test]
    fn test_alter_column_drop_not_null() {
        let ddl = PostgresDdlGenerator;
        let op = SchemaOperation::AlterColumnNullable {
            table: "heroes".to_string(),
            column: make_column("name", "TEXT", true),
            from_nullable: false,
            to_nullable: true,
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("DROP NOT NULL"));
    }

    #[test]
    fn test_alter_column_set_default() {
        let ddl = PostgresDdlGenerator;
        let op = SchemaOperation::AlterColumnDefault {
            table: "heroes".to_string(),
            column: "status".to_string(),
            from_default: None,
            to_default: Some("'active'".to_string()),
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("SET DEFAULT"));
        assert!(stmts[0].contains("'active'"));
    }

    #[test]
    fn test_alter_column_drop_default() {
        let ddl = PostgresDdlGenerator;
        let op = SchemaOperation::AlterColumnDefault {
            table: "heroes".to_string(),
            column: "status".to_string(),
            from_default: Some("'active'".to_string()),
            to_default: None,
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("DROP DEFAULT"));
    }

    #[test]
    fn test_rename_column() {
        let ddl = PostgresDdlGenerator;
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
    fn test_add_primary_key() {
        let ddl = PostgresDdlGenerator;
        let op = SchemaOperation::AddPrimaryKey {
            table: "heroes".to_string(),
            columns: vec!["id".to_string()],
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("ADD PRIMARY KEY"));
    }

    #[test]
    fn test_drop_primary_key() {
        let ddl = PostgresDdlGenerator;
        let op = SchemaOperation::DropPrimaryKey {
            table: "heroes".to_string(),
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("DROP CONSTRAINT"));
        assert!(stmts[0].contains("\"heroes_pkey\""));
    }

    #[test]
    fn test_add_foreign_key() {
        let ddl = PostgresDdlGenerator;
        let op = SchemaOperation::AddForeignKey {
            table: "heroes".to_string(),
            fk: ForeignKeyInfo {
                name: Some("fk_heroes_team".to_string()),
                column: "team_id".to_string(),
                foreign_table: "teams".to_string(),
                foreign_column: "id".to_string(),
                on_delete: Some("CASCADE".to_string()),
                on_update: None,
            },
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("ADD CONSTRAINT"));
        assert!(stmts[0].contains("FOREIGN KEY"));
        assert!(stmts[0].contains("ON DELETE CASCADE"));
    }

    #[test]
    fn test_drop_foreign_key() {
        let ddl = PostgresDdlGenerator;
        let op = SchemaOperation::DropForeignKey {
            table: "heroes".to_string(),
            name: "fk_heroes_team".to_string(),
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("DROP CONSTRAINT"));
        assert!(stmts[0].contains("\"fk_heroes_team\""));
    }

    #[test]
    fn test_add_unique() {
        let ddl = PostgresDdlGenerator;
        let op = SchemaOperation::AddUnique {
            table: "heroes".to_string(),
            constraint: UniqueConstraintInfo {
                name: Some("uk_heroes_name".to_string()),
                columns: vec!["name".to_string()],
            },
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("ADD CONSTRAINT"));
        assert!(stmts[0].contains("UNIQUE"));
    }

    #[test]
    fn test_drop_unique() {
        let ddl = PostgresDdlGenerator;
        let op = SchemaOperation::DropUnique {
            table: "heroes".to_string(),
            name: "uk_heroes_name".to_string(),
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("DROP CONSTRAINT"));
    }

    #[test]
    fn test_create_index() {
        let ddl = PostgresDdlGenerator;
        let op = SchemaOperation::CreateIndex {
            table: "heroes".to_string(),
            index: IndexInfo {
                name: "idx_heroes_name".to_string(),
                columns: vec!["name".to_string()],
                unique: false,
                index_type: Some("btree".to_string()),
                primary: false,
            },
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("CREATE INDEX"));
        assert!(stmts[0].contains("USING btree"));
    }

    #[test]
    fn test_create_gin_index() {
        let ddl = PostgresDdlGenerator;
        let op = SchemaOperation::CreateIndex {
            table: "heroes".to_string(),
            index: IndexInfo {
                name: "idx_heroes_tags".to_string(),
                columns: vec!["tags".to_string()],
                unique: false,
                index_type: Some("gin".to_string()),
                primary: false,
            },
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("USING gin"));
    }

    #[test]
    fn test_drop_index() {
        let ddl = PostgresDdlGenerator;
        let op = SchemaOperation::DropIndex {
            table: "heroes".to_string(),
            name: "idx_heroes_name".to_string(),
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("DROP INDEX IF EXISTS"));
    }

    #[test]
    fn test_dialect() {
        let ddl = PostgresDdlGenerator;
        assert_eq!(ddl.dialect(), "postgres");
    }

    #[test]
    fn test_composite_primary_key() {
        let ddl = PostgresDdlGenerator;
        let op = SchemaOperation::AddPrimaryKey {
            table: "hero_team".to_string(),
            columns: vec!["hero_id".to_string(), "team_id".to_string()],
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("ADD PRIMARY KEY"));
        assert!(stmts[0].contains("\"hero_id\""));
        assert!(stmts[0].contains("\"team_id\""));
    }
}
