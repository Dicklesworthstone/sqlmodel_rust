//! MySQL DDL generator.
//!
//! MySQL has comprehensive ALTER TABLE support for most schema operations.

use super::{
    DdlGenerator, format_column_def, format_fk_constraint, generate_add_column,
    generate_create_index, generate_create_table, generate_drop_index, generate_drop_table,
    generate_rename_column, generate_rename_table, quote_identifier,
};
use crate::diff::SchemaOperation;
use crate::introspect::Dialect;

/// DDL generator for MySQL.
pub struct MysqlDdlGenerator;

impl DdlGenerator for MysqlDdlGenerator {
    fn dialect(&self) -> &'static str {
        "mysql"
    }

    fn generate(&self, op: &SchemaOperation) -> Vec<String> {
        tracing::debug!(dialect = "mysql", op = ?op, "Generating DDL");

        let statements = match op {
            // Tables
            SchemaOperation::CreateTable(table) => {
                vec![generate_create_table(table, Dialect::Mysql)]
            }
            SchemaOperation::DropTable(name) => {
                vec![generate_drop_table(name, Dialect::Mysql)]
            }
            SchemaOperation::RenameTable { from, to } => {
                vec![generate_rename_table(from, to, Dialect::Mysql)]
            }

            // Columns
            SchemaOperation::AddColumn { table, column } => {
                vec![generate_add_column(table, column, Dialect::Mysql)]
            }
            SchemaOperation::DropColumn { table, column, .. } => {
                vec![format!(
                    "ALTER TABLE {} DROP COLUMN {}",
                    quote_identifier(table, Dialect::Mysql),
                    quote_identifier(column, Dialect::Mysql)
                )]
            }
            SchemaOperation::AlterColumnType {
                table,
                column,
                to_type,
                ..
            } => {
                // MySQL uses MODIFY COLUMN for type changes
                vec![format!(
                    "ALTER TABLE {} MODIFY COLUMN {} {}",
                    quote_identifier(table, Dialect::Mysql),
                    quote_identifier(column, Dialect::Mysql),
                    to_type
                )]
            }
            SchemaOperation::AlterColumnNullable {
                table,
                column,
                to_nullable,
                ..
            } => {
                // MySQL uses MODIFY COLUMN and requires a full column definition.
                // We carry `ColumnInfo` on this operation so we can generate a correct statement.
                let mut col = column.clone();
                col.nullable = *to_nullable;
                vec![format!(
                    "ALTER TABLE {} MODIFY COLUMN {}",
                    quote_identifier(table, Dialect::Mysql),
                    format_column_def(&col, Dialect::Mysql)
                )]
            }
            SchemaOperation::AlterColumnDefault {
                table,
                column,
                to_default,
                ..
            } => {
                // MySQL supports ALTER COLUMN ... SET DEFAULT / DROP DEFAULT
                if let Some(default) = to_default {
                    vec![format!(
                        "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {}",
                        quote_identifier(table, Dialect::Mysql),
                        quote_identifier(column, Dialect::Mysql),
                        default
                    )]
                } else {
                    vec![format!(
                        "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT",
                        quote_identifier(table, Dialect::Mysql),
                        quote_identifier(column, Dialect::Mysql)
                    )]
                }
            }
            SchemaOperation::RenameColumn { table, from, to } => {
                vec![generate_rename_column(table, from, to, Dialect::Mysql)]
            }

            // Primary Keys
            SchemaOperation::AddPrimaryKey { table, columns } => {
                let cols: Vec<String> = columns
                    .iter()
                    .map(|c| quote_identifier(c, Dialect::Mysql))
                    .collect();
                vec![format!(
                    "ALTER TABLE {} ADD PRIMARY KEY ({})",
                    quote_identifier(table, Dialect::Mysql),
                    cols.join(", ")
                )]
            }
            SchemaOperation::DropPrimaryKey { table } => {
                vec![format!(
                    "ALTER TABLE {} DROP PRIMARY KEY",
                    quote_identifier(table, Dialect::Mysql)
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
                    quote_identifier(table, Dialect::Mysql),
                    quote_identifier(&constraint_name, Dialect::Mysql),
                    format_fk_constraint(fk, Dialect::Mysql)
                )]
            }
            SchemaOperation::DropForeignKey { table, name } => {
                vec![format!(
                    "ALTER TABLE {} DROP FOREIGN KEY {}",
                    quote_identifier(table, Dialect::Mysql),
                    quote_identifier(name, Dialect::Mysql)
                )]
            }

            // Unique Constraints
            SchemaOperation::AddUnique { table, constraint } => {
                let cols: Vec<String> = constraint
                    .columns
                    .iter()
                    .map(|c| quote_identifier(c, Dialect::Mysql))
                    .collect();
                let name = constraint
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("uk_{}_{}", table, constraint.columns.join("_")));
                vec![format!(
                    "ALTER TABLE {} ADD CONSTRAINT {} UNIQUE ({})",
                    quote_identifier(table, Dialect::Mysql),
                    quote_identifier(&name, Dialect::Mysql),
                    cols.join(", ")
                )]
            }
            SchemaOperation::DropUnique { table, name } => {
                // MySQL drops unique constraints via DROP INDEX
                vec![format!(
                    "ALTER TABLE {} DROP INDEX {}",
                    quote_identifier(table, Dialect::Mysql),
                    quote_identifier(name, Dialect::Mysql)
                )]
            }

            // Indexes
            SchemaOperation::CreateIndex { table, index } => {
                vec![generate_create_index(table, index, Dialect::Mysql)]
            }
            SchemaOperation::DropIndex { table, name } => {
                vec![generate_drop_index(table, name, Dialect::Mysql)]
            }
        };

        for stmt in &statements {
            tracing::trace!(sql = %stmt, "Generated MySQL DDL statement");
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
        let ddl = MysqlDdlGenerator;
        let table = make_table(
            "heroes",
            vec![
                make_column("id", "INT", false),
                make_column("name", "VARCHAR(100)", false),
            ],
            vec!["id"],
        );
        let op = SchemaOperation::CreateTable(table);
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("CREATE TABLE IF NOT EXISTS"));
        assert!(stmts[0].contains("`heroes`"));
    }

    #[test]
    fn test_drop_table() {
        let ddl = MysqlDdlGenerator;
        let op = SchemaOperation::DropTable("heroes".to_string());
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "DROP TABLE IF EXISTS `heroes`");
    }

    #[test]
    fn test_rename_table() {
        let ddl = MysqlDdlGenerator;
        let op = SchemaOperation::RenameTable {
            from: "old_heroes".to_string(),
            to: "heroes".to_string(),
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("RENAME TABLE"));
    }

    #[test]
    fn test_add_column() {
        let ddl = MysqlDdlGenerator;
        let op = SchemaOperation::AddColumn {
            table: "heroes".to_string(),
            column: make_column("age", "INT", true),
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("ALTER TABLE"));
        assert!(stmts[0].contains("ADD COLUMN"));
    }

    #[test]
    fn test_drop_column() {
        let ddl = MysqlDdlGenerator;
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
        let ddl = MysqlDdlGenerator;
        let op = SchemaOperation::AlterColumnType {
            table: "heroes".to_string(),
            column: "age".to_string(),
            from_type: "INT".to_string(),
            to_type: "BIGINT".to_string(),
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("MODIFY COLUMN"));
        assert!(stmts[0].contains("BIGINT"));
    }

    #[test]
    fn test_alter_column_default_set() {
        let ddl = MysqlDdlGenerator;
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
    fn test_alter_column_default_drop() {
        let ddl = MysqlDdlGenerator;
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
        let ddl = MysqlDdlGenerator;
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
        let ddl = MysqlDdlGenerator;
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
        let ddl = MysqlDdlGenerator;
        let op = SchemaOperation::DropPrimaryKey {
            table: "heroes".to_string(),
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("DROP PRIMARY KEY"));
    }

    #[test]
    fn test_add_foreign_key() {
        let ddl = MysqlDdlGenerator;
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
        let ddl = MysqlDdlGenerator;
        let op = SchemaOperation::DropForeignKey {
            table: "heroes".to_string(),
            name: "fk_heroes_team".to_string(),
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("DROP FOREIGN KEY"));
    }

    #[test]
    fn test_add_unique() {
        let ddl = MysqlDdlGenerator;
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
        let ddl = MysqlDdlGenerator;
        let op = SchemaOperation::DropUnique {
            table: "heroes".to_string(),
            name: "uk_heroes_name".to_string(),
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("DROP INDEX"));
    }

    #[test]
    fn test_create_index() {
        let ddl = MysqlDdlGenerator;
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
    }

    #[test]
    fn test_drop_index() {
        let ddl = MysqlDdlGenerator;
        let op = SchemaOperation::DropIndex {
            table: "heroes".to_string(),
            name: "idx_heroes_name".to_string(),
        };
        let stmts = ddl.generate(&op);

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("DROP INDEX"));
        assert!(stmts[0].contains("ON `heroes`"));
    }

    #[test]
    fn test_dialect() {
        let ddl = MysqlDdlGenerator;
        assert_eq!(ddl.dialect(), "mysql");
    }
}
