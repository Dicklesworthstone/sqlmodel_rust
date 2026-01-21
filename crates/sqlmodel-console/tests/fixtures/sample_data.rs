//! Sample data for testing console components.

use sqlmodel_console::renderables::{ErrorPanel, ErrorSeverity};
use sqlmodel_schema::introspect::{ColumnInfo, ForeignKeyInfo, IndexInfo, TableInfo};

/// Sample user table schema.
pub fn user_table_info() -> TableInfo {
    TableInfo {
        name: "users".to_string(),
        columns: vec![
            ColumnInfo {
                name: "id".to_string(),
                sql_type: "INTEGER".to_string(),
                nullable: false,
                default: None,
                primary_key: true,
                auto_increment: true,
            },
            ColumnInfo {
                name: "name".to_string(),
                sql_type: "TEXT".to_string(),
                nullable: false,
                default: None,
                primary_key: false,
                auto_increment: false,
            },
            ColumnInfo {
                name: "email".to_string(),
                sql_type: "TEXT".to_string(),
                nullable: false,
                default: None,
                primary_key: false,
                auto_increment: false,
            },
            ColumnInfo {
                name: "created_at".to_string(),
                sql_type: "TIMESTAMP".to_string(),
                nullable: false,
                default: Some("NOW()".to_string()),
                primary_key: false,
                auto_increment: false,
            },
        ],
        primary_key: vec!["id".to_string()],
        foreign_keys: Vec::new(),
        indexes: vec![IndexInfo {
            name: "idx_users_email".to_string(),
            columns: vec!["email".to_string()],
            unique: true,
        }],
    }
}

/// Sample posts table schema with foreign key.
pub fn posts_table_info() -> TableInfo {
    TableInfo {
        name: "posts".to_string(),
        columns: vec![
            ColumnInfo {
                name: "id".to_string(),
                sql_type: "INTEGER".to_string(),
                nullable: false,
                default: None,
                primary_key: true,
                auto_increment: true,
            },
            ColumnInfo {
                name: "user_id".to_string(),
                sql_type: "INTEGER".to_string(),
                nullable: false,
                default: None,
                primary_key: false,
                auto_increment: false,
            },
            ColumnInfo {
                name: "title".to_string(),
                sql_type: "TEXT".to_string(),
                nullable: false,
                default: None,
                primary_key: false,
                auto_increment: false,
            },
            ColumnInfo {
                name: "content".to_string(),
                sql_type: "TEXT".to_string(),
                nullable: true,
                default: None,
                primary_key: false,
                auto_increment: false,
            },
        ],
        primary_key: vec!["id".to_string()],
        foreign_keys: vec![ForeignKeyInfo {
            name: Some("fk_posts_user".to_string()),
            column: "user_id".to_string(),
            foreign_table: "users".to_string(),
            foreign_column: "id".to_string(),
            on_delete: Some("CASCADE".to_string()),
            on_update: None,
        }],
        indexes: vec![IndexInfo {
            name: "idx_posts_user".to_string(),
            columns: vec!["user_id".to_string()],
            unique: false,
        }],
    }
}

/// Sample query results - small dataset.
pub fn sample_query_results_small() -> (Vec<String>, Vec<Vec<String>>) {
    let columns = vec![
        "id".to_string(),
        "name".to_string(),
        "email".to_string(),
    ];
    let rows = vec![
        vec!["1".to_string(), "Alice".to_string(), "alice@example.com".to_string()],
        vec!["2".to_string(), "Bob".to_string(), "bob@example.com".to_string()],
        vec!["3".to_string(), "Carol".to_string(), "carol@example.com".to_string()],
    ];
    (columns, rows)
}

/// Sample query results - large dataset.
pub fn sample_query_results_large(rows: usize, cols: usize) -> (Vec<String>, Vec<Vec<String>>) {
    let columns: Vec<String> = (0..cols).map(|i| format!("col_{i}")).collect();
    let rows: Vec<Vec<String>> = (0..rows)
        .map(|r| (0..cols).map(|c| format!("r{r}c{c}")).collect())
        .collect();
    (columns, rows)
}

/// Sample SQL syntax error.
pub fn sample_syntax_error() -> ErrorPanel {
    ErrorPanel::new("SQL Syntax Error", "Unexpected token near 'FORM'")
        .with_sql("SELECT * FORM users WHERE id = 1")
        .with_position(10)
        .with_sqlstate("42601")
        .with_hint("Did you mean 'FROM'?")
}

/// Sample connection error.
pub fn sample_connection_error() -> ErrorPanel {
    ErrorPanel::new("Connection Failed", "Could not connect to database")
        .severity(ErrorSeverity::Critical)
        .with_detail("Connection refused (os error 111)")
        .add_context("Host: localhost:5432")
        .add_context("User: postgres")
        .with_hint("Check that the database server is running")
}

/// Sample timeout error.
pub fn sample_timeout_error() -> ErrorPanel {
    ErrorPanel::new("Query Timeout", "Query exceeded maximum execution time")
        .severity(ErrorSeverity::Warning)
        .with_sql("SELECT * FROM large_table WHERE complex_condition")
        .with_detail("Timeout after 30 seconds")
        .with_hint("Consider adding an index or simplifying the query")
}
