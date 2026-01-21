mod fixtures;

use fixtures::*;
use sqlmodel_console::renderables::PoolStatsProvider;

#[test]
fn test_user_table_info() {
    let table = user_table_info();
    assert_eq!(table.name, "users");
    assert_eq!(table.columns.len(), 4);
    assert_eq!(table.primary_key, vec!["id".to_string()]);
}

#[test]
fn test_posts_table_info() {
    let table = posts_table_info();
    assert_eq!(table.name, "posts");
    assert_eq!(table.foreign_keys.len(), 1);
    assert_eq!(table.indexes.len(), 1);
}

#[test]
fn test_sample_query_results_small() {
    let (cols, rows) = sample_query_results_small();
    assert_eq!(cols.len(), 3);
    assert_eq!(rows.len(), 3);
}

#[test]
fn test_sample_errors_render_plain() {
    let plain = sample_syntax_error().render_plain();
    assert_golden("error_panel_plain.txt", &plain);
}

#[test]
fn test_mock_connection_records_calls() {
    let conn = MockConnection::new();
    conn.emit_status("connecting");
    conn.emit_error("failed");
    assert_eq!(conn.status_calls.borrow().len(), 1);
    assert_eq!(conn.error_calls.borrow().len(), 1);
}

#[test]
fn test_mock_pool_stats_provider() {
    let stats = MockPoolStats::busy();
    assert_eq!(stats.active_connections(), 8);
    assert_eq!(stats.idle_connections(), 2);
    assert_eq!(stats.pending_requests(), 0);
    assert_eq!(stats.max_connections(), 10);
}

#[test]
fn test_golden_loader() {
    let content = load_golden("query_table_small.txt");
    assert!(content.contains("Alice"));
}
