//! Performance benchmarks for SQLModel Console.
//!
//! These benchmarks verify that console output does not introduce significant
//! overhead to database operations.
//!
//! # Running Benchmarks
//!
//! ```bash
//! cargo bench -p sqlmodel-console --bench console_bench
//! ```
//!
//! # Performance Targets
//!
//! - Mode detection: <100ns
//! - Small table render (10 rows): <1ms
//! - Large table render (1000 rows): <10ms
//! - Error panel render: <1ms
//! - Progress update: <10us
//! - SQL highlighting (100 chars): <100us

#![feature(test)]

extern crate test;

use test::{black_box, Bencher};

use sqlmodel_console::renderables::{
    BatchOperationTracker, Cell, ErrorPanel, IndeterminateSpinner, OperationProgress, PlainFormat,
    QueryResultTable, SpinnerStyle, SqlHighlighter, ValueType,
};
use sqlmodel_console::{OutputMode, SqlModelConsole, Theme};

// ============================================================================
// Mode Detection Benchmarks
// ============================================================================

#[bench]
fn bench_mode_detect(b: &mut Bencher) {
    b.iter(|| black_box(OutputMode::detect()));
}

#[bench]
fn bench_mode_is_agent_environment(b: &mut Bencher) {
    b.iter(|| black_box(OutputMode::is_agent_environment()));
}

#[bench]
fn bench_mode_supports_ansi(b: &mut Bencher) {
    let mode = OutputMode::Rich;
    b.iter(|| black_box(mode.supports_ansi()));
}

// ============================================================================
// Console Creation Benchmarks
// ============================================================================

#[bench]
fn bench_console_new(b: &mut Bencher) {
    b.iter(|| black_box(SqlModelConsole::new()));
}

#[bench]
fn bench_console_with_mode(b: &mut Bencher) {
    b.iter(|| black_box(SqlModelConsole::with_mode(OutputMode::Plain)));
}

#[bench]
fn bench_console_with_theme(b: &mut Bencher) {
    b.iter(|| black_box(SqlModelConsole::with_theme(Theme::dark())));
}

// ============================================================================
// Theme Benchmarks
// ============================================================================

#[bench]
fn bench_theme_dark(b: &mut Bencher) {
    b.iter(|| black_box(Theme::dark()));
}

#[bench]
fn bench_theme_light(b: &mut Bencher) {
    b.iter(|| black_box(Theme::light()));
}

#[bench]
fn bench_theme_color_code(b: &mut Bencher) {
    let theme = Theme::dark();
    b.iter(|| black_box(theme.success.color_code()));
}

// ============================================================================
// QueryResultTable Benchmarks
// ============================================================================

fn create_sample_table(rows: usize, cols: usize) -> QueryResultTable {
    let columns: Vec<String> = (0..cols).map(|i| format!("col_{i}")).collect();
    let mut table = QueryResultTable::new().columns(columns);

    for row_idx in 0..rows {
        let values: Vec<String> = (0..cols)
            .map(|col_idx| format!("value_{row_idx}_{col_idx}"))
            .collect();
        table = table.row(values);
    }

    table.timing_ms(12.34)
}

#[bench]
fn bench_table_creation_small(b: &mut Bencher) {
    b.iter(|| black_box(create_sample_table(10, 5)));
}

#[bench]
fn bench_table_creation_medium(b: &mut Bencher) {
    b.iter(|| black_box(create_sample_table(100, 10)));
}

#[bench]
fn bench_table_creation_large(b: &mut Bencher) {
    b.iter(|| black_box(create_sample_table(1000, 10)));
}

#[bench]
fn bench_table_render_plain_small(b: &mut Bencher) {
    let table = create_sample_table(10, 5);
    b.iter(|| black_box(table.render_plain()));
}

#[bench]
fn bench_table_render_plain_medium(b: &mut Bencher) {
    let table = create_sample_table(100, 10);
    b.iter(|| black_box(table.render_plain()));
}

#[bench]
fn bench_table_render_plain_large(b: &mut Bencher) {
    let table = create_sample_table(1000, 10);
    b.iter(|| black_box(table.render_plain()));
}

#[bench]
fn bench_table_render_styled_small(b: &mut Bencher) {
    let table = create_sample_table(10, 5);
    b.iter(|| black_box(table.render_styled()));
}

#[bench]
fn bench_table_render_styled_medium(b: &mut Bencher) {
    let table = create_sample_table(100, 10);
    b.iter(|| black_box(table.render_styled()));
}

#[bench]
fn bench_table_render_csv(b: &mut Bencher) {
    let table = create_sample_table(100, 10);
    b.iter(|| black_box(table.render_plain_format(PlainFormat::Csv)));
}

#[bench]
fn bench_table_render_json_lines(b: &mut Bencher) {
    let table = create_sample_table(100, 10);
    b.iter(|| black_box(table.render_plain_format(PlainFormat::JsonLines)));
}

#[bench]
fn bench_table_render_json_array(b: &mut Bencher) {
    let table = create_sample_table(100, 10);
    b.iter(|| black_box(table.render_plain_format(PlainFormat::JsonArray)));
}

#[bench]
fn bench_table_to_json(b: &mut Bencher) {
    let table = create_sample_table(100, 10);
    b.iter(|| black_box(table.to_json()));
}

// ============================================================================
// Value Type Inference Benchmarks
// ============================================================================

#[bench]
fn bench_value_type_infer_null(b: &mut Bencher) {
    b.iter(|| black_box(ValueType::infer("NULL")));
}

#[bench]
fn bench_value_type_infer_integer(b: &mut Bencher) {
    b.iter(|| black_box(ValueType::infer("12345")));
}

#[bench]
fn bench_value_type_infer_float(b: &mut Bencher) {
    b.iter(|| black_box(ValueType::infer("123.456")));
}

#[bench]
fn bench_value_type_infer_string(b: &mut Bencher) {
    b.iter(|| black_box(ValueType::infer("hello world")));
}

#[bench]
fn bench_value_type_infer_uuid(b: &mut Bencher) {
    b.iter(|| black_box(ValueType::infer("550e8400-e29b-41d4-a716-446655440000")));
}

#[bench]
fn bench_value_type_infer_timestamp(b: &mut Bencher) {
    b.iter(|| black_box(ValueType::infer("2024-01-15T10:30:00")));
}

#[bench]
fn bench_cell_new(b: &mut Bencher) {
    b.iter(|| black_box(Cell::new("12345")));
}

// ============================================================================
// ErrorPanel Benchmarks
// ============================================================================

fn create_sample_error() -> ErrorPanel {
    ErrorPanel::new("Connection timeout", "Failed to connect within timeout period")
        .with_sql("SELECT * FROM users WHERE id = $1")
        .with_position(1)
        .with_detail("Host: localhost:5432")
        .with_hint("Check if the database server is running")
        .with_sqlstate("08001")
}

#[bench]
fn bench_error_panel_creation(b: &mut Bencher) {
    b.iter(|| black_box(create_sample_error()));
}

#[bench]
fn bench_error_panel_render_plain(b: &mut Bencher) {
    let error = create_sample_error();
    b.iter(|| black_box(error.render_plain()));
}

#[bench]
fn bench_error_panel_render_styled(b: &mut Bencher) {
    let error = create_sample_error();
    b.iter(|| black_box(error.render_styled()));
}

// ============================================================================
// OperationProgress Benchmarks
// ============================================================================

#[bench]
fn bench_progress_creation(b: &mut Bencher) {
    b.iter(|| black_box(OperationProgress::new("Inserting rows", 10000)));
}

#[bench]
fn bench_progress_update(b: &mut Bencher) {
    let mut progress = OperationProgress::new("Inserting rows", 10000);
    let mut i = 0u64;
    b.iter(|| {
        i = (i + 1) % 10000;
        progress.set_completed(i);
        black_box(&progress)
    });
}

#[bench]
fn bench_progress_increment(b: &mut Bencher) {
    let mut progress = OperationProgress::new("Inserting rows", 10000);
    b.iter(|| {
        progress.increment();
        black_box(&progress)
    });
}

#[bench]
fn bench_progress_render_plain(b: &mut Bencher) {
    let progress = OperationProgress::new("Inserting rows", 10000).completed(5000);
    b.iter(|| black_box(progress.render_plain()));
}

#[bench]
fn bench_progress_render_styled(b: &mut Bencher) {
    let progress = OperationProgress::new("Inserting rows", 10000).completed(5000);
    b.iter(|| black_box(progress.render_styled()));
}

// ============================================================================
// IndeterminateSpinner Benchmarks
// ============================================================================

#[bench]
fn bench_spinner_creation(b: &mut Bencher) {
    b.iter(|| black_box(IndeterminateSpinner::new("Loading")));
}

#[bench]
fn bench_spinner_current_frame(b: &mut Bencher) {
    let spinner = IndeterminateSpinner::new("Loading");
    b.iter(|| black_box(spinner.current_frame()));
}

#[bench]
fn bench_spinner_style_frame_at(b: &mut Bencher) {
    let style = SpinnerStyle::Braille;
    let mut ms = 0u64;
    b.iter(|| {
        ms = (ms + 80) % 10000;
        black_box(style.frame_at(ms))
    });
}

#[bench]
fn bench_spinner_render_plain(b: &mut Bencher) {
    let spinner = IndeterminateSpinner::new("Loading");
    b.iter(|| black_box(spinner.render_plain()));
}

#[bench]
fn bench_spinner_render_styled(b: &mut Bencher) {
    let spinner = IndeterminateSpinner::new("Loading");
    b.iter(|| black_box(spinner.render_styled()));
}

// ============================================================================
// BatchOperationTracker Benchmarks
// ============================================================================

#[bench]
fn bench_batch_tracker_creation(b: &mut Bencher) {
    b.iter(|| black_box(BatchOperationTracker::new("Bulk insert", 100, 10000)));
}

#[bench]
fn bench_batch_tracker_complete_batch(b: &mut Bencher) {
    let mut tracker = BatchOperationTracker::new("Bulk insert", 100, 10000);
    b.iter(|| {
        tracker.complete_batch(100);
        black_box(&tracker)
    });
}

#[bench]
fn bench_batch_tracker_render_plain(b: &mut Bencher) {
    let mut tracker = BatchOperationTracker::new("Bulk insert", 100, 10000);
    tracker.complete_batch(5000);
    b.iter(|| black_box(tracker.render_plain()));
}

#[bench]
fn bench_batch_tracker_render_styled(b: &mut Bencher) {
    let mut tracker = BatchOperationTracker::new("Bulk insert", 100, 10000);
    tracker.complete_batch(5000);
    b.iter(|| black_box(tracker.render_styled()));
}

// ============================================================================
// SQL Highlighter Benchmarks
// ============================================================================

const SAMPLE_SQL_SIMPLE: &str = "SELECT * FROM users WHERE id = 1";

const SAMPLE_SQL_COMPLEX: &str = r#"
SELECT u.id, u.name, u.email, COUNT(o.id) as order_count
FROM users u
LEFT JOIN orders o ON o.user_id = u.id
WHERE u.active = true AND u.created_at > '2024-01-01'
GROUP BY u.id, u.name, u.email
HAVING COUNT(o.id) > 5
ORDER BY order_count DESC
LIMIT 100 OFFSET 0
"#;

#[bench]
fn bench_highlighter_creation(b: &mut Bencher) {
    b.iter(|| black_box(SqlHighlighter::new()));
}

#[bench]
fn bench_highlighter_tokenize_simple(b: &mut Bencher) {
    let highlighter = SqlHighlighter::new();
    b.iter(|| black_box(highlighter.tokenize(SAMPLE_SQL_SIMPLE)));
}

#[bench]
fn bench_highlighter_tokenize_complex(b: &mut Bencher) {
    let highlighter = SqlHighlighter::new();
    b.iter(|| black_box(highlighter.tokenize(SAMPLE_SQL_COMPLEX)));
}

#[bench]
fn bench_highlighter_highlight_simple(b: &mut Bencher) {
    let highlighter = SqlHighlighter::new();
    b.iter(|| black_box(highlighter.highlight(SAMPLE_SQL_SIMPLE)));
}

#[bench]
fn bench_highlighter_highlight_complex(b: &mut Bencher) {
    let highlighter = SqlHighlighter::new();
    b.iter(|| black_box(highlighter.highlight(SAMPLE_SQL_COMPLEX)));
}

#[bench]
fn bench_highlighter_format_simple(b: &mut Bencher) {
    let highlighter = SqlHighlighter::new();
    b.iter(|| black_box(highlighter.format(SAMPLE_SQL_SIMPLE)));
}

#[bench]
fn bench_highlighter_format_complex(b: &mut Bencher) {
    let highlighter = SqlHighlighter::new();
    b.iter(|| black_box(highlighter.format(SAMPLE_SQL_COMPLEX)));
}

#[bench]
fn bench_highlighter_plain(b: &mut Bencher) {
    let highlighter = SqlHighlighter::new();
    b.iter(|| black_box(highlighter.plain(SAMPLE_SQL_COMPLEX)));
}
