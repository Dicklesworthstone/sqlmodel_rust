use std::time::Duration;

use sqlmodel_console::renderables::{
    BatchOperationTracker, ErrorPanel, ErrorSeverity, IndeterminateSpinner, OperationProgress,
    PoolStatusDisplay, QueryResultTable, QueryTiming, QueryTreeView, SpinnerStyle, SqlHighlighter,
};
use sqlmodel_console::{OutputMode, SqlModelConsole};

fn main() {
    let rich = SqlModelConsole::with_mode(OutputMode::Rich);
    let plain = SqlModelConsole::with_mode(OutputMode::Plain);

    rich.rule(Some("Error Panel"));
    let error = ErrorPanel::new("SQL Syntax Error", "Unexpected token near 'FORM'")
        .with_sql("SELECT * FORM users WHERE id = 1")
        .with_position(10)
        .with_sqlstate("42601")
        .with_hint("Did you mean 'FROM'?")
        .severity(ErrorSeverity::Error);
    rich.print(&error.render_styled());

    plain.rule(Some("Error Panel (Plain)"));
    plain.print(&error.render_plain());

    rich.rule(Some("Query Results"));
    let table = QueryResultTable::new()
        .title("Users")
        .columns(["id", "name", "email"])
        .row(["1", "Alice", "alice@example.com"])
        .row(["2", "Bob", "bob@example.com"])
        .row(["3", "Carol", "carol@example.com"])
        .timing_ms(12.34)
        .with_row_numbers();
    rich.print(&table.render_styled());

    plain.rule(Some("Query Results (Plain)"));
    plain.print(&table.render_plain());

    rich.rule(Some("Query Tree"));
    let tree = QueryTreeView::new("SELECT users")
        .add_child("Columns", ["id", "name", "email"])
        .add_node("WHERE", "active = true")
        .add_node("ORDER BY", "name ASC")
        .add_node("LIMIT", "10");
    rich.print(&tree.render_styled());
    plain.print(&tree.render_plain());

    rich.rule(Some("Query Timing"));
    let timing = QueryTiming::new()
        .total(Duration::from_millis(12))
        .parse(Duration::from_micros(1200))
        .plan(Duration::from_micros(3400))
        .execute(Duration::from_micros(7700))
        .rows(3);
    rich.print(&timing.render_styled());
    plain.print(&timing.render_plain());

    rich.rule(Some("Pool Status"));
    let pool = PoolStatusDisplay::new(8, 2, 10, 1, 0)
        .name("primary")
        .with_acquisition_stats(120, 1)
        .with_lifetime_stats(15, 3)
        .uptime(Duration::from_secs(2 * 60 * 60 + 15 * 60));
    rich.print(&pool.render_styled());
    plain.print(&pool.render_plain());

    rich.rule(Some("Progress"));
    let progress = OperationProgress::new("Inserting rows", 1000)
        .completed(420)
        .unit("rows");
    rich.print(&progress.render_styled());
    plain.print(&progress.render_plain());

    rich.rule(Some("Spinner"));
    let spinner = IndeterminateSpinner::new("Connecting to database").style(SpinnerStyle::Dots);
    rich.print(&spinner.render_styled());
    plain.print(&spinner.render_plain());

    rich.rule(Some("Batch Tracker"));
    let mut tracker = BatchOperationTracker::new("Batch insert", 4, 400);
    tracker.complete_batch(100);
    tracker.complete_batch(100);
    rich.print(&tracker.render_styled());
    plain.print(&tracker.render_plain());

    rich.rule(Some("SQL Highlight"));
    let sql = "SELECT id, name FROM users WHERE active = true ORDER BY name";
    let highlighter = SqlHighlighter::new();
    rich.print(&highlighter.highlight(sql));
    plain.print(&highlighter.plain(sql));
}
