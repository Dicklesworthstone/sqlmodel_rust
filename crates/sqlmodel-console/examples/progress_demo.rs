use std::time::Duration;

use sqlmodel_console::renderables::{
    BatchOperationTracker, IndeterminateSpinner, OperationProgress, PoolStatusDisplay, SpinnerStyle,
};
use sqlmodel_console::{OutputMode, SqlModelConsole};

fn main() {
    let rich = SqlModelConsole::with_mode(OutputMode::Rich);
    let plain = SqlModelConsole::with_mode(OutputMode::Plain);

    rich.rule(Some("Determinate Progress"));
    let progress = OperationProgress::new("Backfilling rows", 1000)
        .completed(250)
        .unit("rows");
    rich.print(&progress.render_styled());
    plain.print(&progress.render_plain());

    rich.rule(Some("Completed Progress"));
    let completed = OperationProgress::new("Migration", 10)
        .completed(10)
        .unit("batches");
    rich.print(&completed.render_styled());
    plain.print(&completed.render_plain());

    rich.rule(Some("Indeterminate Spinners"));
    for style in [
        SpinnerStyle::Dots,
        SpinnerStyle::Braille,
        SpinnerStyle::Line,
        SpinnerStyle::Arrow,
        SpinnerStyle::Simple,
    ] {
        let spinner = IndeterminateSpinner::new("Connecting").style(style);
        rich.print(&spinner.render_styled());
        plain.print(&spinner.render_plain());
    }

    rich.rule(Some("Batch Tracker"));
    let mut tracker = BatchOperationTracker::new("Batch insert", 5, 500);
    tracker.complete_batch(100);
    tracker.complete_batch(100);
    rich.print(&tracker.render_styled());
    plain.print(&tracker.render_plain());

    rich.rule(Some("Pool Status"));
    let pool = PoolStatusDisplay::new(9, 1, 10, 1, 2)
        .name("primary")
        .uptime(Duration::from_secs(3600))
        .with_acquisition_stats(200, 3)
        .with_lifetime_stats(20, 5);
    rich.print(&pool.render_styled());
    plain.print(&pool.render_plain());
}
