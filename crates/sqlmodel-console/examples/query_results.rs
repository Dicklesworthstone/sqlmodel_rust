use sqlmodel_console::renderables::{PlainFormat, QueryResultTable};
use sqlmodel_console::{OutputMode, SqlModelConsole};

fn main() {
    let rich = SqlModelConsole::with_mode(OutputMode::Rich);
    let plain = SqlModelConsole::with_mode(OutputMode::Plain);

    rich.rule(Some("Small Result Set"));
    let small = QueryResultTable::new()
        .title("Users")
        .columns(["id", "name", "email"])
        .rows([
            ["1", "Alice", "alice@example.com"],
            ["2", "Bob", "bob@example.com"],
            ["3", "Carol", "carol@example.com"],
        ])
        .timing_ms(4.2);
    rich.print(&small.render_styled());
    plain.print(&small.render_plain());

    rich.rule(Some("Wide Result Set"));
    let wide = QueryResultTable::new()
        .title("Wide Rows")
        .columns(["id", "very_long_column_name", "another_column", "notes"])
        .rows([
            ["1", "alpha", "beta", "this row is quite wide"],
            ["2", "gamma", "delta", "more text for wrapping"],
        ])
        .max_width(60);
    rich.print(&wide.render_styled());
    plain.print(&wide.render_plain());

    rich.rule(Some("Long Result Set"));
    let long = QueryResultTable::new()
        .title("Many Rows")
        .columns(["id", "value"])
        .rows([
            ["1", "row-1"],
            ["2", "row-2"],
            ["3", "row-3"],
            ["4", "row-4"],
            ["5", "row-5"],
            ["6", "row-6"],
        ])
        .max_rows(4);
    rich.print(&long.render_styled());
    plain.print(&long.render_plain());

    rich.rule(Some("Plain Formats"));
    rich.print("CSV:");
    rich.print(&small.render_plain_format(PlainFormat::Csv));
    rich.print("JSON Lines:");
    rich.print(&small.render_plain_format(PlainFormat::JsonLines));
    rich.print("JSON Array:");
    rich.print(&small.render_plain_format(PlainFormat::JsonArray));
}
