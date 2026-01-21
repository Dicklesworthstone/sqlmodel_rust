use sqlmodel_console::renderables::{ErrorPanel, ErrorSeverity};
use sqlmodel_console::{OutputMode, SqlModelConsole};

fn main() {
    let rich = SqlModelConsole::with_mode(OutputMode::Rich);
    let plain = SqlModelConsole::with_mode(OutputMode::Plain);

    let errors = vec![
        ErrorPanel::new("Connection Failed", "Could not connect to database")
            .severity(ErrorSeverity::Critical)
            .with_detail("Connection refused (os error 111)")
            .add_context("Host: localhost:5432")
            .add_context("User: postgres")
            .with_hint("Check that the database server is running"),
        ErrorPanel::new("SQL Syntax Error", "Unexpected token near 'FORM'")
            .severity(ErrorSeverity::Error)
            .with_sql("SELECT * FORM users WHERE id = 1")
            .with_position(10)
            .with_sqlstate("42601")
            .with_hint("Did you mean 'FROM'?")
            .with_detail("Syntax error at or near \"FORM\""),
        ErrorPanel::new("Validation Error", "Invalid email format")
            .severity(ErrorSeverity::Warning)
            .with_detail("email must contain '@'")
            .add_context("Field: email")
            .add_context("Value: user-at-example.com")
            .with_hint("Use a valid email address"),
        ErrorPanel::new("Notice", "Using default transaction isolation")
            .severity(ErrorSeverity::Notice)
            .add_context("Isolation: READ COMMITTED"),
    ];

    rich.rule(Some("Error Showcase (Styled)"));
    for panel in &errors {
        rich.print(&panel.render_styled());
        rich.rule(None);
    }

    plain.rule(Some("Error Showcase (Plain)"));
    for panel in &errors {
        plain.print(&panel.render_plain());
        plain.rule(None);
    }
}
