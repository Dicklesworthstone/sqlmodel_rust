use sqlmodel_console::renderables::{QueryTreeView, SqlHighlighter};
use sqlmodel_console::{OutputMode, SqlModelConsole};

fn main() {
    let rich = SqlModelConsole::with_mode(OutputMode::Rich);
    let plain = SqlModelConsole::with_mode(OutputMode::Plain);

    rich.rule(Some("Schema Tree (Preview)"));
    let tree = QueryTreeView::new("Schema: app")
        .add_child("users", ["id (PK)", "name", "email (UNIQUE)", "created_at"])
        .add_child(
            "posts",
            [
                "id (PK)",
                "user_id (FK -> users.id)",
                "title",
                "content (NULL)",
            ],
        );
    rich.print(&tree.render_styled());
    plain.print(&tree.render_plain());

    rich.rule(Some("DDL Highlight"));
    let ddl = r"
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT UNIQUE NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE posts (
  id INTEGER PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES users(id),
  title TEXT NOT NULL,
  content TEXT
);
";

    let highlighter = SqlHighlighter::new();
    rich.print(&highlighter.highlight(ddl));
    plain.print(&highlighter.plain(ddl));
}
