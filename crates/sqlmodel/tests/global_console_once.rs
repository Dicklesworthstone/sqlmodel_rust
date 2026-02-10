#![cfg(feature = "console")]

use std::sync::Arc;

use sqlmodel::{SqlModelConsole, global_console, set_global_shared_console};

#[test]
fn global_console_is_set_only_once_per_process() {
    let first = Arc::new(SqlModelConsole::new());
    set_global_shared_console(first.clone());

    let got = global_console().expect("expected global console to be set");
    assert!(Arc::ptr_eq(&first, &got));

    // Subsequent sets are ignored.
    let second = Arc::new(SqlModelConsole::new());
    set_global_shared_console(second);

    let got2 = global_console().expect("expected global console to remain set");
    assert!(Arc::ptr_eq(&first, &got2));
}
