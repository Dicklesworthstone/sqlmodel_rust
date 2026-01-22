//! Global console support for SQLModel Rust.
//!
//! This module provides an optional global console pattern for users who prefer
//! simple setup without passing console through all builders.
//!
//! # Example
//!
//! ```rust,ignore
//! use sqlmodel::prelude::*;
//!
//! // Initialize global console with auto-detection
//! sqlmodel::init_auto_console();
//!
//! // All sessions now have console output automatically
//! let session = Session::builder()
//!     .build_with(connection);
//! ```
//!
//! # Precedence Rules
//!
//! Console selection follows these priorities (highest first):
//! 1. Explicit console on builder via `with_console()`
//! 2. Global console (if set via `set_global_console()` or `init_auto_console()`)
//! 3. No console (silent operation)

use std::sync::{Arc, Mutex, OnceLock};

use sqlmodel_console::SqlModelConsole;

/// Global console storage using `OnceLock` with `Mutex` for thread-safety.
///
/// We need the `Mutex` wrapper because `SqlModelConsole` contains `rich_rust::Console`
/// which has interior mutability types (`Cell`, `RefCell`) that are not `Sync`.
/// The `Mutex` provides the necessary synchronization for safe concurrent access.
static GLOBAL_CONSOLE: OnceLock<Mutex<Arc<SqlModelConsole>>> = OnceLock::new();

/// Set the global console. Can only be called once per process.
///
/// Subsequent calls are silently ignored (no panic). Use this for custom
/// console configurations.
///
/// # Example
///
/// ```rust,ignore
/// use sqlmodel::{set_global_console, SqlModelConsole, Theme};
///
/// let console = SqlModelConsole::with_theme(Theme::dark());
/// set_global_console(console);
///
/// // Subsequent calls are ignored
/// set_global_console(SqlModelConsole::new()); // No effect
/// ```
pub fn set_global_console(console: SqlModelConsole) {
    // OnceLock::set returns Err if already set, which we silently ignore
    let _ = GLOBAL_CONSOLE.set(Mutex::new(Arc::new(console)));
}

/// Set a shared global console. Can only be called once per process.
///
/// Use this when you need to share the same console instance across
/// multiple parts of your application.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use sqlmodel::{set_global_shared_console, SqlModelConsole};
///
/// let console = Arc::new(SqlModelConsole::new());
/// set_global_shared_console(console.clone());
///
/// // console can still be used elsewhere
/// console.print("Hello");
/// ```
pub fn set_global_shared_console(console: Arc<SqlModelConsole>) {
    let _ = GLOBAL_CONSOLE.set(Mutex::new(console));
}

/// Get the global console if set.
///
/// Returns `None` if no global console has been initialized.
///
/// # Example
///
/// ```rust,ignore
/// use sqlmodel::global_console;
///
/// if let Some(console) = global_console() {
///     console.print("Global console is available");
/// }
/// ```
#[must_use]
pub fn global_console() -> Option<Arc<SqlModelConsole>> {
    GLOBAL_CONSOLE
        .get()
        .and_then(|m| m.lock().ok())
        .map(|guard| Arc::clone(&guard))
}

/// Check if a global console has been initialized.
///
/// # Example
///
/// ```rust,ignore
/// use sqlmodel::{has_global_console, init_auto_console};
///
/// assert!(!has_global_console());
/// init_auto_console();
/// assert!(has_global_console());
/// ```
#[must_use]
pub fn has_global_console() -> bool {
    GLOBAL_CONSOLE.get().is_some()
}

/// Initialize global console with auto-detection.
///
/// This is the simplest way to enable console output. The console will
/// automatically detect the appropriate output mode (Rich, Plain, or Json)
/// based on the environment.
///
/// Can only be called once per process; subsequent calls are ignored.
///
/// # Example
///
/// ```rust,ignore
/// use sqlmodel::init_auto_console;
///
/// fn main() {
///     init_auto_console();
///
///     // All sqlmodel operations now have rich console output
/// }
/// ```
pub fn init_auto_console() {
    let _ = GLOBAL_CONSOLE.set(Mutex::new(Arc::new(SqlModelConsole::new())));
}

#[cfg(test)]
mod tests {
    // Note: These tests cannot fully test OnceLock behavior since it's global state
    // that persists across tests. The tests here verify the API works correctly.

    use super::*;

    #[test]
    fn test_global_console_api() {
        // Test that the API compiles and is callable
        // We can't fully test OnceLock behavior in unit tests due to global state
        let _ = has_global_console();
        let _ = global_console();
    }

    #[test]
    fn test_set_global_console_does_not_panic() {
        // Verify that calling set_global_console doesn't panic even if
        // another test already initialized it
        set_global_console(SqlModelConsole::new());
        // Should not panic on second call
        set_global_console(SqlModelConsole::new());
    }

    #[test]
    fn test_init_auto_console_does_not_panic() {
        // Verify that init_auto_console doesn't panic
        init_auto_console();
        // Should not panic on second call
        init_auto_console();
    }

    #[test]
    fn test_set_global_shared_console_does_not_panic() {
        let console = Arc::new(SqlModelConsole::new());
        set_global_shared_console(console.clone());
        // Should not panic on second call
        set_global_shared_console(console);
    }

    // Integration tests for global console behavior would need to be
    // in separate test binaries to ensure clean global state.
}
