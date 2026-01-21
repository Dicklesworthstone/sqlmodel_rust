//! SQLModel Console - Beautiful terminal output for sqlmodel_rust
//!
//! This crate provides styled console output that automatically adapts to
//! the terminal environment. When running under an AI coding agent, output
//! is plain text. When running interactively, output is richly formatted.
//!
//! # Features
//!
//! - `rich` - Enable rich formatted output with colors, tables, panels
//! - `syntax` - Enable SQL syntax highlighting (requires `rich`)
//! - `full` - Enable all features
//!
//! # Output Mode Detection
//!
//! The crate automatically detects the appropriate output mode:
//!
//! - **Plain**: AI agents (Claude Code, Codex, etc.), CI systems, piped output
//! - **Rich**: Interactive human terminal sessions
//! - **Json**: Structured output for tool integrations
//!
//! You can override detection via environment variables:
//!
//! - `SQLMODEL_PLAIN=1` - Force plain text output
//! - `SQLMODEL_RICH=1` - Force rich output (even for agents)
//! - `SQLMODEL_JSON=1` - Force JSON structured output
//!
//! # Example
//!
//! ```rust
//! use sqlmodel_console::OutputMode;
//!
//! let mode = OutputMode::detect();
//! if mode.supports_ansi() {
//!     println!("Rich formatting available!");
//! } else {
//!     println!("Plain text mode");
//! }
//! ```

// Forbid unsafe code in production, but allow in tests for env manipulation
#![cfg_attr(not(test), forbid(unsafe_code))]
#![cfg_attr(test, allow(unsafe_code))]

pub mod console;
pub mod logging;
pub mod mode;
pub mod renderables;
pub mod theme;
pub mod traits;
pub mod widgets;

// Re-export primary types
pub use console::SqlModelConsole;
pub use mode::OutputMode;
pub use theme::Theme;
pub use traits::ConsoleAware;

/// Prelude for convenient imports.
pub mod prelude {
    pub use crate::console::SqlModelConsole;
    pub use crate::mode::OutputMode;
    pub use crate::theme::Theme;
    pub use crate::traits::ConsoleAware;
}
