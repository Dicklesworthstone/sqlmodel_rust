//! Logging infrastructure for sqlmodel-console.
//!
//! This module provides lightweight logging support for debugging console
//! operations without requiring external dependencies. Logging can be
//! enabled via environment variables.
//!
//! # Environment Variables
//!
//! - `SQLMODEL_LOG=1` - Enable logging output
//! - `SQLMODEL_LOG_LEVEL=debug|info|warn|error` - Set minimum log level
//!
//! # Usage
//!
//! ```rust,ignore
//! use sqlmodel_console::logging::{log_debug, log_info};
//!
//! log_info!("Initializing console");
//! log_debug!("Mode detected: {}", mode);
//! ```
//!
//! In tests, use `with_logging_enabled` to capture logs:
//!
//! ```rust,ignore
//! use sqlmodel_console::logging::with_logging_enabled;
//!
//! with_logging_enabled(|| {
//!     // Your code here - logs will be printed to stderr
//! });
//! ```

use std::env;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

/// Log levels for console operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum LogLevel {
    /// Detailed trace information (most verbose).
    Trace = 0,
    /// Debug information for development.
    Debug = 1,
    /// General information about operations.
    Info = 2,
    /// Warnings about potential issues.
    Warn = 3,
    /// Errors that occurred during operations.
    Error = 4,
    /// No logging (disabled).
    Off = 5,
}

impl LogLevel {
    /// Parse a log level from a string.
    ///
    /// Accepts: "trace", "debug", "info", "warn", "error", "off" (case-insensitive).
    #[must_use]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "trace" => Some(Self::Trace),
            "debug" => Some(Self::Debug),
            "info" => Some(Self::Info),
            "warn" | "warning" => Some(Self::Warn),
            "error" => Some(Self::Error),
            "off" | "none" => Some(Self::Off),
            _ => None,
        }
    }

    /// Get the level name as a string.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Trace => "TRACE",
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
            Self::Off => "OFF",
        }
    }
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// Global logging state
static LOGGING_ENABLED: AtomicBool = AtomicBool::new(false);
static MIN_LOG_LEVEL: AtomicU8 = AtomicU8::new(LogLevel::Info as u8);

/// Check if logging is enabled.
#[must_use]
pub fn is_logging_enabled() -> bool {
    LOGGING_ENABLED.load(Ordering::Relaxed)
}

/// Get the current minimum log level.
#[must_use]
pub fn min_log_level() -> LogLevel {
    match MIN_LOG_LEVEL.load(Ordering::Relaxed) {
        0 => LogLevel::Trace,
        1 => LogLevel::Debug,
        2 => LogLevel::Info,
        3 => LogLevel::Warn,
        4 => LogLevel::Error,
        _ => LogLevel::Off,
    }
}

/// Initialize logging from environment variables.
///
/// Called automatically on first log attempt, but can be called
/// explicitly to ensure logging is set up early.
pub fn init_logging() {
    let enabled = env::var("SQLMODEL_LOG").is_ok_and(|v| {
        let v = v.to_lowercase();
        v == "1" || v == "true" || v == "yes" || v == "on"
    });

    LOGGING_ENABLED.store(enabled, Ordering::Relaxed);

    if let Ok(level_str) = env::var("SQLMODEL_LOG_LEVEL")
        && let Some(level) = LogLevel::from_str(&level_str)
    {
        MIN_LOG_LEVEL.store(level as u8, Ordering::Relaxed);
    }
}

/// Enable logging programmatically (useful for tests).
pub fn enable_logging() {
    LOGGING_ENABLED.store(true, Ordering::Relaxed);
}

/// Disable logging programmatically.
pub fn disable_logging() {
    LOGGING_ENABLED.store(false, Ordering::Relaxed);
}

/// Set the minimum log level programmatically.
pub fn set_log_level(level: LogLevel) {
    MIN_LOG_LEVEL.store(level as u8, Ordering::Relaxed);
}

/// Run a closure with logging enabled, then restore previous state.
///
/// Useful for tests that need to capture log output.
pub fn with_logging_enabled<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    let was_enabled = LOGGING_ENABLED.swap(true, Ordering::Relaxed);
    let prev_level = MIN_LOG_LEVEL.swap(LogLevel::Trace as u8, Ordering::Relaxed);
    let result = f();
    LOGGING_ENABLED.store(was_enabled, Ordering::Relaxed);
    MIN_LOG_LEVEL.store(prev_level, Ordering::Relaxed);
    result
}

/// Internal logging function - use the macros instead.
#[doc(hidden)]
pub fn log_impl(level: LogLevel, module: &str, message: &str) {
    if !is_logging_enabled() {
        return;
    }
    if level < min_log_level() {
        return;
    }
    eprintln!("[sqlmodel-console] [{level}] [{module}] {message}");
}

/// Log a trace message.
#[macro_export]
macro_rules! log_trace {
    ($($arg:tt)*) => {
        $crate::logging::log_impl(
            $crate::logging::LogLevel::Trace,
            module_path!(),
            &format!($($arg)*)
        )
    };
}

/// Log a debug message.
#[macro_export]
macro_rules! log_debug {
    ($($arg:tt)*) => {
        $crate::logging::log_impl(
            $crate::logging::LogLevel::Debug,
            module_path!(),
            &format!($($arg)*)
        )
    };
}

/// Log an info message.
#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => {
        $crate::logging::log_impl(
            $crate::logging::LogLevel::Info,
            module_path!(),
            &format!($($arg)*)
        )
    };
}

/// Log a warning message.
#[macro_export]
macro_rules! log_warn {
    ($($arg:tt)*) => {
        $crate::logging::log_impl(
            $crate::logging::LogLevel::Warn,
            module_path!(),
            &format!($($arg)*)
        )
    };
}

/// Log an error message.
#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => {
        $crate::logging::log_impl(
            $crate::logging::LogLevel::Error,
            module_path!(),
            &format!($($arg)*)
        )
    };
}

// Re-export macros at crate root
pub use log_debug;
pub use log_error;
pub use log_info;
pub use log_trace;
pub use log_warn;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_level_ordering() {
        assert!(LogLevel::Trace < LogLevel::Debug);
        assert!(LogLevel::Debug < LogLevel::Info);
        assert!(LogLevel::Info < LogLevel::Warn);
        assert!(LogLevel::Warn < LogLevel::Error);
        assert!(LogLevel::Error < LogLevel::Off);
    }

    #[test]
    fn test_log_level_from_str() {
        assert_eq!(LogLevel::from_str("trace"), Some(LogLevel::Trace));
        assert_eq!(LogLevel::from_str("DEBUG"), Some(LogLevel::Debug));
        assert_eq!(LogLevel::from_str("Info"), Some(LogLevel::Info));
        assert_eq!(LogLevel::from_str("WARN"), Some(LogLevel::Warn));
        assert_eq!(LogLevel::from_str("warning"), Some(LogLevel::Warn));
        assert_eq!(LogLevel::from_str("error"), Some(LogLevel::Error));
        assert_eq!(LogLevel::from_str("off"), Some(LogLevel::Off));
        assert_eq!(LogLevel::from_str("invalid"), None);
    }

    #[test]
    fn test_log_level_as_str() {
        assert_eq!(LogLevel::Trace.as_str(), "TRACE");
        assert_eq!(LogLevel::Debug.as_str(), "DEBUG");
        assert_eq!(LogLevel::Info.as_str(), "INFO");
        assert_eq!(LogLevel::Warn.as_str(), "WARN");
        assert_eq!(LogLevel::Error.as_str(), "ERROR");
        assert_eq!(LogLevel::Off.as_str(), "OFF");
    }

    #[test]
    fn test_with_logging_enabled() {
        let was_enabled = is_logging_enabled();

        with_logging_enabled(|| {
            assert!(is_logging_enabled());
        });

        assert_eq!(is_logging_enabled(), was_enabled);
    }

    #[test]
    fn test_enable_disable_logging() {
        let original = is_logging_enabled();

        enable_logging();
        assert!(is_logging_enabled());

        disable_logging();
        assert!(!is_logging_enabled());

        // Restore original state
        if original {
            enable_logging();
        } else {
            disable_logging();
        }
    }

    #[test]
    fn test_set_log_level() {
        let original = min_log_level();

        set_log_level(LogLevel::Debug);
        assert_eq!(min_log_level(), LogLevel::Debug);

        set_log_level(LogLevel::Error);
        assert_eq!(min_log_level(), LogLevel::Error);

        // Restore
        set_log_level(original);
    }

    #[test]
    fn test_log_macros_compile() {
        // Just verify the macros compile - they won't actually log
        // unless SQLMODEL_LOG is set
        log_trace!("trace message: {}", 42);
        log_debug!("debug message: {}", "test");
        log_info!("info message");
        log_warn!("warn message");
        log_error!("error message: {:?}", vec![1, 2, 3]);
    }
}
