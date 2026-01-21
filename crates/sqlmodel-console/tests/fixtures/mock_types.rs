//! Mock implementations for testing.

use std::cell::RefCell;
use std::sync::Arc;

use sqlmodel_console::renderables::PoolStatsProvider;
use sqlmodel_console::{ConsoleAware, SqlModelConsole};

/// Mock connection that tracks console interactions.
pub struct MockConnection {
    pub console: Option<Arc<SqlModelConsole>>,
    pub status_calls: RefCell<Vec<String>>,
    pub error_calls: RefCell<Vec<String>>,
}

impl MockConnection {
    pub fn new() -> Self {
        Self {
            console: None,
            status_calls: RefCell::new(Vec::new()),
            error_calls: RefCell::new(Vec::new()),
        }
    }
}

impl ConsoleAware for MockConnection {
    fn set_console(&mut self, console: Option<Arc<SqlModelConsole>>) {
        self.console = console;
    }

    fn console(&self) -> Option<&Arc<SqlModelConsole>> {
        self.console.as_ref()
    }

    fn emit_status(&self, message: &str) {
        self.status_calls.borrow_mut().push(message.to_string());
        if let Some(console) = self.console() {
            console.status(message);
        }
    }

    fn emit_error(&self, message: &str) {
        self.error_calls.borrow_mut().push(message.to_string());
        if let Some(console) = self.console() {
            console.error(message);
        }
    }
}

/// Mock pool stats for testing pool status display.
#[derive(Debug, Clone)]
pub struct MockPoolStats {
    pub active: usize,
    pub idle: usize,
    pub max: usize,
    pub min: usize,
    pub pending: usize,
    pub created: u64,
    pub closed: u64,
    pub acquires: u64,
    pub timeouts: u64,
}

impl MockPoolStats {
    pub fn healthy() -> Self {
        Self {
            active: 2,
            idle: 8,
            max: 10,
            min: 1,
            pending: 0,
            created: 10,
            closed: 0,
            acquires: 20,
            timeouts: 0,
        }
    }

    pub fn busy() -> Self {
        Self {
            active: 8,
            idle: 2,
            max: 10,
            min: 1,
            pending: 0,
            created: 12,
            closed: 1,
            acquires: 120,
            timeouts: 0,
        }
    }

    pub fn degraded() -> Self {
        Self {
            active: 9,
            idle: 1,
            max: 10,
            min: 1,
            pending: 3,
            created: 15,
            closed: 4,
            acquires: 240,
            timeouts: 2,
        }
    }

    pub fn exhausted() -> Self {
        Self {
            active: 10,
            idle: 0,
            max: 10,
            min: 1,
            pending: 5,
            created: 20,
            closed: 10,
            acquires: 500,
            timeouts: 8,
        }
    }
}

impl PoolStatsProvider for MockPoolStats {
    fn active_connections(&self) -> usize {
        self.active
    }

    fn idle_connections(&self) -> usize {
        self.idle
    }

    fn max_connections(&self) -> usize {
        self.max
    }

    fn min_connections(&self) -> usize {
        self.min
    }

    fn pending_requests(&self) -> usize {
        self.pending
    }

    fn connections_created(&self) -> u64 {
        self.created
    }

    fn connections_closed(&self) -> u64 {
        self.closed
    }

    fn total_acquires(&self) -> u64 {
        self.acquires
    }

    fn total_timeouts(&self) -> u64 {
        self.timeouts
    }
}
