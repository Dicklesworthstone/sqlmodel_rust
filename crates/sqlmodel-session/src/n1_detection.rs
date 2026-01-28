//! N+1 Query Detection for SQLModel Rust.
//!
//! This module provides detection and warning for the N+1 query anti-pattern,
//! which occurs when code loads N objects and then lazily loads a relationship
//! for each, resulting in N+1 database queries instead of 2.
//!
//! # Example
//!
//! ```ignore
//! // Enable N+1 detection
//! session.enable_n1_detection(3);  // Warn after 3 lazy loads
//!
//! // This will trigger a warning:
//! for hero in &mut heroes {
//!     hero.team.load(&mut session).await?;  // N queries!
//! }
//!
//! // This is the fix:
//! session.load_many(&mut heroes, |h| &mut h.team).await?;  // 1 query
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Tracks lazy load queries for N+1 detection.
#[derive(Debug)]
pub struct N1QueryTracker {
    /// (parent_type, relationship_name) -> query count
    counts: HashMap<(&'static str, &'static str), AtomicUsize>,
    /// Threshold for warning (queries per relationship)
    threshold: usize,
    /// Whether detection is enabled
    enabled: bool,
    /// Captured call sites for debugging
    call_sites: Vec<CallSite>,
}

impl Default for N1QueryTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Information about where a lazy load was triggered.
#[derive(Debug, Clone)]
pub struct CallSite {
    /// The parent model type name
    pub parent_type: &'static str,
    /// The relationship field name
    pub relationship: &'static str,
    /// Source file where the load was triggered
    pub file: &'static str,
    /// Line number in the source file
    pub line: u32,
    /// When the load occurred
    pub timestamp: std::time::Instant,
}

/// Statistics about N+1 detection.
#[derive(Debug, Clone, Default)]
pub struct N1Stats {
    /// Total number of lazy loads recorded
    pub total_loads: usize,
    /// Number of distinct relationships loaded
    pub relationships_loaded: usize,
    /// Number of relationships that exceeded the threshold
    pub potential_n1: usize,
}

impl N1QueryTracker {
    /// Create a new tracker with default threshold (3).
    #[must_use]
    pub fn new() -> Self {
        Self {
            counts: HashMap::new(),
            threshold: 3,
            enabled: true,
            call_sites: Vec::new(),
        }
    }

    /// Set the threshold for N+1 warnings.
    ///
    /// A warning is emitted when the number of lazy loads for a single
    /// relationship reaches this threshold.
    #[must_use]
    pub fn with_threshold(mut self, threshold: usize) -> Self {
        self.threshold = threshold;
        self
    }

    /// Get the current threshold.
    #[must_use]
    pub fn threshold(&self) -> usize {
        self.threshold
    }

    /// Check if detection is enabled.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Disable N+1 detection.
    pub fn disable(&mut self) {
        self.enabled = false;
    }

    /// Enable N+1 detection.
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    /// Record a lazy load query.
    ///
    /// This should be called whenever a lazy relationship is loaded.
    /// When the count for a (parent_type, relationship) pair reaches
    /// the threshold, a warning is emitted.
    #[track_caller]
    pub fn record_load(&mut self, parent_type: &'static str, relationship: &'static str) {
        if !self.enabled {
            return;
        }

        let key = (parent_type, relationship);
        let count = self
            .counts
            .entry(key)
            .or_insert_with(|| AtomicUsize::new(0))
            .fetch_add(1, Ordering::Relaxed)
            + 1;

        // Capture call site
        let caller = std::panic::Location::caller();
        self.call_sites.push(CallSite {
            parent_type,
            relationship,
            file: caller.file(),
            line: caller.line(),
            timestamp: std::time::Instant::now(),
        });

        // Check threshold
        if count == self.threshold {
            self.emit_warning(parent_type, relationship, count);
        }
    }

    /// Emit a warning about potential N+1 query pattern.
    fn emit_warning(&self, parent_type: &'static str, relationship: &'static str, count: usize) {
        tracing::warn!(
            target: "sqlmodel::n1",
            parent = parent_type,
            relationship = relationship,
            queries = count,
            threshold = self.threshold,
            "N+1 QUERY PATTERN DETECTED! Consider using Session::load_many() for batch loading."
        );

        // Log recent call sites for this relationship
        let sites: Vec<_> = self
            .call_sites
            .iter()
            .filter(|s| s.parent_type == parent_type && s.relationship == relationship)
            .take(5)
            .collect();

        for (i, site) in sites.iter().enumerate() {
            tracing::debug!(
                target: "sqlmodel::n1",
                index = i,
                file = site.file,
                line = site.line,
                "  [{}] {}:{}",
                i,
                site.file,
                site.line
            );
        }
    }

    /// Reset all counts and call sites.
    ///
    /// Call this at the start of a new request or transaction scope.
    pub fn reset(&mut self) {
        self.counts.clear();
        self.call_sites.clear();
    }

    /// Get the current count for a specific relationship.
    #[must_use]
    pub fn count_for(&self, parent_type: &'static str, relationship: &'static str) -> usize {
        self.counts
            .get(&(parent_type, relationship))
            .map_or(0, |c| c.load(Ordering::Relaxed))
    }

    /// Get statistics about N+1 detection.
    #[must_use]
    pub fn stats(&self) -> N1Stats {
        N1Stats {
            total_loads: self
                .counts
                .values()
                .map(|c| c.load(Ordering::Relaxed))
                .sum(),
            relationships_loaded: self.counts.len(),
            potential_n1: self
                .counts
                .iter()
                .filter(|(_, c)| c.load(Ordering::Relaxed) >= self.threshold)
                .count(),
        }
    }

    /// Get all call sites (for debugging).
    #[must_use]
    pub fn call_sites(&self) -> &[CallSite] {
        &self.call_sites
    }
}

// ============================================================================
// N1DetectionScope - RAII Guard
// ============================================================================

/// Scope helper for N+1 detection tracking.
///
/// This helper captures the initial N+1 stats when created, allowing you to
/// compare against final stats and log a summary of issues detected within
/// the scope.
///
/// **Note:** This is NOT an automatic RAII guard - you must call `log_summary()`
/// manually with the final stats. For automatic logging, wrap your code in a
/// block and call `log_summary` at the end.
///
/// # Example
///
/// ```ignore
/// // Capture initial state
/// let scope = N1DetectionScope::from_tracker(session.n1_tracker());
///
/// // Do work that might cause N+1...
/// for hero in &mut heroes {
///     hero.team.load(&mut session).await?;
/// }
///
/// // Manually log summary with final stats
/// scope.log_summary(&session.n1_stats());
/// ```
pub struct N1DetectionScope {
    /// Stats captured when the scope was created (for comparison)
    initial_stats: N1Stats,
    /// Threshold used for this scope
    threshold: usize,
    /// Whether to log on drop even if no issues
    verbose: bool,
}

impl N1DetectionScope {
    /// Create a new detection scope.
    ///
    /// This does NOT automatically enable detection on a Session - the caller
    /// should have already called `session.enable_n1_detection()`. This scope
    /// captures the initial state and logs a summary on drop.
    ///
    /// # Arguments
    ///
    /// * `initial_stats` - The current N1Stats (from `session.n1_stats()`)
    /// * `threshold` - The threshold being used for detection
    #[must_use]
    pub fn new(initial_stats: N1Stats, threshold: usize) -> Self {
        tracing::debug!(
            target: "sqlmodel::n1",
            threshold = threshold,
            "N+1 detection scope started"
        );

        Self {
            initial_stats,
            threshold,
            verbose: false,
        }
    }

    /// Create a scope from a tracker reference.
    ///
    /// Convenience method that extracts stats and threshold from an existing tracker.
    #[must_use]
    pub fn from_tracker(tracker: &N1QueryTracker) -> Self {
        Self::new(tracker.stats(), tracker.threshold())
    }

    /// Enable verbose logging (log summary even if no issues).
    #[must_use]
    pub fn verbose(mut self) -> Self {
        self.verbose = true;
        self
    }

    /// Log a summary of the detection results.
    ///
    /// Called automatically on drop, but can be called manually for
    /// intermediate reporting.
    pub fn log_summary(&self, final_stats: &N1Stats) {
        let new_loads = final_stats
            .total_loads
            .saturating_sub(self.initial_stats.total_loads);
        let new_relationships = final_stats
            .relationships_loaded
            .saturating_sub(self.initial_stats.relationships_loaded);
        let new_n1 = final_stats
            .potential_n1
            .saturating_sub(self.initial_stats.potential_n1);

        if new_n1 > 0 {
            tracing::warn!(
                target: "sqlmodel::n1",
                potential_n1 = new_n1,
                total_loads = new_loads,
                relationships = new_relationships,
                threshold = self.threshold,
                "N+1 ISSUES DETECTED in this scope! Consider using Session::load_many() for batch loading."
            );
        } else if self.verbose {
            tracing::info!(
                target: "sqlmodel::n1",
                total_loads = new_loads,
                relationships = new_relationships,
                "N+1 detection scope completed (no issues)"
            );
        } else {
            tracing::debug!(
                target: "sqlmodel::n1",
                total_loads = new_loads,
                relationships = new_relationships,
                "N+1 detection scope completed (no issues)"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tracker_new_defaults() {
        let tracker = N1QueryTracker::new();
        assert_eq!(tracker.threshold(), 3);
        assert!(tracker.is_enabled());
    }

    #[test]
    fn test_tracker_with_threshold() {
        let tracker = N1QueryTracker::new().with_threshold(5);
        assert_eq!(tracker.threshold(), 5);
    }

    #[test]
    fn test_tracker_enable_disable() {
        let mut tracker = N1QueryTracker::new();
        assert!(tracker.is_enabled());

        tracker.disable();
        assert!(!tracker.is_enabled());

        tracker.enable();
        assert!(tracker.is_enabled());
    }

    #[test]
    fn test_tracker_records_single_load() {
        let mut tracker = N1QueryTracker::new();
        tracker.record_load("Hero", "team");
        assert_eq!(tracker.count_for("Hero", "team"), 1);
    }

    #[test]
    fn test_tracker_records_multiple_loads() {
        let mut tracker = N1QueryTracker::new().with_threshold(10);
        for _ in 0..5 {
            tracker.record_load("Hero", "team");
        }
        assert_eq!(tracker.count_for("Hero", "team"), 5);
    }

    #[test]
    fn test_tracker_records_multiple_relationships() {
        let mut tracker = N1QueryTracker::new();
        tracker.record_load("Hero", "team");
        tracker.record_load("Hero", "team");
        tracker.record_load("Hero", "powers");
        tracker.record_load("Team", "heroes");

        assert_eq!(tracker.count_for("Hero", "team"), 2);
        assert_eq!(tracker.count_for("Hero", "powers"), 1);
        assert_eq!(tracker.count_for("Team", "heroes"), 1);
    }

    #[test]
    fn test_tracker_disabled_no_recording() {
        let mut tracker = N1QueryTracker::new();
        tracker.disable();
        tracker.record_load("Hero", "team");
        assert_eq!(tracker.count_for("Hero", "team"), 0);
    }

    #[test]
    fn test_tracker_reset_clears_counts() {
        let mut tracker = N1QueryTracker::new();
        tracker.record_load("Hero", "team");
        tracker.record_load("Hero", "team");
        assert_eq!(tracker.count_for("Hero", "team"), 2);

        tracker.reset();
        assert_eq!(tracker.count_for("Hero", "team"), 0);
        assert!(tracker.call_sites().is_empty());
    }

    #[test]
    fn test_callsite_captures_location() {
        let mut tracker = N1QueryTracker::new();
        tracker.record_load("Hero", "team");

        assert_eq!(tracker.call_sites().len(), 1);
        let site = &tracker.call_sites()[0];
        assert_eq!(site.parent_type, "Hero");
        assert_eq!(site.relationship, "team");
        assert!(site.file.contains("n1_detection.rs"));
        assert!(site.line > 0);
    }

    #[test]
    fn test_callsite_timestamp_monotonic() {
        let mut tracker = N1QueryTracker::new();
        tracker.record_load("Hero", "team");
        tracker.record_load("Hero", "team");

        let sites = tracker.call_sites();
        assert!(sites[1].timestamp >= sites[0].timestamp);
    }

    #[test]
    fn test_stats_total_loads_accurate() {
        let mut tracker = N1QueryTracker::new().with_threshold(10);
        tracker.record_load("Hero", "team");
        tracker.record_load("Hero", "team");
        tracker.record_load("Hero", "powers");

        let stats = tracker.stats();
        assert_eq!(stats.total_loads, 3);
    }

    #[test]
    fn test_stats_relationships_count() {
        let mut tracker = N1QueryTracker::new();
        tracker.record_load("Hero", "team");
        tracker.record_load("Hero", "powers");
        tracker.record_load("Team", "heroes");

        let stats = tracker.stats();
        assert_eq!(stats.relationships_loaded, 3);
    }

    #[test]
    fn test_stats_potential_n1_count() {
        let mut tracker = N1QueryTracker::new().with_threshold(2);
        tracker.record_load("Hero", "team");
        tracker.record_load("Hero", "team"); // Reaches threshold
        tracker.record_load("Hero", "powers"); // Only 1

        let stats = tracker.stats();
        assert_eq!(stats.potential_n1, 1);
    }

    #[test]
    fn test_stats_default() {
        let stats = N1Stats::default();
        assert_eq!(stats.total_loads, 0);
        assert_eq!(stats.relationships_loaded, 0);
        assert_eq!(stats.potential_n1, 0);
    }

    // ========================================================================
    // N1DetectionScope Tests
    // ========================================================================

    #[test]
    fn test_scope_new_captures_initial_state() {
        let initial = N1Stats {
            total_loads: 5,
            relationships_loaded: 2,
            potential_n1: 1,
        };
        let scope = N1DetectionScope::new(initial.clone(), 3);
        assert_eq!(scope.initial_stats.total_loads, 5);
        assert_eq!(scope.threshold, 3);
    }

    #[test]
    fn test_scope_from_tracker() {
        let mut tracker = N1QueryTracker::new().with_threshold(5);
        tracker.record_load("Hero", "team");
        tracker.record_load("Hero", "team");

        let scope = N1DetectionScope::from_tracker(&tracker);
        assert_eq!(scope.threshold, 5);
        assert_eq!(scope.initial_stats.total_loads, 2);
    }

    #[test]
    fn test_scope_verbose_flag() {
        let initial = N1Stats::default();
        let scope = N1DetectionScope::new(initial, 3);
        assert!(!scope.verbose);

        let verbose_scope = scope.verbose();
        assert!(verbose_scope.verbose);
    }

    #[test]
    fn test_scope_log_summary_no_issues() {
        let initial = N1Stats::default();
        let scope = N1DetectionScope::new(initial, 3);

        // Final stats same as initial - no issues
        let final_stats = N1Stats {
            total_loads: 2,
            relationships_loaded: 1,
            potential_n1: 0,
        };

        // Should not panic and should log at debug level
        scope.log_summary(&final_stats);
    }

    #[test]
    fn test_scope_log_summary_with_issues() {
        let initial = N1Stats::default();
        let scope = N1DetectionScope::new(initial, 3);

        // Final stats show N+1 issues
        let final_stats = N1Stats {
            total_loads: 10,
            relationships_loaded: 2,
            potential_n1: 1,
        };

        // Should log warning
        scope.log_summary(&final_stats);
    }

    #[test]
    fn test_scope_calculates_delta() {
        let initial = N1Stats {
            total_loads: 5,
            relationships_loaded: 2,
            potential_n1: 0,
        };
        let scope = N1DetectionScope::new(initial, 3);

        let final_stats = N1Stats {
            total_loads: 15,
            relationships_loaded: 4,
            potential_n1: 2,
        };

        // The scope should calculate: 15-5=10 new loads, 4-2=2 new relationships, 2-0=2 new N+1s
        // We can't directly test the calculation, but the log_summary should use deltas
        scope.log_summary(&final_stats);
    }
}
