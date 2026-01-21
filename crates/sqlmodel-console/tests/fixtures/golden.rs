//! Golden file comparison utilities.

use std::fs;
use std::path::Path;

/// Load a golden file for comparison.
pub fn load_golden(name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/golden")
        .join(name);
    fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to load golden file {:?}: {}", path, e))
}

/// Compare output against golden file, updating if UPDATE_GOLDEN=1.
pub fn assert_golden(name: &str, actual: &str) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/golden")
        .join(name);

    if std::env::var("UPDATE_GOLDEN").is_ok() {
        fs::write(&path, actual).expect("Failed to update golden file");
        return;
    }

    let expected = load_golden(name);
    if actual != expected {
        eprintln!("Golden file mismatch: {}", name);
        eprintln!("Expected:");
        for line in expected.lines() {
            eprintln!("  {}", line);
        }
        eprintln!("Actual:");
        for line in actual.lines() {
            eprintln!("  {}", line);
        }
        panic!("Golden file mismatch");
    }
}
