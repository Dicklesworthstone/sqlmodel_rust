//! Deterministic sample data generators.

/// Generate a set of column names: col_0, col_1, ...
pub fn generate_columns(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("col_{i}")).collect()
}

/// Generate a grid of values (r{row}c{col}).
pub fn generate_rows(rows: usize, cols: usize) -> Vec<Vec<String>> {
    (0..rows)
        .map(|r| (0..cols).map(|c| format!("r{r}c{c}")).collect())
        .collect()
}

/// Generate query results with columns and rows.
pub fn generate_query_results(rows: usize, cols: usize) -> (Vec<String>, Vec<Vec<String>>) {
    (generate_columns(cols), generate_rows(rows, cols))
}

/// Generate rows filled with a repeated value (useful for width tests).
pub fn generate_repeated_rows(rows: usize, cols: usize, value: &str) -> Vec<Vec<String>> {
    (0..rows)
        .map(|_| (0..cols).map(|_| value.to_string()).collect())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_columns() {
        let cols = generate_columns(3);
        assert_eq!(cols, vec!["col_0", "col_1", "col_2"]);
    }

    #[test]
    fn test_generate_rows() {
        let rows = generate_rows(2, 2);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec!["r0c0", "r0c1"]);
        assert_eq!(rows[1], vec!["r1c0", "r1c1"]);
    }

    #[test]
    fn test_generate_query_results() {
        let (cols, rows) = generate_query_results(1, 2);
        assert_eq!(cols, vec!["col_0", "col_1"]);
        assert_eq!(rows, vec![vec!["r0c0".to_string(), "r0c1".to_string()]]);
    }

    #[test]
    fn test_generate_repeated_rows() {
        let rows = generate_repeated_rows(2, 3, "x");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec!["x", "x", "x"]);
    }
}
