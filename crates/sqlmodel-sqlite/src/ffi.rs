//! Low-level FFI bindings to libsqlite3.
//!
//! These bindings are manually written to provide full control over the
//! interface. We only expose what we need for the driver implementation.

#![allow(non_camel_case_types)]
#![allow(clippy::upper_case_acronyms)]
#![allow(clippy::unreadable_literal)] // FFI constants use standard hex format

use std::ffi::{c_char, c_double, c_int, c_void};

/// Opaque sqlite3 database connection handle.
#[repr(C)]
pub struct sqlite3 {
    _private: [u8; 0],
}

/// Opaque sqlite3_stmt prepared statement handle.
#[repr(C)]
pub struct sqlite3_stmt {
    _private: [u8; 0],
}

// SQLite result codes
pub const SQLITE_OK: c_int = 0;
pub const SQLITE_ERROR: c_int = 1;
pub const SQLITE_INTERNAL: c_int = 2;
pub const SQLITE_PERM: c_int = 3;
pub const SQLITE_ABORT: c_int = 4;
pub const SQLITE_BUSY: c_int = 5;
pub const SQLITE_LOCKED: c_int = 6;
pub const SQLITE_NOMEM: c_int = 7;
pub const SQLITE_READONLY: c_int = 8;
pub const SQLITE_INTERRUPT: c_int = 9;
pub const SQLITE_IOERR: c_int = 10;
pub const SQLITE_CORRUPT: c_int = 11;
pub const SQLITE_NOTFOUND: c_int = 12;
pub const SQLITE_FULL: c_int = 13;
pub const SQLITE_CANTOPEN: c_int = 14;
pub const SQLITE_PROTOCOL: c_int = 15;
pub const SQLITE_EMPTY: c_int = 16;
pub const SQLITE_SCHEMA: c_int = 17;
pub const SQLITE_TOOBIG: c_int = 18;
pub const SQLITE_CONSTRAINT: c_int = 19;
pub const SQLITE_MISMATCH: c_int = 20;
pub const SQLITE_MISUSE: c_int = 21;
pub const SQLITE_NOLFS: c_int = 22;
pub const SQLITE_AUTH: c_int = 23;
pub const SQLITE_FORMAT: c_int = 24;
pub const SQLITE_RANGE: c_int = 25;
pub const SQLITE_NOTADB: c_int = 26;
pub const SQLITE_NOTICE: c_int = 27;
pub const SQLITE_WARNING: c_int = 28;
pub const SQLITE_ROW: c_int = 100;
pub const SQLITE_DONE: c_int = 101;

// sqlite3_open_v2 flags
pub const SQLITE_OPEN_READONLY: c_int = 0x00000001;
pub const SQLITE_OPEN_READWRITE: c_int = 0x00000002;
pub const SQLITE_OPEN_CREATE: c_int = 0x00000004;
pub const SQLITE_OPEN_URI: c_int = 0x00000040;
pub const SQLITE_OPEN_MEMORY: c_int = 0x00000080;
pub const SQLITE_OPEN_NOMUTEX: c_int = 0x00008000;
pub const SQLITE_OPEN_FULLMUTEX: c_int = 0x00010000;
pub const SQLITE_OPEN_SHAREDCACHE: c_int = 0x00020000;
pub const SQLITE_OPEN_PRIVATECACHE: c_int = 0x00040000;

// Fundamental data types
pub const SQLITE_INTEGER: c_int = 1;
pub const SQLITE_FLOAT: c_int = 2;
pub const SQLITE_TEXT: c_int = 3;
pub const SQLITE_BLOB: c_int = 4;
pub const SQLITE_NULL: c_int = 5;

// Type alias for destructor callback
pub type sqlite3_destructor_type = Option<unsafe extern "C" fn(*mut c_void)>;

// Special destructor value that tells SQLite to copy the data immediately.
// SQLITE_TRANSIENT is defined in SQLite as ((void(*)(void*))(-1))
// We use transmute at runtime since const transmute is unstable.
/// Returns the SQLITE_TRANSIENT destructor value.
///
/// This value tells SQLite to immediately copy any bound string or blob data.
/// It is the safest option when the source data's lifetime is uncertain.
#[inline]
pub fn sqlite_transient() -> sqlite3_destructor_type {
    // SAFETY: This is how SQLite defines SQLITE_TRANSIENT in its C API.
    // The address 0xFFFF...FFFF is never a valid function pointer, and
    // SQLite specifically checks for this sentinel value.
    unsafe { std::mem::transmute(!0usize) }
}

#[link(name = "sqlite3")]
unsafe extern "C" {
    // Connection management
    pub fn sqlite3_open(filename: *const c_char, ppDb: *mut *mut sqlite3) -> c_int;

    pub fn sqlite3_open_v2(
        filename: *const c_char,
        ppDb: *mut *mut sqlite3,
        flags: c_int,
        zVfs: *const c_char,
    ) -> c_int;

    pub fn sqlite3_close(db: *mut sqlite3) -> c_int;
    pub fn sqlite3_close_v2(db: *mut sqlite3) -> c_int;

    // Error handling
    pub fn sqlite3_errmsg(db: *mut sqlite3) -> *const c_char;
    pub fn sqlite3_errcode(db: *mut sqlite3) -> c_int;
    pub fn sqlite3_extended_errcode(db: *mut sqlite3) -> c_int;
    pub fn sqlite3_errstr(errcode: c_int) -> *const c_char;

    // Statement preparation
    pub fn sqlite3_prepare_v2(
        db: *mut sqlite3,
        zSql: *const c_char,
        nByte: c_int,
        ppStmt: *mut *mut sqlite3_stmt,
        pzTail: *mut *const c_char,
    ) -> c_int;

    pub fn sqlite3_finalize(pStmt: *mut sqlite3_stmt) -> c_int;
    pub fn sqlite3_reset(pStmt: *mut sqlite3_stmt) -> c_int;
    pub fn sqlite3_clear_bindings(pStmt: *mut sqlite3_stmt) -> c_int;

    // Parameter binding
    pub fn sqlite3_bind_null(pStmt: *mut sqlite3_stmt, index: c_int) -> c_int;

    pub fn sqlite3_bind_int(pStmt: *mut sqlite3_stmt, index: c_int, value: c_int) -> c_int;

    pub fn sqlite3_bind_int64(pStmt: *mut sqlite3_stmt, index: c_int, value: i64) -> c_int;

    pub fn sqlite3_bind_double(pStmt: *mut sqlite3_stmt, index: c_int, value: c_double) -> c_int;

    pub fn sqlite3_bind_text(
        pStmt: *mut sqlite3_stmt,
        index: c_int,
        value: *const c_char,
        nBytes: c_int,
        destructor: sqlite3_destructor_type,
    ) -> c_int;

    pub fn sqlite3_bind_blob(
        pStmt: *mut sqlite3_stmt,
        index: c_int,
        value: *const c_void,
        nBytes: c_int,
        destructor: sqlite3_destructor_type,
    ) -> c_int;

    pub fn sqlite3_bind_parameter_count(pStmt: *mut sqlite3_stmt) -> c_int;
    pub fn sqlite3_bind_parameter_index(pStmt: *mut sqlite3_stmt, name: *const c_char) -> c_int;
    pub fn sqlite3_bind_parameter_name(pStmt: *mut sqlite3_stmt, index: c_int) -> *const c_char;

    // Stepping through results
    pub fn sqlite3_step(pStmt: *mut sqlite3_stmt) -> c_int;

    // Result column information
    pub fn sqlite3_column_count(pStmt: *mut sqlite3_stmt) -> c_int;
    pub fn sqlite3_column_name(pStmt: *mut sqlite3_stmt, index: c_int) -> *const c_char;
    pub fn sqlite3_column_type(pStmt: *mut sqlite3_stmt, index: c_int) -> c_int;
    pub fn sqlite3_column_decltype(pStmt: *mut sqlite3_stmt, index: c_int) -> *const c_char;

    // Result column values
    pub fn sqlite3_column_int(pStmt: *mut sqlite3_stmt, index: c_int) -> c_int;
    pub fn sqlite3_column_int64(pStmt: *mut sqlite3_stmt, index: c_int) -> i64;
    pub fn sqlite3_column_double(pStmt: *mut sqlite3_stmt, index: c_int) -> c_double;
    pub fn sqlite3_column_text(pStmt: *mut sqlite3_stmt, index: c_int) -> *const c_char;
    pub fn sqlite3_column_blob(pStmt: *mut sqlite3_stmt, index: c_int) -> *const c_void;
    pub fn sqlite3_column_bytes(pStmt: *mut sqlite3_stmt, index: c_int) -> c_int;

    // Execution helpers
    pub fn sqlite3_exec(
        db: *mut sqlite3,
        sql: *const c_char,
        callback: Option<
            unsafe extern "C" fn(*mut c_void, c_int, *mut *mut c_char, *mut *mut c_char) -> c_int,
        >,
        arg: *mut c_void,
        errmsg: *mut *mut c_char,
    ) -> c_int;

    pub fn sqlite3_free(ptr: *mut c_void);

    // Metadata
    pub fn sqlite3_changes(db: *mut sqlite3) -> c_int;
    pub fn sqlite3_total_changes(db: *mut sqlite3) -> c_int;
    pub fn sqlite3_last_insert_rowid(db: *mut sqlite3) -> i64;

    // Configuration
    pub fn sqlite3_busy_timeout(db: *mut sqlite3, ms: c_int) -> c_int;

    // Version info
    pub fn sqlite3_libversion() -> *const c_char;
    pub fn sqlite3_libversion_number() -> c_int;
}

/// Get the SQLite library version as a string.
pub fn version() -> &'static str {
    // SAFETY: sqlite3_libversion returns a static string
    unsafe {
        let ptr = sqlite3_libversion();
        std::ffi::CStr::from_ptr(ptr).to_str().unwrap_or("unknown")
    }
}

/// Get the SQLite library version as a number.
pub fn version_number() -> i32 {
    // SAFETY: sqlite3_libversion_number is always safe to call
    unsafe { sqlite3_libversion_number() }
}

/// Convert an SQLite result code to a human-readable string.
pub fn error_string(code: c_int) -> &'static str {
    // SAFETY: sqlite3_errstr returns a static string
    unsafe {
        let ptr = sqlite3_errstr(code);
        std::ffi::CStr::from_ptr(ptr)
            .to_str()
            .unwrap_or("unknown error")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        let v = version();
        assert!(!v.is_empty());
        // SQLite version should start with 3.
        assert!(v.starts_with('3'));
    }

    #[test]
    fn test_version_number() {
        let v = version_number();
        // SQLite 3.x.x version numbers are in the form 3XXYYZZ
        // e.g., 3.45.0 = 3045000
        assert!(v >= 3_000_000);
    }

    #[test]
    fn test_error_string() {
        assert_eq!(error_string(SQLITE_OK), "not an error");
        assert_eq!(error_string(SQLITE_ERROR), "SQL logic error");
        assert_eq!(error_string(SQLITE_BUSY), "database is locked");
        assert_eq!(error_string(SQLITE_CONSTRAINT), "constraint failed");
    }

    #[test]
    fn test_result_codes() {
        // Verify result code constants match expected values
        assert_eq!(SQLITE_OK, 0);
        assert_eq!(SQLITE_ROW, 100);
        assert_eq!(SQLITE_DONE, 101);
    }
}
