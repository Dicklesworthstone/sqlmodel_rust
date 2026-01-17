//! PostgreSQL wire protocol implementation.
//!
//! PostgreSQL uses a simple message format with a type byte and length prefix.
//! This module provides encoding/decoding for all frontend and backend messages.
//!
//! # Message Format
//!
//! ## Standard Message (after startup)
//! ```text
//! +------+--------+------------------+
//! | Type | Length | Payload          |
//! | 1B   | 4B     | (Length-4) bytes |
//! +------+--------+------------------+
//! ```
//!
//! Length includes itself (4 bytes) but not the type byte.
//!
//! ## Startup Message (first message from client)
//! ```text
//! +--------+------------------+
//! | Length | Payload          |
//! | 4B     | (Length-4) bytes |
//! +--------+------------------+
//! ```
//!
//! No type byte for startup message.

mod messages;
mod reader;
mod writer;

pub use messages::*;
pub use reader::MessageReader;
pub use writer::MessageWriter;
