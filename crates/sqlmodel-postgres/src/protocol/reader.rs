//! PostgreSQL message decoder.
//!
//! This module handles decoding backend messages from the wire protocol format.

use super::messages::{
    BackendMessage, ErrorFields, FieldDescription, TransactionStatus, auth_type, backend_type,
};
use std::error::Error as StdError;
use std::fmt;

/// Errors that can occur while decoding PostgreSQL protocol messages.
#[derive(Debug)]
pub enum ProtocolError {
    /// Not enough bytes to parse a full message.
    Incomplete,
    /// Invalid length prefix encountered.
    InvalidLength { length: i32 },
    /// Message exceeds configured maximum size.
    MessageTooLarge { length: usize, max: usize },
    /// Unknown message type byte.
    UnknownMessageType(u8),
    /// UTF-8 decoding error while parsing strings.
    Utf8(std::string::FromUtf8Error),
    /// Unexpected end of buffer while parsing a field.
    UnexpectedEof,
    /// Invalid field encoding or value.
    InvalidField(&'static str),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::Incomplete => write!(f, "incomplete message"),
            ProtocolError::InvalidLength { length } => {
                write!(f, "invalid message length: {}", length)
            }
            ProtocolError::MessageTooLarge { length, max } => {
                write!(f, "message too large: {} > {}", length, max)
            }
            ProtocolError::UnknownMessageType(ty) => {
                write!(f, "unknown message type: 0x{:02x}", ty)
            }
            ProtocolError::Utf8(err) => write!(f, "utf-8 error: {}", err),
            ProtocolError::UnexpectedEof => write!(f, "unexpected end of buffer"),
            ProtocolError::InvalidField(msg) => write!(f, "invalid field: {}", msg),
        }
    }
}

impl StdError for ProtocolError {}

impl From<std::string::FromUtf8Error> for ProtocolError {
    fn from(err: std::string::FromUtf8Error) -> Self {
        ProtocolError::Utf8(err)
    }
}

/// Incremental reader for PostgreSQL backend messages.
#[derive(Debug, Clone)]
pub struct MessageReader {
    buf: Vec<u8>,
    max_message_size: usize,
}

impl Default for MessageReader {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageReader {
    /// Create a new reader with a default max message size.
    pub fn new() -> Self {
        Self::with_max_size(8 * 1024 * 1024)
    }

    /// Create a new reader with a custom max message size.
    pub fn with_max_size(max_message_size: usize) -> Self {
        Self {
            buf: Vec::new(),
            max_message_size,
        }
    }

    /// Number of bytes currently buffered.
    pub fn buffered_len(&self) -> usize {
        self.buf.len()
    }

    /// Feed bytes into the reader and return any complete messages.
    pub fn feed(&mut self, data: &[u8]) -> Result<Vec<BackendMessage>, ProtocolError> {
        self.buf.extend_from_slice(data);

        let mut messages = Vec::new();
        while let Some(msg) = self.next_message()? {
            messages.push(msg);
        }
        Ok(messages)
    }

    /// Attempt to parse the next message from the internal buffer.
    pub fn next_message(&mut self) -> Result<Option<BackendMessage>, ProtocolError> {
        if self.buf.len() < 5 {
            return Ok(None);
        }

        let length = i32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]);
        if length < 4 {
            return Err(ProtocolError::InvalidLength { length });
        }

        let total_len = length as usize + 1;
        if total_len > self.max_message_size {
            return Err(ProtocolError::MessageTooLarge {
                length: total_len,
                max: self.max_message_size,
            });
        }

        if self.buf.len() < total_len {
            return Ok(None);
        }

        let frame = self.buf[..total_len].to_vec();
        self.buf.drain(..total_len);
        Ok(Some(Self::parse_message(&frame)?))
    }

    /// Parse a single full message frame (type + length + payload).
    pub fn parse_message(frame: &[u8]) -> Result<BackendMessage, ProtocolError> {
        if frame.len() < 5 {
            return Err(ProtocolError::Incomplete);
        }

        let ty = frame[0];
        let length = i32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]);
        if length < 4 {
            return Err(ProtocolError::InvalidLength { length });
        }

        let total_len = length as usize + 1;
        if frame.len() < total_len {
            return Err(ProtocolError::Incomplete);
        }

        let payload = &frame[5..total_len];
        let mut cur = Cursor::new(payload);

        match ty {
            backend_type::AUTHENTICATION => parse_authentication(&mut cur),
            backend_type::BACKEND_KEY_DATA => parse_backend_key_data(&mut cur),
            backend_type::PARAMETER_STATUS => parse_parameter_status(&mut cur),
            backend_type::READY_FOR_QUERY => parse_ready_for_query(&mut cur),
            backend_type::ROW_DESCRIPTION => parse_row_description(&mut cur),
            backend_type::DATA_ROW => parse_data_row(&mut cur),
            backend_type::COMMAND_COMPLETE => parse_command_complete(&mut cur),
            backend_type::EMPTY_QUERY => Ok(BackendMessage::EmptyQueryResponse),
            backend_type::PARSE_COMPLETE => Ok(BackendMessage::ParseComplete),
            backend_type::BIND_COMPLETE => Ok(BackendMessage::BindComplete),
            backend_type::CLOSE_COMPLETE => Ok(BackendMessage::CloseComplete),
            backend_type::PARAMETER_DESCRIPTION => parse_parameter_description(&mut cur),
            backend_type::NO_DATA => Ok(BackendMessage::NoData),
            backend_type::PORTAL_SUSPENDED => Ok(BackendMessage::PortalSuspended),
            backend_type::ERROR_RESPONSE => parse_error_response(&mut cur, true),
            backend_type::NOTICE_RESPONSE => parse_error_response(&mut cur, false),
            backend_type::COPY_IN_RESPONSE => parse_copy_in_response(&mut cur),
            backend_type::COPY_OUT_RESPONSE => parse_copy_out_response(&mut cur),
            backend_type::COPY_BOTH_RESPONSE => parse_copy_both_response(&mut cur),
            backend_type::COPY_DATA => Ok(BackendMessage::CopyData(cur.take_remaining())),
            backend_type::COPY_DONE => Ok(BackendMessage::CopyDone),
            backend_type::NOTIFICATION_RESPONSE => parse_notification_response(&mut cur),
            backend_type::FUNCTION_CALL_RESPONSE => parse_function_call_response(&mut cur),
            backend_type::NEGOTIATE_PROTOCOL_VERSION => parse_negotiate_protocol_version(&mut cur),
            _ => Err(ProtocolError::UnknownMessageType(ty)),
        }
    }
}

fn parse_authentication(cur: &mut Cursor<'_>) -> Result<BackendMessage, ProtocolError> {
    let auth_type = cur.read_i32()?;
    match auth_type {
        auth_type::OK => Ok(BackendMessage::AuthenticationOk),
        auth_type::CLEARTEXT_PASSWORD => Ok(BackendMessage::AuthenticationCleartextPassword),
        auth_type::MD5_PASSWORD => {
            let salt = cur.read_bytes(4)?;
            let mut buf = [0_u8; 4];
            buf.copy_from_slice(salt);
            Ok(BackendMessage::AuthenticationMD5Password(buf))
        }
        auth_type::SASL => {
            let mut mechanisms = Vec::new();
            loop {
                let mech = cur.read_cstring()?;
                if mech.is_empty() {
                    break;
                }
                mechanisms.push(mech);
            }
            Ok(BackendMessage::AuthenticationSASL(mechanisms))
        }
        auth_type::SASL_CONTINUE => Ok(BackendMessage::AuthenticationSASLContinue(
            cur.take_remaining(),
        )),
        auth_type::SASL_FINAL => Ok(BackendMessage::AuthenticationSASLFinal(
            cur.take_remaining(),
        )),
        _ => Err(ProtocolError::InvalidField("unknown auth type")),
    }
}

fn parse_backend_key_data(cur: &mut Cursor<'_>) -> Result<BackendMessage, ProtocolError> {
    let process_id = cur.read_i32()?;
    let secret_key = cur.read_i32()?;
    Ok(BackendMessage::BackendKeyData {
        process_id,
        secret_key,
    })
}

fn parse_parameter_status(cur: &mut Cursor<'_>) -> Result<BackendMessage, ProtocolError> {
    let name = cur.read_cstring()?;
    let value = cur.read_cstring()?;
    Ok(BackendMessage::ParameterStatus { name, value })
}

fn parse_ready_for_query(cur: &mut Cursor<'_>) -> Result<BackendMessage, ProtocolError> {
    let status = cur.read_u8()?;
    let status = TransactionStatus::from_byte(status)
        .ok_or(ProtocolError::InvalidField("invalid transaction status"))?;
    Ok(BackendMessage::ReadyForQuery(status))
}

fn parse_row_description(cur: &mut Cursor<'_>) -> Result<BackendMessage, ProtocolError> {
    let count = cur.read_i16()?;
    let mut fields = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let name = cur.read_cstring()?;
        let table_oid = cur.read_u32()?;
        let column_id = cur.read_i16()?;
        let type_oid = cur.read_u32()?;
        let type_size = cur.read_i16()?;
        let type_modifier = cur.read_i32()?;
        let format = cur.read_i16()?;
        fields.push(FieldDescription {
            name,
            table_oid,
            column_id,
            type_oid,
            type_size,
            type_modifier,
            format,
        });
    }
    Ok(BackendMessage::RowDescription(fields))
}

fn parse_data_row(cur: &mut Cursor<'_>) -> Result<BackendMessage, ProtocolError> {
    let count = cur.read_i16()?;
    let mut values = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let len = cur.read_i32()?;
        if len == -1 {
            values.push(None);
            continue;
        }
        if len < 0 {
            return Err(ProtocolError::InvalidField("negative data length"));
        }
        let bytes = cur.read_bytes(len as usize)?.to_vec();
        values.push(Some(bytes));
    }
    Ok(BackendMessage::DataRow(values))
}

fn parse_command_complete(cur: &mut Cursor<'_>) -> Result<BackendMessage, ProtocolError> {
    let tag = cur.read_cstring()?;
    Ok(BackendMessage::CommandComplete(tag))
}

fn parse_parameter_description(cur: &mut Cursor<'_>) -> Result<BackendMessage, ProtocolError> {
    let count = cur.read_i16()?;
    let mut oids = Vec::with_capacity(count as usize);
    for _ in 0..count {
        oids.push(cur.read_u32()?);
    }
    Ok(BackendMessage::ParameterDescription(oids))
}

fn parse_copy_in_response(cur: &mut Cursor<'_>) -> Result<BackendMessage, ProtocolError> {
    let format = cur.read_i8()?;
    let column_formats = read_column_formats(cur)?;
    Ok(BackendMessage::CopyInResponse {
        format,
        column_formats,
    })
}

fn parse_copy_out_response(cur: &mut Cursor<'_>) -> Result<BackendMessage, ProtocolError> {
    let format = cur.read_i8()?;
    let column_formats = read_column_formats(cur)?;
    Ok(BackendMessage::CopyOutResponse {
        format,
        column_formats,
    })
}

fn parse_copy_both_response(cur: &mut Cursor<'_>) -> Result<BackendMessage, ProtocolError> {
    let format = cur.read_i8()?;
    let column_formats = read_column_formats(cur)?;
    Ok(BackendMessage::CopyBothResponse {
        format,
        column_formats,
    })
}

fn read_column_formats(cur: &mut Cursor<'_>) -> Result<Vec<i16>, ProtocolError> {
    let count = cur.read_i16()?;
    let mut formats = Vec::with_capacity(count as usize);
    for _ in 0..count {
        formats.push(cur.read_i16()?);
    }
    Ok(formats)
}

fn parse_notification_response(cur: &mut Cursor<'_>) -> Result<BackendMessage, ProtocolError> {
    let process_id = cur.read_i32()?;
    let channel = cur.read_cstring()?;
    let payload = cur.read_cstring()?;
    Ok(BackendMessage::NotificationResponse {
        process_id,
        channel,
        payload,
    })
}

fn parse_function_call_response(cur: &mut Cursor<'_>) -> Result<BackendMessage, ProtocolError> {
    let len = cur.read_i32()?;
    if len == -1 {
        return Ok(BackendMessage::FunctionCallResponse(None));
    }
    if len < 0 {
        return Err(ProtocolError::InvalidField("negative function length"));
    }
    let bytes = cur.read_bytes(len as usize)?.to_vec();
    Ok(BackendMessage::FunctionCallResponse(Some(bytes)))
}

fn parse_negotiate_protocol_version(cur: &mut Cursor<'_>) -> Result<BackendMessage, ProtocolError> {
    let newest_minor = cur.read_i32()?;
    let count = cur.read_i32()?;
    if count < 0 {
        return Err(ProtocolError::InvalidField(
            "negative protocol option count",
        ));
    }
    let mut unrecognized = Vec::with_capacity(count as usize);
    for _ in 0..count {
        unrecognized.push(cur.read_cstring()?);
    }
    Ok(BackendMessage::NegotiateProtocolVersion {
        newest_minor,
        unrecognized,
    })
}

fn parse_error_response(
    cur: &mut Cursor<'_>,
    is_error: bool,
) -> Result<BackendMessage, ProtocolError> {
    let mut fields = ErrorFields::default();
    loop {
        let code = cur.read_u8()?;
        if code == 0 {
            break;
        }
        let value = cur.read_cstring()?;
        match code {
            b'S' => fields.severity = value,
            b'V' => fields.severity_localized = Some(value),
            b'C' => fields.code = value,
            b'M' => fields.message = value,
            b'D' => fields.detail = Some(value),
            b'H' => fields.hint = Some(value),
            b'P' => fields.position = value.parse().ok(),
            b'p' => fields.internal_position = value.parse().ok(),
            b'q' => fields.internal_query = Some(value),
            b'W' => fields.where_ = Some(value),
            b's' => fields.schema = Some(value),
            b't' => fields.table = Some(value),
            b'c' => fields.column = Some(value),
            b'd' => fields.data_type = Some(value),
            b'n' => fields.constraint = Some(value),
            b'F' => fields.file = Some(value),
            b'L' => fields.line = value.parse().ok(),
            b'R' => fields.routine = Some(value),
            _ => {
                // Ignore unknown fields.
            }
        }
    }

    if is_error {
        Ok(BackendMessage::ErrorResponse(fields))
    } else {
        Ok(BackendMessage::NoticeResponse(fields))
    }
}

#[derive(Debug)]
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    fn read_u8(&mut self) -> Result<u8, ProtocolError> {
        if self.remaining() < 1 {
            return Err(ProtocolError::UnexpectedEof);
        }
        let b = self.buf[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn read_i8(&mut self) -> Result<i8, ProtocolError> {
        let b = self.read_u8()?;
        Ok(b as i8)
    }

    fn read_i16(&mut self) -> Result<i16, ProtocolError> {
        let bytes = self.read_bytes(2)?;
        Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> Result<u32, ProtocolError> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_i32(&mut self) -> Result<i32, ProtocolError> {
        let bytes = self.read_bytes(4)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], ProtocolError> {
        if self.remaining() < n {
            return Err(ProtocolError::UnexpectedEof);
        }
        let start = self.pos;
        let end = self.pos + n;
        self.pos = end;
        Ok(&self.buf[start..end])
    }

    fn read_cstring(&mut self) -> Result<String, ProtocolError> {
        let start = self.pos;
        while self.pos < self.buf.len() && self.buf[self.pos] != 0 {
            self.pos += 1;
        }
        if self.pos >= self.buf.len() {
            return Err(ProtocolError::UnexpectedEof);
        }
        let bytes = self.buf[start..self.pos].to_vec();
        self.pos += 1; // consume null terminator
        Ok(String::from_utf8(bytes)?)
    }

    fn take_remaining(&mut self) -> Vec<u8> {
        let remaining = self.buf[self.pos..].to_vec();
        self.pos = self.buf.len();
        remaining
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_message(ty: u8, payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(ty);
        let len = (payload.len() + 4) as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(payload);
        buf
    }

    #[test]
    fn parse_auth_ok() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&auth_type::OK.to_be_bytes());
        let msg = build_message(backend_type::AUTHENTICATION, &payload);
        let decoded = MessageReader::parse_message(&msg).unwrap();
        assert!(matches!(decoded, BackendMessage::AuthenticationOk));
    }

    #[test]
    fn parse_ready_for_query() {
        let payload = [TransactionStatus::Idle.as_byte()];
        let msg = build_message(backend_type::READY_FOR_QUERY, &payload);
        let decoded = MessageReader::parse_message(&msg).unwrap();
        assert!(matches!(
            decoded,
            BackendMessage::ReadyForQuery(TransactionStatus::Idle)
        ));
    }

    #[test]
    fn parse_error_response() {
        let mut payload = Vec::new();
        payload.push(b'S');
        payload.extend_from_slice(b"ERROR\0");
        payload.push(b'C');
        payload.extend_from_slice(b"12345\0");
        payload.push(b'M');
        payload.extend_from_slice(b"bad\0");
        payload.push(0);

        let msg = build_message(backend_type::ERROR_RESPONSE, &payload);
        let decoded = MessageReader::parse_message(&msg).unwrap();
        match decoded {
            BackendMessage::ErrorResponse(fields) => {
                assert_eq!(fields.severity, "ERROR");
                assert_eq!(fields.code, "12345");
                assert_eq!(fields.message, "bad");
            }
            _ => panic!("unexpected message"),
        }
    }

    #[test]
    fn parse_data_row() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(2_i16).to_be_bytes());
        payload.extend_from_slice(&(3_i32).to_be_bytes());
        payload.extend_from_slice(b"foo");
        payload.extend_from_slice(&(-1_i32).to_be_bytes());

        let msg = build_message(backend_type::DATA_ROW, &payload);
        let decoded = MessageReader::parse_message(&msg).unwrap();
        match decoded {
            BackendMessage::DataRow(values) => {
                assert_eq!(values.len(), 2);
                assert_eq!(values[0].as_deref(), Some(b"foo".as_slice()));
                assert!(values[1].is_none());
            }
            _ => panic!("unexpected message"),
        }
    }

    #[test]
    fn reader_buffers_partial_frames() {
        let payload = [TransactionStatus::Idle.as_byte()];
        let msg = build_message(backend_type::READY_FOR_QUERY, &payload);
        let (left, right) = msg.split_at(3);

        let mut reader = MessageReader::new();
        let first = reader.feed(left).unwrap();
        assert!(first.is_empty());

        let second = reader.feed(right).unwrap();
        assert_eq!(second.len(), 1);
    }
}
