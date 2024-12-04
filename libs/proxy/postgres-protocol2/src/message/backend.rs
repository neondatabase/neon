#![allow(missing_docs)]

use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use bytes::{Bytes, BytesMut};
use fallible_iterator::FallibleIterator;
use memchr::memchr;
use std::cmp;
use std::io::{self, Read};
use std::ops::Range;
use std::str;

use crate::Oid;

// top-level message tags
const PARSE_COMPLETE_TAG: u8 = b'1';
const BIND_COMPLETE_TAG: u8 = b'2';
const CLOSE_COMPLETE_TAG: u8 = b'3';
pub const NOTIFICATION_RESPONSE_TAG: u8 = b'A';
const COPY_DONE_TAG: u8 = b'c';
const COMMAND_COMPLETE_TAG: u8 = b'C';
const COPY_DATA_TAG: u8 = b'd';
const DATA_ROW_TAG: u8 = b'D';
const ERROR_RESPONSE_TAG: u8 = b'E';
const COPY_IN_RESPONSE_TAG: u8 = b'G';
const COPY_OUT_RESPONSE_TAG: u8 = b'H';
const COPY_BOTH_RESPONSE_TAG: u8 = b'W';
const EMPTY_QUERY_RESPONSE_TAG: u8 = b'I';
const BACKEND_KEY_DATA_TAG: u8 = b'K';
pub const NO_DATA_TAG: u8 = b'n';
pub const NOTICE_RESPONSE_TAG: u8 = b'N';
const AUTHENTICATION_TAG: u8 = b'R';
const PORTAL_SUSPENDED_TAG: u8 = b's';
pub const PARAMETER_STATUS_TAG: u8 = b'S';
const PARAMETER_DESCRIPTION_TAG: u8 = b't';
const ROW_DESCRIPTION_TAG: u8 = b'T';
pub const READY_FOR_QUERY_TAG: u8 = b'Z';

#[derive(Debug, Copy, Clone)]
pub struct Header {
    tag: u8,
    len: i32,
}

#[allow(clippy::len_without_is_empty)]
impl Header {
    #[inline]
    pub fn parse(buf: &[u8]) -> io::Result<Option<Header>> {
        if buf.len() < 5 {
            return Ok(None);
        }

        let tag = buf[0];
        let len = BigEndian::read_i32(&buf[1..]);

        if len < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid message length: header length < 4",
            ));
        }

        Ok(Some(Header { tag, len }))
    }

    #[inline]
    pub fn tag(self) -> u8 {
        self.tag
    }

    #[inline]
    pub fn len(self) -> i32 {
        self.len
    }
}

/// An enum representing Postgres backend messages.
#[non_exhaustive]
pub enum Message {
    AuthenticationCleartextPassword,
    AuthenticationGss,
    AuthenticationKerberosV5,
    AuthenticationMd5Password,
    AuthenticationOk,
    AuthenticationScmCredential,
    AuthenticationSspi,
    AuthenticationGssContinue,
    AuthenticationSasl(AuthenticationSaslBody),
    AuthenticationSaslContinue(AuthenticationSaslContinueBody),
    AuthenticationSaslFinal(AuthenticationSaslFinalBody),
    BackendKeyData(BackendKeyDataBody),
    BindComplete,
    CloseComplete,
    CommandComplete(CommandCompleteBody),
    CopyData,
    CopyDone,
    CopyInResponse,
    CopyOutResponse,
    CopyBothResponse,
    DataRow(DataRowBody),
    EmptyQueryResponse,
    ErrorResponse(ErrorResponseBody),
    NoData,
    NoticeResponse(NoticeResponseBody),
    NotificationResponse(NotificationResponseBody),
    ParameterDescription(ParameterDescriptionBody),
    ParameterStatus(ParameterStatusBody),
    ParseComplete,
    PortalSuspended,
    ReadyForQuery(ReadyForQueryBody),
    RowDescription(RowDescriptionBody),
}

impl Message {
    #[inline]
    pub fn parse(buf: &mut BytesMut) -> io::Result<Option<Message>> {
        if buf.len() < 5 {
            let to_read = 5 - buf.len();
            buf.reserve(to_read);
            return Ok(None);
        }

        let tag = buf[0];
        let len = (&buf[1..5]).read_u32::<BigEndian>().unwrap();

        if len < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid message length: parsing u32",
            ));
        }

        let total_len = len as usize + 1;
        if buf.len() < total_len {
            let to_read = total_len - buf.len();
            buf.reserve(to_read);
            return Ok(None);
        }

        let mut buf = Buffer {
            bytes: buf.split_to(total_len).freeze(),
            idx: 5,
        };

        let message = match tag {
            PARSE_COMPLETE_TAG => Message::ParseComplete,
            BIND_COMPLETE_TAG => Message::BindComplete,
            CLOSE_COMPLETE_TAG => Message::CloseComplete,
            NOTIFICATION_RESPONSE_TAG => {
                let process_id = buf.read_i32::<BigEndian>()?;
                let channel = buf.read_cstr()?;
                let message = buf.read_cstr()?;
                Message::NotificationResponse(NotificationResponseBody {
                    process_id,
                    channel,
                    message,
                })
            }
            COPY_DONE_TAG => Message::CopyDone,
            COMMAND_COMPLETE_TAG => {
                let tag = buf.read_cstr()?;
                Message::CommandComplete(CommandCompleteBody { tag })
            }
            COPY_DATA_TAG => Message::CopyData,
            DATA_ROW_TAG => {
                let len = buf.read_u16::<BigEndian>()?;
                let storage = buf.read_all();
                Message::DataRow(DataRowBody { storage, len })
            }
            ERROR_RESPONSE_TAG => {
                let storage = buf.read_all();
                Message::ErrorResponse(ErrorResponseBody { storage })
            }
            COPY_IN_RESPONSE_TAG => Message::CopyInResponse,
            COPY_OUT_RESPONSE_TAG => Message::CopyOutResponse,
            COPY_BOTH_RESPONSE_TAG => Message::CopyBothResponse,
            EMPTY_QUERY_RESPONSE_TAG => Message::EmptyQueryResponse,
            BACKEND_KEY_DATA_TAG => {
                let process_id = buf.read_i32::<BigEndian>()?;
                let secret_key = buf.read_i32::<BigEndian>()?;
                Message::BackendKeyData(BackendKeyDataBody {
                    process_id,
                    secret_key,
                })
            }
            NO_DATA_TAG => Message::NoData,
            NOTICE_RESPONSE_TAG => {
                let storage = buf.read_all();
                Message::NoticeResponse(NoticeResponseBody { storage })
            }
            AUTHENTICATION_TAG => match buf.read_i32::<BigEndian>()? {
                0 => Message::AuthenticationOk,
                2 => Message::AuthenticationKerberosV5,
                3 => Message::AuthenticationCleartextPassword,
                5 => Message::AuthenticationMd5Password,
                6 => Message::AuthenticationScmCredential,
                7 => Message::AuthenticationGss,
                8 => Message::AuthenticationGssContinue,
                9 => Message::AuthenticationSspi,
                10 => {
                    let storage = buf.read_all();
                    Message::AuthenticationSasl(AuthenticationSaslBody(storage))
                }
                11 => {
                    let storage = buf.read_all();
                    Message::AuthenticationSaslContinue(AuthenticationSaslContinueBody(storage))
                }
                12 => {
                    let storage = buf.read_all();
                    Message::AuthenticationSaslFinal(AuthenticationSaslFinalBody(storage))
                }
                tag => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("unknown authentication tag `{}`", tag),
                    ));
                }
            },
            PORTAL_SUSPENDED_TAG => Message::PortalSuspended,
            PARAMETER_STATUS_TAG => {
                let name = buf.read_cstr()?;
                let value = buf.read_cstr()?;
                Message::ParameterStatus(ParameterStatusBody { name, value })
            }
            PARAMETER_DESCRIPTION_TAG => {
                let len = buf.read_u16::<BigEndian>()?;
                let storage = buf.read_all();
                Message::ParameterDescription(ParameterDescriptionBody { storage, len })
            }
            ROW_DESCRIPTION_TAG => {
                let len = buf.read_u16::<BigEndian>()?;
                let storage = buf.read_all();
                Message::RowDescription(RowDescriptionBody { storage, len })
            }
            READY_FOR_QUERY_TAG => {
                let status = buf.read_u8()?;
                Message::ReadyForQuery(ReadyForQueryBody { status })
            }
            tag => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown message tag `{}`", tag),
                ));
            }
        };

        if !buf.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid message length: expected buffer to be empty",
            ));
        }

        Ok(Some(message))
    }
}

struct Buffer {
    bytes: Bytes,
    idx: usize,
}

impl Buffer {
    #[inline]
    fn slice(&self) -> &[u8] {
        &self.bytes[self.idx..]
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.slice().is_empty()
    }

    #[inline]
    fn read_cstr(&mut self) -> io::Result<Bytes> {
        match memchr(0, self.slice()) {
            Some(pos) => {
                let start = self.idx;
                let end = start + pos;
                let cstr = self.bytes.slice(start..end);
                self.idx = end + 1;
                Ok(cstr)
            }
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            )),
        }
    }

    #[inline]
    fn read_all(&mut self) -> Bytes {
        let buf = self.bytes.slice(self.idx..);
        self.idx = self.bytes.len();
        buf
    }
}

impl Read for Buffer {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = {
            let slice = self.slice();
            let len = cmp::min(slice.len(), buf.len());
            buf[..len].copy_from_slice(&slice[..len]);
            len
        };
        self.idx += len;
        Ok(len)
    }
}

pub struct AuthenticationMd5PasswordBody {
    salt: [u8; 4],
}

impl AuthenticationMd5PasswordBody {
    #[inline]
    pub fn salt(&self) -> [u8; 4] {
        self.salt
    }
}

pub struct AuthenticationSaslBody(Bytes);

impl AuthenticationSaslBody {
    #[inline]
    pub fn mechanisms(&self) -> SaslMechanisms<'_> {
        SaslMechanisms(&self.0)
    }
}

pub struct SaslMechanisms<'a>(&'a [u8]);

impl<'a> FallibleIterator for SaslMechanisms<'a> {
    type Item = &'a str;
    type Error = io::Error;

    #[inline]
    fn next(&mut self) -> io::Result<Option<&'a str>> {
        let value_end = find_null(self.0, 0)?;
        if value_end == 0 {
            if self.0.len() != 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid message length: expected to be at end of iterator for sasl",
                ));
            }
            Ok(None)
        } else {
            let value = get_str(&self.0[..value_end])?;
            self.0 = &self.0[value_end + 1..];
            Ok(Some(value))
        }
    }
}

pub struct AuthenticationSaslContinueBody(Bytes);

impl AuthenticationSaslContinueBody {
    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.0
    }
}

pub struct AuthenticationSaslFinalBody(Bytes);

impl AuthenticationSaslFinalBody {
    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.0
    }
}

pub struct BackendKeyDataBody {
    process_id: i32,
    secret_key: i32,
}

impl BackendKeyDataBody {
    #[inline]
    pub fn process_id(&self) -> i32 {
        self.process_id
    }

    #[inline]
    pub fn secret_key(&self) -> i32 {
        self.secret_key
    }
}

pub struct CommandCompleteBody {
    tag: Bytes,
}

impl CommandCompleteBody {
    #[inline]
    pub fn tag(&self) -> io::Result<&str> {
        get_str(&self.tag)
    }
}

#[derive(Debug)]
pub struct DataRowBody {
    storage: Bytes,
    len: u16,
}

impl DataRowBody {
    #[inline]
    pub fn ranges(&self) -> DataRowRanges<'_> {
        DataRowRanges {
            buf: &self.storage,
            len: self.storage.len(),
            remaining: self.len,
        }
    }

    #[inline]
    pub fn buffer(&self) -> &[u8] {
        &self.storage
    }
}

pub struct DataRowRanges<'a> {
    buf: &'a [u8],
    len: usize,
    remaining: u16,
}

impl FallibleIterator for DataRowRanges<'_> {
    type Item = Option<Range<usize>>;
    type Error = io::Error;

    #[inline]
    fn next(&mut self) -> io::Result<Option<Option<Range<usize>>>> {
        if self.remaining == 0 {
            if self.buf.is_empty() {
                return Ok(None);
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid message length: datarowrange is not empty",
                ));
            }
        }

        self.remaining -= 1;
        let len = self.buf.read_i32::<BigEndian>()?;
        if len < 0 {
            Ok(Some(None))
        } else {
            let len = len as usize;
            if self.buf.len() < len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected EOF",
                ));
            }
            let base = self.len - self.buf.len();
            self.buf = &self.buf[len..];
            Ok(Some(Some(base..base + len)))
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.remaining as usize;
        (len, Some(len))
    }
}

pub struct ErrorResponseBody {
    storage: Bytes,
}

impl ErrorResponseBody {
    #[inline]
    pub fn fields(&self) -> ErrorFields<'_> {
        ErrorFields { buf: &self.storage }
    }
}

pub struct ErrorFields<'a> {
    buf: &'a [u8],
}

impl<'a> FallibleIterator for ErrorFields<'a> {
    type Item = ErrorField<'a>;
    type Error = io::Error;

    #[inline]
    fn next(&mut self) -> io::Result<Option<ErrorField<'a>>> {
        let type_ = self.buf.read_u8()?;
        if type_ == 0 {
            if self.buf.is_empty() {
                return Ok(None);
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid message length: error fields is not drained",
                ));
            }
        }

        let value_end = find_null(self.buf, 0)?;
        let value = get_str(&self.buf[..value_end])?;
        self.buf = &self.buf[value_end + 1..];

        Ok(Some(ErrorField { type_, value }))
    }
}

pub struct ErrorField<'a> {
    type_: u8,
    value: &'a str,
}

impl ErrorField<'_> {
    #[inline]
    pub fn type_(&self) -> u8 {
        self.type_
    }

    #[inline]
    pub fn value(&self) -> &str {
        self.value
    }
}

pub struct NoticeResponseBody {
    storage: Bytes,
}

impl NoticeResponseBody {
    #[inline]
    pub fn fields(&self) -> ErrorFields<'_> {
        ErrorFields { buf: &self.storage }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.storage
    }
}

pub struct NotificationResponseBody {
    process_id: i32,
    channel: Bytes,
    message: Bytes,
}

impl NotificationResponseBody {
    #[inline]
    pub fn process_id(&self) -> i32 {
        self.process_id
    }

    #[inline]
    pub fn channel(&self) -> io::Result<&str> {
        get_str(&self.channel)
    }

    #[inline]
    pub fn message(&self) -> io::Result<&str> {
        get_str(&self.message)
    }
}

pub struct ParameterDescriptionBody {
    storage: Bytes,
    len: u16,
}

impl ParameterDescriptionBody {
    #[inline]
    pub fn parameters(&self) -> Parameters<'_> {
        Parameters {
            buf: &self.storage,
            remaining: self.len,
        }
    }
}

pub struct Parameters<'a> {
    buf: &'a [u8],
    remaining: u16,
}

impl FallibleIterator for Parameters<'_> {
    type Item = Oid;
    type Error = io::Error;

    #[inline]
    fn next(&mut self) -> io::Result<Option<Oid>> {
        if self.remaining == 0 {
            if self.buf.is_empty() {
                return Ok(None);
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid message length: parameters is not drained",
                ));
            }
        }

        self.remaining -= 1;
        self.buf.read_u32::<BigEndian>().map(Some)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.remaining as usize;
        (len, Some(len))
    }
}

pub struct ParameterStatusBody {
    name: Bytes,
    value: Bytes,
}

impl ParameterStatusBody {
    #[inline]
    pub fn name(&self) -> io::Result<&str> {
        get_str(&self.name)
    }

    #[inline]
    pub fn value(&self) -> io::Result<&str> {
        get_str(&self.value)
    }
}

pub struct ReadyForQueryBody {
    status: u8,
}

impl ReadyForQueryBody {
    #[inline]
    pub fn status(&self) -> u8 {
        self.status
    }
}

pub struct RowDescriptionBody {
    storage: Bytes,
    len: u16,
}

impl RowDescriptionBody {
    #[inline]
    pub fn fields(&self) -> Fields<'_> {
        Fields {
            buf: &self.storage,
            remaining: self.len,
        }
    }
}

pub struct Fields<'a> {
    buf: &'a [u8],
    remaining: u16,
}

impl<'a> FallibleIterator for Fields<'a> {
    type Item = Field<'a>;
    type Error = io::Error;

    #[inline]
    fn next(&mut self) -> io::Result<Option<Field<'a>>> {
        if self.remaining == 0 {
            if self.buf.is_empty() {
                return Ok(None);
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid message length: field is not drained",
                ));
            }
        }

        self.remaining -= 1;
        let name_end = find_null(self.buf, 0)?;
        let name = get_str(&self.buf[..name_end])?;
        self.buf = &self.buf[name_end + 1..];
        let table_oid = self.buf.read_u32::<BigEndian>()?;
        let column_id = self.buf.read_i16::<BigEndian>()?;
        let type_oid = self.buf.read_u32::<BigEndian>()?;
        let type_size = self.buf.read_i16::<BigEndian>()?;
        let type_modifier = self.buf.read_i32::<BigEndian>()?;
        let format = self.buf.read_i16::<BigEndian>()?;

        Ok(Some(Field {
            name,
            table_oid,
            column_id,
            type_oid,
            type_size,
            type_modifier,
            format,
        }))
    }
}

pub struct Field<'a> {
    name: &'a str,
    table_oid: Oid,
    column_id: i16,
    type_oid: Oid,
    type_size: i16,
    type_modifier: i32,
    format: i16,
}

impl<'a> Field<'a> {
    #[inline]
    pub fn name(&self) -> &'a str {
        self.name
    }

    #[inline]
    pub fn table_oid(&self) -> Oid {
        self.table_oid
    }

    #[inline]
    pub fn column_id(&self) -> i16 {
        self.column_id
    }

    #[inline]
    pub fn type_oid(&self) -> Oid {
        self.type_oid
    }

    #[inline]
    pub fn type_size(&self) -> i16 {
        self.type_size
    }

    #[inline]
    pub fn type_modifier(&self) -> i32 {
        self.type_modifier
    }

    #[inline]
    pub fn format(&self) -> i16 {
        self.format
    }
}

#[inline]
fn find_null(buf: &[u8], start: usize) -> io::Result<usize> {
    match memchr(0, &buf[start..]) {
        Some(pos) => Ok(pos + start),
        None => Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "unexpected EOF",
        )),
    }
}

#[inline]
fn get_str(buf: &[u8]) -> io::Result<&str> {
    str::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
}
