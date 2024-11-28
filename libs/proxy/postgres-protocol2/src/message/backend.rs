#![allow(missing_docs)]

use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use bytes::{Bytes, BytesMut};
use fallible_iterator::FallibleIterator;
use memchr::memchr;
use std::cmp;
use std::io::{self, Read};
use std::ops::Range;
use std::str;
use std::time::{Duration, SystemTime};

use crate::{Lsn, Oid, PG_EPOCH};

// top-level message tags
pub const PARSE_COMPLETE_TAG: u8 = b'1';
pub const BIND_COMPLETE_TAG: u8 = b'2';
pub const CLOSE_COMPLETE_TAG: u8 = b'3';
pub const NOTIFICATION_RESPONSE_TAG: u8 = b'A';
pub const COPY_DONE_TAG: u8 = b'c';
pub const COMMAND_COMPLETE_TAG: u8 = b'C';
pub const COPY_DATA_TAG: u8 = b'd';
pub const DATA_ROW_TAG: u8 = b'D';
pub const ERROR_RESPONSE_TAG: u8 = b'E';
pub const COPY_IN_RESPONSE_TAG: u8 = b'G';
pub const COPY_OUT_RESPONSE_TAG: u8 = b'H';
pub const COPY_BOTH_RESPONSE_TAG: u8 = b'W';
pub const EMPTY_QUERY_RESPONSE_TAG: u8 = b'I';
pub const BACKEND_KEY_DATA_TAG: u8 = b'K';
pub const NO_DATA_TAG: u8 = b'n';
pub const NOTICE_RESPONSE_TAG: u8 = b'N';
pub const AUTHENTICATION_TAG: u8 = b'R';
pub const PORTAL_SUSPENDED_TAG: u8 = b's';
pub const PARAMETER_STATUS_TAG: u8 = b'S';
pub const PARAMETER_DESCRIPTION_TAG: u8 = b't';
pub const ROW_DESCRIPTION_TAG: u8 = b'T';
pub const READY_FOR_QUERY_TAG: u8 = b'Z';

// replication message tags
pub const XLOG_DATA_TAG: u8 = b'w';
pub const PRIMARY_KEEPALIVE_TAG: u8 = b'k';
pub const INTERPRETED_WAL_RECORD_TAG: u8 = b'0';

// logical replication message tags
const BEGIN_TAG: u8 = b'B';
const COMMIT_TAG: u8 = b'C';
const ORIGIN_TAG: u8 = b'O';
const RELATION_TAG: u8 = b'R';
const TYPE_TAG: u8 = b'Y';
const INSERT_TAG: u8 = b'I';
const UPDATE_TAG: u8 = b'U';
const DELETE_TAG: u8 = b'D';
const TRUNCATE_TAG: u8 = b'T';
const TUPLE_NEW_TAG: u8 = b'N';
const TUPLE_KEY_TAG: u8 = b'K';
const TUPLE_OLD_TAG: u8 = b'O';
const TUPLE_DATA_NULL_TAG: u8 = b'n';
const TUPLE_DATA_TOAST_TAG: u8 = b'u';
const TUPLE_DATA_TEXT_TAG: u8 = b't';

// replica identity tags
const REPLICA_IDENTITY_DEFAULT_TAG: u8 = b'd';
const REPLICA_IDENTITY_NOTHING_TAG: u8 = b'n';
const REPLICA_IDENTITY_FULL_TAG: u8 = b'f';
const REPLICA_IDENTITY_INDEX_TAG: u8 = b'i';

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
    AuthenticationMd5Password(AuthenticationMd5PasswordBody),
    AuthenticationOk,
    AuthenticationScmCredential,
    AuthenticationSspi,
    AuthenticationGssContinue(AuthenticationGssContinueBody),
    AuthenticationSasl(AuthenticationSaslBody),
    AuthenticationSaslContinue(AuthenticationSaslContinueBody),
    AuthenticationSaslFinal(AuthenticationSaslFinalBody),
    BackendKeyData(BackendKeyDataBody),
    BindComplete,
    CloseComplete,
    CommandComplete(CommandCompleteBody),
    CopyData(CopyDataBody),
    CopyDone,
    CopyInResponse(CopyInResponseBody),
    CopyOutResponse(CopyOutResponseBody),
    CopyBothResponse(CopyBothResponseBody),
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
            COPY_DATA_TAG => {
                let storage = buf.read_all();
                Message::CopyData(CopyDataBody { storage })
            }
            DATA_ROW_TAG => {
                let len = buf.read_u16::<BigEndian>()?;
                let storage = buf.read_all();
                Message::DataRow(DataRowBody { storage, len })
            }
            ERROR_RESPONSE_TAG => {
                let storage = buf.read_all();
                Message::ErrorResponse(ErrorResponseBody { storage })
            }
            COPY_IN_RESPONSE_TAG => {
                let format = buf.read_u8()?;
                let len = buf.read_u16::<BigEndian>()?;
                let storage = buf.read_all();
                Message::CopyInResponse(CopyInResponseBody {
                    format,
                    len,
                    storage,
                })
            }
            COPY_OUT_RESPONSE_TAG => {
                let format = buf.read_u8()?;
                let len = buf.read_u16::<BigEndian>()?;
                let storage = buf.read_all();
                Message::CopyOutResponse(CopyOutResponseBody {
                    format,
                    len,
                    storage,
                })
            }
            COPY_BOTH_RESPONSE_TAG => {
                let format = buf.read_u8()?;
                let len = buf.read_u16::<BigEndian>()?;
                let storage = buf.read_all();
                Message::CopyBothResponse(CopyBothResponseBody {
                    format,
                    len,
                    storage,
                })
            }
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
                5 => {
                    let mut salt = [0; 4];
                    buf.read_exact(&mut salt)?;
                    Message::AuthenticationMd5Password(AuthenticationMd5PasswordBody { salt })
                }
                6 => Message::AuthenticationScmCredential,
                7 => Message::AuthenticationGss,
                8 => {
                    let storage = buf.read_all();
                    Message::AuthenticationGssContinue(AuthenticationGssContinueBody(storage))
                }
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

/// An enum representing Postgres backend replication messages.
#[non_exhaustive]
#[derive(Debug)]
pub enum ReplicationMessage<D> {
    XLogData(XLogDataBody<D>),
    PrimaryKeepAlive(PrimaryKeepAliveBody),
    RawInterpretedWalRecords(RawInterpretedWalRecordsBody<D>),
}

impl ReplicationMessage<Bytes> {
    #[inline]
    pub fn parse(buf: &Bytes) -> io::Result<Self> {
        let mut buf = Buffer {
            bytes: buf.clone(),
            idx: 0,
        };

        let tag = buf.read_u8()?;

        let replication_message = match tag {
            XLOG_DATA_TAG => {
                let wal_start = buf.read_u64::<BigEndian>()?;
                let wal_end = buf.read_u64::<BigEndian>()?;
                let ts = buf.read_i64::<BigEndian>()?;
                let timestamp = if ts > 0 {
                    *PG_EPOCH + Duration::from_micros(ts as u64)
                } else {
                    *PG_EPOCH - Duration::from_micros(-ts as u64)
                };
                let data = buf.read_all();
                ReplicationMessage::XLogData(XLogDataBody {
                    wal_start,
                    wal_end,
                    timestamp,
                    data,
                })
            }
            PRIMARY_KEEPALIVE_TAG => {
                let wal_end = buf.read_u64::<BigEndian>()?;
                let ts = buf.read_i64::<BigEndian>()?;
                let timestamp = if ts > 0 {
                    *PG_EPOCH + Duration::from_micros(ts as u64)
                } else {
                    *PG_EPOCH - Duration::from_micros(-ts as u64)
                };
                let reply = buf.read_u8()?;
                ReplicationMessage::PrimaryKeepAlive(PrimaryKeepAliveBody {
                    wal_end,
                    timestamp,
                    reply,
                })
            }
            INTERPRETED_WAL_RECORD_TAG => {
                let streaming_lsn = buf.read_u64::<BigEndian>()?;
                let commit_lsn = buf.read_u64::<BigEndian>()?;

                ReplicationMessage::RawInterpretedWalRecords(RawInterpretedWalRecordsBody {
                    streaming_lsn,
                    commit_lsn,
                    data: buf.read_all(),
                })
            }
            tag => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown replication message tag `{}`", tag),
                ));
            }
        };

        Ok(replication_message)
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

pub struct AuthenticationGssContinueBody(Bytes);

impl AuthenticationGssContinueBody {
    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.0
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

pub struct CopyDataBody {
    storage: Bytes,
}

impl CopyDataBody {
    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.storage
    }

    #[inline]
    pub fn into_bytes(self) -> Bytes {
        self.storage
    }
}

pub struct CopyInResponseBody {
    format: u8,
    len: u16,
    storage: Bytes,
}

impl CopyInResponseBody {
    #[inline]
    pub fn format(&self) -> u8 {
        self.format
    }

    #[inline]
    pub fn column_formats(&self) -> ColumnFormats<'_> {
        ColumnFormats {
            remaining: self.len,
            buf: &self.storage,
        }
    }
}

pub struct ColumnFormats<'a> {
    buf: &'a [u8],
    remaining: u16,
}

impl<'a> FallibleIterator for ColumnFormats<'a> {
    type Item = u16;
    type Error = io::Error;

    #[inline]
    fn next(&mut self) -> io::Result<Option<u16>> {
        if self.remaining == 0 {
            if self.buf.is_empty() {
                return Ok(None);
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid message length: wrong column formats",
                ));
            }
        }

        self.remaining -= 1;
        self.buf.read_u16::<BigEndian>().map(Some)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.remaining as usize;
        (len, Some(len))
    }
}

pub struct CopyOutResponseBody {
    format: u8,
    len: u16,
    storage: Bytes,
}

impl CopyOutResponseBody {
    #[inline]
    pub fn format(&self) -> u8 {
        self.format
    }

    #[inline]
    pub fn column_formats(&self) -> ColumnFormats<'_> {
        ColumnFormats {
            remaining: self.len,
            buf: &self.storage,
        }
    }
}

pub struct CopyBothResponseBody {
    storage: Bytes,
    len: u16,
    format: u8,
}

impl CopyBothResponseBody {
    #[inline]
    pub fn format(&self) -> u8 {
        self.format
    }

    #[inline]
    pub fn column_formats(&self) -> ColumnFormats<'_> {
        ColumnFormats {
            remaining: self.len,
            buf: &self.storage,
        }
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

impl<'a> FallibleIterator for DataRowRanges<'a> {
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

impl<'a> ErrorField<'a> {
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

impl<'a> FallibleIterator for Parameters<'a> {
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

#[derive(Debug)]
pub struct XLogDataBody<D> {
    wal_start: u64,
    wal_end: u64,
    timestamp: SystemTime,
    data: D,
}

impl<D> XLogDataBody<D> {
    #[inline]
    pub fn wal_start(&self) -> u64 {
        self.wal_start
    }

    #[inline]
    pub fn wal_end(&self) -> u64 {
        self.wal_end
    }

    #[inline]
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }

    #[inline]
    pub fn data(&self) -> &D {
        &self.data
    }

    #[inline]
    pub fn into_data(self) -> D {
        self.data
    }

    pub fn map_data<F, D2, E>(self, f: F) -> Result<XLogDataBody<D2>, E>
    where
        F: Fn(D) -> Result<D2, E>,
    {
        let data = f(self.data)?;
        Ok(XLogDataBody {
            wal_start: self.wal_start,
            wal_end: self.wal_end,
            timestamp: self.timestamp,
            data,
        })
    }
}

#[derive(Debug)]
pub struct RawInterpretedWalRecordsBody<D> {
    streaming_lsn: u64,
    commit_lsn: u64,
    data: D,
}

impl<D> RawInterpretedWalRecordsBody<D> {
    #[inline]
    pub fn streaming_lsn(&self) -> u64 {
        self.streaming_lsn
    }

    #[inline]
    pub fn commit_lsn(&self) -> u64 {
        self.commit_lsn
    }

    #[inline]
    pub fn data(&self) -> &D {
        &self.data
    }
}

#[derive(Debug)]
pub struct PrimaryKeepAliveBody {
    wal_end: u64,
    timestamp: SystemTime,
    reply: u8,
}

impl PrimaryKeepAliveBody {
    #[inline]
    pub fn wal_end(&self) -> u64 {
        self.wal_end
    }

    #[inline]
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }

    #[inline]
    pub fn reply(&self) -> u8 {
        self.reply
    }
}

#[non_exhaustive]
/// A message of the logical replication stream
#[derive(Debug)]
pub enum LogicalReplicationMessage {
    /// A BEGIN statement
    Begin(BeginBody),
    /// A BEGIN statement
    Commit(CommitBody),
    /// An Origin replication message
    /// Note that there can be multiple Origin messages inside a single transaction.
    Origin(OriginBody),
    /// A Relation replication message
    Relation(RelationBody),
    /// A Type replication message
    Type(TypeBody),
    /// An INSERT statement
    Insert(InsertBody),
    /// An UPDATE statement
    Update(UpdateBody),
    /// A DELETE statement
    Delete(DeleteBody),
    /// A TRUNCATE statement
    Truncate(TruncateBody),
}

impl LogicalReplicationMessage {
    pub fn parse(buf: &Bytes) -> io::Result<Self> {
        let mut buf = Buffer {
            bytes: buf.clone(),
            idx: 0,
        };

        let tag = buf.read_u8()?;

        let logical_replication_message = match tag {
            BEGIN_TAG => Self::Begin(BeginBody {
                final_lsn: buf.read_u64::<BigEndian>()?,
                timestamp: buf.read_i64::<BigEndian>()?,
                xid: buf.read_u32::<BigEndian>()?,
            }),
            COMMIT_TAG => Self::Commit(CommitBody {
                flags: buf.read_i8()?,
                commit_lsn: buf.read_u64::<BigEndian>()?,
                end_lsn: buf.read_u64::<BigEndian>()?,
                timestamp: buf.read_i64::<BigEndian>()?,
            }),
            ORIGIN_TAG => Self::Origin(OriginBody {
                commit_lsn: buf.read_u64::<BigEndian>()?,
                name: buf.read_cstr()?,
            }),
            RELATION_TAG => {
                let rel_id = buf.read_u32::<BigEndian>()?;
                let namespace = buf.read_cstr()?;
                let name = buf.read_cstr()?;
                let replica_identity = match buf.read_u8()? {
                    REPLICA_IDENTITY_DEFAULT_TAG => ReplicaIdentity::Default,
                    REPLICA_IDENTITY_NOTHING_TAG => ReplicaIdentity::Nothing,
                    REPLICA_IDENTITY_FULL_TAG => ReplicaIdentity::Full,
                    REPLICA_IDENTITY_INDEX_TAG => ReplicaIdentity::Index,
                    tag => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("unknown replica identity tag `{}`", tag),
                        ));
                    }
                };
                let column_len = buf.read_i16::<BigEndian>()?;

                let mut columns = Vec::with_capacity(column_len as usize);
                for _ in 0..column_len {
                    columns.push(Column::parse(&mut buf)?);
                }

                Self::Relation(RelationBody {
                    rel_id,
                    namespace,
                    name,
                    replica_identity,
                    columns,
                })
            }
            TYPE_TAG => Self::Type(TypeBody {
                id: buf.read_u32::<BigEndian>()?,
                namespace: buf.read_cstr()?,
                name: buf.read_cstr()?,
            }),
            INSERT_TAG => {
                let rel_id = buf.read_u32::<BigEndian>()?;
                let tag = buf.read_u8()?;

                let tuple = match tag {
                    TUPLE_NEW_TAG => Tuple::parse(&mut buf)?,
                    tag => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("unexpected tuple tag `{}`", tag),
                        ));
                    }
                };

                Self::Insert(InsertBody { rel_id, tuple })
            }
            UPDATE_TAG => {
                let rel_id = buf.read_u32::<BigEndian>()?;
                let tag = buf.read_u8()?;

                let mut key_tuple = None;
                let mut old_tuple = None;

                let new_tuple = match tag {
                    TUPLE_NEW_TAG => Tuple::parse(&mut buf)?,
                    TUPLE_OLD_TAG | TUPLE_KEY_TAG => {
                        if tag == TUPLE_OLD_TAG {
                            old_tuple = Some(Tuple::parse(&mut buf)?);
                        } else {
                            key_tuple = Some(Tuple::parse(&mut buf)?);
                        }

                        match buf.read_u8()? {
                            TUPLE_NEW_TAG => Tuple::parse(&mut buf)?,
                            tag => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    format!("unexpected tuple tag `{}`", tag),
                                ));
                            }
                        }
                    }
                    tag => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("unknown tuple tag `{}`", tag),
                        ));
                    }
                };

                Self::Update(UpdateBody {
                    rel_id,
                    key_tuple,
                    old_tuple,
                    new_tuple,
                })
            }
            DELETE_TAG => {
                let rel_id = buf.read_u32::<BigEndian>()?;
                let tag = buf.read_u8()?;

                let mut key_tuple = None;
                let mut old_tuple = None;

                match tag {
                    TUPLE_OLD_TAG => old_tuple = Some(Tuple::parse(&mut buf)?),
                    TUPLE_KEY_TAG => key_tuple = Some(Tuple::parse(&mut buf)?),
                    tag => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("unknown tuple tag `{}`", tag),
                        ));
                    }
                }

                Self::Delete(DeleteBody {
                    rel_id,
                    key_tuple,
                    old_tuple,
                })
            }
            TRUNCATE_TAG => {
                let relation_len = buf.read_i32::<BigEndian>()?;
                let options = buf.read_i8()?;

                let mut rel_ids = Vec::with_capacity(relation_len as usize);
                for _ in 0..relation_len {
                    rel_ids.push(buf.read_u32::<BigEndian>()?);
                }

                Self::Truncate(TruncateBody { options, rel_ids })
            }
            tag => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown replication message tag `{}`", tag),
                ));
            }
        };

        Ok(logical_replication_message)
    }
}

/// A row as it appears in the replication stream
#[derive(Debug)]
pub struct Tuple(Vec<TupleData>);

impl Tuple {
    #[inline]
    /// The tuple data of this tuple
    pub fn tuple_data(&self) -> &[TupleData] {
        &self.0
    }
}

impl Tuple {
    fn parse(buf: &mut Buffer) -> io::Result<Self> {
        let col_len = buf.read_i16::<BigEndian>()?;
        let mut tuple = Vec::with_capacity(col_len as usize);
        for _ in 0..col_len {
            tuple.push(TupleData::parse(buf)?);
        }

        Ok(Tuple(tuple))
    }
}

/// A column as it appears in the replication stream
#[derive(Debug)]
pub struct Column {
    flags: i8,
    name: Bytes,
    type_id: i32,
    type_modifier: i32,
}

impl Column {
    #[inline]
    /// Flags for the column. Currently can be either 0 for no flags or 1 which marks the column as
    /// part of the key.
    pub fn flags(&self) -> i8 {
        self.flags
    }

    #[inline]
    /// Name of the column.
    pub fn name(&self) -> io::Result<&str> {
        get_str(&self.name)
    }

    #[inline]
    /// ID of the column's data type.
    pub fn type_id(&self) -> i32 {
        self.type_id
    }

    #[inline]
    /// Type modifier of the column (`atttypmod`).
    pub fn type_modifier(&self) -> i32 {
        self.type_modifier
    }
}

impl Column {
    fn parse(buf: &mut Buffer) -> io::Result<Self> {
        Ok(Self {
            flags: buf.read_i8()?,
            name: buf.read_cstr()?,
            type_id: buf.read_i32::<BigEndian>()?,
            type_modifier: buf.read_i32::<BigEndian>()?,
        })
    }
}

/// The data of an individual column as it appears in the replication stream
#[derive(Debug)]
pub enum TupleData {
    /// Represents a NULL value
    Null,
    /// Represents an unchanged TOASTed value (the actual value is not sent).
    UnchangedToast,
    /// Column data as text formatted value.
    Text(Bytes),
}

impl TupleData {
    fn parse(buf: &mut Buffer) -> io::Result<Self> {
        let type_tag = buf.read_u8()?;

        let tuple = match type_tag {
            TUPLE_DATA_NULL_TAG => TupleData::Null,
            TUPLE_DATA_TOAST_TAG => TupleData::UnchangedToast,
            TUPLE_DATA_TEXT_TAG => {
                let len = buf.read_i32::<BigEndian>()?;
                let mut data = vec![0; len as usize];
                buf.read_exact(&mut data)?;
                TupleData::Text(data.into())
            }
            tag => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown replication message tag `{}`", tag),
                ));
            }
        };

        Ok(tuple)
    }
}

/// A BEGIN statement
#[derive(Debug)]
pub struct BeginBody {
    final_lsn: u64,
    timestamp: i64,
    xid: u32,
}

impl BeginBody {
    #[inline]
    /// Gets the final lsn of the transaction
    pub fn final_lsn(&self) -> Lsn {
        self.final_lsn
    }

    #[inline]
    /// Commit timestamp of the transaction. The value is in number of microseconds since PostgreSQL epoch (2000-01-01).
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    #[inline]
    /// Xid of the transaction.
    pub fn xid(&self) -> u32 {
        self.xid
    }
}

/// A COMMIT statement
#[derive(Debug)]
pub struct CommitBody {
    flags: i8,
    commit_lsn: u64,
    end_lsn: u64,
    timestamp: i64,
}

impl CommitBody {
    #[inline]
    /// The LSN of the commit.
    pub fn commit_lsn(&self) -> Lsn {
        self.commit_lsn
    }

    #[inline]
    /// The end LSN of the transaction.
    pub fn end_lsn(&self) -> Lsn {
        self.end_lsn
    }

    #[inline]
    /// Commit timestamp of the transaction. The value is in number of microseconds since PostgreSQL epoch (2000-01-01).
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    #[inline]
    /// Flags; currently unused (will be 0).
    pub fn flags(&self) -> i8 {
        self.flags
    }
}

/// An Origin replication message
///
/// Note that there can be multiple Origin messages inside a single transaction.
#[derive(Debug)]
pub struct OriginBody {
    commit_lsn: u64,
    name: Bytes,
}

impl OriginBody {
    #[inline]
    /// The LSN of the commit on the origin server.
    pub fn commit_lsn(&self) -> Lsn {
        self.commit_lsn
    }

    #[inline]
    /// Name of the origin.
    pub fn name(&self) -> io::Result<&str> {
        get_str(&self.name)
    }
}

/// Describes the REPLICA IDENTITY setting of a table
#[derive(Debug)]
pub enum ReplicaIdentity {
    /// default selection for replica identity (primary key or nothing)
    Default,
    /// no replica identity is logged for this relation
    Nothing,
    /// all columns are logged as replica identity
    Full,
    /// An explicitly chosen candidate key's columns are used as replica identity.
    /// Note this will still be set if the index has been dropped; in that case it
    /// has the same meaning as 'd'.
    Index,
}

/// A Relation replication message
#[derive(Debug)]
pub struct RelationBody {
    rel_id: u32,
    namespace: Bytes,
    name: Bytes,
    replica_identity: ReplicaIdentity,
    columns: Vec<Column>,
}

impl RelationBody {
    #[inline]
    /// ID of the relation.
    pub fn rel_id(&self) -> u32 {
        self.rel_id
    }

    #[inline]
    /// Namespace (empty string for pg_catalog).
    pub fn namespace(&self) -> io::Result<&str> {
        get_str(&self.namespace)
    }

    #[inline]
    /// Relation name.
    pub fn name(&self) -> io::Result<&str> {
        get_str(&self.name)
    }

    #[inline]
    /// Replica identity setting for the relation
    pub fn replica_identity(&self) -> &ReplicaIdentity {
        &self.replica_identity
    }

    #[inline]
    /// The column definitions of this relation
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }
}

/// A Type replication message
#[derive(Debug)]
pub struct TypeBody {
    id: u32,
    namespace: Bytes,
    name: Bytes,
}

impl TypeBody {
    #[inline]
    /// ID of the data type.
    pub fn id(&self) -> Oid {
        self.id
    }

    #[inline]
    /// Namespace (empty string for pg_catalog).
    pub fn namespace(&self) -> io::Result<&str> {
        get_str(&self.namespace)
    }

    #[inline]
    /// Name of the data type.
    pub fn name(&self) -> io::Result<&str> {
        get_str(&self.name)
    }
}

/// An INSERT statement
#[derive(Debug)]
pub struct InsertBody {
    rel_id: u32,
    tuple: Tuple,
}

impl InsertBody {
    #[inline]
    /// ID of the relation corresponding to the ID in the relation message.
    pub fn rel_id(&self) -> u32 {
        self.rel_id
    }

    #[inline]
    /// The inserted tuple
    pub fn tuple(&self) -> &Tuple {
        &self.tuple
    }
}

/// An UPDATE statement
#[derive(Debug)]
pub struct UpdateBody {
    rel_id: u32,
    old_tuple: Option<Tuple>,
    key_tuple: Option<Tuple>,
    new_tuple: Tuple,
}

impl UpdateBody {
    #[inline]
    /// ID of the relation corresponding to the ID in the relation message.
    pub fn rel_id(&self) -> u32 {
        self.rel_id
    }

    #[inline]
    /// This field is optional and is only present if the update changed data in any of the
    /// column(s) that are part of the REPLICA IDENTITY index.
    pub fn key_tuple(&self) -> Option<&Tuple> {
        self.key_tuple.as_ref()
    }

    #[inline]
    /// This field is optional and is only present if table in which the update happened has
    /// REPLICA IDENTITY set to FULL.
    pub fn old_tuple(&self) -> Option<&Tuple> {
        self.old_tuple.as_ref()
    }

    #[inline]
    /// The new tuple
    pub fn new_tuple(&self) -> &Tuple {
        &self.new_tuple
    }
}

/// A DELETE statement
#[derive(Debug)]
pub struct DeleteBody {
    rel_id: u32,
    old_tuple: Option<Tuple>,
    key_tuple: Option<Tuple>,
}

impl DeleteBody {
    #[inline]
    /// ID of the relation corresponding to the ID in the relation message.
    pub fn rel_id(&self) -> u32 {
        self.rel_id
    }

    #[inline]
    /// This field is present if the table in which the delete has happened uses an index as
    /// REPLICA IDENTITY.
    pub fn key_tuple(&self) -> Option<&Tuple> {
        self.key_tuple.as_ref()
    }

    #[inline]
    /// This field is present if the table in which the delete has happened has REPLICA IDENTITY
    /// set to FULL.
    pub fn old_tuple(&self) -> Option<&Tuple> {
        self.old_tuple.as_ref()
    }
}

/// A TRUNCATE statement
#[derive(Debug)]
pub struct TruncateBody {
    options: i8,
    rel_ids: Vec<u32>,
}

impl TruncateBody {
    #[inline]
    /// The IDs of the relations corresponding to the ID in the relation messages
    pub fn rel_ids(&self) -> &[u32] {
        &self.rel_ids
    }

    #[inline]
    /// Option bits for TRUNCATE: 1 for CASCADE, 2 for RESTART IDENTITY
    pub fn options(&self) -> i8 {
        self.options
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
