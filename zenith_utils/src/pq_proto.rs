//! Postgres protocol messages serialization-deserialization. See
//! <https://www.postgresql.org/docs/devel/protocol-message-formats.html>
//! on message formats.

use anyhow::{anyhow, bail, ensure, Result};
use byteorder::{BigEndian, ByteOrder};
use byteorder::{ReadBytesExt, BE};
use bytes::{Buf, BufMut, Bytes, BytesMut};
// use postgres_ffi::xlog_utils::TimestampTz;
use postgres_protocol::PG_EPOCH;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Read;
use std::io::{self, Cursor};
use std::str;
use std::time::{Duration, SystemTime};
use tracing::info;

pub type Oid = u32;
pub type SystemId = u64;

pub const INT8_OID: Oid = 20;
pub const INT4_OID: Oid = 23;
pub const TEXT_OID: Oid = 25;

#[derive(Debug)]
pub enum FeMessage {
    StartupPacket(FeStartupPacket),
    Query(FeQueryMessage), // Simple query
    Parse(FeParseMessage), // Extended query protocol
    Describe(FeDescribeMessage),
    Bind(FeBindMessage),
    Execute(FeExecuteMessage),
    Close(FeCloseMessage),
    Sync,
    Terminate,
    CopyData(Bytes),
    CopyDone,
    CopyFail,
    PasswordMessage(Bytes),
}

#[derive(Debug)]
pub enum FeStartupPacket {
    CancelRequest(CancelKeyData),
    SslRequest,
    GssEncRequest,
    StartupMessage {
        major_version: u32,
        minor_version: u32,
        params: HashMap<String, String>,
    },
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct CancelKeyData {
    pub backend_pid: i32,
    pub cancel_key: i32,
}

#[derive(Debug)]
pub struct FeQueryMessage {
    pub body: Bytes,
}

// We only support the simple case of Parse on unnamed prepared statement and
// no params
#[derive(Debug)]
pub struct FeParseMessage {
    pub query_string: Bytes,
}

#[derive(Debug)]
pub struct FeDescribeMessage {
    pub kind: u8, // 'S' to describe a prepared statement; or 'P' to describe a portal.
                  // we only support unnamed prepared stmt or portal
}

// we only support unnamed prepared stmt and portal
#[derive(Debug)]
pub struct FeBindMessage {}

// we only support unnamed prepared stmt or portal
#[derive(Debug)]
pub struct FeExecuteMessage {
    /// max # of rows
    pub maxrows: i32,
}

// we only support unnamed prepared stmt and portal
#[derive(Debug)]
pub struct FeCloseMessage {}

impl FeMessage {
    /// Read one message from the stream.
    /// This function returns `Ok(None)` in case of EOF.
    /// One way to handle this properly:
    ///
    /// ```
    /// # use std::io::Read;
    /// # use zenith_utils::pq_proto::FeMessage;
    /// #
    /// # fn process_message(msg: FeMessage) -> anyhow::Result<()> {
    /// #     Ok(())
    /// # };
    /// #
    /// fn do_the_job(stream: &mut impl Read) -> anyhow::Result<()> {
    ///     while let Some(msg) = FeMessage::read(stream)? {
    ///         process_message(msg)?;
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn read(stream: &mut impl Read) -> anyhow::Result<Option<FeMessage>> {
        // Each libpq message begins with a message type byte, followed by message length
        // If the client closes the connection, return None. But if the client closes the
        // connection in the middle of a message, we will return an error.
        let tag = match stream.read_u8() {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let len = stream.read_u32::<BE>()?;

        // The message length includes itself, so it better be at least 4
        let bodylen = len
            .checked_sub(4)
            .ok_or_else(|| anyhow!("invalid message length: parsing u32"))?;

        // Read message body
        let mut body_buf: Vec<u8> = vec![0; bodylen as usize];
        stream.read_exact(&mut body_buf)?;

        let body = Bytes::from(body_buf);

        // Parse it
        match tag {
            b'Q' => Ok(Some(FeMessage::Query(FeQueryMessage { body }))),
            b'P' => Ok(Some(FeParseMessage::parse(body)?)),
            b'D' => Ok(Some(FeDescribeMessage::parse(body)?)),
            b'E' => Ok(Some(FeExecuteMessage::parse(body)?)),
            b'B' => Ok(Some(FeBindMessage::parse(body)?)),
            b'C' => Ok(Some(FeCloseMessage::parse(body)?)),
            b'S' => Ok(Some(FeMessage::Sync)),
            b'X' => Ok(Some(FeMessage::Terminate)),
            b'd' => Ok(Some(FeMessage::CopyData(body))),
            b'c' => Ok(Some(FeMessage::CopyDone)),
            b'f' => Ok(Some(FeMessage::CopyFail)),
            b'p' => Ok(Some(FeMessage::PasswordMessage(body))),
            tag => Err(anyhow!("unknown message tag: {},'{:?}'", tag, body)),
        }
    }
}

impl FeStartupPacket {
    /// Read startup message from the stream.
    pub fn read(stream: &mut impl std::io::Read) -> anyhow::Result<Option<FeMessage>> {
        const MAX_STARTUP_PACKET_LENGTH: usize = 10000;
        const RESERVED_INVALID_MAJOR_VERSION: u32 = 1234;
        const CANCEL_REQUEST_CODE: u32 = 5678;
        const NEGOTIATE_SSL_CODE: u32 = 5679;
        const NEGOTIATE_GSS_CODE: u32 = 5680;

        // Read length. If the connection is closed before reading anything (or before
        // reading 4 bytes, to be precise), return None to indicate that the connection
        // was closed. This matches the PostgreSQL server's behavior, which avoids noise
        // in the log if the client opens connection but closes it immediately.
        let len = match stream.read_u32::<BE>() {
            Ok(len) => len as usize,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        if len < 4 || len > MAX_STARTUP_PACKET_LENGTH {
            bail!("invalid message length");
        }

        let request_code = stream.read_u32::<BE>()?;

        // the rest of startup packet are params
        let params_len = len - 8;
        let mut params_bytes = vec![0u8; params_len];
        stream.read_exact(params_bytes.as_mut())?;

        // Parse params depending on request code
        let most_sig_16_bits = request_code >> 16;
        let least_sig_16_bits = request_code & ((1 << 16) - 1);
        let message = match (most_sig_16_bits, least_sig_16_bits) {
            (RESERVED_INVALID_MAJOR_VERSION, CANCEL_REQUEST_CODE) => {
                ensure!(params_len == 8, "expected 8 bytes for CancelRequest params");
                let mut cursor = Cursor::new(params_bytes);
                FeStartupPacket::CancelRequest(CancelKeyData {
                    backend_pid: cursor.read_i32::<BigEndian>()?,
                    cancel_key: cursor.read_i32::<BigEndian>()?,
                })
            }
            (RESERVED_INVALID_MAJOR_VERSION, NEGOTIATE_SSL_CODE) => FeStartupPacket::SslRequest,
            (RESERVED_INVALID_MAJOR_VERSION, NEGOTIATE_GSS_CODE) => FeStartupPacket::GssEncRequest,
            (RESERVED_INVALID_MAJOR_VERSION, unrecognized_code) => {
                bail!("Unrecognized request code {}", unrecognized_code)
            }
            (major_version, minor_version) => {
                // TODO bail if protocol major_version is not 3?
                // Parse null-terminated (String) pairs of param name / param value
                let params_str = str::from_utf8(&params_bytes).unwrap();
                let mut params_tokens = params_str.split('\0');
                let mut params: HashMap<String, String> = HashMap::new();
                while let Some(name) = params_tokens.next() {
                    let value = params_tokens.next().ok_or_else(|| {
                        anyhow!("expected even number of params in StartupMessage")
                    })?;
                    if name == "options" {
                        // deprecated way of passing params as cmd line args
                        for cmdopt in value.split(' ') {
                            let nameval: Vec<&str> = cmdopt.split('=').collect();
                            if nameval.len() == 2 {
                                params.insert(nameval[0].to_string(), nameval[1].to_string());
                            }
                        }
                    } else {
                        params.insert(name.to_string(), value.to_string());
                    }
                }
                FeStartupPacket::StartupMessage {
                    major_version,
                    minor_version,
                    params,
                }
            }
        };
        Ok(Some(FeMessage::StartupPacket(message)))
    }
}

impl FeParseMessage {
    pub fn parse(mut buf: Bytes) -> anyhow::Result<FeMessage> {
        let _pstmt_name = read_null_terminated(&mut buf)?;
        let query_string = read_null_terminated(&mut buf)?;
        let nparams = buf.get_i16();

        // FIXME: the rust-postgres driver uses a named prepared statement
        // for copy_out(). We're not prepared to handle that correctly. For
        // now, just ignore the statement name, assuming that the client never
        // uses more than one prepared statement at a time.
        /*
        if !pstmt_name.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "named prepared statements not implemented in Parse",
            ));
        }
         */

        if nparams != 0 {
            bail!("query params not implemented");
        }

        Ok(FeMessage::Parse(FeParseMessage { query_string }))
    }
}

impl FeDescribeMessage {
    pub fn parse(mut buf: Bytes) -> anyhow::Result<FeMessage> {
        let kind = buf.get_u8();
        let _pstmt_name = read_null_terminated(&mut buf)?;

        // FIXME: see FeParseMessage::parse
        /*
        if !pstmt_name.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "named prepared statements not implemented in Describe",
            ));
        }
        */

        if kind != b'S' {
            bail!("only prepared statmement Describe is implemented");
        }

        Ok(FeMessage::Describe(FeDescribeMessage { kind }))
    }
}

impl FeExecuteMessage {
    pub fn parse(mut buf: Bytes) -> anyhow::Result<FeMessage> {
        let portal_name = read_null_terminated(&mut buf)?;
        let maxrows = buf.get_i32();

        if !portal_name.is_empty() {
            bail!("named portals not implemented");
        }

        if maxrows != 0 {
            bail!("row limit in Execute message not supported");
        }

        Ok(FeMessage::Execute(FeExecuteMessage { maxrows }))
    }
}

impl FeBindMessage {
    pub fn parse(mut buf: Bytes) -> anyhow::Result<FeMessage> {
        let portal_name = read_null_terminated(&mut buf)?;
        let _pstmt_name = read_null_terminated(&mut buf)?;

        if !portal_name.is_empty() {
            bail!("named portals not implemented");
        }

        // FIXME: see FeParseMessage::parse
        /*
        if !pstmt_name.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "named prepared statements not implemented",
            ));
        }
        */

        Ok(FeMessage::Bind(FeBindMessage {}))
    }
}

impl FeCloseMessage {
    pub fn parse(mut buf: Bytes) -> anyhow::Result<FeMessage> {
        let _kind = buf.get_u8();
        let _pstmt_or_portal_name = read_null_terminated(&mut buf)?;

        // FIXME: we do nothing with Close

        Ok(FeMessage::Close(FeCloseMessage {}))
    }
}

fn read_null_terminated(buf: &mut Bytes) -> anyhow::Result<Bytes> {
    let mut result = BytesMut::new();

    loop {
        if !buf.has_remaining() {
            bail!("no null-terminator in string");
        }

        let byte = buf.get_u8();

        if byte == 0 {
            break;
        }
        result.put_u8(byte);
    }
    Ok(result.freeze())
}

// Backend

#[derive(Debug)]
pub enum BeMessage<'a> {
    AuthenticationOk,
    AuthenticationMD5Password(&'a [u8; 4]),
    AuthenticationCleartextPassword,
    BackendKeyData(CancelKeyData),
    BindComplete,
    CommandComplete(&'a [u8]),
    CopyData(&'a [u8]),
    CopyDone,
    CopyFail,
    CopyInResponse,
    CopyOutResponse,
    CopyBothResponse,
    CloseComplete,
    // None means column is NULL
    DataRow(&'a [Option<&'a [u8]>]),
    ErrorResponse(String),
    // single byte - used in response to SSLRequest/GSSENCRequest
    EncryptionResponse(bool),
    NoData,
    ParameterDescription,
    ParameterStatus(BeParameterStatusMessage<'a>),
    ParseComplete,
    ReadyForQuery,
    RowDescription(&'a [RowDescriptor<'a>]),
    XLogData(XLogDataBody<'a>),
    NoticeResponse(String),
    KeepAlive(WalSndKeepAlive),
}

#[derive(Debug)]
pub enum BeParameterStatusMessage<'a> {
    Encoding(&'a str),
    ServerVersion(&'a str),
}

impl BeParameterStatusMessage<'static> {
    pub fn encoding() -> BeMessage<'static> {
        BeMessage::ParameterStatus(Self::Encoding("UTF8"))
    }
}

// One row desciption in RowDescription packet.
#[derive(Debug)]
pub struct RowDescriptor<'a> {
    pub name: &'a [u8],
    pub tableoid: Oid,
    pub attnum: i16,
    pub typoid: Oid,
    pub typlen: i16,
    pub typmod: i32,
    pub formatcode: i16,
}

impl Default for RowDescriptor<'_> {
    fn default() -> RowDescriptor<'static> {
        RowDescriptor {
            name: b"",
            tableoid: 0,
            attnum: 0,
            typoid: 0,
            typlen: 0,
            typmod: 0,
            formatcode: 0,
        }
    }
}

impl RowDescriptor<'_> {
    /// Convenience function to create a RowDescriptor message for an int8 column
    pub const fn int8_col(name: &[u8]) -> RowDescriptor {
        RowDescriptor {
            name,
            tableoid: 0,
            attnum: 0,
            typoid: INT8_OID,
            typlen: 8,
            typmod: 0,
            formatcode: 0,
        }
    }
}

#[derive(Debug)]
pub struct XLogDataBody<'a> {
    pub wal_start: u64,
    pub wal_end: u64,
    pub timestamp: i64,
    pub data: &'a [u8],
}

#[derive(Debug)]
pub struct WalSndKeepAlive {
    pub sent_ptr: u64,
    pub timestamp: i64,
    pub request_reply: bool,
}

pub static HELLO_WORLD_ROW: BeMessage = BeMessage::DataRow(&[Some(b"hello world")]);

// single text column
pub static SINGLE_COL_ROWDESC: BeMessage = BeMessage::RowDescription(&[RowDescriptor {
    name: b"data",
    tableoid: 0,
    attnum: 0,
    typoid: TEXT_OID,
    typlen: -1,
    typmod: 0,
    formatcode: 0,
}]);

// Safe usize -> i32|i16 conversion, from rust-postgres
trait FromUsize: Sized {
    fn from_usize(x: usize) -> Result<Self, io::Error>;
}

macro_rules! from_usize {
    ($t:ty) => {
        impl FromUsize for $t {
            #[inline]
            fn from_usize(x: usize) -> io::Result<$t> {
                if x > <$t>::max_value() as usize {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "value too large to transmit",
                    ))
                } else {
                    Ok(x as $t)
                }
            }
        }
    };
}

from_usize!(i32);

/// Call f() to write body of the message and prepend it with 4-byte len as
/// prescribed by the protocol.
fn write_body<F>(buf: &mut BytesMut, f: F) -> io::Result<()>
where
    F: FnOnce(&mut BytesMut) -> io::Result<()>,
{
    let base = buf.len();
    buf.extend_from_slice(&[0; 4]);

    f(buf)?;

    let size = i32::from_usize(buf.len() - base)?;
    BigEndian::write_i32(&mut buf[base..], size);
    Ok(())
}

/// Safe write of s into buf as cstring (String in the protocol).
pub fn write_cstr(s: &[u8], buf: &mut BytesMut) -> Result<(), io::Error> {
    if s.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "string contains embedded null",
        ));
    }
    buf.put_slice(s);
    buf.put_u8(0);
    Ok(())
}

// Truncate 0 from C string in Bytes and stringify it (returns slice, no allocations)
// PG protocol strings are always C strings.
fn cstr_to_str(b: &Bytes) -> Result<&str> {
    let without_null = if b.last() == Some(&0) {
        &b[..b.len() - 1]
    } else {
        &b[..]
    };
    std::str::from_utf8(without_null).map_err(|e| e.into())
}

impl<'a> BeMessage<'a> {
    /// Write message to the given buf.
    // Unlike the reading side, we use BytesMut
    // here as msg len preceeds its body and it is handy to write it down first
    // and then fill the length. With Write we would have to either calc it
    // manually or have one more buffer.
    pub fn write(buf: &mut BytesMut, message: &BeMessage) -> io::Result<()> {
        match message {
            BeMessage::AuthenticationOk => {
                buf.put_u8(b'R');
                write_body(buf, |buf| {
                    buf.put_i32(0); // Specifies that the authentication was successful.
                    Ok::<_, io::Error>(())
                })
                .unwrap(); // write into BytesMut can't fail
            }

            BeMessage::AuthenticationCleartextPassword => {
                buf.put_u8(b'R');
                write_body(buf, |buf| {
                    buf.put_i32(3); // Specifies that clear text password is required.
                    Ok::<_, io::Error>(())
                })
                .unwrap(); // write into BytesMut can't fail
            }

            BeMessage::AuthenticationMD5Password(salt) => {
                buf.put_u8(b'R');
                write_body(buf, |buf| {
                    buf.put_i32(5); // Specifies that an MD5-encrypted password is required.
                    buf.put_slice(&salt[..]);
                    Ok::<_, io::Error>(())
                })
                .unwrap(); // write into BytesMut can't fail
            }

            BeMessage::BackendKeyData(key_data) => {
                buf.put_u8(b'K');
                write_body(buf, |buf| {
                    buf.put_i32(key_data.backend_pid);
                    buf.put_i32(key_data.cancel_key);
                    Ok(())
                })
                .unwrap();
            }

            BeMessage::BindComplete => {
                buf.put_u8(b'2');
                write_body(buf, |_| Ok::<(), io::Error>(())).unwrap();
            }

            BeMessage::CloseComplete => {
                buf.put_u8(b'3');
                write_body(buf, |_| Ok::<(), io::Error>(())).unwrap();
            }

            BeMessage::CommandComplete(cmd) => {
                buf.put_u8(b'C');
                write_body(buf, |buf| {
                    write_cstr(cmd, buf)?;
                    Ok::<_, io::Error>(())
                })?;
            }

            BeMessage::CopyData(data) => {
                buf.put_u8(b'd');
                write_body(buf, |buf| {
                    buf.put_slice(data);
                    Ok::<_, io::Error>(())
                })
                .unwrap();
            }

            BeMessage::CopyDone => {
                buf.put_u8(b'c');
                write_body(buf, |_| Ok::<(), io::Error>(())).unwrap();
            }

            BeMessage::CopyFail => {
                buf.put_u8(b'f');
                write_body(buf, |_| Ok::<(), io::Error>(())).unwrap();
            }

            BeMessage::CopyInResponse => {
                buf.put_u8(b'G');
                write_body(buf, |buf| {
                    buf.put_u8(1); /* copy_is_binary */
                    buf.put_i16(0); /* numAttributes */
                    Ok::<_, io::Error>(())
                })
                .unwrap();
            }

            BeMessage::CopyOutResponse => {
                buf.put_u8(b'H');
                write_body(buf, |buf| {
                    buf.put_u8(0); /* copy_is_binary */
                    buf.put_i16(0); /* numAttributes */
                    Ok::<_, io::Error>(())
                })
                .unwrap();
            }

            BeMessage::CopyBothResponse => {
                buf.put_u8(b'W');
                write_body(buf, |buf| {
                    // doesn't matter, used only for replication
                    buf.put_u8(0); /* copy_is_binary */
                    buf.put_i16(0); /* numAttributes */
                    Ok::<_, io::Error>(())
                })
                .unwrap();
            }

            BeMessage::DataRow(vals) => {
                buf.put_u8(b'D');
                write_body(buf, |buf| {
                    buf.put_u16(vals.len() as u16); // num of cols
                    for val_opt in vals.iter() {
                        if let Some(val) = val_opt {
                            buf.put_u32(val.len() as u32);
                            buf.put_slice(val);
                        } else {
                            buf.put_i32(-1);
                        }
                    }
                    Ok::<_, io::Error>(())
                })
                .unwrap();
            }

            // ErrorResponse is a zero-terminated array of zero-terminated fields.
            // First byte of each field represents type of this field. Set just enough fields
            // to satisfy rust-postgres client: 'S' -- severity, 'C' -- error, 'M' -- error
            // message text.
            BeMessage::ErrorResponse(error_msg) => {
                // For all the errors set Severity to Error and error code to
                // 'internal error'.

                // 'E' signalizes ErrorResponse messages
                buf.put_u8(b'E');
                write_body(buf, |buf| {
                    buf.put_u8(b'S'); // severity
                    write_cstr(&Bytes::from("ERROR"), buf)?;

                    buf.put_u8(b'C'); // SQLSTATE error code
                    write_cstr(&Bytes::from("CXX000"), buf)?;

                    buf.put_u8(b'M'); // the message
                    write_cstr(error_msg.as_bytes(), buf)?;

                    buf.put_u8(0); // terminator
                    Ok::<_, io::Error>(())
                })
                .unwrap();
            }

            // NoticeResponse has the same format as ErrorResponse. From doc: "The frontend should display the
            // message but continue listening for ReadyForQuery or ErrorResponse"
            BeMessage::NoticeResponse(error_msg) => {
                // For all the errors set Severity to Error and error code to
                // 'internal error'.

                // 'N' signalizes NoticeResponse messages
                buf.put_u8(b'N');
                write_body(buf, |buf| {
                    buf.put_u8(b'S'); // severity
                    write_cstr(&Bytes::from("NOTICE"), buf)?;

                    buf.put_u8(b'C'); // SQLSTATE error code
                    write_cstr(&Bytes::from("CXX000"), buf)?;

                    buf.put_u8(b'M'); // the message
                    write_cstr(error_msg.as_bytes(), buf)?;

                    buf.put_u8(0); // terminator
                    Ok::<_, io::Error>(())
                })
                .unwrap();
            }

            BeMessage::NoData => {
                buf.put_u8(b'n');
                write_body(buf, |_| Ok::<(), io::Error>(())).unwrap();
            }

            BeMessage::EncryptionResponse(should_negotiate) => {
                let response = if *should_negotiate { b'S' } else { b'N' };
                buf.put_u8(response);
            }

            BeMessage::ParameterStatus(param) => {
                use std::io::{IoSlice, Write};
                use BeParameterStatusMessage::*;

                let [name, value] = match param {
                    Encoding(name) => [b"client_encoding", name.as_bytes()],
                    ServerVersion(version) => [b"server_version", version.as_bytes()],
                };

                // Parameter names and values are passed as null-terminated strings
                let iov = &mut [name, b"\0", value, b"\0"].map(IoSlice::new);
                let mut buffer = [0u8; 64]; // this should be enough
                let cnt = buffer.as_mut().write_vectored(iov).unwrap();

                buf.put_u8(b'S');
                write_body(buf, |buf| {
                    buf.put_slice(&buffer[..cnt]);
                    Ok::<_, io::Error>(())
                })
                .unwrap();
            }

            BeMessage::ParameterDescription => {
                buf.put_u8(b't');
                write_body(buf, |buf| {
                    // we don't support params, so always 0
                    buf.put_i16(0);
                    Ok::<_, io::Error>(())
                })
                .unwrap();
            }

            BeMessage::ParseComplete => {
                buf.put_u8(b'1');
                write_body(buf, |_| Ok::<(), io::Error>(())).unwrap();
            }

            BeMessage::ReadyForQuery => {
                buf.put_u8(b'Z');
                write_body(buf, |buf| {
                    buf.put_u8(b'I');
                    Ok::<_, io::Error>(())
                })
                .unwrap();
            }

            BeMessage::RowDescription(rows) => {
                buf.put_u8(b'T');
                write_body(buf, |buf| {
                    buf.put_i16(rows.len() as i16); // # of fields
                    for row in rows.iter() {
                        write_cstr(row.name, buf)?;
                        buf.put_i32(0); /* table oid */
                        buf.put_i16(0); /* attnum */
                        buf.put_u32(row.typoid);
                        buf.put_i16(row.typlen);
                        buf.put_i32(-1); /* typmod */
                        buf.put_i16(0); /* format code */
                    }
                    Ok::<_, io::Error>(())
                })?;
            }

            BeMessage::XLogData(body) => {
                buf.put_u8(b'd');
                write_body(buf, |buf| {
                    buf.put_u8(b'w');
                    buf.put_u64(body.wal_start);
                    buf.put_u64(body.wal_end);
                    buf.put_i64(body.timestamp);
                    buf.put_slice(body.data);
                    Ok::<_, io::Error>(())
                })
                .unwrap();
            }

            BeMessage::KeepAlive(req) => {
                buf.put_u8(b'd');
                write_body(buf, |buf| {
                    buf.put_u8(b'k');
                    buf.put_u64(req.sent_ptr);
                    buf.put_i64(req.timestamp);
                    buf.put_u8(if req.request_reply { 1u8 } else { 0u8 });
                    Ok::<_, io::Error>(())
                })
                .unwrap();
            }
        }
        Ok(())
    }
}

// Zenith extension of postgres replication protocol
// See ZENITH_STATUS_UPDATE_TAG_BYTE
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ZenithFeedback {
    // Last known size of the timeline. Used to enforce timeline size limit.
    pub current_instance_size: u64,
    // Parts of StandbyStatusUpdate we resend to compute via safekeeper
    pub ps_writelsn: u64,
    pub ps_applylsn: u64,
    pub ps_flushlsn: u64,
    pub ps_replytime: SystemTime,
}
pub const ZENITH_FEEDBACK_FIELDS_NUMBER: u8 = 5;

impl ZenithFeedback {
    pub fn empty() -> ZenithFeedback {
        ZenithFeedback {
            current_instance_size: 0,
            ps_writelsn: 0,
            ps_applylsn: 0,
            ps_flushlsn: 0,
            ps_replytime: SystemTime::now(),
        }
    }

    // use custom serialization to support protocol extensibility
    pub fn serialize(&self, buf: &mut BytesMut) -> Result<()> {
        buf.put_u8(ZENITH_FEEDBACK_FIELDS_NUMBER); // # of keys
        write_cstr(&Bytes::from("current_instance_size"), buf)?;
        buf.put_i32(8);
        buf.put_u64(self.current_instance_size);

        write_cstr(&Bytes::from("ps_writelsn"), buf)?;
        buf.put_i32(8);
        buf.put_u64(self.ps_writelsn);
        write_cstr(&Bytes::from("ps_flushlsn"), buf)?;
        buf.put_i32(8);
        buf.put_u64(self.ps_flushlsn);
        write_cstr(&Bytes::from("ps_applylsn"), buf)?;
        buf.put_i32(8);
        buf.put_u64(self.ps_applylsn);

        let timestamp = match self.ps_replytime.duration_since(*PG_EPOCH) {
            Ok(d) => d.as_micros() as i64,
            Err(e) => -(e.duration().as_micros() as i64),
        };

        write_cstr(&Bytes::from("ps_replytime"), buf)?;
        buf.put_i32(8);
        buf.put_i64(timestamp);
        Ok(())
    }

    pub fn parse(mut buf: Bytes) -> ZenithFeedback {
        let mut zf = ZenithFeedback::empty();
        let nfields = buf.get_u8();
        let mut i = 0;
        while i < nfields {
            i += 1;
            let key_cstr = read_null_terminated(&mut buf).unwrap();
            let key = cstr_to_str(&key_cstr).unwrap();
            match key {
                "current_instance_size" => {
                    let len = buf.get_i32();
                    assert_eq!(len, 8);
                    zf.current_instance_size = buf.get_u64();
                }
                "ps_writelsn" => {
                    let len = buf.get_i32();
                    assert_eq!(len, 8);
                    zf.ps_writelsn = buf.get_u64();
                }
                "ps_flushlsn" => {
                    let len = buf.get_i32();
                    assert_eq!(len, 8);
                    zf.ps_flushlsn = buf.get_u64();
                }
                "ps_applylsn" => {
                    let len = buf.get_i32();
                    assert_eq!(len, 8);
                    zf.ps_applylsn = buf.get_u64();
                }
                "ps_replytime" => {
                    let len = buf.get_i32();
                    assert_eq!(len, 8);
                    let raw_time = buf.get_i64();
                    if raw_time > 0 {
                        zf.ps_replytime = *PG_EPOCH + Duration::from_micros(raw_time as u64);
                    } else {
                        zf.ps_replytime = *PG_EPOCH - Duration::from_micros(-raw_time as u64);
                    }
                }
                _ => {
                    let len = buf.get_i32();
                    info!(
                        "ZenithFeedback parse. unknown key {} of len {}. Skip it.",
                        key, len
                    );
                    println!("{}", key);
                    buf.advance(len as usize);
                }
            }
        }
        info!("ZenithFeedback parsed is {:?}", zf);
        zf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zenithfeedback_serialization() {
        let mut zf = ZenithFeedback::empty();
        // Fill zf wih some values
        zf.current_instance_size = 12345678;
        // Set rounded time to be able to compare it with deserialized value,
        // because it is rounded up to microseconds during serialization.
        zf.ps_replytime = *PG_EPOCH + Duration::from_secs(100_000_000);
        let mut data = BytesMut::new();
        zf.serialize(&mut data).unwrap();

        let zf_parsed = ZenithFeedback::parse(data.freeze());
        assert_eq!(zf, zf_parsed);
    }

    #[test]
    fn test_zenithfeedback_unknown_key() {
        let mut zf = ZenithFeedback::empty();
        // Fill zf wih some values
        zf.current_instance_size = 12345678;
        // Set rounded time to be able to compare it with deserialized value,
        // because it is rounded up to microseconds during serialization.
        zf.ps_replytime = *PG_EPOCH + Duration::from_secs(100_000_000);
        let mut data = BytesMut::new();
        zf.serialize(&mut data).unwrap();

        // Add an extra field to the buffer and adjust number of keys
        if let Some(first) = data.first_mut() {
            *first = ZENITH_FEEDBACK_FIELDS_NUMBER + 1;
        }

        write_cstr(&Bytes::from("new_field_one"), &mut data).unwrap();
        data.put_i32(8);
        data.put_u64(42);

        // Parse serialized data and check that new field is not parsed
        let zf_parsed = ZenithFeedback::parse(data.freeze());
        assert_eq!(zf, zf_parsed);
    }
}
