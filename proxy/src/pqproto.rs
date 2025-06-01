//! Postgres protocol codec

use std::fmt;
use std::io::{self, Cursor};

use bytes::{Buf, BufMut};
use itertools::Itertools;
use rand::distributions::{Distribution, Standard};
use tokio::io::{AsyncRead, AsyncReadExt};
use zerocopy::{FromBytes, Immutable, IntoBytes, big_endian};

pub type ErrorCode = [u8; 5];

pub const FE_PASSWORD_MESSAGE: u8 = b'p';

#[derive(Clone, Copy, PartialEq, PartialOrd, FromBytes, IntoBytes, Immutable)]
#[repr(C)]
pub struct ProtocolVersion {
    major: big_endian::U16,
    minor: big_endian::U16,
}

impl ProtocolVersion {
    pub const fn new(major: u16, minor: u16) -> Self {
        Self {
            major: big_endian::U16::new(major),
            minor: big_endian::U16::new(minor),
        }
    }
    pub const fn minor(self) -> u16 {
        self.minor.get()
    }
    pub const fn major(self) -> u16 {
        self.major.get()
    }
}

impl fmt::Debug for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entry(&self.major())
            .entry(&self.minor())
            .finish()
    }
}

/// read the type from the stream using zerocopy.
/// not cancel safe.
// cannot be implemented as a function due to lack of const-generic-expr
macro_rules! read {
    ($s:expr => $t:ty) => {{
        let mut buf = [0; size_of::<$t>()];
        $s.read_exact(&mut buf).await?;
        let res: $t = zerocopy::transmute!(buf);
        res
    }};
}

pub async fn read_startup<S>(stream: &mut S) -> io::Result<FeStartupPacket>
where
    S: AsyncRead + Unpin,
{
    /// <https://github.com/postgres/postgres/blob/ca481d3c9ab7bf69ff0c8d71ad3951d407f6a33c/src/include/libpq/pqcomm.h#L118>
    const MAX_STARTUP_PACKET_LENGTH: usize = 10000;
    const RESERVED_INVALID_MAJOR_VERSION: u16 = 1234;
    /// <https://github.com/postgres/postgres/blob/ca481d3c9ab7bf69ff0c8d71ad3951d407f6a33c/src/include/libpq/pqcomm.h#L132>
    const CANCEL_REQUEST_CODE: ProtocolVersion = ProtocolVersion::new(1234, 5678);
    /// <https://github.com/postgres/postgres/blob/ca481d3c9ab7bf69ff0c8d71ad3951d407f6a33c/src/include/libpq/pqcomm.h#L166>
    const NEGOTIATE_SSL_CODE: ProtocolVersion = ProtocolVersion::new(1234, 5679);
    /// <https://github.com/postgres/postgres/blob/ca481d3c9ab7bf69ff0c8d71ad3951d407f6a33c/src/include/libpq/pqcomm.h#L167>
    const NEGOTIATE_GSS_CODE: ProtocolVersion = ProtocolVersion::new(1234, 5680);

    #[derive(Clone, Copy, FromBytes, IntoBytes, Immutable)]
    #[repr(C)]
    struct StartupHeader {
        len: big_endian::U32,
        version: ProtocolVersion,
    }

    let StartupHeader { len, version } = read!(stream => StartupHeader);

    // <https://github.com/postgres/postgres/blob/04bcf9e19a4261fe9c7df37c777592c2e10c32a7/src/backend/tcop/backend_startup.c#L378-L382>
    // First byte indicates standard SSL handshake message
    // (It can't be a Postgres startup length because in network byte order
    // that would be a startup packet hundreds of megabytes long)
    if len.as_bytes()[0] == 0x16 {
        return Ok(FeStartupPacket::SslRequest {
            direct: Some(zerocopy::transmute!(StartupHeader { len, version })),
        });
    }

    let len = len.get() as usize;

    // TODO: add a histogram for startup packet lengths
    let valid_lengths = 8..=MAX_STARTUP_PACKET_LENGTH;
    if !valid_lengths.contains(&len) {
        tracing::warn!("large startup message detected: {len} bytes");
        return Err(io::Error::other(format!(
            "invalid startup message length {len}"
        )));
    }

    match version {
        CANCEL_REQUEST_CODE => {
            if len != 16 {
                return Err(io::Error::other(
                    "CancelRequest message is malformed, backend PID / secret key missing",
                ));
            }

            Ok(FeStartupPacket::CancelRequest(
                read!(stream => CancelKeyData),
            ))
        }
        NEGOTIATE_SSL_CODE => {
            // Requested upgrade to SSL (aka TLS)
            Ok(FeStartupPacket::SslRequest { direct: None })
        }
        NEGOTIATE_GSS_CODE => {
            // Requested upgrade to GSSAPI
            Ok(FeStartupPacket::GssEncRequest)
        }
        version if version.major() == RESERVED_INVALID_MAJOR_VERSION => Err(io::Error::other(
            format!("Unrecognized request code {version:?}"),
        )),
        // StartupMessage
        version => {
            let mut buf = vec![0; len - 8];
            stream.read_exact(&mut buf).await?;

            if buf.pop() != Some(b'\0') {
                return Err(io::Error::other(
                    "StartupMessage params: missing null terminator",
                ));
            }
            let params = String::from_utf8(buf)
                .map_err(|_| io::Error::other("StartupMessage params: invalid utf-8"))?;

            Ok(FeStartupPacket::StartupMessage {
                version,
                params: StartupMessageParams { params },
            })
        }
    }
}

/// Read a raw postgres packet, which will respect the max length requested.
/// This is not cancel safe.
pub async fn read_message<'a, S>(
    stream: &mut S,
    buf: &'a mut Vec<u8>,
    max: usize,
) -> io::Result<(u8, &'a mut [u8])>
where
    S: AsyncRead + Unpin,
{
    #[derive(Clone, Copy, FromBytes)]
    #[repr(C)]
    struct Header {
        tag: u8,
        len: big_endian::U32,
    }

    let Header { tag, len } = read!(stream => Header);
    let len = len.get() as usize;

    let valid_lengths = 4..=max;
    if !valid_lengths.contains(&len) {
        return Err(io::Error::other(format!("invalid message length {len}")));
    }

    buf.resize(len - 4, 0);
    stream.read_exact(buf).await?;

    Ok((tag, buf))
}

pub struct WriteBuf(Cursor<Vec<u8>>);

impl Buf for WriteBuf {
    #[inline]
    fn remaining(&self) -> usize {
        self.0.remaining()
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        self.0.chunk()
    }

    #[inline]
    fn advance(&mut self, cnt: usize) {
        self.0.advance(cnt);
    }
}

impl WriteBuf {
    pub const fn new() -> Self {
        Self(Cursor::new(Vec::new()))
    }

    #[inline]
    fn should_shrink(&self) -> bool {
        let n = self.0.position() as usize;
        let len = self.0.get_ref().len();

        // the unused space at the front of our buffer is 2x the size of our filled portion.
        n + n > len
    }

    #[cold]
    fn shrink(&mut self) {
        let n = self.0.position() as usize;
        let buf = self.0.get_mut();

        // buf repr:
        // [----unused------|-----filled-----|-----uninit-----]
        //                  ^ n              ^ buf.len()      ^ buf.capacity()
        let filled = n..buf.len();
        let filled_len = filled.len();
        buf.copy_within(filled, 0);
        buf.truncate(filled_len);
        self.0.set_position(0);
    }

    /// clear the write buffer.
    pub fn reset(&mut self) {
        let buf = self.0.get_mut();
        buf.clear();
        self.0.set_position(0);
    }

    /// Write a raw message to the internal buffer.
    pub fn write_raw(&mut self, size_hint: usize, tag: u8, f: impl FnOnce(&mut Vec<u8>)) {
        if self.should_shrink() {
            self.shrink();
        }

        let buf = self.0.get_mut();
        buf.reserve(5 + size_hint);

        buf.push(tag);
        let start = buf.len();
        buf.extend_from_slice(&[0, 0, 0, 0]);

        f(buf);

        let end = buf.len();
        let len = (end - start) as u32;
        buf[start..start + 4].copy_from_slice(&len.to_be_bytes());
    }

    /// Write an encryption response message.
    pub fn encryption(&mut self, m: u8) {
        self.0.get_mut().push(m);
    }

    pub fn write_error(&mut self, msg: &str, error_code: ErrorCode) {
        self.shrink();

        // 'E' signalizes ErrorResponse messages
        self.write_raw(17 + msg.len(), b'E', |buf| {
            // severity
            buf.put_slice(b"SERROR\0");

            buf.put_u8(b'C'); // SQLSTATE error code
            buf.put_slice(&error_code);
            buf.put_u8(0);

            buf.put_u8(b'M'); // the message
            buf.put_slice(msg.as_bytes());
            buf.put_u8(0);

            buf.put_u8(0); // terminator
        });
    }
}

#[derive(Debug)]
pub enum FeStartupPacket {
    CancelRequest(CancelKeyData),
    SslRequest {
        direct: Option<[u8; 8]>,
    },
    GssEncRequest,
    StartupMessage {
        version: ProtocolVersion,
        params: StartupMessageParams,
    },
}

#[derive(Debug, Clone, Default)]
pub struct StartupMessageParams {
    pub params: String,
}

impl StartupMessageParams {
    /// Get parameter's value by its name.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.iter().find_map(|(k, v)| (k == name).then_some(v))
    }

    /// Split command-line options according to PostgreSQL's logic,
    /// taking into account all escape sequences but leaving them as-is.
    /// [`None`] means that there's no `options` in [`Self`].
    pub fn options_raw(&self) -> Option<impl Iterator<Item = &str>> {
        self.get("options").map(Self::parse_options_raw)
    }

    /// Split command-line options according to PostgreSQL's logic,
    /// taking into account all escape sequences but leaving them as-is.
    pub fn parse_options_raw(input: &str) -> impl Iterator<Item = &str> {
        // See `postgres: pg_split_opts`.
        let mut last_was_escape = false;
        input
            .split(move |c: char| {
                // We split by non-escaped whitespace symbols.
                let should_split = c.is_ascii_whitespace() && !last_was_escape;
                last_was_escape = c == '\\' && !last_was_escape;
                should_split
            })
            .filter(|s| !s.is_empty())
    }

    /// Iterate through key-value pairs in an arbitrary order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.params.split_terminator('\0').tuples()
    }

    // This function is mostly useful in tests.
    #[cfg(test)]
    pub fn new<'a, const N: usize>(pairs: [(&'a str, &'a str); N]) -> Self {
        let mut b = Self {
            params: String::new(),
        };
        for (k, v) in pairs {
            b.insert(k, v);
        }
        b
    }

    /// Set parameter's value by its name.
    /// name and value must not contain a \0 byte
    pub fn insert(&mut self, name: &str, value: &str) {
        self.params.reserve(name.len() + value.len() + 2);
        self.params.push_str(name);
        self.params.push('\0');
        self.params.push_str(value);
        self.params.push('\0');
    }
}

/// Cancel keys usually are represented as PID+SecretKey, but to proxy they're just
/// opaque bytes.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, FromBytes, IntoBytes, Immutable)]
pub struct CancelKeyData(pub big_endian::U64);

pub fn id_to_cancel_key(id: u64) -> CancelKeyData {
    CancelKeyData(big_endian::U64::new(id))
}

impl fmt::Display for CancelKeyData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let id = self.0;
        f.debug_tuple("CancelKeyData")
            .field(&format_args!("{id:x}"))
            .finish()
    }
}
impl Distribution<CancelKeyData> for Standard {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> CancelKeyData {
        id_to_cancel_key(rng.r#gen())
    }
}

pub enum BeMessage<'a> {
    AuthenticationOk,
    AuthenticationSasl(BeAuthenticationSaslMessage<'a>),
    AuthenticationCleartextPassword,
    BackendKeyData(CancelKeyData),
    ParameterStatus {
        name: &'a [u8],
        value: &'a [u8],
    },
    ReadyForQuery,
    NoticeResponse(&'a str),
    NegotiateProtocolVersion {
        version: ProtocolVersion,
        options: &'a [&'a str],
    },
}

#[derive(Debug)]
pub enum BeAuthenticationSaslMessage<'a> {
    Methods(&'a [&'a str]),
    Continue(&'a [u8]),
    Final(&'a [u8]),
}

impl BeMessage<'_> {
    /// Write the message into an internal buffer
    pub fn write_message(self, buf: &mut WriteBuf) {
        match self {
            BeMessage::AuthenticationOk => {
                buf.write_raw(1, b'R', |buf| buf.put_i32(0));
            }
            BeMessage::AuthenticationCleartextPassword => {
                buf.write_raw(1, b'R', |buf| buf.put_i32(3));
            }
            BeMessage::AuthenticationSasl(msg) => {
                match msg {
                    BeAuthenticationSaslMessage::Methods(methods) => {
                        let len: usize = methods.iter().map(|m| m.len() + 1).sum();
                        buf.write_raw(len + 2, b'R', |buf| {
                            buf.put_i32(10); // Specifies that SASL auth method is used.
                            for method in methods {
                                buf.put_slice(method.as_bytes());
                                buf.put_u8(0);
                            }
                            buf.put_u8(0); // zero terminator for the list
                        });
                    }
                    BeAuthenticationSaslMessage::Continue(extra) => {
                        buf.write_raw(extra.len() + 1, b'R', |buf| {
                            buf.put_i32(11); // Continue SASL auth.
                            buf.put_slice(extra);
                        });
                    }
                    BeAuthenticationSaslMessage::Final(extra) => {
                        buf.write_raw(extra.len() + 1, b'R', |buf| {
                            buf.put_i32(12); // Send final SASL message.
                            buf.put_slice(extra);
                        });
                    }
                }
            }

            // <https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-BACKENDKEYDATA>
            BeMessage::BackendKeyData(key_data) => {
                buf.write_raw(8, b'K', |buf| buf.put_slice(key_data.as_bytes()));
            }

            // <https://www.postgresql.org/docs/current/protocol-error-fields.html>
            // NoticeResponse has the same format as ErrorResponse. From doc: "The frontend should display the
            // message but continue listening for ReadyForQuery or ErrorResponse"
            BeMessage::NoticeResponse(msg) => {
                // 'N' signalizes NoticeResponse messages
                buf.write_raw(18 + msg.len(), b'N', |buf| {
                    // severity
                    buf.put_slice(b"SNOTICE\0");

                    // error code (ignored by the client, but required)
                    buf.put_slice(b"CXX000\0");

                    buf.put_u8(b'M'); // the message
                    buf.put_slice(msg.as_bytes());
                    buf.put_u8(0);

                    buf.put_u8(0); // terminator
                });
            }

            BeMessage::ParameterStatus { name, value } => {
                buf.write_raw(name.len() + value.len() + 2, b'S', |buf| {
                    buf.put_slice(name.as_bytes());
                    buf.put_u8(0);
                    buf.put_slice(value.as_bytes());
                    buf.put_u8(0);
                });
            }

            BeMessage::ReadyForQuery => {
                buf.write_raw(1, b'Z', |buf| buf.put_u8(b'I'));
            }

            BeMessage::NegotiateProtocolVersion { version, options } => {
                let len: usize = options.iter().map(|o| o.len() + 1).sum();
                buf.write_raw(8 + len, b'v', |buf| {
                    buf.put_slice(version.as_bytes());
                    buf.put_u32(options.len() as u32);
                    for option in options {
                        buf.put_slice(option.as_bytes());
                        buf.put_u8(0);
                    }
                });
            }
        }
    }
}
