//! Postgres protocol codec
//!
//! <https://www.postgresql.org/docs/current/protocol-message-formats.html>

use std::fmt;
use std::io::{self, Cursor};

use bytes::{Buf, BufMut};
use itertools::Itertools;
use rand::distributions::{Distribution, Standard};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use zerocopy::{FromBytes, Immutable, IntoBytes, big_endian};

pub type ErrorCode = [u8; 5];

pub const FE_PASSWORD_MESSAGE: u8 = b'p';

pub const SQLSTATE_INTERNAL_ERROR: [u8; 5] = *b"XX000";

/// The protocol version number.
///
/// The most significant 16 bits are the major version number (3 for the protocol described here).
/// The least significant 16 bits are the minor version number (0 for the protocol described here).
/// <https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-STARTUPMESSAGE>
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

/// <https://github.com/postgres/postgres/blob/ca481d3c9ab7bf69ff0c8d71ad3951d407f6a33c/src/include/libpq/pqcomm.h#L118>
const MAX_STARTUP_PACKET_LENGTH: usize = 10000;
const RESERVED_INVALID_MAJOR_VERSION: u16 = 1234;
/// <https://github.com/postgres/postgres/blob/ca481d3c9ab7bf69ff0c8d71ad3951d407f6a33c/src/include/libpq/pqcomm.h#L132>
const CANCEL_REQUEST_CODE: ProtocolVersion = ProtocolVersion::new(1234, 5678);
/// <https://github.com/postgres/postgres/blob/ca481d3c9ab7bf69ff0c8d71ad3951d407f6a33c/src/include/libpq/pqcomm.h#L166>
const NEGOTIATE_SSL_CODE: ProtocolVersion = ProtocolVersion::new(1234, 5679);
/// <https://github.com/postgres/postgres/blob/ca481d3c9ab7bf69ff0c8d71ad3951d407f6a33c/src/include/libpq/pqcomm.h#L167>
const NEGOTIATE_GSS_CODE: ProtocolVersion = ProtocolVersion::new(1234, 5680);

/// This first reads the startup message header, is 8 bytes.
/// The first 4 bytes is a big-endian message length, and the next 4 bytes is a version number.
///
/// The length value is inclusive of the header. For example,
/// an empty message will always have length 8.
#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable)]
#[repr(C)]
struct StartupHeader {
    len: big_endian::U32,
    version: ProtocolVersion,
}

/// read the type from the stream using zerocopy.
///
/// not cancel safe.
macro_rules! read {
    ($s:expr => $t:ty) => {{
        // cannot be implemented as a function due to lack of const-generic-expr
        let mut buf = [0; size_of::<$t>()];
        $s.read_exact(&mut buf).await?;
        let res: $t = zerocopy::transmute!(buf);
        res
    }};
}

/// Returns true if TLS is supported.
///
/// This is not cancel safe.
pub async fn request_tls<S>(stream: &mut S) -> io::Result<bool>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let payload = StartupHeader {
        len: 8.into(),
        version: NEGOTIATE_SSL_CODE,
    };
    stream.write_all(payload.as_bytes()).await?;
    stream.flush().await?;

    // we expect back either `S` or `N` as a single byte.
    let mut res = *b"0";
    stream.read_exact(&mut res).await?;

    debug_assert!(
        res == *b"S" || res == *b"N",
        "unexpected SSL negotiation response: {}",
        char::from(res[0]),
    );

    // S for SSL.
    Ok(res == *b"S")
}

pub async fn read_startup<S>(stream: &mut S) -> io::Result<FeStartupPacket>
where
    S: AsyncRead + Unpin,
{
    let header = read!(stream => StartupHeader);

    // <https://github.com/postgres/postgres/blob/04bcf9e19a4261fe9c7df37c777592c2e10c32a7/src/backend/tcop/backend_startup.c#L378-L382>
    // First byte indicates standard SSL handshake message
    // (It can't be a Postgres startup length because in network byte order
    // that would be a startup packet hundreds of megabytes long)
    if header.as_bytes()[0] == 0x16 {
        return Ok(FeStartupPacket::SslRequest {
            // The bytes we read for the header are actually part of a TLS ClientHello.
            // In theory, if the ClientHello was < 8 bytes we would fail with EOF before we get here.
            // In practice though, I see no world where a ClientHello is less than 8 bytes
            // since it includes ephemeral keys etc.
            direct: Some(zerocopy::transmute!(header)),
        });
    }

    let Some(len) = (header.len.get() as usize).checked_sub(8) else {
        return Err(io::Error::other(format!(
            "invalid startup message length {}, must be at least 8.",
            header.len,
        )));
    };

    // TODO: add a histogram for startup packet lengths
    if len > MAX_STARTUP_PACKET_LENGTH {
        tracing::warn!("large startup message detected: {len} bytes");
        return Err(io::Error::other(format!(
            "invalid startup message length {len}"
        )));
    }

    match header.version {
        // <https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-CANCELREQUEST>
        CANCEL_REQUEST_CODE => {
            if len != 8 {
                return Err(io::Error::other(
                    "CancelRequest message is malformed, backend PID / secret key missing",
                ));
            }

            Ok(FeStartupPacket::CancelRequest(
                read!(stream => CancelKeyData),
            ))
        }
        // <https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-SSLREQUEST>
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
            // The protocol version number is followed by one or more pairs of parameter name and value strings.
            // A zero byte is required as a terminator after the last name/value pair.
            // Parameters can appear in any order. user is required, others are optional.

            let mut buf = vec![0; len];
            stream.read_exact(&mut buf).await?;

            if buf.pop() != Some(b'\0') {
                return Err(io::Error::other(
                    "StartupMessage params: missing null terminator",
                ));
            }

            // TODO: Don't do this.
            // There's no guarantee that these messages are utf8,
            // but they usually happen to be simple ascii.
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
///
/// This returns the message tag, as well as the message body. The message
/// body is written into `buf`, and it is otherwise completely overwritten.
///
/// This is not cancel safe.
pub async fn read_message<'a, S>(
    stream: &mut S,
    buf: &'a mut Vec<u8>,
    max: u32,
) -> io::Result<(u8, &'a mut [u8])>
where
    S: AsyncRead + Unpin,
{
    /// This first reads the header, which for regular messages in the 3.0 protocol is 5 bytes.
    /// The first byte is a message tag, and the next 4 bytes is a big-endian length.
    ///
    /// Awkwardly, the length value is inclusive of itself, but not of the tag. For example,
    /// an empty message will always have length 4.
    #[derive(Clone, Copy, FromBytes)]
    #[repr(C)]
    struct Header {
        tag: u8,
        len: big_endian::U32,
    }

    let header = read!(stream => Header);

    // as described above, the length must be at least 4.
    let Some(len) = header.len.get().checked_sub(4) else {
        return Err(io::Error::other(format!(
            "invalid startup message length {}, must be at least 4.",
            header.len,
        )));
    };

    // TODO: add a histogram for message lengths

    // check if the message exceeds our desired max.
    if len > max {
        tracing::warn!("large postgres message detected: {len} bytes");
        return Err(io::Error::other(format!("invalid message length {len}")));
    }

    // read in our entire message.
    buf.resize(len as usize, 0);
    stream.read_exact(buf).await?;

    Ok((header.tag, buf))
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

    /// Use a heuristic to determine if we should shrink the write buffer.
    #[inline]
    fn should_shrink(&self) -> bool {
        let n = self.0.position() as usize;
        let len = self.0.get_ref().len();

        // the unused space at the front of our buffer is 2x the size of our filled portion.
        n + n > len
    }

    /// Shrink the write buffer so that subsequent writes have more spare capacity.
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
    ///
    /// The size_hint value is only a hint for reserving space. It's ok if it's incorrect, since
    /// we calculate the length after the fact.
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

        // <https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-ERRORRESPONSE>
        // <https://www.postgresql.org/docs/current/protocol-error-fields.html>
        // "SERROR\0CXXXXX\0M\0\0".len() == 17
        self.write_raw(17 + msg.len(), b'E', |buf| {
            // Severity: ERROR
            buf.put_slice(b"SERROR\0");

            // Code: error_code
            buf.put_u8(b'C');
            buf.put_slice(&error_code);
            buf.put_u8(0);

            // Message: msg
            buf.put_u8(b'M');
            buf.put_slice(msg.as_bytes());
            buf.put_u8(0);

            // End.
            buf.put_u8(0);
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
            // <https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-AUTHENTICATIONCLEARTEXTPASSWORD>
            BeMessage::AuthenticationOk => {
                buf.write_raw(1, b'R', |buf| buf.put_i32(0));
            }
            // <https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-AUTHENTICATIONCLEARTEXTPASSWORD>
            BeMessage::AuthenticationCleartextPassword => {
                buf.write_raw(1, b'R', |buf| buf.put_i32(3));
            }

            // <https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-AUTHENTICATIONSASL>
            BeMessage::AuthenticationSasl(BeAuthenticationSaslMessage::Methods(methods)) => {
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
            // <https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-AUTHENTICATIONSASL>
            BeMessage::AuthenticationSasl(BeAuthenticationSaslMessage::Continue(extra)) => {
                buf.write_raw(extra.len() + 1, b'R', |buf| {
                    buf.put_i32(11); // Continue SASL auth.
                    buf.put_slice(extra);
                });
            }
            // <https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-AUTHENTICATIONSASL>
            BeMessage::AuthenticationSasl(BeAuthenticationSaslMessage::Final(extra)) => {
                buf.write_raw(extra.len() + 1, b'R', |buf| {
                    buf.put_i32(12); // Send final SASL message.
                    buf.put_slice(extra);
                });
            }

            // <https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-BACKENDKEYDATA>
            BeMessage::BackendKeyData(key_data) => {
                buf.write_raw(8, b'K', |buf| buf.put_slice(key_data.as_bytes()));
            }

            // <https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-NOTICERESPONSE>
            // <https://www.postgresql.org/docs/current/protocol-error-fields.html>
            BeMessage::NoticeResponse(msg) => {
                // 'N' signalizes NoticeResponse messages
                buf.write_raw(18 + msg.len(), b'N', |buf| {
                    // Severity: NOTICE
                    buf.put_slice(b"SNOTICE\0");

                    // Code: XX000 (ignored for notice, but still required)
                    buf.put_slice(b"CXX000\0");

                    // Message: msg
                    buf.put_u8(b'M');
                    buf.put_slice(msg.as_bytes());
                    buf.put_u8(0);

                    // End notice.
                    buf.put_u8(0);
                });
            }

            // <https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-PARAMETERSTATUS>
            BeMessage::ParameterStatus { name, value } => {
                buf.write_raw(name.len() + value.len() + 2, b'S', |buf| {
                    buf.put_slice(name.as_bytes());
                    buf.put_u8(0);
                    buf.put_slice(value.as_bytes());
                    buf.put_u8(0);
                });
            }

            // <https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-NEGOTIATEPROTOCOLVERSION>
            BeMessage::ReadyForQuery => {
                buf.write_raw(1, b'Z', |buf| buf.put_u8(b'I'));
            }

            // <https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-NEGOTIATEPROTOCOLVERSION>
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

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tokio::io::{AsyncWriteExt, duplex};
    use zerocopy::IntoBytes;

    use super::ProtocolVersion;
    use crate::pqproto::{FeStartupPacket, read_message, read_startup};

    #[tokio::test]
    async fn reject_large_startup() {
        // we're going to define a v3.0 startup message with far too many parameters.
        let mut payload = vec![];
        // 10001 + 8 bytes.
        payload.extend_from_slice(&10009_u32.to_be_bytes());
        payload.extend_from_slice(ProtocolVersion::new(3, 0).as_bytes());
        payload.resize(10009, b'a');

        let (mut server, mut client) = duplex(128);
        #[rustfmt::skip]
        let (server, client) = tokio::join!(
            async move { read_startup(&mut server).await.unwrap_err() },
            async move { client.write_all(&payload).await.unwrap_err() },
        );

        assert_eq!(server.to_string(), "invalid startup message length 10001");
        assert_eq!(client.to_string(), "broken pipe");
    }

    #[tokio::test]
    async fn reject_large_password() {
        // we're going to define a password message that is far too long.
        let mut payload = vec![];
        payload.push(b'p');
        payload.extend_from_slice(&517_u32.to_be_bytes());
        payload.resize(518, b'a');

        let (mut server, mut client) = duplex(128);
        #[rustfmt::skip]
        let (server, client) = tokio::join!(
            async move { read_message(&mut server, &mut vec![], 512).await.unwrap_err() },
            async move { client.write_all(&payload).await.unwrap_err() },
        );

        assert_eq!(server.to_string(), "invalid message length 513");
        assert_eq!(client.to_string(), "broken pipe");
    }

    #[tokio::test]
    async fn read_startup_message() {
        let mut payload = vec![];
        payload.extend_from_slice(&17_u32.to_be_bytes());
        payload.extend_from_slice(ProtocolVersion::new(3, 0).as_bytes());
        payload.extend_from_slice(b"abc\0def\0\0");

        let startup = read_startup(&mut Cursor::new(&payload)).await.unwrap();
        let FeStartupPacket::StartupMessage { version, params } = startup else {
            panic!("unexpected startup message: {startup:?}");
        };

        assert_eq!(version.major(), 3);
        assert_eq!(version.minor(), 0);
        assert_eq!(params.params, "abc\0def\0");
    }

    #[tokio::test]
    async fn read_ssl_message() {
        let mut payload = vec![];
        payload.extend_from_slice(&8_u32.to_be_bytes());
        payload.extend_from_slice(ProtocolVersion::new(1234, 5679).as_bytes());

        let startup = read_startup(&mut Cursor::new(&payload)).await.unwrap();
        let FeStartupPacket::SslRequest { direct: None } = startup else {
            panic!("unexpected startup message: {startup:?}");
        };
    }

    #[tokio::test]
    async fn read_tls_message() {
        // sample client hello taken from <https://tls13.xargs.org/#client-hello>
        let client_hello = [
            0x16, 0x03, 0x01, 0x00, 0xf8, 0x01, 0x00, 0x00, 0xf4, 0x03, 0x03, 0x00, 0x01, 0x02,
            0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
            0x1f, 0x20, 0xe0, 0xe1, 0xe2, 0xe3, 0xe4, 0xe5, 0xe6, 0xe7, 0xe8, 0xe9, 0xea, 0xeb,
            0xec, 0xed, 0xee, 0xef, 0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9,
            0xfa, 0xfb, 0xfc, 0xfd, 0xfe, 0xff, 0x00, 0x08, 0x13, 0x02, 0x13, 0x03, 0x13, 0x01,
            0x00, 0xff, 0x01, 0x00, 0x00, 0xa3, 0x00, 0x00, 0x00, 0x18, 0x00, 0x16, 0x00, 0x00,
            0x13, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2e, 0x75, 0x6c, 0x66, 0x68, 0x65,
            0x69, 0x6d, 0x2e, 0x6e, 0x65, 0x74, 0x00, 0x0b, 0x00, 0x04, 0x03, 0x00, 0x01, 0x02,
            0x00, 0x0a, 0x00, 0x16, 0x00, 0x14, 0x00, 0x1d, 0x00, 0x17, 0x00, 0x1e, 0x00, 0x19,
            0x00, 0x18, 0x01, 0x00, 0x01, 0x01, 0x01, 0x02, 0x01, 0x03, 0x01, 0x04, 0x00, 0x23,
            0x00, 0x00, 0x00, 0x16, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00, 0x0d, 0x00, 0x1e,
            0x00, 0x1c, 0x04, 0x03, 0x05, 0x03, 0x06, 0x03, 0x08, 0x07, 0x08, 0x08, 0x08, 0x09,
            0x08, 0x0a, 0x08, 0x0b, 0x08, 0x04, 0x08, 0x05, 0x08, 0x06, 0x04, 0x01, 0x05, 0x01,
            0x06, 0x01, 0x00, 0x2b, 0x00, 0x03, 0x02, 0x03, 0x04, 0x00, 0x2d, 0x00, 0x02, 0x01,
            0x01, 0x00, 0x33, 0x00, 0x26, 0x00, 0x24, 0x00, 0x1d, 0x00, 0x20, 0x35, 0x80, 0x72,
            0xd6, 0x36, 0x58, 0x80, 0xd1, 0xae, 0xea, 0x32, 0x9a, 0xdf, 0x91, 0x21, 0x38, 0x38,
            0x51, 0xed, 0x21, 0xa2, 0x8e, 0x3b, 0x75, 0xe9, 0x65, 0xd0, 0xd2, 0xcd, 0x16, 0x62,
            0x54,
        ];

        let mut cursor = Cursor::new(&client_hello);

        let startup = read_startup(&mut cursor).await.unwrap();
        let FeStartupPacket::SslRequest {
            direct: Some(prefix),
        } = startup
        else {
            panic!("unexpected startup message: {startup:?}");
        };

        // check that no data is lost.
        assert_eq!(prefix, [0x16, 0x03, 0x01, 0x00, 0xf8, 0x01, 0x00, 0x00]);
        assert_eq!(cursor.position(), 8);
    }

    #[tokio::test]
    async fn read_message_success() {
        let query = b"Q\0\0\0\x0cSELECT 1Q\0\0\0\x0cSELECT 2";
        let mut cursor = Cursor::new(&query);

        let mut buf = vec![];
        let (tag, message) = read_message(&mut cursor, &mut buf, 100).await.unwrap();
        assert_eq!(tag, b'Q');
        assert_eq!(message, b"SELECT 1");

        let (tag, message) = read_message(&mut cursor, &mut buf, 100).await.unwrap();
        assert_eq!(tag, b'Q');
        assert_eq!(message, b"SELECT 2");
    }
}
