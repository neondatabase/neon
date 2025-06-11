//! Postgres connection from backend, proxy is the frontend.

use std::io;

use bytes::Bytes;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::pqproto::{
    AuthTag, BE_AUTH_MESSAGE, BE_ERR_MESSAGE, BeTag, ErrorCode, SQLSTATE_INTERNAL_ERROR,
    StartupMessageParams, WriteBuf, read_message,
};

/// Stream wrapper which implements libpq's protocol.
pub struct PqBeStream<S> {
    stream: S,
    read: Vec<u8>,
    write: WriteBuf,
}

impl<S> PqBeStream<S> {
    pub fn get_ref(&self) -> &S {
        &self.stream
    }

    /// Construct a new libpq protocol wrapper and write the first startup message.
    pub fn new(stream: S, params: &StartupMessageParams) -> Self {
        let mut write = WriteBuf::new();
        write.startup(params);
        Self {
            stream,
            read: Vec::new(),
            write,
        }
    }
}

impl<S: AsyncRead + Unpin> PqBeStream<S> {
    /// Read a raw postgres packet from the backend, which will respect the max length requested,
    /// as well as handling postgres error messages.
    ///
    /// This is not cancel safe.
    pub async fn read_raw_be(&mut self, max: u32) -> Result<(BeTag, &mut [u8]), PostgresError> {
        let (tag, msg) = read_message(&mut self.stream, &mut self.read, max).await?;
        match BeTag(tag) {
            BE_ERR_MESSAGE => Err(PostgresError::Error(BackendError {
                data: msg.to_vec().into(),
            })),
            tag => Ok((tag, msg)),
        }
    }

    /// Read a raw postgres packet, which will respect the max length requested.
    /// This is not cancel safe.
    async fn read_raw_be_expect(
        &mut self,
        tag: BeTag,
        max: u32,
    ) -> Result<&mut [u8], PostgresError> {
        let (actual_tag, msg) = self.read_raw_be(max).await?;
        if actual_tag != tag {
            return Err(PostgresError::Unexpected(UnexpectedMessage {
                expected: tag,
                tag: actual_tag,
                data: msg.to_vec().into(),
            }));
        }
        Ok(msg)
    }

    /// Read a postgres backend auth message.
    /// This is not cancel safe.
    pub async fn read_auth_message(&mut self) -> Result<(AuthTag, &mut [u8]), PostgresError> {
        const MAX_AUTH_LENGTH: u32 = 512;

        self.read_raw_be_expect(BE_AUTH_MESSAGE, MAX_AUTH_LENGTH)
            .await?
            .split_first_chunk_mut()
            .map(|(tag, msg)| (AuthTag(i32::from_be_bytes(*tag)), msg))
            .ok_or(PostgresError::InvalidAuthMessage)
    }
}

impl<S: AsyncWrite + Unpin> PqBeStream<S> {
    /// Write a raw message to the internal buffer.
    pub fn write_raw(&mut self, size_hint: usize, tag: u8, f: impl FnOnce(&mut Vec<u8>)) {
        self.write.write_raw(size_hint, tag, f);
    }

    /// Flush the output buffer into the underlying stream.
    ///
    /// This is cancel safe.
    pub async fn flush(&mut self) -> io::Result<()> {
        self.stream.write_all_buf(&mut self.write).await?;
        self.write.reset();

        self.stream.flush().await?;

        Ok(())
    }

    /// Flush the output buffer into the underlying stream.
    ///
    /// This is cancel safe.
    pub async fn flush_and_into_inner(mut self) -> io::Result<S> {
        self.flush().await?;
        Ok(self.stream)
    }
}

#[derive(Debug, Error)]
pub enum PostgresError {
    #[error("postgres responded with error {0}")]
    Error(#[from] BackendError),
    #[error("postgres responded with an unexpected message: {0}")]
    Unexpected(#[from] UnexpectedMessage),
    #[error("postgres responded with an invalid authentication message")]
    InvalidAuthMessage,
    #[error("IO error from compute: {0}")]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
#[error("expected {expected}, got {tag} with data {data:?}")]
pub struct UnexpectedMessage {
    expected: BeTag,
    tag: BeTag,
    data: Bytes,
}

pub struct BackendError {
    data: Bytes,
}

impl BackendError {
    pub fn parse(&self) -> (ErrorCode, &[u8]) {
        let mut code = &[] as &[u8];
        let mut message = &[] as &[u8];

        for param in self.data.split(|b| *b == 0) {
            match param {
                [b'M', rest @ ..] => message = rest,
                [b'C', rest @ ..] => code = rest,
                _ => {}
            }
        }

        let code = code.try_into().map_or(SQLSTATE_INTERNAL_ERROR, ErrorCode);

        (code, message)
    }
}

impl std::fmt::Debug for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self.data)
    }
}
impl std::error::Error for BackendError {}
