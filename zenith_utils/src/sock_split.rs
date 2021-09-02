use std::{
    io::{self, BufReader, Write},
    net::{Shutdown, TcpStream},
    sync::Arc,
};

use rustls::Session;

/// Wrapper supporting reads of a shared TcpStream.
pub struct ArcTcpRead(Arc<TcpStream>);

impl io::Read for ArcTcpRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        (&*self.0).read(buf)
    }
}

impl std::ops::Deref for ArcTcpRead {
    type Target = TcpStream;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

/// Wrapper around a TCP Stream supporting buffered reads.
pub struct BufStream(BufReader<ArcTcpRead>);

impl io::Read for BufStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl io::Write for BufStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.get_ref().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.get_ref().flush()
    }
}

impl BufStream {
    /// Unwrap into the internal BufReader.
    fn into_reader(self) -> BufReader<ArcTcpRead> {
        self.0
    }

    /// Returns a reference to the underlying TcpStream.
    fn get_ref(&self) -> &TcpStream {
        &*self.0.get_ref().0
    }
}

pub enum ReadStream {
    Tcp(BufReader<ArcTcpRead>),
    Tls(rustls_split::ReadHalf<rustls::ServerSession>),
}

impl io::Read for ReadStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(reader) => reader.read(buf),
            Self::Tls(read_half) => read_half.read(buf),
        }
    }
}

impl ReadStream {
    pub fn shutdown(&mut self, how: Shutdown) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.get_ref().shutdown(how),
            Self::Tls(write_half) => write_half.shutdown(how),
        }
    }
}

pub enum WriteStream {
    Tcp(Arc<TcpStream>),
    Tls(rustls_split::WriteHalf<rustls::ServerSession>),
}

impl WriteStream {
    pub fn shutdown(&mut self, how: Shutdown) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.shutdown(how),
            Self::Tls(write_half) => write_half.shutdown(how),
        }
    }
}

impl io::Write for WriteStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.as_ref().write(buf),
            Self::Tls(write_half) => write_half.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.as_ref().flush(),
            Self::Tls(write_half) => write_half.flush(),
        }
    }
}

pub struct TlsBoxed {
    stream: BufStream,
    session: rustls::ServerSession,
}

impl TlsBoxed {
    fn rustls_stream(&mut self) -> rustls::Stream<rustls::ServerSession, BufStream> {
        rustls::Stream::new(&mut self.session, &mut self.stream)
    }
}

pub enum BidiStream {
    Tcp(BufStream),
    /// This variant is boxed, because [`rustls::ServerSession`] is quite larger than [`BufStream`].
    Tls(Box<TlsBoxed>),
}

impl BidiStream {
    pub fn from_tcp(stream: TcpStream) -> Self {
        Self::Tcp(BufStream(BufReader::new(ArcTcpRead(Arc::new(stream)))))
    }

    pub fn shutdown(&mut self, how: Shutdown) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.get_ref().shutdown(how),
            Self::Tls(tls_boxed) => {
                if how == Shutdown::Read {
                    tls_boxed.stream.get_ref().shutdown(how)
                } else {
                    tls_boxed.session.send_close_notify();
                    let res = tls_boxed.rustls_stream().flush();
                    tls_boxed.stream.get_ref().shutdown(how)?;
                    res
                }
            }
        }
    }

    /// Split the bi-directional stream into two owned read and write halves.
    pub fn split(self) -> (ReadStream, WriteStream) {
        match self {
            Self::Tcp(stream) => {
                let reader = stream.into_reader();
                let stream: Arc<TcpStream> = reader.get_ref().0.clone();

                (ReadStream::Tcp(reader), WriteStream::Tcp(stream))
            }
            Self::Tls(tls_boxed) => {
                let reader = tls_boxed.stream.into_reader();
                let buffer_data = reader.buffer().to_owned();
                let read_buf_cfg = rustls_split::BufCfg::with_data(buffer_data, 8192);
                let write_buf_cfg = rustls_split::BufCfg::with_capacity(8192);

                // TODO would be nice to avoid the Arc here
                let socket = Arc::try_unwrap(reader.into_inner().0).unwrap();

                let (read_half, write_half) =
                    rustls_split::split(socket, tls_boxed.session, read_buf_cfg, write_buf_cfg);
                (ReadStream::Tls(read_half), WriteStream::Tls(write_half))
            }
        }
    }

    pub fn start_tls(self, mut session: rustls::ServerSession) -> io::Result<Self> {
        match self {
            Self::Tcp(mut stream) => {
                session.complete_io(&mut stream)?;
                assert!(!session.is_handshaking());
                Ok(Self::Tls(Box::new(TlsBoxed { stream, session })))
            }
            Self::Tls { .. } => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TLS is already started on this stream",
            )),
        }
    }
}

impl io::Read for BidiStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.read(buf),
            Self::Tls(tls_boxed) => tls_boxed.rustls_stream().read(buf),
        }
    }
}

impl io::Write for BidiStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.write(buf),
            Self::Tls(tls_boxed) => tls_boxed.rustls_stream().write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.flush(),
            Self::Tls(tls_boxed) => tls_boxed.rustls_stream().flush(),
        }
    }
}
