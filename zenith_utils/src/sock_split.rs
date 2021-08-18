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

pub enum ReadHalf {
    Tcp(BufReader<ArcTcpRead>),
    Tls(rustls_split::ReadHalf<rustls::ServerSession>),
}

impl io::Read for ReadHalf {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(reader) => reader.read(buf),
            Self::Tls(read_half) => read_half.read(buf),
        }
    }
}

impl ReadHalf {
    pub fn shutdown(&mut self, how: Shutdown) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.get_ref().shutdown(how),
            Self::Tls(write_half) => write_half.shutdown(how),
        }
    }
}

pub enum WriteHalf {
    Tcp(Arc<TcpStream>),
    Tls(rustls_split::WriteHalf<rustls::ServerSession>),
}

impl WriteHalf {
    pub fn shutdown(&mut self, how: Shutdown) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.shutdown(how),
            Self::Tls(write_half) => write_half.shutdown(how),
        }
    }
}

impl io::Write for WriteHalf {
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

pub enum BiDiStream {
    Tcp(BufStream),
    Tls {
        stream: BufStream,
        session: rustls::ServerSession,
    },
}

impl BiDiStream {
    pub fn from_tcp(stream: TcpStream) -> Self {
        Self::Tcp(BufStream(BufReader::new(ArcTcpRead(Arc::new(stream)))))
    }

    pub fn shutdown(&mut self, how: Shutdown) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.get_ref().shutdown(how),
            Self::Tls {
                stream: reader,
                session,
            } => {
                if how == Shutdown::Read {
                    reader.get_ref().shutdown(how)
                } else {
                    session.send_close_notify();
                    let mut stream = rustls::Stream::new(session, reader);
                    let res = stream.flush();
                    reader.get_ref().shutdown(how)?;
                    res
                }
            }
        }
    }

    /// Split the bi-directional stream into two owned read and write halves.
    pub fn split(self) -> (ReadHalf, WriteHalf) {
        match self {
            Self::Tcp(stream) => {
                let reader = stream.into_reader();
                let stream: Arc<TcpStream> = reader.get_ref().0.clone();

                (ReadHalf::Tcp(reader), WriteHalf::Tcp(stream))
            }
            Self::Tls { stream, session } => {
                let reader = stream.into_reader();
                let buffer_data = reader.buffer().to_owned();
                let read_buf_cfg = rustls_split::BufCfg::with_data(buffer_data, 8192);
                let write_buf_cfg = rustls_split::BufCfg::with_capacity(8192);

                // TODO would be nice to avoid the Arc here
                let socket = Arc::try_unwrap(reader.into_inner().0).unwrap();

                let (read_half, write_half) =
                    rustls_split::split(socket, session, read_buf_cfg, write_buf_cfg);
                (ReadHalf::Tls(read_half), WriteHalf::Tls(write_half))
            }
        }
    }

    pub fn start_tls(self, mut session: rustls::ServerSession) -> io::Result<Self> {
        match self {
            Self::Tcp(mut stream) => {
                session.complete_io(&mut stream)?;
                assert!(!session.is_handshaking());
                Ok(Self::Tls { stream, session })
            }
            Self::Tls { .. } => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TLS is already started on this stream",
            )),
        }
    }
}

impl io::Read for BiDiStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.read(buf),
            Self::Tls { stream, session } => rustls::Stream::new(session, stream).read(buf),
        }
    }
}

impl io::Write for BiDiStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.write(buf),
            Self::Tls { stream, session } => rustls::Stream::new(session, stream).write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.flush(),
            Self::Tls { stream, session } => rustls::Stream::new(session, stream).flush(),
        }
    }
}
