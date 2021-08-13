use std::{
    io::{self, BufReader, Write},
    net::{Shutdown, TcpStream},
    sync::Arc,
};

use rustls::Session;

#[derive(Clone)]
pub struct TCPArc(Arc<TcpStream>);

impl io::Read for TCPArc {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        (&*self.0).read(buf)
    }
}

impl std::ops::Deref for TCPArc {
    type Target = TcpStream;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

pub enum OwnedReadHalf {
    Tcp(BufReader<TCPArc>),
    Tls(rustls_split::ReadHalf<rustls::ServerSession>),
}

impl io::Read for OwnedReadHalf {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(reader) => reader.read(buf),
            Self::Tls(read_half) => read_half.read(buf),
        }
    }
}

impl OwnedReadHalf {
    pub fn shutdown(&mut self, how: Shutdown) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.get_ref().shutdown(how),
            Self::Tls(write_half) => write_half.shutdown(how),
        }
    }
}

pub enum OwnedWriteHalf {
    Tcp(Arc<TcpStream>),
    Tls(rustls_split::WriteHalf<rustls::ServerSession>),
}

impl OwnedWriteHalf {
    pub fn shutdown(&mut self, how: Shutdown) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.shutdown(how),
            Self::Tls(write_half) => write_half.shutdown(how),
        }
    }
}

impl io::Write for OwnedWriteHalf {
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
    RawTcp(TcpStream),
    BufferedTcp(BufReader<TCPArc>),
    Tls {
        socket: TcpStream,
        session: rustls::ServerSession,
    },
}

impl BiDiStream {
    pub fn from_tcp(stream: TcpStream) -> Self {
        Self::RawTcp(stream)
    }

    pub fn shutdown(&mut self, how: Shutdown) -> io::Result<()> {
        match self {
            Self::RawTcp(socket) => socket.shutdown(how),
            Self::BufferedTcp(reader) => reader.get_ref().shutdown(how),
            Self::Tls { socket, session } => {
                if how == Shutdown::Read {
                    socket.shutdown(how)
                } else {
                    session.send_close_notify();
                    let mut stream = rustls::Stream::new(session, socket);
                    let res = stream.flush();
                    socket.shutdown(how)?;
                    res
                }
            }
        }
    }

    pub fn split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        match self {
            Self::RawTcp(stream) => {
                let stream = Arc::new(stream);
                let reader = BufReader::new(TCPArc(stream.clone()));

                (OwnedReadHalf::Tcp(reader), OwnedWriteHalf::Tcp(stream))
            }
            Self::BufferedTcp(reader) => {
                let stream: Arc<TcpStream> = reader.get_ref().0.clone();

                (OwnedReadHalf::Tcp(reader), OwnedWriteHalf::Tcp(stream))
            }
            Self::Tls { socket, session } => {
                const BUFFER_SIZE: usize = 8192;
                let (read_half, write_half) = rustls_split::split(socket, session, BUFFER_SIZE);
                (
                    OwnedReadHalf::Tls(read_half),
                    OwnedWriteHalf::Tls(write_half),
                )
            }
        }
    }

    pub fn start_tls(self, mut session: rustls::ServerSession) -> io::Result<Self> {
        match self {
            Self::RawTcp(mut socket) => {
                session.complete_io(&mut socket)?;
                assert!(!session.is_handshaking());
                Ok(Self::Tls { session, socket })
            }
            Self::BufferedTcp(_) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "can't start TLS on a buffered stream",
            )),
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
            Self::RawTcp(stream) => stream.read(buf),
            Self::BufferedTcp(reader) => reader.read(buf),
            Self::Tls { socket, session } => rustls::Stream::new(session, socket).read(buf),
        }
    }
}

impl io::Write for BiDiStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::RawTcp(stream) => stream.write(buf),
            Self::BufferedTcp(reader) => (&*reader.get_ref().0).write(buf),
            Self::Tls { socket, session } => rustls::Stream::new(session, socket).write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::RawTcp(stream) => stream.flush(),
            Self::BufferedTcp(reader) => (&*reader.get_ref().0).flush(),
            Self::Tls { socket, session } => rustls::Stream::new(session, socket).flush(),
        }
    }
}
