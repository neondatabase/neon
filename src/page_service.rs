//
// The Page Service listens for client connections and serves their GetPage@LSN requests
//

use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::runtime;
use tokio::task;
use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, Bytes, BytesMut};
use std::io::{self};

type Result<T> = std::result::Result<T, io::Error>;

///
/// Basic support for postgres backend protocol.
///

enum FeMessage {
    StartupMessage(FeStartupMessage),
    Query(FeQueryMessage),
    Terminate
}

enum BeMessage {
    AuthenticationOk,
    ReadyForQuery,
    RowDescription,
    DataRow,
    CommandComplete
}


#[derive(Debug)]
struct FeStartupMessage {
    version: u32,
    kind: StartupRequestCode,
}

#[derive(Debug)]
enum StartupRequestCode {
    Cancel,
    NegotiateSsl,
    NegotiateGss,
    Normal
}

impl FeStartupMessage {
    pub fn parse(buf: &mut BytesMut) -> Result<Option<FeMessage>> {
        const MAX_STARTUP_PACKET_LENGTH: u32 = 10000;
        const CANCEL_REQUEST_CODE: u32 = (1234 << 16) | 5678;
        const NEGOTIATE_SSL_CODE: u32  = (1234 << 16) | 5679;
        const NEGOTIATE_GSS_CODE: u32  = (1234 << 16) | 5680;

        if buf.len() < 4 {
            return Ok(None);
        }
        let len = BigEndian::read_u32(&buf[0..4]);

        if len < 4 || len as u32 > MAX_STARTUP_PACKET_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid message length",
            ));
        }

        let version = BigEndian::read_u32(&buf[4..8]);

        let kind = match version {
            CANCEL_REQUEST_CODE => StartupRequestCode::Cancel,
            NEGOTIATE_SSL_CODE => StartupRequestCode::NegotiateSsl,
            NEGOTIATE_GSS_CODE => StartupRequestCode::NegotiateGss,
            _ => StartupRequestCode::Normal
        };

        buf.advance(len as usize);
        Ok(Some(FeMessage::StartupMessage(FeStartupMessage{ version, kind})))
    }
}

#[derive(Debug)]
struct Buffer {
    bytes: Bytes,
    idx: usize,
}

#[derive(Debug)]
struct FeQueryMessage {
    body: Buffer
}

impl FeMessage {
    pub fn parse(buf: &mut BytesMut) -> Result<Option<FeMessage>> {
        if buf.len() < 5 {
            let to_read = 5 - buf.len();
            buf.reserve(to_read);
            return Ok(None);
        }

        let tag = buf[0];
        let len = BigEndian::read_u32(&buf[1..5]);

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

        let buf = Buffer {
            bytes: buf.split_to(total_len).freeze(),
            idx: 5,
        };

        match tag {
            b'Q' => Ok(Some(FeMessage::Query(FeQueryMessage{body: buf}))),
            b'X' => Ok(Some(FeMessage::Terminate)),
            tag => {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown message tag: {}", tag),
                ))
            }
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

pub fn thread_main() {

    // Create a new thread pool
    let runtime = runtime::Runtime::new().unwrap();

    let listen_address = "127.0.0.1:5430";
    println!("Starting page server on {}", listen_address);

    runtime.block_on(async {
        let _unused = page_service_main(listen_address).await;
    });
}

async fn page_service_main(listen_address: &str) {

    let listener = TcpListener::bind(listen_address).await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();

        let mut conn_handler = Connection::new(socket);

        task::spawn(async move {
            if let Err(err) = conn_handler.run().await {
                println!("error: {}", err);
            }
        });
    }
}

#[derive(Debug)]
struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
    init_done: bool
}

impl Connection {

    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(10 * 1024),
            init_done: false
        }
    }

    //
    // Read full message or return None if connection is closed
    //
    async fn read_message(&mut self) -> Result<Option<FeMessage>> {
        loop {
            if let Some(message) = self.parse_message()? {
                return Ok(Some(message));
            }

            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(io::Error::new(io::ErrorKind::Other,"connection reset by peer"));
                }
            }
        }
    }

    fn parse_message(&mut self) -> Result<Option<FeMessage>> {

        if !self.init_done {
            FeStartupMessage::parse(&mut self.buffer)
        } else {
            FeMessage::parse(&mut self.buffer)
        }

    }

    async fn write_message_noflush(&mut self, message: &BeMessage) -> io::Result<()> {

        match message {
            BeMessage::AuthenticationOk => {
                self.stream.write_u8(b'R').await?;
                self.stream.write_i32(4 + 4).await?;
                self.stream.write_i32(0).await?;
            }

            BeMessage::ReadyForQuery => {
                self.stream.write_u8(b'Z').await?;
                self.stream.write_i32(4 + 1).await?;
                self.stream.write_u8(b'I').await?;
            }

            BeMessage::RowDescription => {
                // XXX
                let mut b = Bytes::from("data\0");

                self.stream.write_u8(b'T').await?;
                self.stream.write_i32(4 + 2 + b.len() as i32 + 3*(4 + 2)).await?;

                self.stream.write_i16(1).await?;
                self.stream.write_buf(&mut b).await?;
                self.stream.write_i32(0).await?;    /* table oid */
                self.stream.write_i16(0).await?;    /* attnum */
                self.stream.write_i32(25).await?;   /* TEXTOID */
                self.stream.write_i16(-1).await?;   /* typlen */
                self.stream.write_i32(0).await?;    /* typmod */
                self.stream.write_i16(0).await?;    /* format code */
            }

            // XXX: accept some text data
            BeMessage::DataRow => {
                // XXX
                let mut b = Bytes::from("hello world");

                self.stream.write_u8(b'D').await?;
                self.stream.write_i32(4 + 2 + 4 + b.len() as i32).await?;

                self.stream.write_i16(1).await?;
                self.stream.write_i32(b.len() as i32).await?;
                self.stream.write_buf(&mut b).await?;
            }

            BeMessage::CommandComplete => {
                let mut b = Bytes::from("SELECT 1\0");

                self.stream.write_u8(b'C').await?;
                self.stream.write_i32(4 + b.len() as i32).await?;
                self.stream.write_buf(&mut b).await?;
            }
        }

        Ok(())
    }

    async fn write_message(&mut self, message: &BeMessage) -> io::Result<()> {
        self.write_message_noflush(message).await?;
        self.stream.flush().await
    }

    async fn run(&mut self) -> Result<()> {

        loop {

            match self.read_message().await? {
                Some(FeMessage::StartupMessage(m)) => {
                    println!("got message {:?}", m);

                    match m.kind {
                        StartupRequestCode::NegotiateGss | StartupRequestCode::NegotiateSsl => {
                            let mut b = Bytes::from("N");
                            self.stream.write_buf(&mut b).await?;
                            self.stream.flush().await?;
                        }
                        StartupRequestCode::Normal => {
                            self.write_message_noflush(&BeMessage::AuthenticationOk).await?;
                            self.write_message(&BeMessage::ReadyForQuery).await?;
                            self.init_done = true;
                        },
                        StartupRequestCode::Cancel => return Ok(())
                    }
                },
                Some(FeMessage::Query(m)) => {
                    self.write_message_noflush(&BeMessage::RowDescription).await?;
                    self.write_message_noflush(&BeMessage::DataRow).await?;
                    self.write_message_noflush(&BeMessage::CommandComplete).await?;
                    self.write_message(&BeMessage::ReadyForQuery).await?;
                },
                Some(FeMessage::Terminate) => {
                    break;
                }
                None => {
                    println!("connection closed");
                    break;
                }
            }
        }

        Ok(())
    }

}


