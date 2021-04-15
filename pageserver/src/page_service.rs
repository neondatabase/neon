//
//   The Page Service listens for client connections and serves their GetPage@LSN
// requests.
//
//   It is possible to connect here using usual psql/pgbench/libpq. Following
// commands are supported now:
//     *status* -- show actual info about this pageserver,
//     *pagestream* -- enter mode where smgr and pageserver talk with their
//  custom protocol.
//     *callmemaybe $url* -- ask pageserver to start walreceiver on $url
//

use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, Bytes, BytesMut};
use log::*;
use std::io;
use std::thread;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime;
use tokio::task;

use crate::page_cache;
use crate::walreceiver;
use crate::PageServerConf;

type Result<T> = std::result::Result<T, io::Error>;

#[derive(Debug)]
enum FeMessage {
    StartupMessage(FeStartupMessage),
    Query(FeQueryMessage),
    Terminate,

    //
    // All that messages are actually CopyData from libpq point of view.
    //
    ZenithExistsRequest(ZenithRequest),
    ZenithTruncRequest(ZenithRequest),
    ZenithUnlinkRequest(ZenithRequest),
    ZenithNblocksRequest(ZenithRequest),
    ZenithReadRequest(ZenithRequest),
    ZenithCreateRequest(ZenithRequest),
    ZenithExtendRequest(ZenithRequest),
}

#[derive(Debug)]
enum BeMessage {
    AuthenticationOk,
    ReadyForQuery,
    RowDescription,
    DataRow,
    CommandComplete,
    ControlFile,

    //
    // All that messages are actually CopyData from libpq point of view.
    //
    ZenithStatusResponse(ZenithStatusResponse),
    ZenithNblocksResponse(ZenithStatusResponse),
    ZenithReadResponse(ZenithReadResponse),
}

#[derive(Debug)]
struct ZenithRequest {
    spcnode: u32,
    dbnode: u32,
    relnode: u32,
    forknum: u8,
    blkno: u32,
    lsn: u64,
}

#[derive(Debug)]
struct ZenithStatusResponse {
    ok: bool,
    n_blocks: u32,
}

#[derive(Debug)]
struct ZenithReadResponse {
    ok: bool,
    n_blocks: u32,
    page: Bytes,
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
    Normal,
}

impl FeStartupMessage {
    pub fn parse(buf: &mut BytesMut) -> Result<Option<FeMessage>> {
        const MAX_STARTUP_PACKET_LENGTH: u32 = 10000;
        const CANCEL_REQUEST_CODE: u32 = (1234 << 16) | 5678;
        const NEGOTIATE_SSL_CODE: u32 = (1234 << 16) | 5679;
        const NEGOTIATE_GSS_CODE: u32 = (1234 << 16) | 5680;

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
            _ => StartupRequestCode::Normal,
        };

        buf.advance(len as usize);
        Ok(Some(FeMessage::StartupMessage(FeStartupMessage {
            version,
            kind,
        })))
    }
}

#[derive(Debug)]
struct Buffer {
    bytes: Bytes,
    idx: usize,
}

#[derive(Debug)]
struct FeQueryMessage {
    body: Bytes,
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

        let mut body = buf.split_to(total_len);
        body.advance(5);

        match tag {
            b'Q' => Ok(Some(FeMessage::Query(FeQueryMessage {
                body: body.freeze(),
            }))),
            b'X' => Ok(Some(FeMessage::Terminate)),
            b'd' => {
                let smgr_tag = body.get_u8();
                let zreq = ZenithRequest {
                    spcnode: body.get_u32(),
                    dbnode: body.get_u32(),
                    relnode: body.get_u32(),
                    forknum: body.get_u8(),
                    blkno: body.get_u32(),
                    lsn: body.get_u64(),
                };

                // TODO: consider using protobuf or serde bincode for less error prone
                // serialization.
                match smgr_tag {
                    0 => Ok(Some(FeMessage::ZenithExistsRequest(zreq))),
                    1 => Ok(Some(FeMessage::ZenithTruncRequest(zreq))),
                    2 => Ok(Some(FeMessage::ZenithUnlinkRequest(zreq))),
                    3 => Ok(Some(FeMessage::ZenithNblocksRequest(zreq))),
                    4 => Ok(Some(FeMessage::ZenithReadRequest(zreq))),
                    5 => Ok(Some(FeMessage::ZenithCreateRequest(zreq))),
                    6 => Ok(Some(FeMessage::ZenithExtendRequest(zreq))),
                    _ => Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("unknown smgr message tag: {},'{:?}'", smgr_tag, buf),
                    )),
                }
            }
            tag => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown message tag: {},'{:?}'", tag, buf),
            )),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

pub fn thread_main(conf: PageServerConf) {
    // Create a new thread pool
    //
    // FIXME: keep it single-threaded for now, make it easier to debug with gdb,
    // and we're not concerned with performance yet.
    //let runtime = runtime::Runtime::new().unwrap();
    let runtime = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    info!("Starting page server on {}", conf.listen_addr);

    runtime.block_on(async {
        let listener = TcpListener::bind(conf.listen_addr).await.unwrap();

        loop {
            let (socket, peer_addr) = listener.accept().await.unwrap();
            debug!("accepted connection from {}", peer_addr);
            socket.set_nodelay(true).unwrap();
            let mut conn_handler = Connection::new(conf.clone(), socket);

            task::spawn(async move {
                if let Err(err) = conn_handler.run().await {
                    error!("error: {}", err);
                }
            });
        }
    });
}

#[derive(Debug)]
struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
    init_done: bool,
    conf: PageServerConf,
}

impl Connection {
    pub fn new(conf: PageServerConf, socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(10 * 1024),
            init_done: false,
            conf: conf,
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
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "connection reset by peer",
                    ));
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
                self.stream
                    .write_i32(4 + 2 + b.len() as i32 + 3 * (4 + 2))
                    .await?;

                self.stream.write_i16(1).await?;
                self.stream.write_all(&mut b).await?;
                self.stream.write_i32(0).await?; /* table oid */
                self.stream.write_i16(0).await?; /* attnum */
                self.stream.write_i32(25).await?; /* TEXTOID */
                self.stream.write_i16(-1).await?; /* typlen */
                self.stream.write_i32(0).await?; /* typmod */
                self.stream.write_i16(0).await?; /* format code */
            }

            // XXX: accept some text data
            BeMessage::DataRow => {
                // XXX
                let mut b = Bytes::from("hello world");

                self.stream.write_u8(b'D').await?;
                self.stream.write_i32(4 + 2 + 4 + b.len() as i32).await?;

                self.stream.write_i16(1).await?;
                self.stream.write_i32(b.len() as i32).await?;
                self.stream.write_all(&mut b).await?;
            }

            BeMessage::ControlFile => {
                // TODO pass checkpoint and xid info in this message
                let mut b = Bytes::from("hello pg_control");

                self.stream.write_u8(b'D').await?;
                self.stream.write_i32(4 + 2 + 4 + b.len() as i32).await?;

                self.stream.write_i16(1).await?;
                self.stream.write_i32(b.len() as i32).await?;
                self.stream.write_all(&mut b).await?;
            }

            BeMessage::CommandComplete => {
                let mut b = Bytes::from("SELECT 1\0");

                self.stream.write_u8(b'C').await?;
                self.stream.write_i32(4 + b.len() as i32).await?;
                self.stream.write_all(&mut b).await?;
            }

            BeMessage::ZenithStatusResponse(resp) => {
                self.stream.write_u8(b'd').await?;
                self.stream.write_u32(4 + 1 + 1 + 4).await?;
                self.stream.write_u8(100).await?; /* tag from pagestore_client.h */
                self.stream.write_u8(resp.ok as u8).await?;
                self.stream.write_u32(resp.n_blocks).await?;
            }

            BeMessage::ZenithNblocksResponse(resp) => {
                self.stream.write_u8(b'd').await?;
                self.stream.write_u32(4 + 1 + 1 + 4).await?;
                self.stream.write_u8(101).await?; /* tag from pagestore_client.h */
                self.stream.write_u8(resp.ok as u8).await?;
                self.stream.write_u32(resp.n_blocks).await?;
            }

            BeMessage::ZenithReadResponse(resp) => {
                self.stream.write_u8(b'd').await?;
                self.stream
                    .write_u32(4 + 1 + 1 + 4 + resp.page.len() as u32)
                    .await?;
                self.stream.write_u8(102).await?; /* tag from pagestore_client.h */
                self.stream.write_u8(resp.ok as u8).await?;
                self.stream.write_u32(resp.n_blocks).await?;
                self.stream.write_all(&mut resp.page.clone()).await?;
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
                    trace!("got message {:?}", m);

                    match m.kind {
                        StartupRequestCode::NegotiateGss | StartupRequestCode::NegotiateSsl => {
                            let mut b = Bytes::from("N");
                            self.stream.write_all(&mut b).await?;
                            self.stream.flush().await?;
                        }
                        StartupRequestCode::Normal => {
                            self.write_message_noflush(&BeMessage::AuthenticationOk)
                                .await?;
                            self.write_message(&BeMessage::ReadyForQuery).await?;
                            self.init_done = true;
                        }
                        StartupRequestCode::Cancel => return Ok(()),
                    }
                }
                Some(FeMessage::Query(m)) => {
                    self.process_query(&m).await?;
                }
                Some(FeMessage::Terminate) => {
                    break;
                }
                None => {
                    info!("connection closed");
                    break;
                }
                _ => {
                    return Err(io::Error::new(io::ErrorKind::Other, "unexpected message"));
                }
            }
        }

        Ok(())
    }

    async fn process_query(&mut self, q: &FeQueryMessage) -> Result<()> {
        trace!("got query {:?}", q.body);

        if q.body.starts_with(b"controlfile") {
            self.handle_controlfile().await
        } else if q.body.starts_with(b"pagestream ") {
            let (_l, r) = q.body.split_at("pagestream ".len());
            let mut r = r.to_vec();
            r.pop();
            let sysid = String::from_utf8(r).unwrap().trim().to_string();
            let sysid: u64 = sysid.parse().unwrap(); // XXX

            self.handle_pagerequests(sysid).await
        } else if q.body.starts_with(b"callmemaybe ") {
            let (_l, r) = q.body.split_at("callmemaybe ".len());
            let mut r = r.to_vec();
            r.pop();
            let connstr = String::from_utf8(r).unwrap().trim().to_string();

            let conf_copy = self.conf.clone();
            let _walreceiver_thread = thread::Builder::new()
                .name("WAL receiver thread".into())
                .spawn(move || {
                    walreceiver::thread_main(conf_copy, &connstr);
                })
                .unwrap();

            // generic ack:
            self.write_message_noflush(&BeMessage::RowDescription)
                .await?;
            self.write_message_noflush(&BeMessage::DataRow).await?;
            self.write_message_noflush(&BeMessage::CommandComplete)
                .await?;
            self.write_message(&BeMessage::ReadyForQuery).await
        } else if q.body.starts_with(b"status") {
            self.write_message_noflush(&BeMessage::RowDescription)
                .await?;
            self.write_message_noflush(&BeMessage::DataRow).await?;
            self.write_message_noflush(&BeMessage::CommandComplete)
                .await?;
            self.write_message(&BeMessage::ReadyForQuery).await
        } else {
            self.write_message_noflush(&BeMessage::RowDescription)
                .await?;
            self.write_message_noflush(&BeMessage::DataRow).await?;
            self.write_message_noflush(&BeMessage::CommandComplete)
                .await?;
            self.write_message(&BeMessage::ReadyForQuery).await
        }
    }

    async fn handle_controlfile(&mut self) -> Result<()> {
        self.write_message_noflush(&BeMessage::RowDescription)
            .await?;
        self.write_message_noflush(&BeMessage::ControlFile).await?;
        self.write_message_noflush(&BeMessage::CommandComplete)
            .await?;
        self.write_message(&BeMessage::ReadyForQuery).await
    }

    async fn handle_pagerequests(&mut self, sysid: u64) -> Result<()> {
        /* switch client to COPYBOTH */
        self.stream.write_u8(b'W').await?;
        self.stream.write_i32(4 + 1 + 2).await?;
        self.stream.write_u8(0).await?; /* copy_is_binary */
        self.stream.write_i16(0).await?; /* numAttributes */
        self.stream.flush().await?;

        let pcache = page_cache::get_pagecache(self.conf.clone(), sysid);

        loop {
            let message = self.read_message().await?;
            /*
                        if let Some(m) = &message {
                            trace!("query({}): {:?}", sysid, m);
                        };
            */
            if message.is_none() {
                // connection was closed
                return Ok(());
            }

            match message {
                Some(FeMessage::ZenithExistsRequest(req)) => {
                    let tag = page_cache::RelTag {
                        spcnode: req.spcnode,
                        dbnode: req.dbnode,
                        relnode: req.relnode,
                        forknum: req.forknum,
                    };

                    let exist = pcache.relsize_exist(&tag);

                    self.write_message(&BeMessage::ZenithStatusResponse(ZenithStatusResponse {
                        ok: exist,
                        n_blocks: 0,
                    }))
                    .await?
                }
                Some(FeMessage::ZenithTruncRequest(_)) => {
                    self.write_message(&BeMessage::ZenithStatusResponse(ZenithStatusResponse {
                        ok: true,
                        n_blocks: 0,
                    }))
                    .await?
                }
                Some(FeMessage::ZenithUnlinkRequest(_)) => {
                    self.write_message(&BeMessage::ZenithStatusResponse(ZenithStatusResponse {
                        ok: true,
                        n_blocks: 0,
                    }))
                    .await?
                }
                Some(FeMessage::ZenithNblocksRequest(req)) => {
                    let tag = page_cache::RelTag {
                        spcnode: req.spcnode,
                        dbnode: req.dbnode,
                        relnode: req.relnode,
                        forknum: req.forknum,
                    };

                    let n_blocks = pcache.relsize_get(&tag);

                    self.write_message(&BeMessage::ZenithNblocksResponse(ZenithStatusResponse {
                        ok: true,
                        n_blocks: n_blocks,
                    }))
                    .await?
                }
                Some(FeMessage::ZenithReadRequest(req)) => {
                    let buf_tag = page_cache::BufferTag {
                        rel: page_cache::RelTag {
                            spcnode: req.spcnode,
                            dbnode: req.dbnode,
                            relnode: req.relnode,
                            forknum: req.forknum,
                        },
                        blknum: req.blkno,
                    };

                    let msg = match pcache.get_page_at_lsn(buf_tag, req.lsn) {
                        Ok(p) => BeMessage::ZenithReadResponse(ZenithReadResponse {
                            ok: true,
                            n_blocks: 0,
                            page: p,
                        }),
                        Err(e) => {
                            const ZERO_PAGE: [u8; 8192] = [0; 8192];
                            error!("get_page_at_lsn: {}", e);
                            BeMessage::ZenithReadResponse(ZenithReadResponse {
                                ok: false,
                                n_blocks: 0,
                                page: Bytes::from_static(&ZERO_PAGE),
                            })
                        }
                    };

                    self.write_message(&msg).await?
                }
                Some(FeMessage::ZenithCreateRequest(req)) => {
                    let tag = page_cache::RelTag {
                        spcnode: req.spcnode,
                        dbnode: req.dbnode,
                        relnode: req.relnode,
                        forknum: req.forknum,
                    };

                    pcache.relsize_inc(&tag, None);

                    self.write_message(&BeMessage::ZenithStatusResponse(ZenithStatusResponse {
                        ok: true,
                        n_blocks: 0,
                    }))
                    .await?
                }
                Some(FeMessage::ZenithExtendRequest(req)) => {
                    let tag = page_cache::RelTag {
                        spcnode: req.spcnode,
                        dbnode: req.dbnode,
                        relnode: req.relnode,
                        forknum: req.forknum,
                    };

                    pcache.relsize_inc(&tag, Some(req.blkno));

                    self.write_message(&BeMessage::ZenithStatusResponse(ZenithStatusResponse {
                        ok: true,
                        n_blocks: 0,
                    }))
                    .await?
                }
                _ => {}
            }
        }
    }
}
