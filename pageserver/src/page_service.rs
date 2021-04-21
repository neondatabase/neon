//
//   The Page Service listens for client connections and serves their GetPage@LSN
// requests.
//
//   It is possible to connect here using usual psql/pgbench/libpq. Following
// commands are supported now:
//     *status* -- show actual info about this pageserver,
//     *pagestream* -- enter mode where smgr and pageserver talk with their
//  custom protocol.
//     *callmemaybe <zenith timelineid> $url* -- ask pageserver to start walreceiver on $url
//

use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::*;
use regex::Regex;
use std::io;
use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::task;

use crate::basebackup;
use crate::page_cache;
use crate::restore_local_repo;
use crate::walreceiver;
use crate::PageServerConf;
use crate::ZTimelineId;

type Result<T> = std::result::Result<T, io::Error>;

#[derive(Debug)]
enum FeMessage {
    StartupMessage(FeStartupMessage),
    Query(FeQueryMessage), // Simple query
    Parse(FeParseMessage), // Extended query protocol
    Describe(FeDescribeMessage),
    Bind(FeBindMessage),
    Execute(FeExecuteMessage),
    Close(FeCloseMessage),
    Sync,
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
    ParseComplete,
    ParameterDescription,
    NoData,
    BindComplete,
    CloseComplete,
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

// We only support the simple case of Parse on unnamed prepared statement and
// no params
#[derive(Debug)]
struct FeParseMessage {
    query_string: Bytes,
}

fn read_null_terminated(buf: &mut Bytes) -> Result<Bytes> {
    let mut result = BytesMut::new();

    loop {
        if !buf.has_remaining() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "no null-terminator in string",
            ));
        }

        let byte = buf.get_u8();

        if byte == 0 {
            break;
        }
        result.put_u8(byte);
    }
    return Ok(result.freeze());
}

impl FeParseMessage {
    pub fn parse(body: Bytes) -> Result<FeMessage> {
        let mut buf = body.clone();
        let _pstmt_name = read_null_terminated(&mut buf)?;
        let query_string = read_null_terminated(&mut buf)?;
        let nparams = buf.get_i16();

        // FIXME: the rust-postgres driver uses a named prepared statement
        // for copy_out(). We're not prepared to handle that correctly. For
        // now, just ignore the statement name, assuming that the client never
        // uses more than one prepared statement at a time.
        /*
        if pstmt_name.len() != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "named prepared statements not implemented in Parse",
            ));
        }
         */

        if nparams != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "query params not implemented",
            ));
        }

        Ok(FeMessage::Parse(FeParseMessage { query_string }))
    }
}

#[derive(Debug)]
struct FeDescribeMessage {
    kind: u8, // 'S' to describe a prepared statement; or 'P' to describe a portal.
              // we only support unnamed prepared stmt or portal
}

impl FeDescribeMessage {
    pub fn parse(body: Bytes) -> Result<FeMessage> {
        let mut buf = body.clone();
        let kind = buf.get_u8();
        let _pstmt_name = read_null_terminated(&mut buf)?;

        // FIXME: see FeParseMessage::parse
        /*
        if pstmt_name.len() != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "named prepared statements not implemented in Describe",
            ));
        }
        */

        if kind != b'S' {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "only prepared statmement Describe is implemented",
            ));
        }

        Ok(FeMessage::Describe(FeDescribeMessage { kind }))
    }
}

// we only support unnamed prepared stmt or portal
#[derive(Debug)]
struct FeExecuteMessage {
    /// max # of rows
    maxrows: i32,
}

impl FeExecuteMessage {
    pub fn parse(body: Bytes) -> Result<FeMessage> {
        let mut buf = body.clone();
        let portal_name = read_null_terminated(&mut buf)?;
        let maxrows = buf.get_i32();

        if portal_name.len() != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "named portals not implemented",
            ));
        }

        if maxrows != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "row limit in Execute message not supported",
            ));
        }

        Ok(FeMessage::Execute(FeExecuteMessage { maxrows }))
    }
}

// we only support unnamed prepared stmt and portal
#[derive(Debug)]
struct FeBindMessage {}

impl FeBindMessage {
    pub fn parse(body: Bytes) -> Result<FeMessage> {
        let mut buf = body.clone();
        let portal_name = read_null_terminated(&mut buf)?;
        let _pstmt_name = read_null_terminated(&mut buf)?;

        if portal_name.len() != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "named portals not implemented",
            ));
        }

        // FIXME: see FeParseMessage::parse
        /*
        if pstmt_name.len() != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "named prepared statements not implemented",
            ));
        }
        */

        Ok(FeMessage::Bind(FeBindMessage {}))
    }
}

// we only support unnamed prepared stmt and portal
#[derive(Debug)]
struct FeCloseMessage {}

impl FeCloseMessage {
    pub fn parse(body: Bytes) -> Result<FeMessage> {
        let mut buf = body.clone();
        let _kind = buf.get_u8();
        let _pstmt_or_portal_name = read_null_terminated(&mut buf)?;

        // FIXME: we do nothing with Close

        Ok(FeMessage::Close(FeCloseMessage {}))
    }
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

        let mut body = body.freeze();

        match tag {
            b'Q' => Ok(Some(FeMessage::Query(FeQueryMessage { body: body }))),
            b'P' => Ok(Some(FeParseMessage::parse(body)?)),
            b'D' => Ok(Some(FeDescribeMessage::parse(body)?)),
            b'E' => Ok(Some(FeExecuteMessage::parse(body)?)),
            b'B' => Ok(Some(FeBindMessage::parse(body)?)),
            b'C' => Ok(Some(FeCloseMessage::parse(body)?)),
            b'S' => Ok(Some(FeMessage::Sync)),
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

pub fn thread_main(conf: &PageServerConf) {
    // Create a new thread pool
    //
    // FIXME: It would be nice to keep this single-threaded for debugging purposes,
    // but that currently leads to a deadlock: if a GetPage@LSN request arrives
    // for an LSN that hasn't been received yet, the thread gets stuck waiting for
    // the WAL to arrive. If the WAL receiver hasn't been launched yet, i.e
    // we haven't received a "callmemaybe" request yet to tell us where to get the
    // WAL, we will not have a thread available to process the "callmemaybe"
    // request when it does arrive. Using a thread pool alleviates the problem so
    // that it doesn't happen in the tests anymore, but in principle it could still
    // happen if we receive enough GetPage@LSN requests to consume all of the
    // available threads.
    //let runtime = runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let runtime = runtime::Runtime::new().unwrap();

    info!("Starting page server on {}", conf.listen_addr);

    let runtime_ref = Arc::new(runtime);

    runtime_ref.clone().block_on(async {
        let listener = TcpListener::bind(conf.listen_addr).await.unwrap();

        loop {
            let (socket, peer_addr) = listener.accept().await.unwrap();
            debug!("accepted connection from {}", peer_addr);
            let mut conn_handler = Connection::new(conf.clone(), socket, &runtime_ref);

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
    runtime: Arc<Runtime>,
}

impl Connection {
    pub fn new(conf: PageServerConf, socket: TcpStream, runtime: &Arc<Runtime>) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(10 * 1024),
            init_done: false,
            conf,
            runtime: Arc::clone(runtime),
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

            BeMessage::ParseComplete => {
                self.stream.write_u8(b'1').await?;
                self.stream.write_i32(4).await?;
            }

            BeMessage::BindComplete => {
                self.stream.write_u8(b'2').await?;
                self.stream.write_i32(4).await?;
            }

            BeMessage::CloseComplete => {
                self.stream.write_u8(b'3').await?;
                self.stream.write_i32(4).await?;
            }

            BeMessage::NoData => {
                self.stream.write_u8(b'n').await?;
                self.stream.write_i32(4).await?;
            }

            BeMessage::ParameterDescription => {
                self.stream.write_u8(b't').await?;
                self.stream.write_i32(6).await?;
                // we don't support params, so always 0
                self.stream.write_i16(0).await?;
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
        let mut unnamed_query_string = Bytes::new();
        loop {
            let msg = self.read_message().await?;
            info!("got message {:?}", msg);
            match msg {
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
                    self.process_query(m.body).await?;
                }
                Some(FeMessage::Parse(m)) => {
                    unnamed_query_string = m.query_string;
                    self.write_message(&BeMessage::ParseComplete).await?;
                }
                Some(FeMessage::Describe(_)) => {
                    self.write_message_noflush(&BeMessage::ParameterDescription)
                        .await?;
                    self.write_message(&BeMessage::NoData).await?;
                }
                Some(FeMessage::Bind(_)) => {
                    self.write_message(&BeMessage::BindComplete).await?;
                }
                Some(FeMessage::Close(_)) => {
                    self.write_message(&BeMessage::CloseComplete).await?;
                }
                Some(FeMessage::Execute(_)) => {
                    self.process_query(unnamed_query_string.clone()).await?;
                }
                Some(FeMessage::Sync) => {
                    self.write_message(&BeMessage::ReadyForQuery).await?;
                }
                Some(FeMessage::Terminate) => {
                    break;
                }
                None => {
                    info!("connection closed");
                    break;
                }
                x => {
                    error!("unexpected message type : {:?}", x);
                    return Err(io::Error::new(io::ErrorKind::Other, "unexpected message"));
                }
            }
        }

        Ok(())
    }

    async fn process_query(&mut self, query_string: Bytes) -> Result<()> {
        debug!("process query {:?}", query_string);

        // remove null terminator, if any
        let mut query_string = query_string.clone();
        if query_string.last() == Some(&0) {
            query_string.truncate(query_string.len() - 1);
        }

        if query_string.starts_with(b"controlfile") {
            self.handle_controlfile().await
        } else if query_string.starts_with(b"pagestream ") {
            let (_l, r) = query_string.split_at("pagestream ".len());
            let timelineid_str = String::from_utf8(r.to_vec()).unwrap();
            let timelineid = ZTimelineId::from_str(&timelineid_str).unwrap();

            self.handle_pagerequests(timelineid).await
        } else if query_string.starts_with(b"basebackup ") {
            let (_l, r) = query_string.split_at("basebackup ".len());
            let r = r.to_vec();
            let timelineid_str = String::from(String::from_utf8(r).unwrap().trim_end());
            info!("got basebackup command: \"{}\"", timelineid_str);
            let timelineid = ZTimelineId::from_str(&timelineid_str).unwrap();

            // Check that the timeline exists
            self.handle_basebackup_request(timelineid).await?;
            self.write_message_noflush(&BeMessage::CommandComplete)
                .await?;
            self.write_message(&BeMessage::ReadyForQuery).await
        } else if query_string.starts_with(b"callmemaybe ") {
            let query_str = String::from_utf8(query_string.to_vec())
                .unwrap()
                .to_string();

            // callmemaybe <zenith timelineid as hex string> <connstr>
            let re = Regex::new(r"^callmemaybe ([[:xdigit:]]+) (.*)$").unwrap();
            let caps = re.captures(&query_str);
            let caps = caps.unwrap();

            let timelineid = ZTimelineId::from_str(caps.get(1).unwrap().as_str().clone()).unwrap();
            let connstr: String = String::from(caps.get(2).unwrap().as_str());

            // Check that the timeline exists
            let pcache = page_cache::get_or_restore_pagecache(&self.conf, timelineid);
            if pcache.is_err() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("client requested callmemaybe on timeline {} which does not exist in page server", timelineid)));
            }

            walreceiver::launch_wal_receiver(&self.conf, timelineid, &connstr);

            self.write_message_noflush(&BeMessage::CommandComplete)
                .await?;
            self.write_message(&BeMessage::ReadyForQuery).await
        } else if query_string.starts_with(b"status") {
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

    async fn handle_pagerequests(&mut self, timelineid: ZTimelineId) -> Result<()> {
        // Check that the timeline exists
        let pcache = page_cache::get_or_restore_pagecache(&self.conf, timelineid);
        if pcache.is_err() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("client requested pagestream on timeline {} which does not exist in page server", timelineid)));
        }
        let pcache = pcache.unwrap();

        /* switch client to COPYBOTH */
        self.stream.write_u8(b'W').await?;
        self.stream.write_i32(4 + 1 + 2).await?;
        self.stream.write_u8(0).await?; /* copy_is_binary */
        self.stream.write_i16(0).await?; /* numAttributes */
        self.stream.flush().await?;

        loop {
            let message = self.read_message().await?;

            if let Some(m) = &message {
                info!("query({:?}): {:?}", timelineid, m);
            };

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
                        n_blocks,
                    }))
                    .await?
                }
                Some(FeMessage::ZenithReadRequest(req)) => {
                    let buf_tag = page_cache::BufferTag {
                        spcnode: req.spcnode,
                        dbnode: req.dbnode,
                        relnode: req.relnode,
                        forknum: req.forknum,
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

                    pcache.relsize_inc(&tag, 0);

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

                    pcache.relsize_inc(&tag, req.blkno + 1);

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

    async fn handle_basebackup_request(&mut self, timelineid: ZTimelineId) -> Result<()> {
        // check that the timeline exists
        let pcache = page_cache::get_or_restore_pagecache(&self.conf, timelineid);
        if pcache.is_err() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("client requested basebackup on timeline {} which does not exist in page server", timelineid)));
        }

        /* switch client to COPYOUT */
        let stream = &mut self.stream;
        stream.write_u8(b'H').await?;
        stream.write_i32(4 + 1 + 2).await?;
        stream.write_u8(0).await?; /* copy_is_binary */
        stream.write_i16(0).await?; /* numAttributes */
        stream.flush().await?;
        info!("sent CopyOut");

        /* Send a tarball of the latest snapshot on the timeline */

        // find latest snapshot
        let snapshotlsn = restore_local_repo::find_latest_snapshot(&self.conf, timelineid).unwrap();

        // Stream it
        let (s, mut r) = mpsc::channel(5);

        let f_tar = task::spawn_blocking(move || {
            basebackup::send_snapshot_tarball(&mut CopyDataSink(s), timelineid, snapshotlsn)?;
            Ok(())
        });
        let f_tar2 = async {
            let joinres = f_tar.await;

            if joinres.is_err() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    joinres.unwrap_err(),
                ));
            }
            return joinres.unwrap();
        };

        let f_pump = async move {
            loop {
                let buf = r.recv().await;
                if buf.is_none() {
                    break;
                }
                let mut buf = buf.unwrap();

                // CopyData
                stream.write_u8(b'd').await?;
                stream.write_u32((4 + buf.len()) as u32).await?;
                stream.write_all(&mut buf).await?;
                trace!("CopyData sent for {} bytes!", buf.len());

                // FIXME: flush isn't really required, but makes it easier
                // to view in wireshark
                stream.flush().await?;
            }
            Ok(())
        };

        tokio::try_join!(f_tar2, f_pump)?;

        // CopyDone
        self.stream.write_u8(b'c').await?;
        self.stream.write_u32(4).await?;
        self.stream.flush().await?;
        debug!("CopyDone sent!");

        // FIXME: I'm getting an error from the tokio copyout driver without this.
        // I think it happens when the CommandComplete, CloseComplete and ReadyForQuery
        // are sent in the same TCP packet as the CopyDone. I don't understand why.
        thread::sleep(std::time::Duration::from_secs(1));

        Ok(())
    }
}

struct CopyDataSink(mpsc::Sender<Bytes>);

impl std::io::Write for CopyDataSink {
    fn write(&mut self, data: &[u8]) -> std::result::Result<usize, std::io::Error> {
        let buf = Bytes::copy_from_slice(data);

        if let Err(e) = self.0.blocking_send(buf) {
            return Err(io::Error::new(io::ErrorKind::Other, e));
        }

        Ok(data.len())
    }
    fn flush(&mut self) -> std::result::Result<(), std::io::Error> {
        // no-op
        Ok(())
    }
}
