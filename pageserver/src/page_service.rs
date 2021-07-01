//
//! The Page Service listens for client connections and serves their GetPage@LSN
//! requests.
//
//   It is possible to connect here using usual psql/pgbench/libpq. Following
// commands are supported now:
//     *status* -- show actual info about this pageserver,
//     *pagestream* -- enter mode where smgr and pageserver talk with their
//  custom protocol.
//     *callmemaybe <zenith timelineid> $url* -- ask pageserver to start walreceiver on $url
//

use anyhow::{anyhow, bail};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::*;
use regex::Regex;
use std::io::Write;
use std::net::TcpListener;
use std::str::FromStr;
use std::thread;
use std::{io, net::TcpStream};
use zenith_utils::postgres_backend;
use zenith_utils::postgres_backend::PostgresBackend;
use zenith_utils::pq_proto::{
    BeMessage, FeMessage, RowDescriptor, HELLO_WORLD_ROW, SINGLE_COL_ROWDESC,
};
use zenith_utils::{bin_ser::BeSer, lsn::Lsn};

use crate::basebackup;
use crate::branches;
use crate::page_cache;
use crate::repository::{BufferTag, RelTag, RelationUpdate, Update};
use crate::walreceiver;
use crate::PageServerConf;
use crate::ZTimelineId;

// Wrapped in libpq CopyData
enum PagestreamFeMessage {
    Exists(PagestreamRequest),
    Nblocks(PagestreamRequest),
    Read(PagestreamRequest),
}

// Wrapped in libpq CopyData
enum PagestreamBeMessage {
    Status(PagestreamStatusResponse),
    Nblocks(PagestreamStatusResponse),
    Read(PagestreamReadResponse),
}

#[derive(Debug)]
struct PagestreamRequest {
    spcnode: u32,
    dbnode: u32,
    relnode: u32,
    forknum: u8,
    blkno: u32,
    lsn: Lsn,
}

#[derive(Debug)]
struct PagestreamStatusResponse {
    ok: bool,
    n_blocks: u32,
}

#[derive(Debug)]
struct PagestreamReadResponse {
    ok: bool,
    n_blocks: u32,
    page: Bytes,
}

impl PagestreamFeMessage {
    fn parse(mut body: Bytes) -> anyhow::Result<PagestreamFeMessage> {
        // TODO these gets can fail

        let smgr_tag = body.get_u8();
        let zreq = PagestreamRequest {
            spcnode: body.get_u32(),
            dbnode: body.get_u32(),
            relnode: body.get_u32(),
            forknum: body.get_u8(),
            blkno: body.get_u32(),
            lsn: Lsn::from(body.get_u64()),
        };

        // TODO: consider using protobuf or serde bincode for less error prone
        // serialization.
        match smgr_tag {
            0 => Ok(PagestreamFeMessage::Exists(zreq)),
            1 => Ok(PagestreamFeMessage::Nblocks(zreq)),
            2 => Ok(PagestreamFeMessage::Read(zreq)),
            _ => Err(anyhow!(
                "unknown smgr message tag: {},'{:?}'",
                smgr_tag,
                body
            )),
        }
    }
}

impl PagestreamBeMessage {
    fn serialize(&self) -> Bytes {
        let mut bytes = BytesMut::new();

        match self {
            Self::Status(resp) => {
                bytes.put_u8(100); /* tag from pagestore_client.h */
                bytes.put_u8(resp.ok as u8);
                bytes.put_u32(resp.n_blocks);
            }

            Self::Nblocks(resp) => {
                bytes.put_u8(101); /* tag from pagestore_client.h */
                bytes.put_u8(resp.ok as u8);
                bytes.put_u32(resp.n_blocks);
            }

            Self::Read(resp) => {
                bytes.put_u8(102); /* tag from pagestore_client.h */
                bytes.put_u8(resp.ok as u8);
                bytes.put_u32(resp.n_blocks);
                bytes.put(&resp.page[..]);
            }
        }

        bytes.into()
    }
}

///////////////////////////////////////////////////////////////////////////////

///
/// Main loop of the page service.
///
/// Listens for connections, and launches a new handler thread for each.
///
pub fn thread_main(conf: &'static PageServerConf, listener: TcpListener) -> anyhow::Result<()> {
    loop {
        let (socket, peer_addr) = listener.accept()?;
        debug!("accepted connection from {}", peer_addr);
        socket.set_nodelay(true).unwrap();

        thread::spawn(move || {
            if let Err(err) = page_service_conn_main(conf, socket) {
                error!("error: {}", err);
            }
        });
    }
}

fn page_service_conn_main(conf: &'static PageServerConf, socket: TcpStream) -> anyhow::Result<()> {
    let mut conn_handler = PageServerHandler::new(conf);
    let mut pgbackend = PostgresBackend::new(socket)?;
    pgbackend.run(&mut conn_handler)
}

#[derive(Debug)]
struct PageServerHandler {
    conf: &'static PageServerConf,
}

impl PageServerHandler {
    pub fn new(conf: &'static PageServerConf) -> Self {
        PageServerHandler { conf }
    }

    fn handle_controlfile(&self, pgb: &mut PostgresBackend) -> io::Result<()> {
        pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?
            .write_message_noflush(&BeMessage::ControlFile)?
            .write_message(&BeMessage::CommandComplete(b"SELECT 1"))?;

        Ok(())
    }

    fn handle_pagerequests(
        &self,
        pgb: &mut PostgresBackend,
        timelineid: ZTimelineId,
    ) -> anyhow::Result<()> {
        // Check that the timeline exists
        let repository = page_cache::get_repository();
        let timeline = repository.get_timeline(timelineid).map_err(|_| {
            anyhow!(
                "client requested pagestream on timeline {} which does not exist in page server",
                timelineid
            )
        })?;

        /* switch client to COPYBOTH */
        pgb.write_message(&BeMessage::CopyBothResponse)?;

        while let Some(message) = pgb.read_message()? {
            trace!("query({:?}): {:?}", timelineid, message);

            let copy_data_bytes = match message {
                FeMessage::CopyData(bytes) => bytes,
                _ => continue,
            };

            let zenith_fe_msg = PagestreamFeMessage::parse(copy_data_bytes)?;

            let response = match zenith_fe_msg {
                PagestreamFeMessage::Exists(req) => {
                    let tag = RelTag {
                        spcnode: req.spcnode,
                        dbnode: req.dbnode,
                        relnode: req.relnode,
                        forknum: req.forknum,
                    };

                    let exist = timeline.get_rel_exists(tag, req.lsn).unwrap_or(false);

                    PagestreamBeMessage::Status(PagestreamStatusResponse {
                        ok: exist,
                        n_blocks: 0,
                    })
                }
                PagestreamFeMessage::Nblocks(req) => {
                    let tag = RelTag {
                        spcnode: req.spcnode,
                        dbnode: req.dbnode,
                        relnode: req.relnode,
                        forknum: req.forknum,
                    };

                    let n_blocks = timeline.get_rel_size(tag, req.lsn).unwrap_or(0);

                    PagestreamBeMessage::Nblocks(PagestreamStatusResponse { ok: true, n_blocks })
                }
                PagestreamFeMessage::Read(req) => {
                    let buf_tag = BufferTag {
                        rel: RelTag {
                            spcnode: req.spcnode,
                            dbnode: req.dbnode,
                            relnode: req.relnode,
                            forknum: req.forknum,
                        },
                        blknum: req.blkno,
                    };

                    let read_response = match timeline.get_page_at_lsn(buf_tag, req.lsn) {
                        Ok(p) => PagestreamReadResponse {
                            ok: true,
                            n_blocks: 0,
                            page: p,
                        },
                        Err(e) => {
                            const ZERO_PAGE: [u8; 8192] = [0; 8192];
                            error!("get_page_at_lsn: {}", e);
                            PagestreamReadResponse {
                                ok: false,
                                n_blocks: 0,
                                page: Bytes::from_static(&ZERO_PAGE),
                            }
                        }
                    };

                    PagestreamBeMessage::Read(read_response)
                }
            };

            pgb.write_message(&BeMessage::CopyData(&response.serialize()))?;
        }

        Ok(())
    }

    fn handle_basebackup_request(
        &self,
        pgb: &mut PostgresBackend,
        timelineid: ZTimelineId,
        lsn: Option<Lsn>,
    ) -> anyhow::Result<()> {
        // check that the timeline exists
        let repository = page_cache::get_repository();
        let timeline = repository.get_timeline(timelineid).map_err(|e| {
            error!("error fetching timeline: {:?}", e);
            anyhow!(
                "client requested basebackup on timeline {} which does not exist in page server",
                timelineid
            )
        })?;
        /* switch client to COPYOUT */
        pgb.write_message(&BeMessage::CopyOutResponse)?;
        info!("sent CopyOut");

        /* Send a tarball of the latest snapshot on the timeline */

        // find latest snapshot
        let (snapshot_lsn, _) = branches::find_latest_snapshot(&self.conf, timelineid).unwrap();


        let req_lsn = lsn.unwrap_or(snapshot_lsn);
        basebackup::send_tarball_at_lsn(
            &mut CopyDataSink { pgb },
            timelineid,
            &timeline,
            req_lsn,
            snapshot_lsn,
        )?;
        pgb.write_message(&BeMessage::CopyDone)?;
        debug!("CopyDone sent!");

        Ok(())
    }
}

impl postgres_backend::Handler for PageServerHandler {
    fn process_query(
        &mut self,
        pgb: &mut PostgresBackend,
        query_string: Bytes,
    ) -> anyhow::Result<()> {
        debug!("process query {:?}", query_string);

        // remove null terminator, if any
        let mut query_string = query_string;
        if query_string.last() == Some(&0) {
            query_string.truncate(query_string.len() - 1);
        }

        if query_string.starts_with(b"controlfile") {
            self.handle_controlfile(pgb)?;
        } else if query_string.starts_with(b"pagestream ") {
            let (_l, r) = query_string.split_at("pagestream ".len());
            let timelineid_str = String::from_utf8(r.to_vec())?;
            let timelineid = ZTimelineId::from_str(&timelineid_str)?;

            self.handle_pagerequests(pgb, timelineid)?;
        } else if query_string.starts_with(b"basebackup ") {
            let (_l, r) = query_string.split_at("basebackup ".len());
            let r = r.to_vec();
            let basebackup_args = String::from(String::from_utf8(r)?.trim_end());
            let args: Vec<&str> = basebackup_args.rsplit(' ').collect();
            let timelineid_str = args[0];
            info!("got basebackup command: \"{}\"", timelineid_str);
            let timelineid = ZTimelineId::from_str(&timelineid_str)?;
            let lsn = if args.len() > 1 {
                Some(Lsn::from_str(args[1])?)
            } else {
                None
            };
            // Check that the timeline exists
            self.handle_basebackup_request(pgb, timelineid, lsn)?;
            pgb.write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with(b"callmemaybe ") {
            let query_str = String::from_utf8(query_string.to_vec())?;

            // callmemaybe <zenith timelineid as hex string> <connstr>
            // TODO lazy static
            let re = Regex::new(r"^callmemaybe ([[:xdigit:]]+) (.*)$").unwrap();
            let caps = re
                .captures(&query_str)
                .ok_or_else(|| anyhow!("invalid callmemaybe: '{}'", query_str))?;

            let timelineid = ZTimelineId::from_str(caps.get(1).unwrap().as_str())?;
            let connstr: String = String::from(caps.get(2).unwrap().as_str());

            // Check that the timeline exists
            let repository = page_cache::get_repository();
            if repository.get_timeline(timelineid).is_err() {
                bail!("client requested callmemaybe on timeline {} which does not exist in page server", timelineid);
            }

            walreceiver::launch_wal_receiver(&self.conf, timelineid, &connstr);

            pgb.write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with(b"branch_create ") {
            let query_str = String::from_utf8(query_string.to_vec())?;
            let err = || anyhow!("invalid branch_create: '{}'", query_str);

            // branch_create <branchname> <startpoint>
            // TODO lazy static
            // TOOD: escaping, to allow branch names with spaces
            let re = Regex::new(r"^branch_create (\S+) ([^\r\n\s;]+)[\r\n\s;]*;?$").unwrap();
            let caps = re.captures(&query_str).ok_or_else(err)?;

            let branchname: String = String::from(caps.get(1).ok_or_else(err)?.as_str());
            let startpoint_str: String = String::from(caps.get(2).ok_or_else(err)?.as_str());

            let branch = branches::create_branch(&self.conf, &branchname, &startpoint_str)?;
            let branch = serde_json::to_vec(&branch)?;

            pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?
                .write_message_noflush(&BeMessage::DataRow(&[Some(&branch)]))?
                .write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with(b"push ") {
            let query_str = std::str::from_utf8(&query_string)?;
            let mut it = query_str.split(' ');
            it.next().unwrap();
            let timeline_id: ZTimelineId = it
                .next()
                .ok_or_else(|| anyhow!("missing timeline id"))?
                .parse()?;

            let start_lsn = Lsn(0); // TODO this needs to come from the repo
            let timeline =
                page_cache::get_repository().create_empty_timeline(timeline_id, start_lsn)?;

            pgb.write_message(&BeMessage::CopyInResponse)?;

            let mut last_lsn = Lsn(0);

            while let Some(msg) = pgb.read_message()? {
                match msg {
                    FeMessage::CopyData(bytes) => {
                        let relation_update = RelationUpdate::des(&bytes)?;

                        last_lsn = relation_update.lsn;

                        match relation_update.update {
                            Update::Page { blknum, img } => {
                                let tag = BufferTag {
                                    rel: relation_update.rel,
                                    blknum,
                                };

                                timeline.put_page_image(tag, relation_update.lsn, img)?;
                            }
                            Update::WALRecord { blknum, rec } => {
                                let tag = BufferTag {
                                    rel: relation_update.rel,
                                    blknum,
                                };

                                timeline.put_wal_record(tag, rec)?;
                            }
                            Update::Truncate { n_blocks } => {
                                timeline.put_truncation(
                                    relation_update.rel,
                                    relation_update.lsn,
                                    n_blocks,
                                )?;
                            }
                            Update::Unlink => {
                                todo!()
                            }
                        }
                    }
                    FeMessage::CopyDone => {
                        timeline.advance_last_valid_lsn(last_lsn);
                        break;
                    }
                    FeMessage::Sync => {}
                    _ => bail!("unexpected message {:?}", msg),
                }
            }

            pgb.write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with(b"request_push ") {
            let query_str = std::str::from_utf8(&query_string)?;
            let mut it = query_str.split(' ');
            it.next().unwrap();

            let timeline_id: ZTimelineId = it
                .next()
                .ok_or_else(|| anyhow!("missing timeline id"))?
                .parse()?;
            let timeline = page_cache::get_repository().get_timeline(timeline_id)?;

            let postgres_connection_uri =
                it.next().ok_or_else(|| anyhow!("missing postgres uri"))?;

            let mut conn = postgres::Client::connect(postgres_connection_uri, postgres::NoTls)?;
            let mut copy_in = conn.copy_in(format!("push {}", timeline_id.to_string()).as_str())?;

            let history = timeline.history()?;
            for update_res in history {
                let update = update_res?;
                let update_bytes = update.ser()?;
                copy_in.write_all(&update_bytes)?;
                copy_in.flush()?; // ensure that messages are sent inside individual CopyData packets
            }

            copy_in.finish()?;

            pgb.write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with(b"branch_list") {
            let branches = crate::branches::get_branches(&self.conf)?;
            let branches_buf = serde_json::to_vec(&branches)?;

            pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?
                .write_message_noflush(&BeMessage::DataRow(&[Some(&branches_buf)]))?
                .write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with(b"status") {
            pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?
                .write_message_noflush(&HELLO_WORLD_ROW)?
                .write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.to_ascii_lowercase().starts_with(b"set ") {
            // important because psycopg2 executes "SET datestyle TO 'ISO'"
            // on connect
            pgb.write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string
            .to_ascii_lowercase()
            .starts_with(b"identify_system")
        {
            // TODO: match postgres response formarmat for 'identify_system'
            let system_id = crate::branches::get_system_id(&self.conf)?.to_string();

            pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?;
            pgb.write_message_noflush(&BeMessage::DataRow(&[Some(system_id.as_bytes())]))?;
            pgb.write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with(b"do_gc ") {
            // Run GC immediately on given timeline.
            // FIXME: This is just for tests. See test_runner/batch_others/test_gc.py.
            // This probably should require special authentication or a global flag to
            // enable, I don't think we want to or need to allow regular clients to invoke
            // GC.
            let query_str = std::str::from_utf8(&query_string)?;

            let mut it = query_str.split(' ');
            it.next().unwrap();

            let timeline_id: ZTimelineId = it
                .next()
                .ok_or_else(|| anyhow!("missing timeline id"))?
                .parse()?;
            let timeline = page_cache::get_repository().get_timeline(timeline_id)?;

            let horizon: u64 = it
                .next()
                .unwrap_or(&self.conf.gc_horizon.to_string())
                .parse()?;

            let result = timeline.gc_iteration(horizon)?;

            pgb.write_message_noflush(&BeMessage::RowDescription(&[
                RowDescriptor {
                    name: b"n_relations",
                    typoid: 20,
                    typlen: 8,
                    ..Default::default()
                },
                RowDescriptor {
                    name: b"truncated",
                    typoid: 20,
                    typlen: 8,
                    ..Default::default()
                },
                RowDescriptor {
                    name: b"deleted",
                    typoid: 20,
                    typlen: 8,
                    ..Default::default()
                },
                RowDescriptor {
                    name: b"dropped",
                    typoid: 20,
                    typlen: 8,
                    ..Default::default()
                },
                RowDescriptor {
                    name: b"elapsed",
                    typoid: 20,
                    typlen: 8,
                    ..Default::default()
                },
            ]))?
            .write_message_noflush(&BeMessage::DataRow(&[
                Some(&result.n_relations.to_string().as_bytes()),
                Some(&result.truncated.to_string().as_bytes()),
                Some(&result.deleted.to_string().as_bytes()),
                Some(&result.dropped.to_string().as_bytes()),
                Some(&result.elapsed.as_millis().to_string().as_bytes()),
            ]))?
            .write_message(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else {
            bail!("unknown command");
        }

        pgb.flush()?;

        Ok(())
    }
}

///
/// A std::io::Write implementation that wraps all data written to it in CopyData
/// messages.
///
struct CopyDataSink<'a> {
    pgb: &'a mut PostgresBackend,
}

impl<'a> io::Write for CopyDataSink<'a> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        // CopyData
        // FIXME: if the input is large, we should split it into multiple messages.
        // Not sure what the threshold should be, but the ultimate hard limit is that
        // the length cannot exceed u32.
        // FIXME: flush isn't really required, but makes it easier
        // to view in wireshark
        self.pgb.write_message(&BeMessage::CopyData(data))?;
        trace!("CopyData sent for {} bytes!", data.len());

        Ok(data.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        // no-op
        Ok(())
    }
}
