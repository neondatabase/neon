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

use anyhow::{anyhow, bail, ensure, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use lazy_static::lazy_static;
use log::*;
use regex::Regex;
use std::io::Write;
use std::net::TcpListener;
use std::str;
use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use std::{io, net::TcpStream};
use zenith_metrics::{register_histogram_vec, HistogramVec};
use zenith_utils::auth::JwtAuth;
use zenith_utils::auth::{Claims, Scope};
use zenith_utils::postgres_backend::PostgresBackend;
use zenith_utils::postgres_backend::{self, AuthType};
use zenith_utils::pq_proto::{
    BeMessage, FeMessage, RowDescriptor, HELLO_WORLD_ROW, SINGLE_COL_ROWDESC,
};
use zenith_utils::zid::{ZTenantId, ZTimelineId};
use zenith_utils::{bin_ser::BeSer, lsn::Lsn};

use crate::basebackup;
use crate::branches;
use crate::page_cache;
use crate::relish::*;
use crate::repository::Modification;
use crate::walreceiver;
use crate::PageServerConf;

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
pub fn thread_main(
    conf: &'static PageServerConf,
    auth: Arc<Option<JwtAuth>>,
    listener: TcpListener,
    auth_type: AuthType,
) -> anyhow::Result<()> {
    loop {
        let (socket, peer_addr) = listener.accept()?;
        debug!("accepted connection from {}", peer_addr);
        socket.set_nodelay(true).unwrap();
        let local_auth = Arc::clone(&auth);
        thread::spawn(move || {
            if let Err(err) = page_service_conn_main(conf, local_auth, socket, auth_type) {
                error!("error: {}", err);
            }
        });
    }
}

fn page_service_conn_main(
    conf: &'static PageServerConf,
    auth: Arc<Option<JwtAuth>>,
    socket: TcpStream,
    auth_type: AuthType,
) -> anyhow::Result<()> {
    // Immediatsely increment the gauge, then create a job to decrement it on thread exit.
    // One of the pros of `defer!` is that this will *most probably*
    // get called, even in presence of panics.
    let gauge = crate::LIVE_CONNECTIONS_COUNT.with_label_values(&["page_service"]);
    gauge.inc();
    scopeguard::defer! {
        gauge.dec();
    }

    let mut conn_handler = PageServerHandler::new(conf, auth);
    let pgbackend = PostgresBackend::new(socket, auth_type)?;
    pgbackend.run(&mut conn_handler)
}

#[derive(Debug)]
struct PageServerHandler {
    conf: &'static PageServerConf,
    auth: Arc<Option<JwtAuth>>,
    claims: Option<Claims>,
}

const TIME_BUCKETS: &[f64] = &[
    0.00001, // 1/100000 s
    0.0001, 0.00015, 0.0002, 0.00025, 0.0003, 0.00035, 0.0005, 0.00075, // 1/10000 s
    0.001, 0.0025, 0.005, 0.0075, // 1/1000 s
    0.01, 0.0125, 0.015, 0.025, 0.05, // 1/100 s
    0.1,  // 1/10 s
];

lazy_static! {
    static ref SMGR_QUERY_TIME: HistogramVec = register_histogram_vec!(
        "pageserver_smgr_query_time",
        "Time spent on smgr query handling",
        &["smgr_query_type"],
        TIME_BUCKETS.into()
    )
    .expect("failed to define a metric");
}

impl PageServerHandler {
    pub fn new(conf: &'static PageServerConf, auth: Arc<Option<JwtAuth>>) -> Self {
        PageServerHandler {
            conf,
            auth,
            claims: None,
        }
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
        tenantid: ZTenantId,
    ) -> anyhow::Result<()> {
        // Check that the timeline exists
        let repository = page_cache::get_repository_for_tenant(&tenantid)?;
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
                    let rel = RelTag {
                        spcnode: req.spcnode,
                        dbnode: req.dbnode,
                        relnode: req.relnode,
                        forknum: req.forknum,
                    };
                    let tag = RelishTag::Relation(rel);

                    let exist = SMGR_QUERY_TIME
                        .with_label_values(&["get_rel_exists"])
                        .observe_closure_duration(|| {
                            timeline.get_rel_exists(tag, req.lsn).unwrap_or(false)
                        });

                    PagestreamBeMessage::Status(PagestreamStatusResponse {
                        ok: exist,
                        n_blocks: 0,
                    })
                }
                PagestreamFeMessage::Nblocks(req) => {
                    let rel = RelTag {
                        spcnode: req.spcnode,
                        dbnode: req.dbnode,
                        relnode: req.relnode,
                        forknum: req.forknum,
                    };
                    let tag = RelishTag::Relation(rel);

                    let n_blocks = SMGR_QUERY_TIME
                        .with_label_values(&["get_rel_size"])
                        .observe_closure_duration(|| {
                            // Return 0 if relation is not found.
                            // This is what postgres smgr expects.
                            timeline
                                .get_relish_size(tag, req.lsn)
                                .unwrap_or(Some(0))
                                .unwrap_or(0)
                        });

                    PagestreamBeMessage::Nblocks(PagestreamStatusResponse { ok: true, n_blocks })
                }
                PagestreamFeMessage::Read(req) => {
                    let rel = RelTag {
                        spcnode: req.spcnode,
                        dbnode: req.dbnode,
                        relnode: req.relnode,
                        forknum: req.forknum,
                    };
                    let tag = RelishTag::Relation(rel);

                    let read_response = SMGR_QUERY_TIME
                        .with_label_values(&["get_page_at_lsn"])
                        .observe_closure_duration(|| {
                            match timeline.get_page_at_lsn(tag, req.blkno, req.lsn) {
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
                            }
                        });

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
        tenantid: ZTenantId,
    ) -> anyhow::Result<()> {
        // check that the timeline exists
        let repository = page_cache::get_repository_for_tenant(&tenantid)?;
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

        let req_lsn = lsn.unwrap_or_else(|| timeline.get_last_valid_lsn());

        {
            let mut writer = CopyDataSink { pgb };
            let mut basebackup = basebackup::Basebackup::new(
                &mut writer,
                &timeline,
                req_lsn,
                timeline.get_prev_record_lsn(),
            );
            basebackup.send_tarball()?;
        }
        pgb.write_message(&BeMessage::CopyDone)?;
        debug!("CopyDone sent!");

        Ok(())
    }

    // when accessing management api supply None as an argument
    // when using to authorize tenant pass corresponding tenant id
    fn check_permission(&self, tenantid: Option<ZTenantId>) -> Result<()> {
        if self.auth.is_none() {
            // auth is set to Trust, nothing to check so just return ok
            return Ok(());
        }
        // auth is some, just checked above, when auth is some
        // then claims are always present because of checks during connetion init
        // so this expect won't trigger
        let claims = self
            .claims
            .as_ref()
            .expect("claims presence already checked");
        match (&claims.scope, tenantid) {
            (Scope::Tenant, None) => {
                bail!("Attempt to access management api with tenant scope. Permission denied")
            }
            (Scope::Tenant, Some(tenantid)) => {
                if claims.tenant_id.unwrap() != tenantid {
                    bail!("Tenant id mismatch. Permission denied")
                }
                Ok(())
            }
            (Scope::PageServerApi, None) => Ok(()), // access to management api for PageServerApi scope
            (Scope::PageServerApi, Some(_)) => Ok(()), // access to tenant api using PageServerApi scope
        }
    }
}

impl postgres_backend::Handler for PageServerHandler {
    fn check_auth_jwt(
        &mut self,
        _pgb: &mut PostgresBackend,
        jwt_response: &[u8],
    ) -> anyhow::Result<()> {
        // this unwrap is never triggered, because check_auth_jwt only called when auth_type is ZenithJWT
        // which requires auth to be present
        let data = self
            .auth
            .as_ref()
            .as_ref()
            .unwrap()
            .decode(&str::from_utf8(jwt_response)?)?;

        if matches!(data.claims.scope, Scope::Tenant) {
            ensure!(
                data.claims.tenant_id.is_some(),
                "jwt token scope is Tenant, but tenant id is missing"
            )
        }

        info!(
            "jwt auth succeeded for scope: {:#?} by tenantid: {:?}",
            data.claims.scope, data.claims.tenant_id,
        );

        self.claims = Some(data.claims);
        Ok(())
    }

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
        let query_string = std::str::from_utf8(&query_string)?;

        if query_string.starts_with("controlfile") {
            self.handle_controlfile(pgb)?;
        } else if query_string.starts_with("pagestream ") {
            let (_, params_raw) = query_string.split_at("pagestream ".len());
            let params = params_raw.split(" ").collect::<Vec<_>>();
            ensure!(
                params.len() == 2,
                "invalid param number for pagestream command"
            );
            let tenantid = ZTenantId::from_str(params[0])?;
            let timelineid = ZTimelineId::from_str(params[1])?;

            self.check_permission(Some(tenantid))?;

            self.handle_pagerequests(pgb, timelineid, tenantid)?;
        } else if query_string.starts_with("basebackup ") {
            let (_, params_raw) = query_string.split_at("basebackup ".len());
            let params = params_raw.split(" ").collect::<Vec<_>>();
            ensure!(
                params.len() == 2,
                "invalid param number for basebackup command"
            );

            let tenantid = ZTenantId::from_str(params[0])?;
            let timelineid = ZTimelineId::from_str(params[1])?;

            self.check_permission(Some(tenantid))?;

            // TODO are there any tests with lsn option?
            let lsn = if params.len() == 3 {
                Some(Lsn::from_str(params[2])?)
            } else {
                None
            };
            info!(
                "got basebackup command. tenantid=\"{}\" timelineid=\"{}\" lsn=\"{:#?}\"",
                tenantid, timelineid, lsn
            );

            // Check that the timeline exists
            self.handle_basebackup_request(pgb, timelineid, lsn, tenantid)?;
            pgb.write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with("callmemaybe ") {
            // callmemaybe <zenith tenantid as hex string> <zenith timelineid as hex string> <connstr>
            // TODO lazy static
            let re = Regex::new(r"^callmemaybe ([[:xdigit:]]+) ([[:xdigit:]]+) (.*)$").unwrap();
            let caps = re
                .captures(query_string)
                .ok_or_else(|| anyhow!("invalid callmemaybe: '{}'", query_string))?;

            let tenantid = ZTenantId::from_str(caps.get(1).unwrap().as_str())?;
            let timelineid = ZTimelineId::from_str(caps.get(2).unwrap().as_str())?;
            let connstr = caps.get(3).unwrap().as_str().to_owned();

            self.check_permission(Some(tenantid))?;

            // Check that the timeline exists
            let repository = page_cache::get_repository_for_tenant(&tenantid)?;
            if repository.get_timeline(timelineid).is_err() {
                bail!("client requested callmemaybe on timeline {} which does not exist in page server", timelineid);
            }

            walreceiver::launch_wal_receiver(&self.conf, timelineid, &connstr, tenantid.to_owned());

            pgb.write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with("branch_create ") {
            let err = || anyhow!("invalid branch_create: '{}'", query_string);

            // branch_create <tenantid> <branchname> <startpoint>
            // TODO lazy static
            // TOOD: escaping, to allow branch names with spaces
            let re = Regex::new(r"^branch_create ([[:xdigit:]]+) (\S+) ([^\r\n\s;]+)[\r\n\s;]*;?$")
                .unwrap();
            let caps = re.captures(&query_string).ok_or_else(err)?;

            let tenantid = ZTenantId::from_str(caps.get(1).unwrap().as_str())?;
            let branchname = caps.get(2).ok_or_else(err)?.as_str().to_owned();
            let startpoint_str = caps.get(3).ok_or_else(err)?.as_str().to_owned();

            self.check_permission(Some(tenantid))?;

            let branch =
                branches::create_branch(&self.conf, &branchname, &startpoint_str, &tenantid)?;
            let branch = serde_json::to_vec(&branch)?;

            pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?
                .write_message_noflush(&BeMessage::DataRow(&[Some(&branch)]))?
                .write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with("push ") {
            // push <zenith tenantid as hex string> <zenith timelineid as hex string>
            let re = Regex::new(r"^push ([[:xdigit:]]+) ([[:xdigit:]]+)$").unwrap();

            let caps = re
                .captures(query_string)
                .ok_or_else(|| anyhow!("invalid push: '{}'", query_string))?;

            let tenantid = ZTenantId::from_str(caps.get(1).unwrap().as_str())?;
            let timelineid = ZTimelineId::from_str(caps.get(2).unwrap().as_str())?;

            self.check_permission(Some(tenantid))?;

            let start_lsn = Lsn(0); // TODO this needs to come from the repo
            let timeline = page_cache::get_repository_for_tenant(&tenantid)?
                .create_empty_timeline(timelineid, start_lsn)?;

            pgb.write_message(&BeMessage::CopyInResponse)?;

            let mut last_lsn = Lsn(0);

            while let Some(msg) = pgb.read_message()? {
                match msg {
                    FeMessage::CopyData(bytes) => {
                        let modification = Modification::des(&bytes)?;

                        last_lsn = modification.lsn;
                        timeline.put_raw_data(
                            modification.tag,
                            modification.lsn,
                            &modification.data,
                        )?;
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
        } else if query_string.starts_with("request_push ") {
            // request_push <zenith tenantid as hex string> <zenith timelineid as hex string> <postgres_connection_uri>
            let re = Regex::new(r"^request_push ([[:xdigit:]]+) ([[:xdigit:]]+) (.*)$").unwrap();

            let caps = re
                .captures(query_string)
                .ok_or_else(|| anyhow!("invalid request_push: '{}'", query_string))?;

            let tenantid = ZTenantId::from_str(caps.get(1).unwrap().as_str())?;
            let timelineid = ZTimelineId::from_str(caps.get(2).unwrap().as_str())?;
            let postgres_connection_uri = caps.get(3).unwrap().as_str();

            self.check_permission(Some(tenantid))?;

            let timeline =
                page_cache::get_repository_for_tenant(&tenantid)?.get_timeline(timelineid)?;

            let mut conn = postgres::Client::connect(postgres_connection_uri, postgres::NoTls)?;
            let mut copy_in = conn.copy_in(format!("push {}", timelineid.to_string()).as_str())?;

            let history = timeline.history()?;
            for update_res in history {
                let update = update_res?;
                let update_bytes = update.ser()?;
                copy_in.write_all(&update_bytes)?;
                copy_in.flush()?; // ensure that messages are sent inside individual CopyData packets
            }

            copy_in.finish()?;

            pgb.write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with("branch_list ") {
            // branch_list <zenith tenantid as hex string>
            let re = Regex::new(r"^branch_list ([[:xdigit:]]+)$").unwrap();
            let caps = re
                .captures(query_string)
                .ok_or_else(|| anyhow!("invalid branch_list: '{}'", query_string))?;

            let tenantid = ZTenantId::from_str(caps.get(1).unwrap().as_str())?;

            let branches = crate::branches::get_branches(&self.conf, &tenantid)?;
            let branches_buf = serde_json::to_vec(&branches)?;

            pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?
                .write_message_noflush(&BeMessage::DataRow(&[Some(&branches_buf)]))?
                .write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with("tenant_list") {
            let tenants = crate::branches::get_tenants(&self.conf)?;
            let tenants_buf = serde_json::to_vec(&tenants)?;

            pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?
                .write_message_noflush(&BeMessage::DataRow(&[Some(&tenants_buf)]))?
                .write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with("tenant_create") {
            let err = || anyhow!("invalid tenant_create: '{}'", query_string);

            // tenant_create <tenantid>
            let re = Regex::new(r"^tenant_create ([[:xdigit:]]+)$").unwrap();
            let caps = re.captures(&query_string).ok_or_else(err)?;

            self.check_permission(None)?;

            let tenantid = ZTenantId::from_str(caps.get(1).unwrap().as_str())?;

            page_cache::create_repository_for_tenant(&self.conf, tenantid)?;

            pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?
                .write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with("status") {
            pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?
                .write_message_noflush(&HELLO_WORLD_ROW)?
                .write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.to_ascii_lowercase().starts_with("set ") {
            // important because psycopg2 executes "SET datestyle TO 'ISO'"
            // on connect
            pgb.write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with("do_gc ") {
            // Run GC immediately on given timeline.
            // FIXME: This is just for tests. See test_runner/batch_others/test_gc.py.
            // This probably should require special authentication or a global flag to
            // enable, I don't think we want to or need to allow regular clients to invoke
            // GC.

            // do_gc <tenant_id> <timeline_id> <gc_horizon>
            let re = Regex::new(r"^do_gc ([[:xdigit:]]+)\s([[:xdigit:]]+)($|\s)([[:digit:]]+)?")
                .unwrap();

            let caps = re
                .captures(query_string)
                .ok_or_else(|| anyhow!("invalid do_gc: '{}'", query_string))?;

            let tenantid = ZTenantId::from_str(caps.get(1).unwrap().as_str())?;
            let timelineid = ZTimelineId::from_str(caps.get(2).unwrap().as_str())?;
            let gc_horizon: u64 = caps
                .get(4)
                .map(|h| h.as_str().parse())
                .unwrap_or(Ok(self.conf.gc_horizon))?;

            let timeline =
                page_cache::get_repository_for_tenant(&tenantid)?.get_timeline(timelineid)?;

            let result = timeline.gc_iteration(gc_horizon, true)?;

            pgb.write_message_noflush(&BeMessage::RowDescription(&[
                RowDescriptor::int8_col(b"n_relations"),
                RowDescriptor::int8_col(b"truncated"),
                RowDescriptor::int8_col(b"deleted"),
                RowDescriptor::int8_col(b"prep_deleted"),
                RowDescriptor::int8_col(b"slru_deleted"),
                RowDescriptor::int8_col(b"chkp_deleted"),
                RowDescriptor::int8_col(b"control_deleted"),
                RowDescriptor::int8_col(b"filenodemap_deleted"),
                RowDescriptor::int8_col(b"dropped"),
                RowDescriptor::int8_col(b"elapsed"),
            ]))?
            .write_message_noflush(&BeMessage::DataRow(&[
                Some(&result.n_relations.to_string().as_bytes()),
                Some(&result.truncated.to_string().as_bytes()),
                Some(&result.deleted.to_string().as_bytes()),
                Some(&result.prep_deleted.to_string().as_bytes()),
                Some(&result.slru_deleted.to_string().as_bytes()),
                Some(&result.chkp_deleted.to_string().as_bytes()),
                Some(&result.control_deleted.to_string().as_bytes()),
                Some(&result.filenodemap_deleted.to_string().as_bytes()),
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
