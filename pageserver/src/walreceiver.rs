//!
//! WAL receiver connects to the WAL safekeeper service, streams WAL,
//! decodes records and saves them in the repository for the correct
//! timeline.
//!
//! We keep one WAL receiver active per timeline.

use crate::tenant_mgr;
use crate::tenant_mgr::TenantState;
use crate::tenant_threads;
use crate::walingest::WalIngest;
use crate::PageServerConf;
use anyhow::{bail, Context, Error, Result};
use lazy_static::lazy_static;
use postgres::fallible_iterator::FallibleIterator;
use postgres::replication::ReplicationIter;
use postgres::{Client, NoTls, SimpleQueryMessage, SimpleQueryRow};
use postgres_ffi::waldecoder::*;
use postgres_protocol::message::backend::ReplicationMessage;
use postgres_types::PgLsn;
use std::cell::Cell;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Mutex;
use std::thread;
use std::thread::JoinHandle;
use std::thread_local;
use std::time::SystemTime;
use tracing::*;
use zenith_utils::lsn::Lsn;
use zenith_utils::zid::ZTenantId;
use zenith_utils::zid::ZTimelineId;

//
// We keep one WAL Receiver active per timeline.
//
struct WalReceiverEntry {
    wal_producer_connstr: String,
    wal_receiver_handle: Option<JoinHandle<()>>,
    tenantid: ZTenantId,
}

lazy_static! {
    static ref WAL_RECEIVERS: Mutex<HashMap<ZTimelineId, WalReceiverEntry>> =
        Mutex::new(HashMap::new());
}

thread_local! {
    // Boolean that is true only for WAL receiver threads
    //
    // This is used in `wait_lsn` to guard against usage that might lead to a deadlock.
    pub(crate) static IS_WAL_RECEIVER: Cell<bool> = Cell::new(false);
}

// Wait for walreceiver to stop
// Now it stops when pageserver shutdown is requested.
// In future we can make this more granular and send shutdown signals
// per tenant/timeline to cancel inactive walreceivers.
// TODO deal with blocking pg connections
pub fn stop_wal_receiver(timelineid: ZTimelineId) {
    let mut receivers = WAL_RECEIVERS.lock().unwrap();
    if let Some(r) = receivers.get_mut(&timelineid) {
        r.wal_receiver_handle.take();
        // r.wal_receiver_handle.take().map(JoinHandle::join);
    }
}

pub fn drop_wal_receiver(timelineid: ZTimelineId, tenantid: ZTenantId) {
    let mut receivers = WAL_RECEIVERS.lock().unwrap();
    receivers.remove(&timelineid);

    // Check if it was the last walreceiver of the tenant.
    // TODO now we store one WalReceiverEntry per timeline,
    // so this iterator looks a bit strange.
    for (_timelineid, entry) in receivers.iter() {
        if entry.tenantid == tenantid {
            return;
        }
    }

    // When last walreceiver of the tenant is gone, change state to Idle
    tenant_mgr::set_tenant_state(tenantid, TenantState::Idle).unwrap();
}

// Launch a new WAL receiver, or tell one that's running about change in connection string
pub fn launch_wal_receiver(
    conf: &'static PageServerConf,
    timelineid: ZTimelineId,
    wal_producer_connstr: &str,
    tenantid: ZTenantId,
) {
    let mut receivers = WAL_RECEIVERS.lock().unwrap();

    match receivers.get_mut(&timelineid) {
        Some(receiver) => {
            receiver.wal_producer_connstr = wal_producer_connstr.into();
        }
        None => {
            let wal_receiver_handle = thread::Builder::new()
                .name("WAL receiver thread".into())
                .spawn(move || {
                    IS_WAL_RECEIVER.with(|c| c.set(true));
                    thread_main(conf, timelineid, tenantid);
                })
                .unwrap();

            let receiver = WalReceiverEntry {
                wal_producer_connstr: wal_producer_connstr.into(),
                wal_receiver_handle: Some(wal_receiver_handle),
                tenantid,
            };
            receivers.insert(timelineid, receiver);

            // Update tenant state and start tenant threads, if they are not running yet.
            tenant_mgr::set_tenant_state(tenantid, TenantState::Active).unwrap();
            tenant_threads::start_tenant_threads(conf, tenantid);
        }
    };
}

// Look up current WAL producer connection string in the hash table
fn get_wal_producer_connstr(timelineid: ZTimelineId) -> String {
    let receivers = WAL_RECEIVERS.lock().unwrap();

    receivers
        .get(&timelineid)
        .unwrap()
        .wal_producer_connstr
        .clone()
}

//
// This is the entry point for the WAL receiver thread.
//
fn thread_main(conf: &'static PageServerConf, timelineid: ZTimelineId, tenantid: ZTenantId) {
    let _enter = info_span!("WAL receiver", timeline = %timelineid, tenant = %tenantid).entered();
    info!("WAL receiver thread started");

    // Look up the current WAL producer address
    let wal_producer_connstr = get_wal_producer_connstr(timelineid);

    // Make a connection to the WAL safekeeper, or directly to the primary PostgreSQL server,
    // and start streaming WAL from it.
    let res = walreceiver_main(conf, timelineid, &wal_producer_connstr, tenantid);

    // TODO cleanup info messages
    if let Err(e) = res {
        info!("WAL streaming connection failed ({})", e);
    } else {
        info!(
            "walreceiver disconnected tenant {}, timelineid {}",
            tenantid, timelineid
        );
    }

    // Drop it from list of active WAL_RECEIVERS
    // so that next callmemaybe request launched a new thread
    drop_wal_receiver(timelineid, tenantid);
}

fn walreceiver_main(
    _conf: &PageServerConf,
    timelineid: ZTimelineId,
    wal_producer_connstr: &str,
    tenantid: ZTenantId,
) -> Result<(), Error> {
    // Connect to the database in replication mode.
    info!("connecting to {:?}", wal_producer_connstr);
    let connect_cfg = format!(
        "{} application_name=pageserver replication=true",
        wal_producer_connstr
    );

    let mut rclient = Client::connect(&connect_cfg, NoTls)?;
    info!("connected!");

    // Immediately increment the gauge, then create a job to decrement it on thread exit.
    // One of the pros of `defer!` is that this will *most probably*
    // get called, even in presence of panics.
    let gauge = crate::LIVE_CONNECTIONS_COUNT.with_label_values(&["wal_receiver"]);
    gauge.inc();
    scopeguard::defer! {
        gauge.dec();
    }

    let identify = identify_system(&mut rclient)?;
    info!("{:?}", identify);
    let end_of_wal = Lsn::from(u64::from(identify.xlogpos));
    let mut caught_up = false;

    let timeline =
        tenant_mgr::get_timeline_for_tenant(tenantid, timelineid).with_context(|| {
            format!(
                "Can not start the walrecever for a remote tenant {}, timeline {}",
                tenantid, timelineid,
            )
        })?;
    let _timeline_synced_disk_consistent_lsn = tenant_mgr::get_repository_for_tenant(tenantid)?
        .get_timeline_state(timelineid)
        .and_then(|state| state.remote_disk_consistent_lsn());

    //
    // Start streaming the WAL, from where we left off previously.
    //
    // If we had previously received WAL up to some point in the middle of a WAL record, we
    // better start from the end of last full WAL record, not in the middle of one.
    let mut last_rec_lsn = timeline.get_last_record_lsn();
    let mut startpoint = last_rec_lsn;

    if startpoint == Lsn(0) {
        bail!("No previous WAL position");
    }

    // There might be some padding after the last full record, skip it.
    startpoint += startpoint.calc_padding(8u32);

    info!(
        "last_record_lsn {} starting replication from {}, server is at {}...",
        last_rec_lsn, startpoint, end_of_wal
    );

    let query = format!("START_REPLICATION PHYSICAL {}", startpoint);

    let copy_stream = rclient.copy_both_simple(&query)?;
    let mut physical_stream = ReplicationIter::new(copy_stream);

    let mut waldecoder = WalStreamDecoder::new(startpoint);

    let mut walingest = WalIngest::new(&*timeline, startpoint)?;

    while let Some(replication_message) = physical_stream.next()? {
        let status_update = match replication_message {
            ReplicationMessage::XLogData(xlog_data) => {
                // Pass the WAL data to the decoder, and see if we can decode
                // more records as a result.
                let data = xlog_data.data();
                let startlsn = Lsn::from(xlog_data.wal_start());
                let endlsn = startlsn + data.len() as u64;

                trace!("received XLogData between {} and {}", startlsn, endlsn);

                waldecoder.feed_bytes(data);

                while let Some((lsn, recdata)) = waldecoder.poll_decode()? {
                    let _enter = info_span!("processing record", lsn = %lsn).entered();

                    // It is important to deal with the aligned records as lsn in getPage@LSN is
                    // aligned and can be several bytes bigger. Without this alignment we are
                    // at risk of hittind a deadlock.
                    assert!(lsn.is_aligned());

                    let writer = timeline.writer();
                    walingest.ingest_record(writer.as_ref(), recdata, lsn)?;

                    last_rec_lsn = lsn;
                }

                if !caught_up && endlsn >= end_of_wal {
                    info!("caught up at LSN {}", endlsn);
                    caught_up = true;
                }

                Some(endlsn)
            }

            ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                let wal_end = keepalive.wal_end();
                let timestamp = keepalive.timestamp();
                let reply_requested = keepalive.reply() != 0;

                trace!(
                    "received PrimaryKeepAlive(wal_end: {}, timestamp: {:?} reply: {})",
                    wal_end,
                    timestamp,
                    reply_requested,
                );

                if reply_requested {
                    Some(last_rec_lsn)
                } else {
                    None
                }
            }

            _ => None,
        };

        if let Some(last_lsn) = status_update {
            let last_lsn = PgLsn::from(u64::from(last_lsn));

            // The last LSN we processed. It is not guaranteed to survive pageserver crash.
            let write_lsn = last_lsn;
            // This value doesn't guarantee data durability, but it's ok.
            // In setup with WAL service, pageserver durability is guaranteed by safekeepers.
            // In setup without WAL service, we just don't care.
            let flush_lsn = write_lsn;
            // `disk_consistent_lsn` is the LSN at which page server guarantees persistence of all received data
            // Depending on the setup we recieve WAL directly from Compute Node or
            // from a WAL service.
            //
            // Senders use the feedback to determine if we are caught up:
            // - Safekeepers are free to remove WAL preceding `apply_lsn`,
            // as it will never be requested by this page server.
            // - Compute Node uses 'apply_lsn' to calculate a lag for back pressure mechanism
            // (delay WAL inserts to avoid lagging pageserver responses and WAL overflow).
            let apply_lsn = PgLsn::from(u64::from(timeline.get_disk_consistent_lsn()));
            let ts = SystemTime::now();
            const NO_REPLY: u8 = 0;
            physical_stream.standby_status_update(write_lsn, flush_lsn, apply_lsn, ts, NO_REPLY)?;
        }

        if tenant_mgr::shutdown_requested() {
            debug!("stop walreceiver because pageserver shutdown is requested");
            break;
        }
    }

    Ok(())
}

/// Data returned from the postgres `IDENTIFY_SYSTEM` command
///
/// See the [postgres docs] for more details.
///
/// [postgres docs]: https://www.postgresql.org/docs/current/protocol-replication.html
#[derive(Debug)]
// As of nightly 2021-09-11, fields that are only read by the type's `Debug` impl still count as
// unused. Relevant issue: https://github.com/rust-lang/rust/issues/88900
#[allow(dead_code)]
pub struct IdentifySystem {
    systemid: u64,
    timeline: u32,
    xlogpos: PgLsn,
    dbname: Option<String>,
}

/// There was a problem parsing the response to
/// a postgres IDENTIFY_SYSTEM command.
#[derive(Debug, thiserror::Error)]
#[error("IDENTIFY_SYSTEM parse error")]
pub struct IdentifyError;

/// Run the postgres `IDENTIFY_SYSTEM` command
pub fn identify_system(client: &mut Client) -> Result<IdentifySystem, Error> {
    let query_str = "IDENTIFY_SYSTEM";
    let response = client.simple_query(query_str)?;

    // get(N) from row, then parse it as some destination type.
    fn get_parse<T>(row: &SimpleQueryRow, idx: usize) -> Result<T, IdentifyError>
    where
        T: FromStr,
    {
        let val = row.get(idx).ok_or(IdentifyError)?;
        val.parse::<T>().or(Err(IdentifyError))
    }

    // extract the row contents into an IdentifySystem struct.
    // written as a closure so I can use ? for Option here.
    if let Some(SimpleQueryMessage::Row(first_row)) = response.get(0) {
        Ok(IdentifySystem {
            systemid: get_parse(first_row, 0)?,
            timeline: get_parse(first_row, 1)?,
            xlogpos: get_parse(first_row, 2)?,
            dbname: get_parse(first_row, 3).ok(),
        })
    } else {
        Err(IdentifyError.into())
    }
}
