//!
//! WAL receiver connects to the WAL safekeeper service, streams WAL,
//! decodes records and saves them in the repository for the correct
//! timeline.
//!
//! We keep one WAL receiver active per timeline.

use crate::config::PageServerConf;
use crate::repository::Repository;
use crate::repository::Timeline;
use crate::tenant_mgr;
use crate::thread_mgr;
use crate::thread_mgr::ThreadKind;
use crate::walingest::WalIngest;
use anyhow::{bail, Context, Error, Result};
use bytes::BytesMut;
use fail::fail_point;
use lazy_static::lazy_static;
use postgres_ffi::waldecoder::*;
use postgres_protocol::message::backend::ReplicationMessage;
use postgres_types::PgLsn;
use std::cell::Cell;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Mutex;
use std::thread_local;
use std::time::SystemTime;
use tokio::pin;
use tokio_postgres::replication::ReplicationStream;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage, SimpleQueryRow};
use tokio_stream::StreamExt;
use tracing::*;
use zenith_utils::lsn::Lsn;
use zenith_utils::pq_proto::ZenithFeedback;
use zenith_utils::zid::ZTenantId;
use zenith_utils::zid::ZTimelineId;

//
// We keep one WAL Receiver active per timeline.
//
struct WalReceiverEntry {
    wal_producer_connstr: String,
}

lazy_static! {
    static ref WAL_RECEIVERS: Mutex<HashMap<(ZTenantId, ZTimelineId), WalReceiverEntry>> =
        Mutex::new(HashMap::new());
}

thread_local! {
    // Boolean that is true only for WAL receiver threads
    //
    // This is used in `wait_lsn` to guard against usage that might lead to a deadlock.
    pub(crate) static IS_WAL_RECEIVER: Cell<bool> = Cell::new(false);
}

fn drop_wal_receiver(tenantid: ZTenantId, timelineid: ZTimelineId) {
    let mut receivers = WAL_RECEIVERS.lock().unwrap();
    receivers.remove(&(tenantid, timelineid));
}

// Launch a new WAL receiver, or tell one that's running about change in connection string
pub fn launch_wal_receiver(
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
    timelineid: ZTimelineId,
    wal_producer_connstr: &str,
) -> Result<()> {
    let mut receivers = WAL_RECEIVERS.lock().unwrap();

    match receivers.get_mut(&(tenantid, timelineid)) {
        Some(receiver) => {
            info!("wal receiver already running, updating connection string");
            receiver.wal_producer_connstr = wal_producer_connstr.into();
        }
        None => {
            thread_mgr::spawn(
                ThreadKind::WalReceiver,
                Some(tenantid),
                Some(timelineid),
                "WAL receiver thread",
                move || {
                    IS_WAL_RECEIVER.with(|c| c.set(true));
                    thread_main(conf, tenantid, timelineid)
                },
            )?;

            let receiver = WalReceiverEntry {
                wal_producer_connstr: wal_producer_connstr.into(),
            };
            receivers.insert((tenantid, timelineid), receiver);

            // Update tenant state and start tenant threads, if they are not running yet.
            tenant_mgr::activate_tenant(conf, tenantid)?;
        }
    };
    Ok(())
}

// Look up current WAL producer connection string in the hash table
fn get_wal_producer_connstr(tenantid: ZTenantId, timelineid: ZTimelineId) -> String {
    let receivers = WAL_RECEIVERS.lock().unwrap();

    receivers
        .get(&(tenantid, timelineid))
        .unwrap()
        .wal_producer_connstr
        .clone()
}

//
// This is the entry point for the WAL receiver thread.
//
fn thread_main(
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
    timelineid: ZTimelineId,
) -> Result<()> {
    let _enter = info_span!("WAL receiver", timeline = %timelineid, tenant = %tenantid).entered();
    info!("WAL receiver thread started");

    // Look up the current WAL producer address
    let wal_producer_connstr = get_wal_producer_connstr(tenantid, timelineid);

    // Make a connection to the WAL safekeeper, or directly to the primary PostgreSQL server,
    // and start streaming WAL from it.
    let res = walreceiver_main(conf, tenantid, timelineid, &wal_producer_connstr);

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
    drop_wal_receiver(tenantid, timelineid);
    Ok(())
}

fn walreceiver_main(
    _conf: &PageServerConf,
    tenantid: ZTenantId,
    timelineid: ZTimelineId,
    wal_producer_connstr: &str,
) -> Result<(), Error> {
    // Connect to the database in replication mode.
    info!("connecting to {:?}", wal_producer_connstr);
    let connect_cfg = format!(
        "{} application_name=pageserver replication=true",
        wal_producer_connstr
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let (mut replication_client, connection) =
        runtime.block_on(tokio_postgres::connect(&connect_cfg, NoTls))?;
    // This is from tokio-postgres docs, but it is a bit weird in our case because we extensively use block_on
    runtime.spawn(async move {
        if let Err(e) = connection.await {
            error!("connection error: {}", e);
        }
    });

    info!("connected!");

    // Immediately increment the gauge, then create a job to decrement it on thread exit.
    // One of the pros of `defer!` is that this will *most probably*
    // get called, even in presence of panics.
    let gauge = crate::LIVE_CONNECTIONS_COUNT.with_label_values(&["wal_receiver"]);
    gauge.inc();
    scopeguard::defer! {
        gauge.dec();
    }

    let identify = runtime.block_on(identify_system(&mut replication_client))?;
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

    let copy_stream = runtime.block_on(replication_client.copy_both_simple(&query))?;
    let physical_stream = ReplicationStream::new(copy_stream);
    pin!(physical_stream);

    let mut waldecoder = WalStreamDecoder::new(startpoint);

    let mut walingest = WalIngest::new(&*timeline, startpoint)?;

    while let Some(replication_message) = runtime.block_on(async {
        let shutdown_watcher = thread_mgr::shutdown_watcher();
        tokio::select! {
            // check for shutdown first
            biased;
            _ = shutdown_watcher => {
                info!("walreceiver interrupted");
                None
            }
            replication_message = physical_stream.next() => replication_message,
        }
    }) {
        let replication_message = replication_message?;
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
                    // at risk of hitting a deadlock.
                    assert!(lsn.is_aligned());

                    let writer = timeline.writer();
                    walingest.ingest_record(&*timeline, writer.as_ref(), recdata, lsn)?;

                    fail_point!("walreceiver-after-ingest");

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
            let timeline_synced_disk_consistent_lsn =
                tenant_mgr::get_repository_for_tenant(tenantid)?
                    .get_timeline_state(timelineid)
                    .and_then(|state| state.remote_disk_consistent_lsn())
                    .unwrap_or(Lsn(0));

            // The last LSN we processed. It is not guaranteed to survive pageserver crash.
            let write_lsn = u64::from(last_lsn);
            // `disk_consistent_lsn` is the LSN at which page server guarantees local persistence of all received data
            let flush_lsn = u64::from(timeline.get_disk_consistent_lsn());
            // The last LSN that is synced to remote storage and is guaranteed to survive pageserver crash
            // Used by safekeepers to remove WAL preceding `remote_consistent_lsn`.
            let apply_lsn = u64::from(timeline_synced_disk_consistent_lsn);
            let ts = SystemTime::now();

            // Send zenith feedback message.
            // Regular standby_status_update fields are put into this message.
            let zenith_status_update = ZenithFeedback {
                current_timeline_size: timeline.get_current_logical_size() as u64,
                ps_writelsn: write_lsn,
                ps_flushlsn: flush_lsn,
                ps_applylsn: apply_lsn,
                ps_replytime: ts,
            };

            debug!("zenith_status_update {:?}", zenith_status_update);

            let mut data = BytesMut::new();
            zenith_status_update.serialize(&mut data)?;
            runtime.block_on(
                physical_stream
                    .as_mut()
                    .zenith_status_update(data.len() as u64, &data),
            )?;
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
pub async fn identify_system(client: &mut Client) -> Result<IdentifySystem, Error> {
    let query_str = "IDENTIFY_SYSTEM";
    let response = client.simple_query(query_str).await?;

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
