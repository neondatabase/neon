//! Actual Postgres connection handler to stream WAL to the server.

use std::error::Error;
use std::pin::pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, anyhow};
use bytes::BytesMut;
use chrono::{NaiveDateTime, Utc};
use fail::fail_point;
use futures::StreamExt;
use postgres_backend::is_expected_io_error;
use postgres_connection::PgConnectionConfig;
use postgres_ffi::WAL_SEGMENT_SIZE;
use postgres_ffi::v14::xlog_utils::normalize_lsn;
use postgres_ffi::waldecoder::WalDecodeError;
use postgres_protocol::message::backend::ReplicationMessage;
use postgres_types::PgLsn;
use tokio::sync::watch;
use tokio::{select, time};
use tokio_postgres::error::SqlState;
use tokio_postgres::replication::ReplicationStream;
use tokio_postgres::{Client, SimpleQueryMessage, SimpleQueryRow};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info, trace, warn};
use utils::critical;
use utils::id::NodeId;
use utils::lsn::Lsn;
use utils::pageserver_feedback::PageserverFeedback;
use utils::postgres_client::PostgresClientProtocol;
use utils::sync::gate::GateError;
use wal_decoder::models::{FlushUncommittedRecords, InterpretedWalRecords};
use wal_decoder::wire_format::FromWireFormat;

use super::TaskStateUpdate;
use crate::context::RequestContext;
use crate::metrics::{LIVE_CONNECTIONS, WAL_INGEST, WALRECEIVER_STARTED_CONNECTIONS};
use crate::pgdatadir_mapping::DatadirModification;
use crate::task_mgr::{TaskKind, WALRECEIVER_RUNTIME};
use crate::tenant::{
    Timeline, WalReceiverInfo, debug_assert_current_span_has_tenant_and_timeline_id,
};
use crate::walingest::WalIngest;

/// Status of the connection.
#[derive(Debug, Clone, Copy)]
pub(super) struct WalConnectionStatus {
    /// If we were able to initiate a postgres connection, this means that safekeeper process is at least running.
    pub is_connected: bool,
    /// Defines a healthy connection as one on which pageserver received WAL from safekeeper
    /// and is able to process it in walingest without errors.
    pub has_processed_wal: bool,
    /// Connection establishment time or the timestamp of a latest connection message received.
    pub latest_connection_update: NaiveDateTime,
    /// Time of the latest WAL message received.
    pub latest_wal_update: NaiveDateTime,
    /// Latest WAL update contained WAL up to this LSN. Next WAL message with start from that LSN.
    pub streaming_lsn: Option<Lsn>,
    /// Latest commit_lsn received from the safekeeper. Can be zero if no message has been received yet.
    pub commit_lsn: Option<Lsn>,
    /// The node it is connected to
    pub node: NodeId,
}

pub(super) enum WalReceiverError {
    /// An error of a type that does not indicate an issue, e.g. a connection closing
    ExpectedSafekeeperError(tokio_postgres::Error),
    /// An "error" message that carries a SUCCESSFUL_COMPLETION status code.  Carries
    /// the message part of the original postgres error
    SuccessfulCompletion(String),
    /// Generic error
    Other(anyhow::Error),
    ClosedGate,
    Cancelled,
}

impl From<tokio_postgres::Error> for WalReceiverError {
    fn from(err: tokio_postgres::Error) -> Self {
        if let Some(dberror) = err.as_db_error().filter(|db_error| {
            db_error.code() == &SqlState::SUCCESSFUL_COMPLETION
                && db_error.message().contains("ending streaming")
        }) {
            // Strip the outer DbError, which carries a misleading "error" severity
            Self::SuccessfulCompletion(dberror.message().to_string())
        } else if err.is_closed()
            || err
                .source()
                .and_then(|source| source.downcast_ref::<std::io::Error>())
                .map(is_expected_io_error)
                .unwrap_or(false)
        {
            Self::ExpectedSafekeeperError(err)
        } else {
            Self::Other(anyhow::Error::new(err))
        }
    }
}

impl From<anyhow::Error> for WalReceiverError {
    fn from(err: anyhow::Error) -> Self {
        Self::Other(err)
    }
}

impl From<WalDecodeError> for WalReceiverError {
    fn from(err: WalDecodeError) -> Self {
        Self::Other(anyhow::Error::new(err))
    }
}

/// Open a connection to the given safekeeper and receive WAL, sending back progress
/// messages as we go.
#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_walreceiver_connection(
    timeline: Arc<Timeline>,
    protocol: PostgresClientProtocol,
    wal_source_connconf: PgConnectionConfig,
    events_sender: watch::Sender<TaskStateUpdate<WalConnectionStatus>>,
    cancellation: CancellationToken,
    connect_timeout: Duration,
    ctx: RequestContext,
    safekeeper_node: NodeId,
    ingest_batch_size: u64,
    validate_wal_contiguity: bool,
) -> Result<(), WalReceiverError> {
    debug_assert_current_span_has_tenant_and_timeline_id();

    // prevent timeline shutdown from finishing until we have exited
    let _guard = timeline.gate.enter().map_err(|e| match e {
        GateError::GateClosed => WalReceiverError::ClosedGate,
    })?;
    // This function spawns a side-car task (WalReceiverConnectionPoller).
    // Get its gate guard now as well.
    let poller_guard = timeline.gate.enter().map_err(|e| match e {
        GateError::GateClosed => WalReceiverError::ClosedGate,
    })?;

    WALRECEIVER_STARTED_CONNECTIONS.inc();

    // Connect to the database in replication mode.
    info!("connecting to {wal_source_connconf:?}");

    let (replication_client, connection) = {
        let mut config = wal_source_connconf.to_tokio_postgres_config();
        config.application_name(format!("pageserver-{}", timeline.conf.id.0).as_str());
        config.replication_mode(tokio_postgres::config::ReplicationMode::Physical);
        match time::timeout(connect_timeout, config.connect(tokio_postgres::NoTls)).await {
            Ok(client_and_conn) => client_and_conn?,
            Err(_elapsed) => {
                // Timing out to connect to a safekeeper node could happen long time, due to
                // many reasons that pageserver cannot control.
                // Do not produce an error, but make it visible, that timeouts happen by logging the `event.
                info!(
                    "Timed out while waiting {connect_timeout:?} for walreceiver connection to open"
                );
                return Ok(());
            }
        }
    };

    debug!("connected!");
    let mut connection_status = WalConnectionStatus {
        is_connected: true,
        has_processed_wal: false,
        latest_connection_update: Utc::now().naive_utc(),
        latest_wal_update: Utc::now().naive_utc(),
        streaming_lsn: None,
        commit_lsn: None,
        node: safekeeper_node,
    };
    if let Err(e) = events_sender.send(TaskStateUpdate::Progress(connection_status)) {
        warn!(
            "Wal connection event listener dropped right after connection init, aborting the connection: {e}"
        );
        return Ok(());
    }

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own. It shouldn't outlive this function, but,
    // due to lack of async drop, we can't enforce that. However, we ensure that
    // 1. it is sensitive to `cancellation` and
    // 2. holds the Timeline gate open so that after timeline shutdown,
    //    we know this task is gone.
    let _connection_ctx = ctx.detached_child(
        TaskKind::WalReceiverConnectionPoller,
        ctx.download_behavior(),
    );
    let connection_cancellation = cancellation.clone();
    WALRECEIVER_RUNTIME.spawn(
        async move {
            debug_assert_current_span_has_tenant_and_timeline_id();
            select! {
                connection_result = connection => match connection_result {
                    Ok(()) => debug!("Walreceiver db connection closed"),
                    Err(connection_error) => {
                        match WalReceiverError::from(connection_error) {
                            WalReceiverError::ExpectedSafekeeperError(_) => {
                                // silence, because most likely we've already exited the outer call
                                // with a similar error.
                            },
                            WalReceiverError::SuccessfulCompletion(_) => {}
                            WalReceiverError::Cancelled => {
                                debug!("Connection cancelled")
                            }
                            WalReceiverError::ClosedGate => {
                                // doesn't happen at runtime
                            }
                            WalReceiverError::Other(err) => {
                                warn!("Connection aborted: {err:#}")
                            }
                        }
                    }
                },
                _ = connection_cancellation.cancelled() => debug!("Connection cancelled"),
            }
            drop(poller_guard);
        }
        // Enrich the log lines emitted by this closure with meaningful context.
        // TODO: technically, this task outlives the surrounding function, so, the
        // spans won't be properly nested.
        .instrument(tracing::info_span!("poller")),
    );

    let _guard = LIVE_CONNECTIONS
        .with_label_values(&["wal_receiver"])
        .guard();

    let identify = identify_system(&replication_client).await?;
    info!("{identify:?}");

    let end_of_wal = Lsn::from(u64::from(identify.xlogpos));
    let mut caught_up = false;

    connection_status.latest_connection_update = Utc::now().naive_utc();
    connection_status.latest_wal_update = Utc::now().naive_utc();
    connection_status.commit_lsn = Some(end_of_wal);
    if let Err(e) = events_sender.send(TaskStateUpdate::Progress(connection_status)) {
        warn!(
            "Wal connection event listener dropped after IDENTIFY_SYSTEM, aborting the connection: {e}"
        );
        return Ok(());
    }

    //
    // Start streaming the WAL, from where we left off previously.
    //
    // If we had previously received WAL up to some point in the middle of a WAL record, we
    // better start from the end of last full WAL record, not in the middle of one.
    let mut last_rec_lsn = timeline.get_last_record_lsn();
    let mut startpoint = last_rec_lsn;

    if startpoint == Lsn(0) {
        return Err(WalReceiverError::Other(anyhow!("No previous WAL position")));
    }

    // There might be some padding after the last full record, skip it.
    startpoint += startpoint.calc_padding(8u32);

    // If the starting point is at a WAL page boundary, skip past the page header. We don't need the page headers
    // for anything, and in some corner cases, the compute node might have never generated the WAL for page headers
    //. That happens if you create a branch at page boundary: the start point of the branch is at the page boundary,
    // but when the compute node first starts on the branch, we normalize the first REDO position to just after the page
    // header (see generate_pg_control()), so the WAL for the page header is never streamed from the compute node
    //  to the safekeepers.
    startpoint = normalize_lsn(startpoint, WAL_SEGMENT_SIZE);

    info!(
        "last_record_lsn {last_rec_lsn} starting replication from {startpoint}, safekeeper is at {end_of_wal}..."
    );

    let query = format!("START_REPLICATION PHYSICAL {startpoint}");

    let copy_stream = replication_client.copy_both_simple(&query).await?;
    let mut physical_stream = pin!(ReplicationStream::new(copy_stream));

    let walingest_future = WalIngest::new(timeline.as_ref(), startpoint, &ctx);
    let walingest_res = select! {
        walingest_res = walingest_future => walingest_res,
        _ = cancellation.cancelled() => {
            // We are doing reads in WalIngest::new, and those can hang as they come from the network.
            // Timeline cancellation hits the walreceiver cancellation token before it hits the timeline global one.
            debug!("Connection cancelled");
            return Err(WalReceiverError::Cancelled);
        },
    };
    let mut walingest = walingest_res.map_err(|e| match e.kind {
        crate::walingest::WalIngestErrorKind::Cancelled => WalReceiverError::Cancelled,
        _ => WalReceiverError::Other(e.into()),
    })?;

    let (format, compression) = match protocol {
        PostgresClientProtocol::Interpreted {
            format,
            compression,
        } => (format, compression),
        PostgresClientProtocol::Vanilla => {
            return Err(WalReceiverError::Other(anyhow!(
                "Vanilla WAL receiver protocol is no longer supported for ingest"
            )));
        }
    };

    let mut expected_wal_start = startpoint;
    while let Some(replication_message) = {
        select! {
            biased;
            _ = cancellation.cancelled() => {
                debug!("walreceiver interrupted");
                None
            }
            replication_message = physical_stream.next() => replication_message,
        }
    } {
        let replication_message = replication_message?;

        let now = Utc::now().naive_utc();
        let last_rec_lsn_before_msg = last_rec_lsn;

        // Update the connection status before processing the message. If the message processing
        // fails (e.g. in walingest), we still want to know latests LSNs from the safekeeper.
        match &replication_message {
            ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                connection_status.latest_connection_update = now;
                connection_status.commit_lsn = Some(Lsn::from(keepalive.wal_end()));
            }
            ReplicationMessage::RawInterpretedWalRecords(raw) => {
                connection_status.latest_connection_update = now;
                if !raw.data().is_empty() {
                    connection_status.latest_wal_update = now;
                }

                connection_status.commit_lsn = Some(Lsn::from(raw.commit_lsn()));
                connection_status.streaming_lsn = Some(Lsn::from(raw.streaming_lsn()));
            }
            &_ => {}
        };
        if let Err(e) = events_sender.send(TaskStateUpdate::Progress(connection_status)) {
            warn!("Wal connection event listener dropped, aborting the connection: {e}");
            return Ok(());
        }

        let status_update = match replication_message {
            ReplicationMessage::RawInterpretedWalRecords(raw) => {
                WAL_INGEST.bytes_received.inc_by(raw.data().len() as u64);

                let mut uncommitted_records = 0;

                // This is the end LSN of the raw WAL from which the records
                // were interpreted.
                let streaming_lsn = Lsn::from(raw.streaming_lsn());

                let batch = InterpretedWalRecords::from_wire(raw.data(), format, compression)
                    .await
                    .with_context(|| {
                        anyhow::anyhow!(
                        "Failed to deserialize interpreted records ending at LSN {streaming_lsn}"
                    )
                    })?;

                // Guard against WAL gaps. If the start LSN of the PG WAL section
                // from which the interpreted records were extracted, doesn't match
                // the end of the previous batch (or the starting point for the first batch),
                // then kill this WAL receiver connection and start a new one.
                if validate_wal_contiguity {
                    if let Some(raw_wal_start_lsn) = batch.raw_wal_start_lsn {
                        match raw_wal_start_lsn.cmp(&expected_wal_start) {
                            std::cmp::Ordering::Greater => {
                                let msg = format!(
                                    "Gap in streamed WAL: [{expected_wal_start}, {raw_wal_start_lsn})"
                                );
                                critical!("{msg}");
                                return Err(WalReceiverError::Other(anyhow!(msg)));
                            }
                            std::cmp::Ordering::Less => {
                                // Other shards are reading WAL behind us.
                                // This is valid, but check that we received records
                                // that we haven't seen before.
                                if let Some(first_rec) = batch.records.first() {
                                    if first_rec.next_record_lsn < last_rec_lsn {
                                        let msg = format!(
                                            "Received record with next_record_lsn multiple times ({} < {})",
                                            first_rec.next_record_lsn, expected_wal_start
                                        );
                                        critical!("{msg}");
                                        return Err(WalReceiverError::Other(anyhow!(msg)));
                                    }
                                }
                            }
                            std::cmp::Ordering::Equal => {}
                        }
                    }
                }

                let InterpretedWalRecords {
                    records,
                    next_record_lsn,
                    raw_wal_start_lsn: _,
                } = batch;

                tracing::debug!(
                    "Received WAL up to {} with next_record_lsn={}",
                    streaming_lsn,
                    next_record_lsn
                );

                // We start the modification at 0 because each interpreted record
                // advances it to its end LSN. 0 is just an initialization placeholder.
                let mut modification = timeline.begin_modification(Lsn(0));

                async fn commit(
                    modification: &mut DatadirModification<'_>,
                    ctx: &RequestContext,
                    uncommitted: &mut u64,
                ) -> anyhow::Result<()> {
                    let stats = modification.stats();
                    modification.commit(ctx).await?;
                    WAL_INGEST.records_committed.inc_by(*uncommitted);
                    WAL_INGEST.inc_values_committed(&stats);
                    *uncommitted = 0;
                    Ok(())
                }

                if !records.is_empty() {
                    timeline
                        .metrics
                        .wal_records_received
                        .inc_by(records.len() as u64);
                }

                for interpreted in records {
                    if matches!(interpreted.flush_uncommitted, FlushUncommittedRecords::Yes)
                        && uncommitted_records > 0
                    {
                        commit(&mut modification, &ctx, &mut uncommitted_records).await?;
                    }

                    let local_next_record_lsn = interpreted.next_record_lsn;

                    if interpreted.is_observed() {
                        WAL_INGEST.records_observed.inc();
                    }

                    walingest
                        .ingest_record(interpreted, &mut modification, &ctx)
                        .await
                        .with_context(|| {
                            format!("could not ingest record at {local_next_record_lsn}")
                        })
                        .inspect_err(|err| {
                            // TODO: we can't differentiate cancellation errors with
                            // anyhow::Error, so just ignore it if we're cancelled.
                            if !cancellation.is_cancelled() && !timeline.is_stopping() {
                                critical!("{err:?}")
                            }
                        })?;

                    uncommitted_records += 1;

                    // FIXME: this cannot be made pausable_failpoint without fixing the
                    // failpoint library; in tests, the added amount of debugging will cause us
                    // to timeout the tests.
                    fail_point!("walreceiver-after-ingest");

                    // Commit every ingest_batch_size records. Even if we filtered out
                    // all records, we still need to call commit to advance the LSN.
                    if uncommitted_records >= ingest_batch_size
                        || modification.approx_pending_bytes()
                            > DatadirModification::MAX_PENDING_BYTES
                    {
                        commit(&mut modification, &ctx, &mut uncommitted_records).await?;
                    }
                }

                // Records might have been filtered out on the safekeeper side, but we still
                // need to advance last record LSN on all shards. If we've not ingested the latest
                // record, then set the LSN of the modification past it. This way all shards
                // advance their last record LSN at the same time.
                let needs_last_record_lsn_advance = if next_record_lsn > modification.get_lsn() {
                    modification.set_lsn(next_record_lsn).unwrap();
                    true
                } else {
                    false
                };

                if uncommitted_records > 0 || needs_last_record_lsn_advance {
                    // Commit any uncommitted records
                    commit(&mut modification, &ctx, &mut uncommitted_records).await?;
                }

                if !caught_up && streaming_lsn >= end_of_wal {
                    info!("caught up at LSN {streaming_lsn}");
                    caught_up = true;
                }

                tracing::debug!(
                    "Ingested WAL up to {streaming_lsn}. Last record LSN is {}",
                    timeline.get_last_record_lsn()
                );

                last_rec_lsn = next_record_lsn;
                expected_wal_start = streaming_lsn;

                Some(streaming_lsn)
            }

            ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                let wal_end = keepalive.wal_end();
                let timestamp = keepalive.timestamp();
                let reply_requested = keepalive.reply() != 0;

                trace!(
                    "received PrimaryKeepAlive(wal_end: {wal_end}, timestamp: {timestamp:?} reply: {reply_requested})"
                );

                if reply_requested {
                    Some(last_rec_lsn)
                } else {
                    None
                }
            }

            _ => None,
        };

        if !connection_status.has_processed_wal && last_rec_lsn > last_rec_lsn_before_msg {
            // We have successfully processed at least one WAL record.
            connection_status.has_processed_wal = true;
            if let Err(e) = events_sender.send(TaskStateUpdate::Progress(connection_status)) {
                warn!("Wal connection event listener dropped, aborting the connection: {e}");
                return Ok(());
            }
        }

        if let Some(last_lsn) = status_update {
            let timeline_remote_consistent_lsn = timeline
                .get_remote_consistent_lsn_visible()
                .unwrap_or(Lsn(0));

            // The last LSN we processed. It is not guaranteed to survive pageserver crash.
            let last_received_lsn = last_lsn;
            // `disk_consistent_lsn` is the LSN at which page server guarantees local persistence of all received data
            let disk_consistent_lsn = timeline.get_disk_consistent_lsn();
            // The last LSN that is synced to remote storage and is guaranteed to survive pageserver crash
            // Used by safekeepers to remove WAL preceding `remote_consistent_lsn`.
            let remote_consistent_lsn = timeline_remote_consistent_lsn;
            let ts = SystemTime::now();

            // Update the status about what we just received. This is shown in the mgmt API.
            let last_received_wal = WalReceiverInfo {
                wal_source_connconf: wal_source_connconf.clone(),
                last_received_msg_lsn: last_lsn,
                last_received_msg_ts: ts
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("Received message time should be before UNIX EPOCH!")
                    .as_micros(),
            };
            *timeline.last_received_wal.lock().unwrap() = Some(last_received_wal);

            // Send the replication feedback message.
            // Regular standby_status_update fields are put into this message.
            let current_timeline_size = if timeline.tenant_shard_id.is_shard_zero() {
                timeline
                    .get_current_logical_size(
                        crate::tenant::timeline::GetLogicalSizePriority::User,
                        &ctx,
                    )
                    // FIXME: https://github.com/neondatabase/neon/issues/5963
                    .size_dont_care_about_accuracy()
            } else {
                // Non-zero shards send zero for logical size.  The safekeeper will ignore
                // this number.  This is because in a sharded tenant, only shard zero maintains
                // accurate logical size.
                0
            };

            let status_update = PageserverFeedback {
                current_timeline_size,
                last_received_lsn,
                disk_consistent_lsn,
                remote_consistent_lsn,
                replytime: ts,
                shard_number: timeline.tenant_shard_id.shard_number.0 as u32,
            };

            debug!("neon_status_update {status_update:?}");

            let mut data = BytesMut::new();
            status_update.serialize(&mut data);
            physical_stream
                .as_mut()
                .zenith_status_update(data.len() as u64, &data)
                .await?;
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
struct IdentifySystem {
    systemid: u64,
    timeline: u32,
    xlogpos: PgLsn,
    dbname: Option<String>,
}

/// There was a problem parsing the response to
/// a postgres IDENTIFY_SYSTEM command.
#[derive(Debug, thiserror::Error)]
#[error("IDENTIFY_SYSTEM parse error")]
struct IdentifyError;

/// Run the postgres `IDENTIFY_SYSTEM` command
async fn identify_system(client: &Client) -> anyhow::Result<IdentifySystem> {
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
    if let Some(SimpleQueryMessage::Row(first_row)) = response.first() {
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
