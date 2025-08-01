use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use anyhow::{Context, anyhow};
use futures::StreamExt;
use futures::future::Either;
use pageserver_api::shard::ShardIdentity;
use postgres_backend::{CopyStreamHandlerEnd, PostgresBackend};
use postgres_ffi::waldecoder::{WalDecodeError, WalStreamDecoder};
use postgres_ffi::{PgMajorVersion, get_current_timestamp};
use pq_proto::{BeMessage, InterpretedWalRecordsBody, WalSndKeepAlive};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::error::SendError;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::{Instrument, error, info, info_span};
use utils::critical_timeline;
use utils::lsn::Lsn;
use utils::postgres_client::{Compression, InterpretedFormat};
use wal_decoder::models::{InterpretedWalRecord, InterpretedWalRecords};
use wal_decoder::wire_format::ToWireFormat;

use crate::metrics::WAL_READERS;
use crate::send_wal::{EndWatchView, WalSenderGuard};
use crate::timeline::WalResidentTimeline;
use crate::wal_reader_stream::{StreamingWalReader, WalBytes};

/// Identifier used to differentiate between senders of the same
/// shard.
///
/// In the steady state there's only one, but two pageservers may
/// temporarily have the same shard attached and attempt to ingest
/// WAL for it. See also [`ShardSenderId`].
#[derive(Hash, Eq, PartialEq, Copy, Clone)]
struct SenderId(u8);

impl SenderId {
    fn first() -> Self {
        SenderId(0)
    }

    fn next(&self) -> Self {
        SenderId(self.0.checked_add(1).expect("few senders"))
    }
}

#[derive(Hash, Eq, PartialEq)]
struct ShardSenderId {
    shard: ShardIdentity,
    sender_id: SenderId,
}

impl Display for ShardSenderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.sender_id.0, self.shard.shard_slug())
    }
}

impl ShardSenderId {
    fn new(shard: ShardIdentity, sender_id: SenderId) -> Self {
        ShardSenderId { shard, sender_id }
    }

    fn shard(&self) -> ShardIdentity {
        self.shard
    }
}

/// Shard-aware fan-out interpreted record reader.
/// Reads WAL from disk, decodes it, intepretets it, and sends
/// it to any [`InterpretedWalSender`] connected to it.
/// Each [`InterpretedWalSender`] corresponds to one shard
/// and gets interpreted records concerning that shard only.
pub(crate) struct InterpretedWalReader {
    wal_stream: StreamingWalReader,
    shard_senders: HashMap<ShardIdentity, smallvec::SmallVec<[ShardSenderState; 1]>>,
    shard_notification_rx: Option<tokio::sync::mpsc::UnboundedReceiver<AttachShardNotification>>,
    state: Arc<std::sync::RwLock<InterpretedWalReaderState>>,
    pg_version: PgMajorVersion,
}

/// A handle for [`InterpretedWalReader`] which allows for interacting with it
/// when it runs as a separate tokio task.
#[derive(Debug)]
pub(crate) struct InterpretedWalReaderHandle {
    join_handle: JoinHandle<Result<(), InterpretedWalReaderError>>,
    state: Arc<std::sync::RwLock<InterpretedWalReaderState>>,
    shard_notification_tx: tokio::sync::mpsc::UnboundedSender<AttachShardNotification>,
}

struct ShardSenderState {
    sender_id: SenderId,
    tx: tokio::sync::mpsc::Sender<Batch>,
    next_record_lsn: Lsn,
}

/// State of [`InterpretedWalReader`] visible outside of the task running it.
#[derive(Debug)]
pub(crate) enum InterpretedWalReaderState {
    Running {
        current_position: Lsn,
        /// Tracks the start of the PG WAL LSN from which the current batch of
        /// interpreted records originated.
        current_batch_wal_start: Option<Lsn>,
    },
    Done,
}

pub(crate) struct Batch {
    wal_end_lsn: Lsn,
    available_wal_end_lsn: Lsn,
    records: InterpretedWalRecords,
}

#[derive(thiserror::Error, Debug)]
pub enum InterpretedWalReaderError {
    /// Handler initiates the end of streaming.
    #[error("decode error: {0}")]
    Decode(#[from] WalDecodeError),
    #[error("read or interpret error: {0}")]
    ReadOrInterpret(#[from] anyhow::Error),
    #[error("wal stream closed")]
    WalStreamClosed,
}

enum CurrentPositionUpdate {
    Reset { from: Lsn, to: Lsn },
    NotReset(Lsn),
}

impl CurrentPositionUpdate {
    fn current_position(&self) -> Lsn {
        match self {
            CurrentPositionUpdate::Reset { from: _, to } => *to,
            CurrentPositionUpdate::NotReset(lsn) => *lsn,
        }
    }

    fn previous_position(&self) -> Lsn {
        match self {
            CurrentPositionUpdate::Reset { from, to: _ } => *from,
            CurrentPositionUpdate::NotReset(lsn) => *lsn,
        }
    }
}

impl InterpretedWalReaderState {
    fn current_position(&self) -> Option<Lsn> {
        match self {
            InterpretedWalReaderState::Running {
                current_position, ..
            } => Some(*current_position),
            InterpretedWalReaderState::Done => None,
        }
    }

    #[cfg(test)]
    fn current_batch_wal_start(&self) -> Option<Lsn> {
        match self {
            InterpretedWalReaderState::Running {
                current_batch_wal_start,
                ..
            } => *current_batch_wal_start,
            InterpretedWalReaderState::Done => None,
        }
    }

    // Reset the current position of the WAL reader if the requested starting position
    // of the new shard is smaller than the current value.
    fn maybe_reset(&mut self, new_shard_start_pos: Lsn) -> CurrentPositionUpdate {
        match self {
            InterpretedWalReaderState::Running {
                current_position,
                current_batch_wal_start,
            } => {
                if new_shard_start_pos < *current_position {
                    let from = *current_position;
                    *current_position = new_shard_start_pos;
                    *current_batch_wal_start = None;
                    CurrentPositionUpdate::Reset {
                        from,
                        to: *current_position,
                    }
                } else {
                    // Edge case: The new shard is at the same current position as
                    // the reader. Note that the current position is WAL record aligned,
                    // so the reader might have done some partial reads and updated the
                    // batch start. If that's the case, adjust the batch start to match
                    // starting position of the new shard. It can lead to some shards
                    // seeing overlaps, but in that case the actual record LSNs are checked
                    // which should be fine based on the filtering logic.
                    if let Some(start) = current_batch_wal_start {
                        *start = std::cmp::min(*start, new_shard_start_pos);
                    }
                    CurrentPositionUpdate::NotReset(*current_position)
                }
            }
            InterpretedWalReaderState::Done => {
                panic!("maybe_reset called on finished reader")
            }
        }
    }

    fn update_current_batch_wal_start(&mut self, lsn: Lsn) {
        match self {
            InterpretedWalReaderState::Running {
                current_batch_wal_start,
                ..
            } => {
                if current_batch_wal_start.is_none() {
                    *current_batch_wal_start = Some(lsn);
                }
            }
            InterpretedWalReaderState::Done => {
                panic!("update_current_batch_wal_start called on finished reader")
            }
        }
    }

    fn replace_current_batch_wal_start(&mut self, with: Lsn) -> Lsn {
        match self {
            InterpretedWalReaderState::Running {
                current_batch_wal_start,
                ..
            } => current_batch_wal_start.replace(with).unwrap(),
            InterpretedWalReaderState::Done => {
                panic!("take_current_batch_wal_start called on finished reader")
            }
        }
    }

    fn update_current_position(&mut self, lsn: Lsn) {
        match self {
            InterpretedWalReaderState::Running {
                current_position, ..
            } => {
                *current_position = lsn;
            }
            InterpretedWalReaderState::Done => {
                panic!("update_current_position called on finished reader")
            }
        }
    }
}

pub(crate) struct AttachShardNotification {
    shard_id: ShardIdentity,
    sender: tokio::sync::mpsc::Sender<Batch>,
    start_pos: Lsn,
}

impl InterpretedWalReader {
    /// Spawn the reader in a separate tokio task and return a handle
    pub(crate) fn spawn(
        wal_stream: StreamingWalReader,
        start_pos: Lsn,
        tx: tokio::sync::mpsc::Sender<Batch>,
        shard: ShardIdentity,
        pg_version: PgMajorVersion,
        appname: &Option<String>,
    ) -> InterpretedWalReaderHandle {
        let state = Arc::new(std::sync::RwLock::new(InterpretedWalReaderState::Running {
            current_position: start_pos,
            current_batch_wal_start: None,
        }));

        let (shard_notification_tx, shard_notification_rx) = tokio::sync::mpsc::unbounded_channel();

        let ttid = wal_stream.ttid;

        let reader = InterpretedWalReader {
            wal_stream,
            shard_senders: HashMap::from([(
                shard,
                smallvec::smallvec![ShardSenderState {
                    sender_id: SenderId::first(),
                    tx,
                    next_record_lsn: start_pos,
                }],
            )]),
            shard_notification_rx: Some(shard_notification_rx),
            state: state.clone(),
            pg_version,
        };

        let metric = WAL_READERS
            .get_metric_with_label_values(&["task", appname.as_deref().unwrap_or("safekeeper")])
            .unwrap();

        let join_handle = tokio::task::spawn(
            async move {
                metric.inc();
                scopeguard::defer! {
                    metric.dec();
                }

                reader
                    .run_impl(start_pos)
                    .await
                    .inspect_err(|err| match err {
                        // TODO: we may want to differentiate these errors further.
                        InterpretedWalReaderError::Decode(_) => {
                            critical_timeline!(
                                ttid.tenant_id,
                                ttid.timeline_id,
                                // Hadron: The corruption flag is only used in PS so that it can feed this information back to SKs.
                                // We do not use these flags in SKs.
                                None::<&AtomicBool>,
                                "failed to read WAL record: {err:?}"
                            );
                        }
                        err => error!("failed to read WAL record: {err}"),
                    })
            }
            .instrument(info_span!("interpreted wal reader")),
        );

        InterpretedWalReaderHandle {
            join_handle,
            state,
            shard_notification_tx,
        }
    }

    /// Construct the reader without spawning anything
    /// Callers should drive the future returned by [`Self::run`].
    pub(crate) fn new(
        wal_stream: StreamingWalReader,
        start_pos: Lsn,
        tx: tokio::sync::mpsc::Sender<Batch>,
        shard: ShardIdentity,
        pg_version: PgMajorVersion,
        shard_notification_rx: Option<
            tokio::sync::mpsc::UnboundedReceiver<AttachShardNotification>,
        >,
    ) -> InterpretedWalReader {
        let state = Arc::new(std::sync::RwLock::new(InterpretedWalReaderState::Running {
            current_position: start_pos,
            current_batch_wal_start: None,
        }));

        InterpretedWalReader {
            wal_stream,
            shard_senders: HashMap::from([(
                shard,
                smallvec::smallvec![ShardSenderState {
                    sender_id: SenderId::first(),
                    tx,
                    next_record_lsn: start_pos,
                }],
            )]),
            shard_notification_rx,
            state: state.clone(),
            pg_version,
        }
    }

    /// Entry point for future (polling) based wal reader.
    pub(crate) async fn run(
        self,
        start_pos: Lsn,
        appname: &Option<String>,
    ) -> Result<(), CopyStreamHandlerEnd> {
        let metric = WAL_READERS
            .get_metric_with_label_values(&["future", appname.as_deref().unwrap_or("safekeeper")])
            .unwrap();

        metric.inc();
        scopeguard::defer! {
            metric.dec();
        }

        let ttid = self.wal_stream.ttid;
        match self.run_impl(start_pos).await {
            Err(err @ InterpretedWalReaderError::Decode(_)) => {
                critical_timeline!(
                    ttid.tenant_id,
                    ttid.timeline_id,
                    // Hadron: The corruption flag is only used in PS so that it can feed this information back to SKs.
                    // We do not use these flags in SKs.
                    None::<&AtomicBool>,
                    "failed to decode WAL record: {err:?}"
                );
            }
            Err(err) => error!("failed to read WAL record: {err}"),
            Ok(()) => info!("interpreted wal reader exiting"),
        }

        Err(CopyStreamHandlerEnd::Other(anyhow!(
            "interpreted wal reader finished"
        )))
    }

    /// Send interpreted WAL to one or more [`InterpretedWalSender`]s
    /// Stops when an error is encountered or when the [`InterpretedWalReaderHandle`]
    /// goes out of scope.
    async fn run_impl(mut self, start_pos: Lsn) -> Result<(), InterpretedWalReaderError> {
        let defer_state = self.state.clone();
        scopeguard::defer! {
            *defer_state.write().unwrap() = InterpretedWalReaderState::Done;
        }

        let mut wal_decoder = WalStreamDecoder::new(start_pos, self.pg_version);

        loop {
            tokio::select! {
                // Main branch for reading WAL and forwarding it
                wal_or_reset = self.wal_stream.next() => {
                    let wal = wal_or_reset.map(|wor| wor.get_wal().expect("reset handled in select branch below"));
                    let WalBytes {
                        wal,
                        wal_start_lsn,
                        wal_end_lsn,
                        available_wal_end_lsn,
                    } = match wal {
                        Some(some) => some.map_err(InterpretedWalReaderError::ReadOrInterpret)?,
                        None => {
                            // [`StreamingWalReader::next`] is an endless stream of WAL.
                            // It shouldn't ever finish unless it panicked or became internally
                            // inconsistent.
                            return Result::Err(InterpretedWalReaderError::WalStreamClosed);
                        }
                    };

                    self.state.write().unwrap().update_current_batch_wal_start(wal_start_lsn);

                    wal_decoder.feed_bytes(&wal);

                    // Deserialize and interpret WAL records from this batch of WAL.
                    // Interpreted records for each shard are collected separately.
                    let shard_ids = self.shard_senders.keys().copied().collect::<Vec<_>>();
                    let mut records_by_sender: HashMap<ShardSenderId, Vec<InterpretedWalRecord>> = HashMap::new();
                    let mut max_next_record_lsn = None;
                    let mut max_end_record_lsn = None;
                    while let Some((next_record_lsn, recdata)) = wal_decoder.poll_decode()?
                    {
                        assert!(next_record_lsn.is_aligned());
                        max_next_record_lsn = Some(next_record_lsn);
                        max_end_record_lsn = Some(wal_decoder.lsn());

                        let interpreted = InterpretedWalRecord::from_bytes_filtered(
                            recdata,
                            &shard_ids,
                            next_record_lsn,
                            self.pg_version,
                        )
                        .with_context(|| "Failed to interpret WAL")?;

                        for (shard, record) in interpreted {
                            // Shard zero needs to track the start LSN of the latest record
                            // in adition to the LSN of the next record to ingest. The former
                            // is included in basebackup persisted by the compute in WAL.
                            if !shard.is_shard_zero() && record.is_empty() {
                                continue;
                            }

                            let mut states_iter = self.shard_senders
                                .get(&shard)
                                .expect("keys collected above")
                                .iter()
                                .filter(|state| record.next_record_lsn > state.next_record_lsn)
                                .peekable();
                            while let Some(state) = states_iter.next() {
                                let shard_sender_id = ShardSenderId::new(shard, state.sender_id);

                                // The most commont case is one sender per shard. Peek and break to avoid the
                                // clone in that situation.
                                if states_iter.peek().is_none() {
                                    records_by_sender.entry(shard_sender_id).or_default().push(record);
                                    break;
                                } else {
                                    records_by_sender.entry(shard_sender_id).or_default().push(record.clone());
                                }
                            }
                        }
                    }

                    let max_next_record_lsn = match max_next_record_lsn {
                        Some(lsn) => lsn,
                        None => {
                            continue;
                        }
                    };

                    // Update the current position such that new receivers can decide
                    // whether to attach to us or spawn a new WAL reader.
                    let batch_wal_start_lsn = {
                        let mut guard = self.state.write().unwrap();
                        guard.update_current_position(max_next_record_lsn);
                        guard.replace_current_batch_wal_start(max_end_record_lsn.unwrap())
                    };

                    // Send interpreted records downstream. Anything that has already been seen
                    // by a shard is filtered out.
                    let mut shard_senders_to_remove = Vec::new();
                    for (shard, states) in &mut self.shard_senders {
                        for state in states {
                            let shard_sender_id = ShardSenderId::new(*shard, state.sender_id);

                            let batch = if max_next_record_lsn > state.next_record_lsn {
                                // This batch contains at least one record that this shard has not
                                // seen yet.
                                let records = records_by_sender.remove(&shard_sender_id).unwrap_or_default();

                                InterpretedWalRecords {
                                    records,
                                    next_record_lsn: max_next_record_lsn,
                                    raw_wal_start_lsn: Some(batch_wal_start_lsn),
                                }
                            } else if wal_end_lsn > state.next_record_lsn {
                                // All the records in this batch were seen by the shard
                                // However, the batch maps to a chunk of WAL that the
                                // shard has not yet seen. Notify it of the start LSN
                                // of the PG WAL chunk such that it doesn't look like a gap.
                                InterpretedWalRecords {
                                    records: Vec::default(),
                                    next_record_lsn: state.next_record_lsn,
                                    raw_wal_start_lsn: Some(batch_wal_start_lsn),
                                }
                            } else {
                                // The shard has seen this chunk of WAL before. Skip it.
                                continue;
                            };

                            let res = state.tx.send(Batch {
                                wal_end_lsn,
                                available_wal_end_lsn,
                                records: batch,
                            }).await;

                            if res.is_err() {
                                shard_senders_to_remove.push(shard_sender_id);
                            } else {
                                state.next_record_lsn = std::cmp::max(state.next_record_lsn, max_next_record_lsn);
                            }
                        }
                    }

                    // Clean up any shard senders that have dropped out.
                    // This is inefficient, but such events are rare (connection to PS termination)
                    // and the number of subscriptions on the same shards very small (only one
                    // for the steady state).
                    for to_remove in shard_senders_to_remove {
                        let shard_senders = self.shard_senders.get_mut(&to_remove.shard()).expect("saw it above");
                        if let Some(idx) = shard_senders.iter().position(|s| s.sender_id == to_remove.sender_id) {
                            shard_senders.remove(idx);
                            tracing::info!("Removed shard sender {}", to_remove);
                        }

                        if shard_senders.is_empty() {
                            self.shard_senders.remove(&to_remove.shard());
                        }
                    }
                },
                // Listen for new shards that want to attach to this reader.
                // If the reader is not running as a task, then this is not supported
                // (see the pending branch below).
                notification = match self.shard_notification_rx.as_mut() {
                        Some(rx) => Either::Left(rx.recv()),
                        None => Either::Right(std::future::pending())
                    } => {
                    if let Some(n) = notification {
                        let AttachShardNotification { shard_id, sender, start_pos } = n;

                        // Update internal and external state, then reset the WAL stream
                        // if required.
                        let senders = self.shard_senders.entry(shard_id).or_default();

                        // Clean up any shard senders that have dropped out before adding the new
                        // one. This avoids a build up of dead senders.
                        senders.retain(|sender| {
                            let closed = sender.tx.is_closed();

                            if closed {
                                let sender_id = ShardSenderId::new(shard_id, sender.sender_id);
                                tracing::info!("Removed shard sender {}", sender_id);
                            }

                            !closed
                        });

                        let new_sender_id = match senders.last() {
                            Some(sender) => sender.sender_id.next(),
                            None => SenderId::first()
                        };

                        senders.push(ShardSenderState { sender_id: new_sender_id, tx: sender, next_record_lsn: start_pos});

                        // If the shard is subscribing below the current position the we need
                        // to update the cursor that tracks where we are at in the WAL
                        // ([`Self::state`]) and reset the WAL stream itself
                        // (`[Self::wal_stream`]). This must be done atomically from the POV of
                        // anything outside the select statement.
                        let position_reset = self.state.write().unwrap().maybe_reset(start_pos);
                        match position_reset {
                            CurrentPositionUpdate::Reset { from: _, to } => {
                                self.wal_stream.reset(to).await;
                                wal_decoder = WalStreamDecoder::new(to, self.pg_version);
                            },
                            CurrentPositionUpdate::NotReset(_) => {}
                        };

                        tracing::info!(
                            "Added shard sender {} with start_pos={} previous_pos={} current_pos={}",
                            ShardSenderId::new(shard_id, new_sender_id),
                            start_pos,
                            position_reset.previous_position(),
                            position_reset.current_position(),
                        );
                    }
                }
            }
        }
    }

    #[cfg(test)]
    fn state(&self) -> Arc<std::sync::RwLock<InterpretedWalReaderState>> {
        self.state.clone()
    }
}

impl InterpretedWalReaderHandle {
    /// Fan-out the reader by attaching a new shard to it
    pub(crate) fn fanout(
        &self,
        shard_id: ShardIdentity,
        sender: tokio::sync::mpsc::Sender<Batch>,
        start_pos: Lsn,
    ) -> Result<(), SendError<AttachShardNotification>> {
        self.shard_notification_tx.send(AttachShardNotification {
            shard_id,
            sender,
            start_pos,
        })
    }

    /// Get the current WAL position of the reader
    pub(crate) fn current_position(&self) -> Option<Lsn> {
        self.state.read().unwrap().current_position()
    }

    pub(crate) fn abort(&self) {
        self.join_handle.abort()
    }
}

impl Drop for InterpretedWalReaderHandle {
    fn drop(&mut self) {
        tracing::info!("Aborting interpreted wal reader");
        self.abort()
    }
}

pub(crate) struct InterpretedWalSender<'a, IO> {
    pub(crate) format: InterpretedFormat,
    pub(crate) compression: Option<Compression>,
    pub(crate) appname: Option<String>,

    pub(crate) tli: WalResidentTimeline,
    pub(crate) start_lsn: Lsn,

    pub(crate) pgb: &'a mut PostgresBackend<IO>,
    pub(crate) end_watch_view: EndWatchView,
    pub(crate) wal_sender_guard: Arc<WalSenderGuard>,
    pub(crate) rx: tokio::sync::mpsc::Receiver<Batch>,
}

impl<IO: AsyncRead + AsyncWrite + Unpin> InterpretedWalSender<'_, IO> {
    /// Send interpreted WAL records over the network.
    /// Also manages keep-alives if nothing was sent for a while.
    pub(crate) async fn run(mut self) -> Result<(), CopyStreamHandlerEnd> {
        let mut keepalive_ticker = tokio::time::interval(Duration::from_secs(1));
        keepalive_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        keepalive_ticker.reset();

        let mut wal_position = self.start_lsn;

        loop {
            tokio::select! {
                batch = self.rx.recv() => {
                    let batch = match batch {
                        Some(b) => b,
                        None => {
                            return Result::Err(
                                CopyStreamHandlerEnd::Other(anyhow!("Interpreted WAL reader exited early"))
                            );
                        }
                    };

                    wal_position = batch.wal_end_lsn;

                    let buf = batch
                        .records
                        .to_wire(self.format, self.compression)
                        .await
                        .with_context(|| "Failed to serialize interpreted WAL")
                        .map_err(CopyStreamHandlerEnd::from)?;

                    // Reset the keep alive ticker since we are sending something
                    // over the wire now.
                    keepalive_ticker.reset();

                    self.pgb
                        .write_message(&BeMessage::InterpretedWalRecords(InterpretedWalRecordsBody {
                            streaming_lsn: batch.wal_end_lsn.0,
                            commit_lsn: batch.available_wal_end_lsn.0,
                            data: &buf,
                        })).await?;
                }
                // Send a periodic keep alive when the connection has been idle for a while.
                // Since we've been idle, also check if we can stop streaming.
                _ = keepalive_ticker.tick() => {
                    if let Some(remote_consistent_lsn) = self.wal_sender_guard
                        .walsenders()
                        .get_ws_remote_consistent_lsn(self.wal_sender_guard.id())
                    {
                        if self.tli.should_walsender_stop(remote_consistent_lsn).await {
                            // Stop streaming if the receivers are caught up and
                            // there's no active compute. This causes the loop in
                            // [`crate::send_interpreted_wal::InterpretedWalSender::run`]
                            // to exit and terminate the WAL stream.
                            break;
                        }
                    }

                    self.pgb
                        .write_message(&BeMessage::KeepAlive(WalSndKeepAlive {
                            wal_end: self.end_watch_view.get().0,
                            timestamp: get_current_timestamp(),
                            request_reply: true,
                        }))
                        .await?;
                },
            }
        }

        Err(CopyStreamHandlerEnd::ServerInitiated(format!(
            "ending streaming to {:?} at {}, receiver is caughtup and there is no computes",
            self.appname, wal_position,
        )))
    }
}
#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::time::Duration;

    use pageserver_api::shard::{DEFAULT_STRIPE_SIZE, ShardIdentity};
    use postgres_ffi::{MAX_SEND_SIZE, PgMajorVersion};
    use tokio::sync::mpsc::error::TryRecvError;
    use utils::id::{NodeId, TenantTimelineId};
    use utils::lsn::Lsn;
    use utils::shard::{ShardCount, ShardNumber};

    use crate::send_interpreted_wal::{AttachShardNotification, Batch, InterpretedWalReader};
    use crate::test_utils::Env;
    use crate::wal_reader_stream::StreamingWalReader;

    #[tokio::test]
    async fn test_interpreted_wal_reader_fanout() {
        let _ = env_logger::builder().is_test(true).try_init();

        const SIZE: usize = 8 * 1024;
        const MSG_COUNT: usize = 200;
        const PG_VERSION: PgMajorVersion = PgMajorVersion::PG17;
        const SHARD_COUNT: u8 = 2;

        let start_lsn = Lsn::from_str("0/149FD18").unwrap();
        let env = Env::new(true).unwrap();
        let tli = env
            .make_timeline(NodeId(1), TenantTimelineId::generate(), start_lsn)
            .await
            .unwrap();

        let resident_tli = tli.wal_residence_guard().await.unwrap();
        let end_watch = Env::write_wal(tli, start_lsn, SIZE, MSG_COUNT, c"neon-file:", None)
            .await
            .unwrap();
        let end_pos = end_watch.get();

        tracing::info!("Doing first round of reads ...");

        let streaming_wal_reader = StreamingWalReader::new(
            resident_tli,
            None,
            start_lsn,
            end_pos,
            end_watch,
            MAX_SEND_SIZE,
        );

        let shard_0 =
            ShardIdentity::new(ShardNumber(0), ShardCount(SHARD_COUNT), DEFAULT_STRIPE_SIZE)
                .unwrap();

        let shard_1 =
            ShardIdentity::new(ShardNumber(1), ShardCount(SHARD_COUNT), DEFAULT_STRIPE_SIZE)
                .unwrap();

        let mut shards = HashMap::new();

        for shard_number in 0..SHARD_COUNT {
            let shard_id = ShardIdentity::new(
                ShardNumber(shard_number),
                ShardCount(SHARD_COUNT),
                DEFAULT_STRIPE_SIZE,
            )
            .unwrap();
            let (tx, rx) = tokio::sync::mpsc::channel::<Batch>(MSG_COUNT * 2);
            shards.insert(shard_id, (Some(tx), Some(rx)));
        }

        let shard_0_tx = shards.get_mut(&shard_0).unwrap().0.take().unwrap();
        let mut shard_0_rx = shards.get_mut(&shard_0).unwrap().1.take().unwrap();

        let handle = InterpretedWalReader::spawn(
            streaming_wal_reader,
            start_lsn,
            shard_0_tx,
            shard_0,
            PG_VERSION,
            &Some("pageserver".to_string()),
        );

        tracing::info!("Reading all WAL with only shard 0 attached ...");

        let mut shard_0_interpreted_records = Vec::new();
        while let Some(batch) = shard_0_rx.recv().await {
            shard_0_interpreted_records.push(batch.records);
            if batch.wal_end_lsn == batch.available_wal_end_lsn {
                break;
            }
        }

        let shard_1_tx = shards.get_mut(&shard_1).unwrap().0.take().unwrap();
        let mut shard_1_rx = shards.get_mut(&shard_1).unwrap().1.take().unwrap();

        tracing::info!("Attaching shard 1 to the reader at start of WAL");
        handle.fanout(shard_1, shard_1_tx, start_lsn).unwrap();

        tracing::info!("Reading all WAL with shard 0 and shard 1 attached ...");

        let mut shard_1_interpreted_records = Vec::new();
        while let Some(batch) = shard_1_rx.recv().await {
            shard_1_interpreted_records.push(batch.records);
            if batch.wal_end_lsn == batch.available_wal_end_lsn {
                break;
            }
        }

        // This test uses logical messages. Those only go to shard 0. Check that the
        // filtering worked and shard 1 did not get any.
        assert!(
            shard_1_interpreted_records
                .iter()
                .all(|recs| recs.records.is_empty())
        );

        // Shard 0 should not receive anything more since the reader is
        // going through wal that it has already processed.
        let res = shard_0_rx.try_recv();
        if let Ok(ref ok) = res {
            tracing::error!(
                "Shard 0 received batch: wal_end_lsn={} available_wal_end_lsn={}",
                ok.wal_end_lsn,
                ok.available_wal_end_lsn
            );
        }
        assert!(matches!(res, Err(TryRecvError::Empty)));

        // Check that the next records lsns received by the two shards match up.
        let shard_0_next_lsns = shard_0_interpreted_records
            .iter()
            .map(|recs| recs.next_record_lsn)
            .collect::<Vec<_>>();
        let shard_1_next_lsns = shard_1_interpreted_records
            .iter()
            .map(|recs| recs.next_record_lsn)
            .collect::<Vec<_>>();
        assert_eq!(shard_0_next_lsns, shard_1_next_lsns);

        handle.abort();
        let mut done = false;
        for _ in 0..5 {
            if handle.current_position().is_none() {
                done = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        assert!(done);
    }

    #[tokio::test]
    async fn test_interpreted_wal_reader_same_shard_fanout() {
        let _ = env_logger::builder().is_test(true).try_init();

        const SIZE: usize = 8 * 1024;
        const MSG_COUNT: usize = 200;
        const PG_VERSION: PgMajorVersion = PgMajorVersion::PG17;
        const SHARD_COUNT: u8 = 2;

        let start_lsn = Lsn::from_str("0/149FD18").unwrap();
        let env = Env::new(true).unwrap();
        let tli = env
            .make_timeline(NodeId(1), TenantTimelineId::generate(), start_lsn)
            .await
            .unwrap();

        let resident_tli = tli.wal_residence_guard().await.unwrap();
        let mut next_record_lsns = Vec::default();
        let end_watch = Env::write_wal(
            tli,
            start_lsn,
            SIZE,
            MSG_COUNT,
            c"neon-file:",
            Some(&mut next_record_lsns),
        )
        .await
        .unwrap();
        let end_pos = end_watch.get();

        let streaming_wal_reader = StreamingWalReader::new(
            resident_tli,
            None,
            start_lsn,
            end_pos,
            end_watch,
            MAX_SEND_SIZE,
        );

        let shard_0 =
            ShardIdentity::new(ShardNumber(0), ShardCount(SHARD_COUNT), DEFAULT_STRIPE_SIZE)
                .unwrap();

        struct Sender {
            tx: Option<tokio::sync::mpsc::Sender<Batch>>,
            rx: tokio::sync::mpsc::Receiver<Batch>,
            shard: ShardIdentity,
            start_lsn: Lsn,
            received_next_record_lsns: Vec<Lsn>,
        }

        impl Sender {
            fn new(start_lsn: Lsn, shard: ShardIdentity) -> Self {
                let (tx, rx) = tokio::sync::mpsc::channel::<Batch>(MSG_COUNT * 2);
                Self {
                    tx: Some(tx),
                    rx,
                    shard,
                    start_lsn,
                    received_next_record_lsns: Vec::default(),
                }
            }
        }

        assert!(next_record_lsns.len() > 7);
        let start_lsns = vec![
            next_record_lsns[5],
            next_record_lsns[1],
            next_record_lsns[3],
        ];
        let mut senders = start_lsns
            .into_iter()
            .map(|lsn| Sender::new(lsn, shard_0))
            .collect::<Vec<_>>();

        let first_sender = senders.first_mut().unwrap();
        let handle = InterpretedWalReader::spawn(
            streaming_wal_reader,
            first_sender.start_lsn,
            first_sender.tx.take().unwrap(),
            first_sender.shard,
            PG_VERSION,
            &Some("pageserver".to_string()),
        );

        for sender in senders.iter_mut().skip(1) {
            handle
                .fanout(sender.shard, sender.tx.take().unwrap(), sender.start_lsn)
                .unwrap();
        }

        for sender in senders.iter_mut() {
            loop {
                let batch = sender.rx.recv().await.unwrap();
                tracing::info!(
                    "Sender with start_lsn={} received batch ending at {} with {} records",
                    sender.start_lsn,
                    batch.wal_end_lsn,
                    batch.records.records.len()
                );

                for rec in batch.records.records {
                    sender.received_next_record_lsns.push(rec.next_record_lsn);
                }

                if batch.wal_end_lsn == batch.available_wal_end_lsn {
                    break;
                }
            }
        }

        handle.abort();
        let mut done = false;
        for _ in 0..5 {
            if handle.current_position().is_none() {
                done = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        assert!(done);

        for sender in senders {
            tracing::info!(
                "Validating records received by sender with start_lsn={}",
                sender.start_lsn
            );

            assert!(sender.received_next_record_lsns.is_sorted());
            let expected = next_record_lsns
                .iter()
                .filter(|lsn| **lsn > sender.start_lsn)
                .copied()
                .collect::<Vec<_>>();
            assert_eq!(sender.received_next_record_lsns, expected);
        }
    }

    #[tokio::test]
    async fn test_batch_start_tracking_on_reset() {
        // When the WAL stream is reset to an older LSN,
        // the current batch start LSN should be invalidated.
        // This test constructs such a scenario:
        // 1. Shard 0 is reading somewhere ahead
        // 2. Reader reads some WAL, but does not decode a full record (partial read)
        // 3. Shard 1 attaches to the reader and resets it to an older LSN
        // 4. Shard 1 should get the correct batch WAL start LSN
        let _ = env_logger::builder().is_test(true).try_init();

        const SIZE: usize = 64 * 1024;
        const MSG_COUNT: usize = 10;
        const PG_VERSION: PgMajorVersion = PgMajorVersion::PG17;
        const SHARD_COUNT: u8 = 2;
        const WAL_READER_BATCH_SIZE: usize = 8192;

        let start_lsn = Lsn::from_str("0/149FD18").unwrap();
        let env = Env::new(true).unwrap();
        let mut next_record_lsns = Vec::default();
        let tli = env
            .make_timeline(NodeId(1), TenantTimelineId::generate(), start_lsn)
            .await
            .unwrap();

        let resident_tli = tli.wal_residence_guard().await.unwrap();
        let end_watch = Env::write_wal(
            tli,
            start_lsn,
            SIZE,
            MSG_COUNT,
            c"neon-file:",
            Some(&mut next_record_lsns),
        )
        .await
        .unwrap();

        assert!(next_record_lsns.len() > 3);
        let shard_0_start_lsn = next_record_lsns[3];

        let end_pos = end_watch.get();

        let streaming_wal_reader = StreamingWalReader::new(
            resident_tli,
            None,
            shard_0_start_lsn,
            end_pos,
            end_watch,
            WAL_READER_BATCH_SIZE,
        );

        let shard_0 =
            ShardIdentity::new(ShardNumber(0), ShardCount(SHARD_COUNT), DEFAULT_STRIPE_SIZE)
                .unwrap();

        let shard_1 =
            ShardIdentity::new(ShardNumber(1), ShardCount(SHARD_COUNT), DEFAULT_STRIPE_SIZE)
                .unwrap();

        let mut shards = HashMap::new();

        for shard_number in 0..SHARD_COUNT {
            let shard_id = ShardIdentity::new(
                ShardNumber(shard_number),
                ShardCount(SHARD_COUNT),
                DEFAULT_STRIPE_SIZE,
            )
            .unwrap();
            let (tx, rx) = tokio::sync::mpsc::channel::<Batch>(MSG_COUNT * 2);
            shards.insert(shard_id, (Some(tx), Some(rx)));
        }

        let shard_0_tx = shards.get_mut(&shard_0).unwrap().0.take().unwrap();

        let (shard_notification_tx, shard_notification_rx) = tokio::sync::mpsc::unbounded_channel();

        let reader = InterpretedWalReader::new(
            streaming_wal_reader,
            shard_0_start_lsn,
            shard_0_tx,
            shard_0,
            PG_VERSION,
            Some(shard_notification_rx),
        );

        let reader_state = reader.state();
        let mut reader_fut = std::pin::pin!(reader.run(shard_0_start_lsn, &None));
        loop {
            let poll = futures::poll!(reader_fut.as_mut());
            assert!(poll.is_pending());

            let guard = reader_state.read().unwrap();
            if guard.current_batch_wal_start().is_some() {
                break;
            }
        }

        shard_notification_tx
            .send(AttachShardNotification {
                shard_id: shard_1,
                sender: shards.get_mut(&shard_1).unwrap().0.take().unwrap(),
                start_pos: start_lsn,
            })
            .unwrap();

        let mut shard_1_rx = shards.get_mut(&shard_1).unwrap().1.take().unwrap();
        loop {
            let poll = futures::poll!(reader_fut.as_mut());
            assert!(poll.is_pending());

            let try_recv_res = shard_1_rx.try_recv();
            match try_recv_res {
                Ok(batch) => {
                    assert_eq!(batch.records.raw_wal_start_lsn.unwrap(), start_lsn);
                    break;
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    unreachable!();
                }
            }
        }
    }

    #[tokio::test]
    async fn test_shard_zero_does_not_skip_empty_records() {
        let _ = env_logger::builder().is_test(true).try_init();

        const SIZE: usize = 8 * 1024;
        const MSG_COUNT: usize = 10;
        const PG_VERSION: PgMajorVersion = PgMajorVersion::PG17;

        let start_lsn = Lsn::from_str("0/149FD18").unwrap();
        let env = Env::new(true).unwrap();
        let tli = env
            .make_timeline(NodeId(1), TenantTimelineId::generate(), start_lsn)
            .await
            .unwrap();

        let resident_tli = tli.wal_residence_guard().await.unwrap();
        let mut next_record_lsns = Vec::new();
        let end_watch = Env::write_wal(
            tli,
            start_lsn,
            SIZE,
            MSG_COUNT,
            // This is a logical message prefix that is not persisted to key value storage.
            // We use it in order to validate that shard zero receives emtpy interpreted records.
            c"test:",
            Some(&mut next_record_lsns),
        )
        .await
        .unwrap();
        let end_pos = end_watch.get();

        let streaming_wal_reader = StreamingWalReader::new(
            resident_tli,
            None,
            start_lsn,
            end_pos,
            end_watch,
            MAX_SEND_SIZE,
        );

        let shard = ShardIdentity::unsharded();
        let (records_tx, mut records_rx) = tokio::sync::mpsc::channel::<Batch>(MSG_COUNT * 2);

        let handle = InterpretedWalReader::spawn(
            streaming_wal_reader,
            start_lsn,
            records_tx,
            shard,
            PG_VERSION,
            &Some("pageserver".to_string()),
        );

        let mut interpreted_records = Vec::new();
        while let Some(batch) = records_rx.recv().await {
            interpreted_records.push(batch.records);
            if batch.wal_end_lsn == batch.available_wal_end_lsn {
                break;
            }
        }

        let received_next_record_lsns = interpreted_records
            .into_iter()
            .flat_map(|b| b.records)
            .map(|rec| rec.next_record_lsn)
            .collect::<Vec<_>>();

        // By default this also includes the start LSN. Trim it since it shouldn't be received.
        let next_record_lsns = next_record_lsns.into_iter().skip(1).collect::<Vec<_>>();

        assert_eq!(received_next_record_lsns, next_record_lsns);

        handle.abort();
        let mut done = false;
        for _ in 0..5 {
            if handle.current_position().is_none() {
                done = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        assert!(done);
    }
}
