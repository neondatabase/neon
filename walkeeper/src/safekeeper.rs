//! Acceptor part of proposer-acceptor consensus algorithm.

use anyhow::{bail, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use postgres_ffi::waldecoder::WalStreamDecoder;
use postgres_ffi::xlog_utils::TimeLineID;
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::fmt;
use std::io::Read;
use tracing::*;

use lazy_static::lazy_static;

use crate::send_wal::HotStandbyFeedback;
use postgres_ffi::xlog_utils::MAX_SEND_SIZE;
use zenith_metrics::{
    register_gauge_vec, register_histogram_vec, Gauge, GaugeVec, Histogram, HistogramVec,
    DISK_WRITE_SECONDS_BUCKETS,
};
use zenith_utils::bin_ser::LeSer;
use zenith_utils::lsn::Lsn;
use zenith_utils::pq_proto::SystemId;
use zenith_utils::pq_proto::ZenithFeedback;
use zenith_utils::zid::{ZTenantId, ZTimelineId};

pub const SK_MAGIC: u32 = 0xcafeceefu32;
pub const SK_FORMAT_VERSION: u32 = 3;
const SK_PROTOCOL_VERSION: u32 = 1;
const UNKNOWN_SERVER_VERSION: u32 = 0;

/// Consensus logical timestamp.
pub type Term = u64;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TermSwitchEntry {
    pub term: Term,
    pub lsn: Lsn,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct TermHistory(pub Vec<TermSwitchEntry>);

impl TermHistory {
    pub fn empty() -> TermHistory {
        TermHistory(Vec::new())
    }

    // Parse TermHistory as n_entries followed by TermSwitchEntry pairs
    pub fn from_bytes(mut bytes: Bytes) -> Result<TermHistory> {
        if bytes.remaining() < 4 {
            bail!("TermHistory misses len");
        }
        let n_entries = bytes.get_u32_le();
        let mut res = Vec::with_capacity(n_entries as usize);
        for _ in 0..n_entries {
            if bytes.remaining() < 16 {
                bail!("TermHistory is incomplete");
            }
            res.push(TermSwitchEntry {
                term: bytes.get_u64_le(),
                lsn: bytes.get_u64_le().into(),
            })
        }
        Ok(TermHistory(res))
    }

    /// Return copy of self with switches happening strictly after up_to
    /// truncated.
    pub fn up_to(&self, up_to: Lsn) -> TermHistory {
        let mut res = Vec::with_capacity(self.0.len());
        for e in &self.0 {
            if e.lsn > up_to {
                break;
            }
            res.push(*e);
        }
        TermHistory(res)
    }
}

/// Display only latest entries for Debug.
impl fmt::Debug for TermHistory {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let n_printed = 20;
        write!(
            fmt,
            "{}{:?}",
            if self.0.len() > n_printed { "... " } else { "" },
            self.0
                .iter()
                .rev()
                .take(n_printed)
                .map(|&e| (e.term, e.lsn)) // omit TermSwitchEntry
                .collect::<Vec<_>>()
        )
    }
}

/// Unique id of proposer. Not needed for correctness, used for monitoring.
pub type PgUuid = [u8; 16];

/// Persistent consensus state of the acceptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcceptorState {
    /// acceptor's last term it voted for (advanced in 1 phase)
    pub term: Term,
    /// History of term switches for safekeeper's WAL.
    /// Actually it often goes *beyond* WAL contents as we adopt term history
    /// from the proposer before recovery.
    pub term_history: TermHistory,
}

impl AcceptorState {
    /// acceptor's epoch is the term of the highest entry in the log
    pub fn get_epoch(&self, flush_lsn: Lsn) -> Term {
        let th = self.term_history.up_to(flush_lsn);
        match th.0.last() {
            Some(e) => e.term,
            None => 0,
        }
    }
}

/// Information about Postgres. Safekeeper gets it once and then verifies
/// all further connections from computes match.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServerInfo {
    /// Postgres server version
    pub pg_version: u32,
    pub system_id: SystemId,
    #[serde(with = "hex")]
    pub tenant_id: ZTenantId,
    /// Zenith timelineid
    #[serde(with = "hex")]
    pub timeline_id: ZTimelineId,
    pub wal_seg_size: u32,
}

/// Persistent information stored on safekeeper node
/// On disk data is prefixed by magic and format version and followed by checksum.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafeKeeperState {
    /// persistent acceptor state
    pub acceptor_state: AcceptorState,
    /// information about server
    pub server: ServerInfo,
    /// Unique id of the last *elected* proposer we dealed with. Not needed
    /// for correctness, exists for monitoring purposes.
    #[serde(with = "hex")]
    pub proposer_uuid: PgUuid,
    /// part of WAL acknowledged by quorum and available locally
    pub commit_lsn: Lsn,
    /// minimal LSN which may be needed for recovery of some safekeeper (end_lsn
    /// of last record streamed to everyone)
    pub truncate_lsn: Lsn,
    // Safekeeper starts receiving WAL from this LSN, zeros before it ought to
    // be skipped during decoding.
    pub wal_start_lsn: Lsn,
}

impl SafeKeeperState {
    pub fn new() -> SafeKeeperState {
        SafeKeeperState {
            acceptor_state: AcceptorState {
                term: 0,
                term_history: TermHistory::empty(),
            },
            server: ServerInfo {
                pg_version: UNKNOWN_SERVER_VERSION, /* Postgres server version */
                system_id: 0,                       /* Postgres system identifier */
                tenant_id: ZTenantId::from([0u8; 16]),
                timeline_id: ZTimelineId::from([0u8; 16]),
                wal_seg_size: 0,
            },
            proposer_uuid: [0; 16],
            commit_lsn: Lsn(0),   /* part of WAL acknowledged by quorum */
            truncate_lsn: Lsn(0), /* minimal LSN which may be needed for recovery of some safekeeper */
            wal_start_lsn: Lsn(0),
        }
    }
}

impl Default for SafeKeeperState {
    fn default() -> Self {
        Self::new()
    }
}

// protocol messages

/// Initial Proposer -> Acceptor message
#[derive(Debug, Deserialize)]
pub struct ProposerGreeting {
    /// proposer-acceptor protocol version
    pub protocol_version: u32,
    /// Postgres server version
    pub pg_version: u32,
    pub proposer_id: PgUuid,
    pub system_id: SystemId,
    /// Zenith timelineid
    pub ztli: ZTimelineId,
    pub tenant_id: ZTenantId,
    pub tli: TimeLineID,
    pub wal_seg_size: u32,
}

/// Acceptor -> Proposer initial response: the highest term known to me
/// (acceptor voted for).
#[derive(Debug, Serialize)]
pub struct AcceptorGreeting {
    term: u64,
}

/// Vote request sent from proposer to safekeepers
#[derive(Debug, Deserialize)]
pub struct VoteRequest {
    term: Term,
}

/// Vote itself, sent from safekeeper to proposer
#[derive(Debug, Serialize)]
pub struct VoteResponse {
    term: Term, // safekeeper's current term; if it is higher than proposer's, the compute is out of date.
    vote_given: u64, // fixme u64 due to padding
    // Safekeeper flush_lsn (end of WAL) + history of term switches allow
    // proposer to choose the most advanced one.
    flush_lsn: Lsn,
    truncate_lsn: Lsn,
    term_history: TermHistory,
}

/*
 * Proposer -> Acceptor message announcing proposer is elected and communicating
 * term history to it.
 */
#[derive(Debug)]
pub struct ProposerElected {
    pub term: Term,
    pub start_streaming_at: Lsn,
    pub term_history: TermHistory,
}

/// Request with WAL message sent from proposer to safekeeper. Along the way it
/// communicates commit_lsn.
#[derive(Debug)]
pub struct AppendRequest {
    pub h: AppendRequestHeader,
    pub wal_data: Bytes,
}
#[derive(Debug, Clone, Deserialize)]
pub struct AppendRequestHeader {
    // safekeeper's current term; if it is higher than proposer's, the compute is out of date.
    pub term: Term,
    // LSN since the proposer appends WAL; determines epoch switch point.
    pub epoch_start_lsn: Lsn,
    /// start position of message in WAL
    pub begin_lsn: Lsn,
    /// end position of message in WAL
    pub end_lsn: Lsn,
    /// LSN committed by quorum of safekeepers
    pub commit_lsn: Lsn,
    /// minimal LSN which may be needed by proposer to perform recovery of some safekeeper
    pub truncate_lsn: Lsn,
    // only for logging/debugging
    pub proposer_uuid: PgUuid,
}

/// Report safekeeper state to proposer
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct AppendResponse {
    // Current term of the safekeeper; if it is higher than proposer's, the
    // compute is out of date.
    pub term: Term,
    // NOTE: this is physical end of wal on safekeeper; currently it doesn't
    // make much sense without taking epoch into account, as history can be
    // diverged.
    pub flush_lsn: Lsn,
    // We report back our awareness about which WAL is committed, as this is
    // a criterion for walproposer --sync mode exit
    pub commit_lsn: Lsn,
    pub hs_feedback: HotStandbyFeedback,
    pub zenith_feedback: ZenithFeedback,
}

impl AppendResponse {
    fn term_only(term: Term) -> AppendResponse {
        AppendResponse {
            term,
            flush_lsn: Lsn(0),
            commit_lsn: Lsn(0),
            hs_feedback: HotStandbyFeedback::empty(),
            zenith_feedback: ZenithFeedback::empty(),
        }
    }
}

/// Proposer -> Acceptor messages
#[derive(Debug)]
pub enum ProposerAcceptorMessage {
    Greeting(ProposerGreeting),
    VoteRequest(VoteRequest),
    Elected(ProposerElected),
    AppendRequest(AppendRequest),
}

impl ProposerAcceptorMessage {
    /// Parse proposer message.
    pub fn parse(msg_bytes: Bytes) -> Result<ProposerAcceptorMessage> {
        // xxx using Reader is inefficient but easy to work with bincode
        let mut stream = msg_bytes.reader();
        // u64 is here to avoid padding; it will be removed once we stop packing C structs into the wire as is
        let tag = stream.read_u64::<LittleEndian>()? as u8 as char;
        match tag {
            'g' => {
                let msg = ProposerGreeting::des_from(&mut stream)?;
                Ok(ProposerAcceptorMessage::Greeting(msg))
            }
            'v' => {
                let msg = VoteRequest::des_from(&mut stream)?;
                Ok(ProposerAcceptorMessage::VoteRequest(msg))
            }
            'e' => {
                let mut msg_bytes = stream.into_inner();
                if msg_bytes.remaining() < 16 {
                    bail!("ProposerElected message is not complete");
                }
                let term = msg_bytes.get_u64_le();
                let start_streaming_at = msg_bytes.get_u64_le().into();
                let term_history = TermHistory::from_bytes(msg_bytes)?;
                let msg = ProposerElected {
                    term,
                    start_streaming_at,
                    term_history,
                };
                Ok(ProposerAcceptorMessage::Elected(msg))
            }
            'a' => {
                // read header followed by wal data
                let hdr = AppendRequestHeader::des_from(&mut stream)?;
                let rec_size = hdr
                    .end_lsn
                    .checked_sub(hdr.begin_lsn)
                    .context("begin_lsn > end_lsn in AppendRequest")?
                    .0 as usize;
                if rec_size > MAX_SEND_SIZE {
                    bail!(
                        "AppendRequest is longer than MAX_SEND_SIZE ({})",
                        MAX_SEND_SIZE
                    );
                }

                let mut wal_data_vec: Vec<u8> = vec![0; rec_size];
                stream.read_exact(&mut wal_data_vec)?;
                let wal_data = Bytes::from(wal_data_vec);
                let msg = AppendRequest { h: hdr, wal_data };

                Ok(ProposerAcceptorMessage::AppendRequest(msg))
            }
            _ => bail!("unknown proposer-acceptor message tag: {}", tag,),
        }
    }
}

/// Acceptor -> Proposer messages
#[derive(Debug)]
pub enum AcceptorProposerMessage {
    Greeting(AcceptorGreeting),
    VoteResponse(VoteResponse),
    AppendResponse(AppendResponse),
}

impl AcceptorProposerMessage {
    /// Serialize acceptor -> proposer message.
    pub fn serialize(&self, buf: &mut BytesMut) -> Result<()> {
        match self {
            AcceptorProposerMessage::Greeting(msg) => {
                buf.put_u64_le('g' as u64);
                buf.put_u64_le(msg.term);
            }
            AcceptorProposerMessage::VoteResponse(msg) => {
                buf.put_u64_le('v' as u64);
                buf.put_u64_le(msg.term);
                buf.put_u64_le(msg.vote_given);
                buf.put_u64_le(msg.flush_lsn.into());
                buf.put_u64_le(msg.truncate_lsn.into());
                buf.put_u32_le(msg.term_history.0.len() as u32);
                for e in &msg.term_history.0 {
                    buf.put_u64_le(e.term);
                    buf.put_u64_le(e.lsn.into());
                }
            }
            AcceptorProposerMessage::AppendResponse(msg) => {
                buf.put_u64_le('a' as u64);
                buf.put_u64_le(msg.term);
                buf.put_u64_le(msg.flush_lsn.into());
                buf.put_u64_le(msg.commit_lsn.into());
                buf.put_i64_le(msg.hs_feedback.ts);
                buf.put_u64_le(msg.hs_feedback.xmin);
                buf.put_u64_le(msg.hs_feedback.catalog_xmin);

                msg.zenith_feedback.serialize(buf)?
            }
        }

        Ok(())
    }
}

pub trait Storage {
    /// Persist safekeeper state on disk.
    fn persist(&mut self, s: &SafeKeeperState) -> Result<()>;
    /// Write piece of wal in buf to disk and sync it.
    fn write_wal(&mut self, server: &ServerInfo, startpos: Lsn, buf: &[u8]) -> Result<()>;
    // Truncate WAL at specified LSN
    fn truncate_wal(&mut self, s: &ServerInfo, endpos: Lsn) -> Result<()>;
}

lazy_static! {
    // The prometheus crate does not support u64 yet, i64 only (see `IntGauge`).
    // i64 is faster than f64, so update to u64 when available.
    static ref FLUSH_LSN_GAUGE: GaugeVec = register_gauge_vec!(
        "safekeeper_flush_lsn",
        "Current flush_lsn, grouped by timeline",
        &["tenant_id", "timeline_id"]
    )
    .expect("Failed to register safekeeper_flush_lsn gauge vec");
    static ref COMMIT_LSN_GAUGE: GaugeVec = register_gauge_vec!(
        "safekeeper_commit_lsn",
        "Current commit_lsn (not necessarily persisted to disk), grouped by timeline",
        &["tenant_id", "timeline_id"]
    )
    .expect("Failed to register safekeeper_commit_lsn gauge vec");
    static ref WRITE_WAL_BYTES: HistogramVec = register_histogram_vec!(
        "safekeeper_write_wal_bytes",
        "Bytes written to WAL in a single request, grouped by timeline",
        &["tenant_id", "timeline_id"],
        vec![1.0, 10.0, 100.0, 1024.0, 8192.0, 128.0 * 1024.0, 1024.0 * 1024.0, 10.0 * 1024.0 * 1024.0]
    )
    .expect("Failed to register safekeeper_write_wal_bytes histogram vec");
    static ref WRITE_WAL_SECONDS: HistogramVec = register_histogram_vec!(
        "safekeeper_write_wal_seconds",
        "Seconds spent writing and syncing WAL to a disk in a single request, grouped by timeline",
        &["tenant_id", "timeline_id"],
        DISK_WRITE_SECONDS_BUCKETS.to_vec()
    )
    .expect("Failed to register safekeeper_write_wal_seconds histogram vec");
}

struct SafeKeeperMetrics {
    flush_lsn: Gauge,
    commit_lsn: Gauge,
    write_wal_bytes: Histogram,
    write_wal_seconds: Histogram,
}

struct SafeKeeperMetricsBuilder {
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
    flush_lsn: Lsn,
    commit_lsn: Lsn,
}

impl SafeKeeperMetricsBuilder {
    fn build(self) -> SafeKeeperMetrics {
        let tenant_id = self.tenant_id.to_string();
        let timeline_id = self.timeline_id.to_string();
        let m = SafeKeeperMetrics {
            flush_lsn: FLUSH_LSN_GAUGE.with_label_values(&[&tenant_id, &timeline_id]),
            commit_lsn: COMMIT_LSN_GAUGE.with_label_values(&[&tenant_id, &timeline_id]),
            write_wal_bytes: WRITE_WAL_BYTES.with_label_values(&[&tenant_id, &timeline_id]),
            write_wal_seconds: WRITE_WAL_SECONDS.with_label_values(&[&tenant_id, &timeline_id]),
        };
        m.flush_lsn.set(u64::from(self.flush_lsn) as f64);
        m.commit_lsn.set(u64::from(self.commit_lsn) as f64);
        m
    }
}

/// SafeKeeper which consumes events (messages from compute) and provides
/// replies.
pub struct SafeKeeper<ST: Storage> {
    /// Locally flushed part of WAL with full records (end_lsn of last record).
    /// Established by reading wal.
    pub flush_lsn: Lsn,
    // Cached metrics so we don't have to recompute labels on each update.
    metrics: SafeKeeperMetrics,
    /// not-yet-flushed pairs of same named fields in s.*
    pub commit_lsn: Lsn,
    pub truncate_lsn: Lsn,
    pub storage: ST,
    pub s: SafeKeeperState, // persistent part
    decoder: WalStreamDecoder,
}

impl<ST> SafeKeeper<ST>
where
    ST: Storage,
{
    // constructor
    pub fn new(
        ztli: ZTimelineId,
        flush_lsn: Lsn,
        storage: ST,
        state: SafeKeeperState,
    ) -> SafeKeeper<ST> {
        if state.server.timeline_id != ZTimelineId::from([0u8; 16])
            && ztli != state.server.timeline_id
        {
            panic!("Calling SafeKeeper::new with inconsistent ztli ({}) and SafeKeeperState.server.timeline_id ({})", ztli, state.server.timeline_id);
        }
        SafeKeeper {
            flush_lsn,
            metrics: SafeKeeperMetricsBuilder {
                tenant_id: state.server.tenant_id,
                timeline_id: ztli,
                flush_lsn,
                commit_lsn: state.commit_lsn,
            }
            .build(),
            commit_lsn: state.commit_lsn,
            truncate_lsn: state.truncate_lsn,
            storage,
            s: state,
            decoder: WalStreamDecoder::new(Lsn(0)),
        }
    }

    /// Get history of term switches for the available WAL
    fn get_term_history(&self) -> TermHistory {
        self.s.acceptor_state.term_history.up_to(self.flush_lsn)
    }

    #[cfg(test)]
    fn get_epoch(&self) -> Term {
        self.s.acceptor_state.get_epoch(self.flush_lsn)
    }

    /// Process message from proposer and possibly form reply. Concurrent
    /// callers must exclude each other.
    pub fn process_msg(
        &mut self,
        msg: &ProposerAcceptorMessage,
    ) -> Result<Option<AcceptorProposerMessage>> {
        match msg {
            ProposerAcceptorMessage::Greeting(msg) => self.handle_greeting(msg),
            ProposerAcceptorMessage::VoteRequest(msg) => self.handle_vote_request(msg),
            ProposerAcceptorMessage::Elected(msg) => self.handle_elected(msg),
            ProposerAcceptorMessage::AppendRequest(msg) => self.handle_append_request(msg),
        }
    }

    /// Handle initial message from proposer: check its sanity and send my
    /// current term.
    fn handle_greeting(
        &mut self,
        msg: &ProposerGreeting,
    ) -> Result<Option<AcceptorProposerMessage>> {
        /* Check protocol compatibility */
        if msg.protocol_version != SK_PROTOCOL_VERSION {
            bail!(
                "incompatible protocol version {}, expected {}",
                msg.protocol_version,
                SK_PROTOCOL_VERSION
            );
        }
        /* Postgres upgrade is not treated as fatal error */
        if msg.pg_version != self.s.server.pg_version
            && self.s.server.pg_version != UNKNOWN_SERVER_VERSION
        {
            info!(
                "incompatible server version {}, expected {}",
                msg.pg_version, self.s.server.pg_version
            );
        }

        // set basic info about server, if not yet
        self.s.server.system_id = msg.system_id;
        self.s.server.tenant_id = msg.tenant_id;
        self.s.server.timeline_id = msg.ztli;
        self.s.server.wal_seg_size = msg.wal_seg_size;
        self.storage
            .persist(&self.s)
            .context("failed to persist shared state")?;

        self.metrics = SafeKeeperMetricsBuilder {
            tenant_id: self.s.server.tenant_id,
            timeline_id: self.s.server.timeline_id,
            flush_lsn: self.flush_lsn,
            commit_lsn: self.commit_lsn,
        }
        .build();

        info!(
            "processed greeting from proposer {:?}, sending term {:?}",
            msg.proposer_id, self.s.acceptor_state.term
        );
        Ok(Some(AcceptorProposerMessage::Greeting(AcceptorGreeting {
            term: self.s.acceptor_state.term,
        })))
    }

    /// Give vote for the given term, if we haven't done that previously.
    fn handle_vote_request(
        &mut self,
        msg: &VoteRequest,
    ) -> Result<Option<AcceptorProposerMessage>> {
        // initialize with refusal
        let mut resp = VoteResponse {
            term: self.s.acceptor_state.term,
            vote_given: false as u64,
            flush_lsn: self.flush_lsn,
            truncate_lsn: self.s.truncate_lsn,
            term_history: self.get_term_history(),
        };
        if self.s.acceptor_state.term < msg.term {
            self.s.acceptor_state.term = msg.term;
            // persist vote before sending it out
            self.storage.persist(&self.s)?;
            resp.term = self.s.acceptor_state.term;
            resp.vote_given = true as u64;
        }
        info!("processed VoteRequest for term {}: {:?}", msg.term, &resp);
        Ok(Some(AcceptorProposerMessage::VoteResponse(resp)))
    }

    /// Bump our term if received a note from elected proposer with higher one
    fn bump_if_higher(&mut self, term: Term) -> Result<()> {
        if self.s.acceptor_state.term < term {
            self.s.acceptor_state.term = term;
            self.storage.persist(&self.s)?;
        }
        Ok(())
    }

    /// Form AppendResponse from current state.
    fn append_response(&self) -> AppendResponse {
        AppendResponse {
            term: self.s.acceptor_state.term,
            flush_lsn: self.flush_lsn,
            commit_lsn: self.s.commit_lsn,
            // will be filled by the upper code to avoid bothering safekeeper
            hs_feedback: HotStandbyFeedback::empty(),
            zenith_feedback: ZenithFeedback::empty(),
        }
    }

    fn handle_elected(&mut self, msg: &ProposerElected) -> Result<Option<AcceptorProposerMessage>> {
        info!("received ProposerElected {:?}", msg);
        self.bump_if_higher(msg.term)?;
        // If our term is higher, ignore the message (next feedback will inform the compute)
        if self.s.acceptor_state.term > msg.term {
            return Ok(None);
        }

        // TODO: cross check divergence point

        // streaming must not create a hole
        assert!(self.flush_lsn == Lsn(0) || self.flush_lsn >= msg.start_streaming_at);

        // truncate obsolete part of WAL
        if self.flush_lsn != Lsn(0) {
            self.storage
                .truncate_wal(&self.s.server, msg.start_streaming_at)?;
        }
        // update our end of WAL pointer
        self.flush_lsn = msg.start_streaming_at;
        self.metrics.flush_lsn.set(u64::from(self.flush_lsn) as f64);
        // and now adopt term history from proposer
        self.s.acceptor_state.term_history = msg.term_history.clone();
        self.storage.persist(&self.s)?;

        info!("start receiving WAL since {:?}", msg.start_streaming_at);

        Ok(None)
    }

    /// Handle request to append WAL.
    #[allow(clippy::comparison_chain)]
    fn handle_append_request(
        &mut self,
        msg: &AppendRequest,
    ) -> Result<Option<AcceptorProposerMessage>> {
        if self.s.acceptor_state.term < msg.h.term {
            bail!("got AppendRequest before ProposerElected");
        }

        // If our term is higher, immediately refuse the message.
        if self.s.acceptor_state.term > msg.h.term {
            let resp = AppendResponse::term_only(self.s.acceptor_state.term);
            return Ok(Some(AcceptorProposerMessage::AppendResponse(resp)));
        }

        // After ProposerElected, which performs truncation, we should get only
        // indeed append requests (but flush_lsn is advanced only on record
        // boundary, so might be less).
        assert!(self.flush_lsn <= msg.h.begin_lsn);

        self.s.proposer_uuid = msg.h.proposer_uuid;
        let mut sync_control_file = false;

        // do the job
        let mut last_rec_lsn = Lsn(0);
        if !msg.wal_data.is_empty() {
            self.metrics
                .write_wal_bytes
                .observe(msg.wal_data.len() as f64);
            {
                let _timer = self.metrics.write_wal_seconds.start_timer();
                self.storage
                    .write_wal(&self.s.server, msg.h.begin_lsn, &msg.wal_data)?;
            }

            // figure out last record's end lsn for reporting (if we got the
            // whole record)
            if self.decoder.available() != msg.h.begin_lsn {
                info!(
                    "restart decoder from {} to {}",
                    self.decoder.available(),
                    msg.h.begin_lsn,
                );
                self.decoder = WalStreamDecoder::new(msg.h.begin_lsn);
            }
            self.decoder.feed_bytes(&msg.wal_data);
            loop {
                match self.decoder.poll_decode()? {
                    None => break, // no full record yet
                    Some((lsn, _rec)) => {
                        last_rec_lsn = lsn;
                    }
                }
            }

            // If this was the first record we ever receieved, remember LSN to help
            // find_end_of_wal skip the hole in the beginning.
            if self.s.wal_start_lsn == Lsn(0) {
                self.s.wal_start_lsn = msg.h.begin_lsn;
                sync_control_file = true;
            }
        }

        if last_rec_lsn > self.flush_lsn {
            self.flush_lsn = last_rec_lsn;
            self.metrics.flush_lsn.set(u64::from(self.flush_lsn) as f64);
        }

        // Advance commit_lsn taking into account what we have locally.
        // commit_lsn can be 0, being unknown to new walproposer while he hasn't
        // collected majority of its epoch acks yet, ignore it in this case.
        if msg.h.commit_lsn != Lsn(0) {
            let commit_lsn = min(msg.h.commit_lsn, self.flush_lsn);
            // If new commit_lsn reached epoch switch, force sync of control
            // file: walproposer in sync mode is very interested when this
            // happens. Note: this is for sync-safekeepers mode only, as
            // otherwise commit_lsn might jump over epoch_start_lsn.
            sync_control_file |= commit_lsn == msg.h.epoch_start_lsn;
            self.commit_lsn = commit_lsn;
            self.metrics
                .commit_lsn
                .set(u64::from(self.commit_lsn) as f64);
        }

        self.truncate_lsn = msg.h.truncate_lsn;
        /*
         * Update truncate and commit LSN in control file.
         * To avoid negative impact on performance of extra fsync, do it only
         * when truncate_lsn delta exceeds WAL segment size.
         */
        sync_control_file |=
            self.s.truncate_lsn + (self.s.server.wal_seg_size as u64) < self.truncate_lsn;
        if sync_control_file {
            self.s.commit_lsn = self.commit_lsn;
            self.s.truncate_lsn = self.truncate_lsn;
        }

        if sync_control_file {
            self.storage.persist(&self.s)?;
        }

        let resp = self.append_response();
        trace!(
            "processed AppendRequest of len {}, end_lsn={:?}, commit_lsn={:?}, truncate_lsn={:?}, resp {:?}",
            msg.wal_data.len(),
            msg.h.end_lsn,
            msg.h.commit_lsn,
            msg.h.truncate_lsn,
            &resp,
        );
        Ok(Some(AcceptorProposerMessage::AppendResponse(resp)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // fake storage for tests
    struct InMemoryStorage {
        persisted_state: SafeKeeperState,
    }

    impl Storage for InMemoryStorage {
        fn persist(&mut self, s: &SafeKeeperState) -> Result<()> {
            self.persisted_state = s.clone();
            Ok(())
        }

        fn write_wal(&mut self, _server: &ServerInfo, _startpos: Lsn, _buf: &[u8]) -> Result<()> {
            Ok(())
        }

        fn truncate_wal(&mut self, _server: &ServerInfo, _end_pos: Lsn) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_voting() {
        let storage = InMemoryStorage {
            persisted_state: SafeKeeperState::new(),
        };
        let ztli = ZTimelineId::from([0u8; 16]);
        let mut sk = SafeKeeper::new(ztli, Lsn(0), storage, SafeKeeperState::new());

        // check voting for 1 is ok
        let vote_request = ProposerAcceptorMessage::VoteRequest(VoteRequest { term: 1 });
        let mut vote_resp = sk.process_msg(&vote_request);
        match vote_resp.unwrap() {
            Some(AcceptorProposerMessage::VoteResponse(resp)) => assert!(resp.vote_given != 0),
            r => panic!("unexpected response: {:?}", r),
        }

        // reboot...
        let state = sk.storage.persisted_state.clone();
        let storage = InMemoryStorage {
            persisted_state: state.clone(),
        };
        sk = SafeKeeper::new(ztli, Lsn(0), storage, state);

        // and ensure voting second time for 1 is not ok
        vote_resp = sk.process_msg(&vote_request);
        match vote_resp.unwrap() {
            Some(AcceptorProposerMessage::VoteResponse(resp)) => assert!(resp.vote_given == 0),
            r => panic!("unexpected response: {:?}", r),
        }
    }

    #[test]
    fn test_epoch_switch() {
        let storage = InMemoryStorage {
            persisted_state: SafeKeeperState::new(),
        };
        let ztli = ZTimelineId::from([0u8; 16]);
        let mut sk = SafeKeeper::new(ztli, Lsn(0), storage, SafeKeeperState::new());

        let mut ar_hdr = AppendRequestHeader {
            term: 1,
            epoch_start_lsn: Lsn(3),
            begin_lsn: Lsn(1),
            end_lsn: Lsn(2),
            commit_lsn: Lsn(0),
            truncate_lsn: Lsn(0),
            proposer_uuid: [0; 16],
        };
        let mut append_request = AppendRequest {
            h: ar_hdr.clone(),
            wal_data: Bytes::from_static(b"b"),
        };

        let pem = ProposerElected {
            term: 1,
            start_streaming_at: Lsn(1),
            term_history: TermHistory(vec![TermSwitchEntry {
                term: 1,
                lsn: Lsn(3),
            }]),
        };
        sk.process_msg(&ProposerAcceptorMessage::Elected(pem))
            .unwrap();

        // check that AppendRequest before epochStartLsn doesn't switch epoch
        let resp = sk.process_msg(&ProposerAcceptorMessage::AppendRequest(append_request));
        assert!(resp.is_ok());
        assert_eq!(sk.get_epoch(), 0);

        // but record at epochStartLsn does the switch
        ar_hdr.begin_lsn = Lsn(2);
        ar_hdr.end_lsn = Lsn(3);
        append_request = AppendRequest {
            h: ar_hdr,
            wal_data: Bytes::from_static(b"b"),
        };
        let resp = sk.process_msg(&ProposerAcceptorMessage::AppendRequest(append_request));
        assert!(resp.is_ok());
        sk.flush_lsn = Lsn(3); // imitate the complete record at 3 %)
        assert_eq!(sk.get_epoch(), 1);
    }
}
