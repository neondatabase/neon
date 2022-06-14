//! Acceptor part of proposer-acceptor consensus algorithm.

use anyhow::{bail, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use etcd_broker::subscription_value::SkTimelineInfo;
use postgres_ffi::xlog_utils::TimeLineID;

use postgres_ffi::xlog_utils::XLogSegNo;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::cmp::min;
use std::fmt;
use std::io::Read;
use tracing::*;

use crate::control_file;
use crate::send_wal::HotStandbyFeedback;

use crate::wal_storage;
use postgres_ffi::xlog_utils::MAX_SEND_SIZE;
use utils::{
    bin_ser::LeSer,
    lsn::Lsn,
    pq_proto::{ReplicationFeedback, SystemId},
    zid::{NodeId, ZTenantId, ZTenantTimelineId, ZTimelineId},
};

pub const SK_MAGIC: u32 = 0xcafeceefu32;
pub const SK_FORMAT_VERSION: u32 = 5;
const SK_PROTOCOL_VERSION: u32 = 2;
const UNKNOWN_SERVER_VERSION: u32 = 0;

/// Consensus logical timestamp.
pub type Term = u64;
const INVALID_TERM: Term = 0;

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
    pub fn from_bytes(bytes: &mut Bytes) -> Result<TermHistory> {
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
    pub wal_seg_size: u32,
}

/// Data published by safekeeper to the peers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    /// LSN up to which safekeeper offloaded WAL to s3.
    backup_lsn: Lsn,
    /// Term of the last entry.
    term: Term,
    /// LSN of the last record.
    flush_lsn: Lsn,
    /// Up to which LSN safekeeper regards its WAL as committed.
    commit_lsn: Lsn,
}

impl PeerInfo {
    fn new() -> Self {
        Self {
            backup_lsn: Lsn::INVALID,
            term: INVALID_TERM,
            flush_lsn: Lsn(0),
            commit_lsn: Lsn(0),
        }
    }
}

// vector-based node id -> peer state map with very limited functionality we
// need/
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Peers(pub Vec<(NodeId, PeerInfo)>);

/// Persistent information stored on safekeeper node
/// On disk data is prefixed by magic and format version and followed by checksum.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafeKeeperState {
    #[serde(with = "hex")]
    pub tenant_id: ZTenantId,
    /// Zenith timelineid
    #[serde(with = "hex")]
    pub timeline_id: ZTimelineId,
    /// persistent acceptor state
    pub acceptor_state: AcceptorState,
    /// information about server
    pub server: ServerInfo,
    /// Unique id of the last *elected* proposer we dealt with. Not needed
    /// for correctness, exists for monitoring purposes.
    #[serde(with = "hex")]
    pub proposer_uuid: PgUuid,
    /// Since which LSN this timeline generally starts. Safekeeper might have
    /// joined later.
    pub timeline_start_lsn: Lsn,
    /// Since which LSN safekeeper has (had) WAL for this timeline.
    /// All WAL segments next to one containing local_start_lsn are
    /// filled with data from the beginning.
    pub local_start_lsn: Lsn,
    /// Part of WAL acknowledged by quorum and available locally. Always points
    /// to record boundary.
    pub commit_lsn: Lsn,
    /// LSN that points to the end of the last backed up segment. Useful to
    /// persist to avoid finding out offloading progress on boot.
    pub backup_lsn: Lsn,
    /// Minimal LSN which may be needed for recovery of some safekeeper (end_lsn
    /// of last record streamed to everyone). Persisting it helps skipping
    /// recovery in walproposer, generally we compute it from peers. In
    /// walproposer proto called 'truncate_lsn'.
    pub peer_horizon_lsn: Lsn,
    /// LSN of the oldest known checkpoint made by pageserver and successfully
    /// pushed to s3. We don't remove WAL beyond it. Persisted only for
    /// informational purposes, we receive it from pageserver (or broker).
    pub remote_consistent_lsn: Lsn,
    // Peers and their state as we remember it. Knowing peers themselves is
    // fundamental; but state is saved here only for informational purposes and
    // obviously can be stale. (Currently not saved at all, but let's provision
    // place to have less file version upgrades).
    pub peers: Peers,
}

#[derive(Debug, Clone)]
// In memory safekeeper state. Fields mirror ones in `SafeKeeperState`; values
// are not flushed yet.
pub struct SafekeeperMemState {
    pub commit_lsn: Lsn,
    pub backup_lsn: Lsn,
    pub peer_horizon_lsn: Lsn,
    pub remote_consistent_lsn: Lsn,
    pub proposer_uuid: PgUuid,
}

impl SafeKeeperState {
    pub fn new(zttid: &ZTenantTimelineId, peers: Vec<NodeId>) -> SafeKeeperState {
        SafeKeeperState {
            tenant_id: zttid.tenant_id,
            timeline_id: zttid.timeline_id,
            acceptor_state: AcceptorState {
                term: 0,
                term_history: TermHistory::empty(),
            },
            server: ServerInfo {
                pg_version: UNKNOWN_SERVER_VERSION, /* Postgres server version */
                system_id: 0,                       /* Postgres system identifier */
                wal_seg_size: 0,
            },
            proposer_uuid: [0; 16],
            timeline_start_lsn: Lsn(0),
            local_start_lsn: Lsn(0),
            commit_lsn: Lsn(0),
            backup_lsn: Lsn::INVALID,
            peer_horizon_lsn: Lsn(0),
            remote_consistent_lsn: Lsn(0),
            peers: Peers(peers.iter().map(|p| (*p, PeerInfo::new())).collect()),
        }
    }

    #[cfg(test)]
    pub fn empty() -> Self {
        SafeKeeperState::new(&ZTenantTimelineId::empty(), vec![])
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
    node_id: NodeId,
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
    timeline_start_lsn: Lsn,
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
    pub timeline_start_lsn: Lsn,
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
    pub pageserver_feedback: ReplicationFeedback,
}

impl AppendResponse {
    fn term_only(term: Term) -> AppendResponse {
        AppendResponse {
            term,
            flush_lsn: Lsn(0),
            commit_lsn: Lsn(0),
            hs_feedback: HotStandbyFeedback::empty(),
            pageserver_feedback: ReplicationFeedback::empty(),
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
    NoFlushAppendRequest(AppendRequest),
    FlushWAL,
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
                let term_history = TermHistory::from_bytes(&mut msg_bytes)?;
                if msg_bytes.remaining() < 8 {
                    bail!("ProposerElected message is not complete");
                }
                let timeline_start_lsn = msg_bytes.get_u64_le().into();
                let msg = ProposerElected {
                    term,
                    start_streaming_at,
                    timeline_start_lsn,
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
                buf.put_u64_le(msg.node_id.0);
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
                buf.put_u64_le(msg.timeline_start_lsn.into());
            }
            AcceptorProposerMessage::AppendResponse(msg) => {
                buf.put_u64_le('a' as u64);
                buf.put_u64_le(msg.term);
                buf.put_u64_le(msg.flush_lsn.into());
                buf.put_u64_le(msg.commit_lsn.into());
                buf.put_i64_le(msg.hs_feedback.ts);
                buf.put_u64_le(msg.hs_feedback.xmin);
                buf.put_u64_le(msg.hs_feedback.catalog_xmin);

                msg.pageserver_feedback.serialize(buf)?
            }
        }

        Ok(())
    }
}

/// SafeKeeper which consumes events (messages from compute) and provides
/// replies.
pub struct SafeKeeper<CTRL: control_file::Storage, WAL: wal_storage::Storage> {
    /// Maximum commit_lsn between all nodes, can be ahead of local flush_lsn.
    /// Note: be careful to set only if we are sure our WAL (term history) matches
    /// committed one.
    pub global_commit_lsn: Lsn,
    /// LSN since the proposer safekeeper currently talking to appends WAL;
    /// determines epoch switch point.
    pub epoch_start_lsn: Lsn,

    pub inmem: SafekeeperMemState, // in memory part
    pub state: CTRL,               // persistent state storage

    pub wal_store: WAL,

    node_id: NodeId, // safekeeper's node id
}

impl<CTRL, WAL> SafeKeeper<CTRL, WAL>
where
    CTRL: control_file::Storage,
    WAL: wal_storage::Storage,
{
    // constructor
    pub fn new(
        ztli: ZTimelineId,
        state: CTRL,
        mut wal_store: WAL,
        node_id: NodeId,
    ) -> Result<SafeKeeper<CTRL, WAL>> {
        if state.timeline_id != ZTimelineId::from([0u8; 16]) && ztli != state.timeline_id {
            bail!("Calling SafeKeeper::new with inconsistent ztli ({}) and SafeKeeperState.server.timeline_id ({})", ztli, state.timeline_id);
        }

        // initialize wal_store, if state is already initialized
        wal_store.init_storage(&state)?;

        Ok(SafeKeeper {
            global_commit_lsn: state.commit_lsn,
            epoch_start_lsn: Lsn(0),
            inmem: SafekeeperMemState {
                commit_lsn: state.commit_lsn,
                backup_lsn: state.backup_lsn,
                peer_horizon_lsn: state.peer_horizon_lsn,
                remote_consistent_lsn: state.remote_consistent_lsn,
                proposer_uuid: state.proposer_uuid,
            },
            state,
            wal_store,
            node_id,
        })
    }

    /// Get history of term switches for the available WAL
    fn get_term_history(&self) -> TermHistory {
        self.state
            .acceptor_state
            .term_history
            .up_to(self.flush_lsn())
    }

    pub fn get_epoch(&self) -> Term {
        self.state.acceptor_state.get_epoch(self.flush_lsn())
    }

    /// wal_store wrapper avoiding commit_lsn <= flush_lsn violation when we don't have WAL yet.
    fn flush_lsn(&self) -> Lsn {
        max(self.wal_store.flush_lsn(), self.state.timeline_start_lsn)
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
            ProposerAcceptorMessage::AppendRequest(msg) => self.handle_append_request(msg, true),
            ProposerAcceptorMessage::NoFlushAppendRequest(msg) => {
                self.handle_append_request(msg, false)
            }
            ProposerAcceptorMessage::FlushWAL => self.handle_flush(),
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
        if msg.pg_version != self.state.server.pg_version
            && self.state.server.pg_version != UNKNOWN_SERVER_VERSION
        {
            info!(
                "incompatible server version {}, expected {}",
                msg.pg_version, self.state.server.pg_version
            );
        }
        if msg.tenant_id != self.state.tenant_id {
            bail!(
                "invalid tenant ID, got {}, expected {}",
                msg.tenant_id,
                self.state.tenant_id
            );
        }
        if msg.ztli != self.state.timeline_id {
            bail!(
                "invalid timeline ID, got {}, expected {}",
                msg.ztli,
                self.state.timeline_id
            );
        }

        // set basic info about server, if not yet
        // TODO: verify that is doesn't change after
        {
            let mut state = self.state.clone();
            state.server.system_id = msg.system_id;
            state.server.wal_seg_size = msg.wal_seg_size;
            self.state.persist(&state)?;
        }

        self.wal_store.init_storage(&self.state)?;

        info!(
            "processed greeting from proposer {:?}, sending term {:?}",
            msg.proposer_id, self.state.acceptor_state.term
        );
        Ok(Some(AcceptorProposerMessage::Greeting(AcceptorGreeting {
            term: self.state.acceptor_state.term,
            node_id: self.node_id,
        })))
    }

    /// Give vote for the given term, if we haven't done that previously.
    fn handle_vote_request(
        &mut self,
        msg: &VoteRequest,
    ) -> Result<Option<AcceptorProposerMessage>> {
        // initialize with refusal
        let mut resp = VoteResponse {
            term: self.state.acceptor_state.term,
            vote_given: false as u64,
            flush_lsn: self.flush_lsn(),
            truncate_lsn: self.state.peer_horizon_lsn,
            term_history: self.get_term_history(),
            timeline_start_lsn: self.state.timeline_start_lsn,
        };
        if self.state.acceptor_state.term < msg.term {
            let mut state = self.state.clone();
            state.acceptor_state.term = msg.term;
            // persist vote before sending it out
            self.state.persist(&state)?;

            resp.term = self.state.acceptor_state.term;
            resp.vote_given = true as u64;
        }
        info!("processed VoteRequest for term {}: {:?}", msg.term, &resp);
        Ok(Some(AcceptorProposerMessage::VoteResponse(resp)))
    }

    /// Bump our term if received a note from elected proposer with higher one
    fn bump_if_higher(&mut self, term: Term) -> Result<()> {
        if self.state.acceptor_state.term < term {
            let mut state = self.state.clone();
            state.acceptor_state.term = term;
            self.state.persist(&state)?;
        }
        Ok(())
    }

    /// Form AppendResponse from current state.
    fn append_response(&self) -> AppendResponse {
        let ar = AppendResponse {
            term: self.state.acceptor_state.term,
            flush_lsn: self.flush_lsn(),
            commit_lsn: self.state.commit_lsn,
            // will be filled by the upper code to avoid bothering safekeeper
            hs_feedback: HotStandbyFeedback::empty(),
            pageserver_feedback: ReplicationFeedback::empty(),
        };
        trace!("formed AppendResponse {:?}", ar);
        ar
    }

    fn handle_elected(&mut self, msg: &ProposerElected) -> Result<Option<AcceptorProposerMessage>> {
        info!("received ProposerElected {:?}", msg);
        self.bump_if_higher(msg.term)?;
        // If our term is higher, ignore the message (next feedback will inform the compute)
        if self.state.acceptor_state.term > msg.term {
            return Ok(None);
        }

        // TODO: cross check divergence point, check if msg.start_streaming_at corresponds to
        // intersection of our history and history from msg

        // truncate wal, update the LSNs
        self.wal_store.truncate_wal(msg.start_streaming_at)?;

        // and now adopt term history from proposer
        {
            let mut state = self.state.clone();

            // Here we learn initial LSN for the first time, set fields
            // interested in that.

            if state.timeline_start_lsn == Lsn(0) {
                // Remember point where WAL begins globally.
                state.timeline_start_lsn = msg.timeline_start_lsn;
                info!(
                    "setting timeline_start_lsn to {:?}",
                    state.timeline_start_lsn
                );

                state.local_start_lsn = msg.start_streaming_at;
                info!("setting local_start_lsn to {:?}", state.local_start_lsn);
            }
            // Initializing commit_lsn before acking first flushed record is
            // important to let find_end_of_wal skip the whole in the beginning
            // of the first segment.
            //
            // NB: on new clusters, this happens at the same time as
            // timeline_start_lsn initialization, it is taken outside to provide
            // upgrade.
            self.global_commit_lsn = max(self.global_commit_lsn, state.timeline_start_lsn);
            self.inmem.commit_lsn = max(self.inmem.commit_lsn, state.timeline_start_lsn);

            // Initializing backup_lsn is useful to avoid making backup think it should upload 0 segment.
            self.inmem.backup_lsn = max(self.inmem.backup_lsn, state.timeline_start_lsn);

            state.acceptor_state.term_history = msg.term_history.clone();
            self.persist_control_file(state)?;
        }

        info!("start receiving WAL since {:?}", msg.start_streaming_at);

        Ok(None)
    }

    /// Advance commit_lsn taking into account what we have locally
    pub fn update_commit_lsn(&mut self) -> Result<()> {
        let commit_lsn = min(self.global_commit_lsn, self.flush_lsn());
        assert!(commit_lsn >= self.inmem.commit_lsn);

        self.inmem.commit_lsn = commit_lsn;

        // If new commit_lsn reached epoch switch, force sync of control
        // file: walproposer in sync mode is very interested when this
        // happens. Note: this is for sync-safekeepers mode only, as
        // otherwise commit_lsn might jump over epoch_start_lsn.
        // Also note that commit_lsn can reach epoch_start_lsn earlier
        // that we receive new epoch_start_lsn, and we still need to sync
        // control file in this case.
        if commit_lsn == self.epoch_start_lsn && self.state.commit_lsn != commit_lsn {
            self.persist_control_file(self.state.clone())?;
        }

        Ok(())
    }

    /// Persist in-memory state to the disk, taking other data from state.
    fn persist_control_file(&mut self, mut state: SafeKeeperState) -> Result<()> {
        state.commit_lsn = self.inmem.commit_lsn;
        state.backup_lsn = self.inmem.backup_lsn;
        state.peer_horizon_lsn = self.inmem.peer_horizon_lsn;
        state.remote_consistent_lsn = self.inmem.remote_consistent_lsn;
        state.proposer_uuid = self.inmem.proposer_uuid;
        self.state.persist(&state)
    }

    /// Handle request to append WAL.
    #[allow(clippy::comparison_chain)]
    fn handle_append_request(
        &mut self,
        msg: &AppendRequest,
        require_flush: bool,
    ) -> Result<Option<AcceptorProposerMessage>> {
        if self.state.acceptor_state.term < msg.h.term {
            bail!("got AppendRequest before ProposerElected");
        }

        // If our term is higher, immediately refuse the message.
        if self.state.acceptor_state.term > msg.h.term {
            let resp = AppendResponse::term_only(self.state.acceptor_state.term);
            return Ok(Some(AcceptorProposerMessage::AppendResponse(resp)));
        }

        // Now we know that we are in the same term as the proposer,
        // processing the message.

        self.epoch_start_lsn = msg.h.epoch_start_lsn;
        self.inmem.proposer_uuid = msg.h.proposer_uuid;

        // do the job
        if !msg.wal_data.is_empty() {
            self.wal_store.write_wal(msg.h.begin_lsn, &msg.wal_data)?;
        }

        // flush wal to the disk, if required
        if require_flush {
            self.wal_store.flush_wal()?;
        }

        // Update global_commit_lsn
        if msg.h.commit_lsn != Lsn(0) {
            // We also obtain commit lsn from peers, so value arrived here might be stale (less)
            self.global_commit_lsn = max(self.global_commit_lsn, msg.h.commit_lsn);
        }

        self.inmem.peer_horizon_lsn = msg.h.truncate_lsn;
        self.update_commit_lsn()?;

        // Update truncate and commit LSN in control file.
        // To avoid negative impact on performance of extra fsync, do it only
        // when truncate_lsn delta exceeds WAL segment size.
        if self.state.peer_horizon_lsn + (self.state.server.wal_seg_size as u64)
            < self.inmem.peer_horizon_lsn
        {
            self.persist_control_file(self.state.clone())?;
        }

        trace!(
            "processed AppendRequest of len {}, end_lsn={:?}, commit_lsn={:?}, truncate_lsn={:?}, flushed={:?}",
            msg.wal_data.len(),
            msg.h.end_lsn,
            msg.h.commit_lsn,
            msg.h.truncate_lsn,
            require_flush,
        );

        // If flush_lsn hasn't updated, AppendResponse is not very useful.
        if !require_flush {
            return Ok(None);
        }

        let resp = self.append_response();
        Ok(Some(AcceptorProposerMessage::AppendResponse(resp)))
    }

    /// Flush WAL to disk. Return AppendResponse with latest LSNs.
    fn handle_flush(&mut self) -> Result<Option<AcceptorProposerMessage>> {
        self.wal_store.flush_wal()?;

        // commit_lsn can be updated because we have new flushed data locally.
        self.update_commit_lsn()?;

        Ok(Some(AcceptorProposerMessage::AppendResponse(
            self.append_response(),
        )))
    }

    /// Update timeline state with peer safekeeper data.
    pub fn record_safekeeper_info(&mut self, sk_info: &SkTimelineInfo) -> Result<()> {
        let mut sync_control_file = false;
        if let (Some(commit_lsn), Some(last_log_term)) = (sk_info.commit_lsn, sk_info.last_log_term)
        {
            // Note: the check is too restrictive, generally we can update local
            // commit_lsn if our history matches (is part of) history of advanced
            // commit_lsn provider.
            if last_log_term == self.get_epoch() {
                self.global_commit_lsn = max(commit_lsn, self.global_commit_lsn);
                self.update_commit_lsn()?;
            }
        }
        if let Some(backup_lsn) = sk_info.backup_lsn {
            let new_backup_lsn = max(backup_lsn, self.inmem.backup_lsn);
            sync_control_file |=
                self.state.backup_lsn + (self.state.server.wal_seg_size as u64) < new_backup_lsn;
            self.inmem.backup_lsn = new_backup_lsn;
        }
        if let Some(remote_consistent_lsn) = sk_info.remote_consistent_lsn {
            let new_remote_consistent_lsn =
                max(remote_consistent_lsn, self.inmem.remote_consistent_lsn);
            sync_control_file |= self.state.remote_consistent_lsn
                + (self.state.server.wal_seg_size as u64)
                < new_remote_consistent_lsn;
            self.inmem.remote_consistent_lsn = new_remote_consistent_lsn;
        }
        if let Some(peer_horizon_lsn) = sk_info.peer_horizon_lsn {
            let new_peer_horizon_lsn = max(peer_horizon_lsn, self.inmem.peer_horizon_lsn);
            sync_control_file |= self.state.peer_horizon_lsn
                + (self.state.server.wal_seg_size as u64)
                < new_peer_horizon_lsn;
            self.inmem.peer_horizon_lsn = new_peer_horizon_lsn;
        }
        if sync_control_file {
            self.persist_control_file(self.state.clone())?;
        }
        Ok(())
    }

    /// Get oldest segno we still need to keep. We hold WAL till it is consumed
    /// by all of 1) pageserver (remote_consistent_lsn) 2) peers 3) s3
    /// offloading.
    /// While it is safe to use inmem values for determining horizon,
    /// we use persistent to make possible normal states less surprising.
    pub fn get_horizon_segno(&self, wal_backup_enabled: bool) -> XLogSegNo {
        let mut horizon_lsn = min(
            self.state.remote_consistent_lsn,
            self.state.peer_horizon_lsn,
        );
        if wal_backup_enabled {
            horizon_lsn = min(horizon_lsn, self.state.backup_lsn);
        }
        horizon_lsn.segment_number(self.state.server.wal_seg_size as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal_storage::Storage;
    use std::ops::Deref;

    // fake storage for tests
    struct InMemoryState {
        persisted_state: SafeKeeperState,
    }

    impl control_file::Storage for InMemoryState {
        fn persist(&mut self, s: &SafeKeeperState) -> Result<()> {
            self.persisted_state = s.clone();
            Ok(())
        }
    }

    impl Deref for InMemoryState {
        type Target = SafeKeeperState;

        fn deref(&self) -> &Self::Target {
            &self.persisted_state
        }
    }

    struct DummyWalStore {
        lsn: Lsn,
    }

    impl wal_storage::Storage for DummyWalStore {
        fn flush_lsn(&self) -> Lsn {
            self.lsn
        }

        fn init_storage(&mut self, _state: &SafeKeeperState) -> Result<()> {
            Ok(())
        }

        fn write_wal(&mut self, startpos: Lsn, buf: &[u8]) -> Result<()> {
            self.lsn = startpos + buf.len() as u64;
            Ok(())
        }

        fn truncate_wal(&mut self, end_pos: Lsn) -> Result<()> {
            self.lsn = end_pos;
            Ok(())
        }

        fn flush_wal(&mut self) -> Result<()> {
            Ok(())
        }

        fn remove_up_to(&self) -> Box<dyn Fn(XLogSegNo) -> Result<()>> {
            Box::new(move |_segno_up_to: XLogSegNo| Ok(()))
        }
    }

    #[test]
    fn test_voting() {
        let storage = InMemoryState {
            persisted_state: SafeKeeperState::empty(),
        };
        let wal_store = DummyWalStore { lsn: Lsn(0) };
        let ztli = ZTimelineId::from([0u8; 16]);

        let mut sk = SafeKeeper::new(ztli, storage, wal_store, NodeId(0)).unwrap();

        // check voting for 1 is ok
        let vote_request = ProposerAcceptorMessage::VoteRequest(VoteRequest { term: 1 });
        let mut vote_resp = sk.process_msg(&vote_request);
        match vote_resp.unwrap() {
            Some(AcceptorProposerMessage::VoteResponse(resp)) => assert!(resp.vote_given != 0),
            r => panic!("unexpected response: {:?}", r),
        }

        // reboot...
        let state = sk.state.persisted_state.clone();
        let storage = InMemoryState {
            persisted_state: state,
        };

        sk = SafeKeeper::new(ztli, storage, sk.wal_store, NodeId(0)).unwrap();

        // and ensure voting second time for 1 is not ok
        vote_resp = sk.process_msg(&vote_request);
        match vote_resp.unwrap() {
            Some(AcceptorProposerMessage::VoteResponse(resp)) => assert!(resp.vote_given == 0),
            r => panic!("unexpected response: {:?}", r),
        }
    }

    #[test]
    fn test_epoch_switch() {
        let storage = InMemoryState {
            persisted_state: SafeKeeperState::empty(),
        };
        let wal_store = DummyWalStore { lsn: Lsn(0) };
        let ztli = ZTimelineId::from([0u8; 16]);

        let mut sk = SafeKeeper::new(ztli, storage, wal_store, NodeId(0)).unwrap();

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
            timeline_start_lsn: Lsn(0),
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
        sk.wal_store.truncate_wal(Lsn(3)).unwrap(); // imitate the complete record at 3 %)
        assert_eq!(sk.get_epoch(), 1);
    }
}
