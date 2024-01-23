//! Defines per timeline data stored persistently (SafeKeeperPersistentState)
//! and its wrapper with in memory layer (SafekeeperState).

use std::ops::Deref;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use utils::{
    id::{NodeId, TenantId, TenantTimelineId, TimelineId},
    lsn::Lsn,
};

use crate::{
    control_file,
    safekeeper::{AcceptorState, PersistedPeerInfo, PgUuid, ServerInfo, TermHistory},
};

/// Persistent information stored on safekeeper node about timeline.
/// On disk data is prefixed by magic and format version and followed by checksum.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TimelinePersistentState {
    #[serde(with = "hex")]
    pub tenant_id: TenantId,
    #[serde(with = "hex")]
    pub timeline_id: TimelineId,
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
    /// Part of WAL acknowledged by quorum *and available locally*. Always points
    /// to record boundary.
    pub commit_lsn: Lsn,
    /// LSN that points to the end of the last backed up segment. Useful to
    /// persist to avoid finding out offloading progress on boot.
    pub backup_lsn: Lsn,
    /// Minimal LSN which may be needed for recovery of some safekeeper (end_lsn
    /// of last record streamed to everyone). Persisting it helps skipping
    /// recovery in walproposer, generally we compute it from peers. In
    /// walproposer proto called 'truncate_lsn'. Updates are currently drived
    /// only by walproposer.
    pub peer_horizon_lsn: Lsn,
    /// LSN of the oldest known checkpoint made by pageserver and successfully
    /// pushed to s3. We don't remove WAL beyond it. Persisted only for
    /// informational purposes, we receive it from pageserver (or broker).
    pub remote_consistent_lsn: Lsn,
    // Peers and their state as we remember it. Knowing peers themselves is
    // fundamental; but state is saved here only for informational purposes and
    // obviously can be stale. (Currently not saved at all, but let's provision
    // place to have less file version upgrades).
    pub peers: PersistedPeers,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedPeers(pub Vec<(NodeId, PersistedPeerInfo)>);

impl TimelinePersistentState {
    pub fn new(
        ttid: &TenantTimelineId,
        server_info: ServerInfo,
        peers: Vec<NodeId>,
        commit_lsn: Lsn,
        local_start_lsn: Lsn,
    ) -> TimelinePersistentState {
        TimelinePersistentState {
            tenant_id: ttid.tenant_id,
            timeline_id: ttid.timeline_id,
            acceptor_state: AcceptorState {
                term: 0,
                term_history: TermHistory::empty(),
            },
            server: server_info,
            proposer_uuid: [0; 16],
            timeline_start_lsn: Lsn(0),
            local_start_lsn,
            commit_lsn,
            backup_lsn: local_start_lsn,
            peer_horizon_lsn: local_start_lsn,
            remote_consistent_lsn: Lsn(0),
            peers: PersistedPeers(
                peers
                    .iter()
                    .map(|p| (*p, PersistedPeerInfo::new()))
                    .collect(),
            ),
        }
    }

    #[cfg(test)]
    pub fn empty() -> Self {
        use crate::safekeeper::UNKNOWN_SERVER_VERSION;

        TimelinePersistentState::new(
            &TenantTimelineId::empty(),
            ServerInfo {
                pg_version: UNKNOWN_SERVER_VERSION, /* Postgres server version */
                system_id: 0,                       /* Postgres system identifier */
                wal_seg_size: 0,
            },
            vec![],
            Lsn::INVALID,
            Lsn::INVALID,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
// In memory safekeeper state. Fields mirror ones in `SafeKeeperPersistentState`; values
// are not flushed yet.
pub struct TimelineMemState {
    pub commit_lsn: Lsn,
    pub backup_lsn: Lsn,
    pub peer_horizon_lsn: Lsn,
    pub remote_consistent_lsn: Lsn,
    #[serde(with = "hex")]
    pub proposer_uuid: PgUuid,
    pub standby_horizon: Lsn,
}

/// Safekeeper persistent state plus in memory layer, to avoid frequent fsyncs
/// when we update fields like commit_lsn which don't need immediate
/// persistence. Provides transactional like API to atomically update the state.
///
/// Implements Deref into *persistent* part.
pub struct TimelineState<CTRL: control_file::Storage> {
    pub inmem: TimelineMemState,
    pub pers: CTRL, // persistent
}

impl<CTRL> TimelineState<CTRL>
where
    CTRL: control_file::Storage,
{
    pub fn new(state: CTRL) -> Self {
        TimelineState {
            inmem: TimelineMemState {
                commit_lsn: state.commit_lsn,
                backup_lsn: state.backup_lsn,
                peer_horizon_lsn: state.peer_horizon_lsn,
                remote_consistent_lsn: state.remote_consistent_lsn,
                proposer_uuid: state.proposer_uuid,
                standby_horizon: Lsn::INVALID,
            },
            pers: state,
        }
    }

    /// Start atomic change. Returns SafeKeeperPersistentState with in memory
    /// values applied; the protocol is to 1) change returned struct as desired
    /// 2) atomically persist it with finish_change.
    pub fn start_change(&self) -> TimelinePersistentState {
        let mut s = self.pers.clone();
        s.commit_lsn = self.inmem.commit_lsn;
        s.backup_lsn = self.inmem.backup_lsn;
        s.peer_horizon_lsn = self.inmem.peer_horizon_lsn;
        s.remote_consistent_lsn = self.inmem.remote_consistent_lsn;
        s.proposer_uuid = self.inmem.proposer_uuid;
        s
    }

    /// Persist given state. c.f. start_change.
    pub async fn finish_change(&mut self, s: &TimelinePersistentState) -> Result<()> {
        self.pers.persist(s).await?;
        // keep in memory values up to date
        self.inmem.commit_lsn = s.commit_lsn;
        self.inmem.backup_lsn = s.backup_lsn;
        self.inmem.peer_horizon_lsn = s.peer_horizon_lsn;
        self.inmem.remote_consistent_lsn = s.remote_consistent_lsn;
        self.inmem.proposer_uuid = s.proposer_uuid;
        Ok(())
    }

    /// Flush in memory values.
    pub async fn flush(&mut self) -> Result<()> {
        let s = self.start_change();
        self.finish_change(&s).await
    }
}

impl<CTRL> Deref for TimelineState<CTRL>
where
    CTRL: control_file::Storage,
{
    type Target = TimelinePersistentState;

    fn deref(&self) -> &Self::Target {
        &self.pers
    }
}
