//! Code to deal with safekeeper control file upgrades
use crate::safekeeper::{
    AcceptorState, PersistedPeers, PgUuid, SafeKeeperState, ServerInfo, Term, TermHistory, TermLsn,
};
use anyhow::{bail, Result};
use pq_proto::SystemId;
use serde::{Deserialize, Serialize};
use tracing::*;
use utils::{
    bin_ser::LeSer,
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

/// Persistent consensus state of the acceptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AcceptorStateV1 {
    /// acceptor's last term it voted for (advanced in 1 phase)
    term: Term,
    /// acceptor's epoch (advanced, i.e. bumped to 'term' when VCL is reached).
    epoch: Term,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SafeKeeperStateV1 {
    /// persistent acceptor state
    acceptor_state: AcceptorStateV1,
    /// information about server
    server: ServerInfoV2,
    /// Unique id of the last *elected* proposer we dealt with. Not needed
    /// for correctness, exists for monitoring purposes.
    proposer_uuid: PgUuid,
    /// part of WAL acknowledged by quorum and available locally
    commit_lsn: Lsn,
    /// minimal LSN which may be needed for recovery of some safekeeper (end_lsn
    /// of last record streamed to everyone)
    truncate_lsn: Lsn,
    // Safekeeper starts receiving WAL from this LSN, zeros before it ought to
    // be skipped during decoding.
    wal_start_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerInfoV2 {
    /// Postgres server version
    pub pg_version: u32,
    pub system_id: SystemId,
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub wal_seg_size: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafeKeeperStateV2 {
    /// persistent acceptor state
    pub acceptor_state: AcceptorState,
    /// information about server
    pub server: ServerInfoV2,
    /// Unique id of the last *elected* proposer we dealt with. Not needed
    /// for correctness, exists for monitoring purposes.
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerInfoV3 {
    /// Postgres server version
    pub pg_version: u32,
    pub system_id: SystemId,
    #[serde(with = "hex")]
    pub tenant_id: TenantId,
    #[serde(with = "hex")]
    pub timeline_id: TimelineId,
    pub wal_seg_size: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafeKeeperStateV3 {
    /// persistent acceptor state
    pub acceptor_state: AcceptorState,
    /// information about server
    pub server: ServerInfoV3,
    /// Unique id of the last *elected* proposer we dealt with. Not needed
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafeKeeperStateV4 {
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
    /// Part of WAL acknowledged by quorum and available locally. Always points
    /// to record boundary.
    pub commit_lsn: Lsn,
    /// First LSN not yet offloaded to s3. Useful to persist to avoid finding
    /// out offloading progress on boot.
    pub s3_wal_lsn: Lsn,
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
    pub peers: PersistedPeers,
}

pub fn upgrade_control_file(buf: &[u8], version: u32) -> Result<SafeKeeperState> {
    // migrate to storing full term history
    if version == 1 {
        info!("reading safekeeper control file version {}", version);
        let oldstate = SafeKeeperStateV1::des(&buf[..buf.len()])?;
        let ac = AcceptorState {
            term: oldstate.acceptor_state.term,
            term_history: TermHistory(vec![TermLsn {
                term: oldstate.acceptor_state.epoch,
                lsn: Lsn(0),
            }]),
        };
        return Ok(SafeKeeperState {
            tenant_id: oldstate.server.tenant_id,
            timeline_id: oldstate.server.timeline_id,
            acceptor_state: ac,
            server: ServerInfo {
                pg_version: oldstate.server.pg_version,
                system_id: oldstate.server.system_id,
                wal_seg_size: oldstate.server.wal_seg_size,
            },
            proposer_uuid: oldstate.proposer_uuid,
            timeline_start_lsn: Lsn(0),
            local_start_lsn: Lsn(0),
            commit_lsn: oldstate.commit_lsn,
            backup_lsn: Lsn(0),
            peer_horizon_lsn: oldstate.truncate_lsn,
            remote_consistent_lsn: Lsn(0),
            peers: PersistedPeers(vec![]),
        });
    // migrate to hexing some ids
    } else if version == 2 {
        info!("reading safekeeper control file version {}", version);
        let oldstate = SafeKeeperStateV2::des(&buf[..buf.len()])?;
        let server = ServerInfo {
            pg_version: oldstate.server.pg_version,
            system_id: oldstate.server.system_id,
            wal_seg_size: oldstate.server.wal_seg_size,
        };
        return Ok(SafeKeeperState {
            tenant_id: oldstate.server.tenant_id,
            timeline_id: oldstate.server.timeline_id,
            acceptor_state: oldstate.acceptor_state,
            server,
            proposer_uuid: oldstate.proposer_uuid,
            timeline_start_lsn: Lsn(0),
            local_start_lsn: Lsn(0),
            commit_lsn: oldstate.commit_lsn,
            backup_lsn: Lsn(0),
            peer_horizon_lsn: oldstate.truncate_lsn,
            remote_consistent_lsn: Lsn(0),
            peers: PersistedPeers(vec![]),
        });
    // migrate to moving tenant_id/timeline_id to the top and adding some lsns
    } else if version == 3 {
        info!("reading safekeeper control file version {version}");
        let oldstate = SafeKeeperStateV3::des(&buf[..buf.len()])?;
        let server = ServerInfo {
            pg_version: oldstate.server.pg_version,
            system_id: oldstate.server.system_id,
            wal_seg_size: oldstate.server.wal_seg_size,
        };
        return Ok(SafeKeeperState {
            tenant_id: oldstate.server.tenant_id,
            timeline_id: oldstate.server.timeline_id,
            acceptor_state: oldstate.acceptor_state,
            server,
            proposer_uuid: oldstate.proposer_uuid,
            timeline_start_lsn: Lsn(0),
            local_start_lsn: Lsn(0),
            commit_lsn: oldstate.commit_lsn,
            backup_lsn: Lsn(0),
            peer_horizon_lsn: oldstate.truncate_lsn,
            remote_consistent_lsn: Lsn(0),
            peers: PersistedPeers(vec![]),
        });
    // migrate to having timeline_start_lsn
    } else if version == 4 {
        info!("reading safekeeper control file version {}", version);
        let oldstate = SafeKeeperStateV4::des(&buf[..buf.len()])?;
        let server = ServerInfo {
            pg_version: oldstate.server.pg_version,
            system_id: oldstate.server.system_id,
            wal_seg_size: oldstate.server.wal_seg_size,
        };
        return Ok(SafeKeeperState {
            tenant_id: oldstate.tenant_id,
            timeline_id: oldstate.timeline_id,
            acceptor_state: oldstate.acceptor_state,
            server,
            proposer_uuid: oldstate.proposer_uuid,
            timeline_start_lsn: Lsn(0),
            local_start_lsn: Lsn(0),
            commit_lsn: oldstate.commit_lsn,
            backup_lsn: Lsn::INVALID,
            peer_horizon_lsn: oldstate.peer_horizon_lsn,
            remote_consistent_lsn: Lsn(0),
            peers: PersistedPeers(vec![]),
        });
    } else if version == 5 {
        info!("reading safekeeper control file version {}", version);
        let mut oldstate = SafeKeeperState::des(&buf[..buf.len()])?;
        if oldstate.timeline_start_lsn != Lsn(0) {
            return Ok(oldstate);
        }

        // set special timeline_start_lsn because we don't know the real one
        info!("setting timeline_start_lsn and local_start_lsn to Lsn(1)");
        oldstate.timeline_start_lsn = Lsn(1);
        oldstate.local_start_lsn = Lsn(1);

        return Ok(oldstate);
    } else if version == 6 {
        info!("reading safekeeper control file version {}", version);
        let mut oldstate = SafeKeeperState::des(&buf[..buf.len()])?;
        if oldstate.server.pg_version != 0 {
            return Ok(oldstate);
        }

        // set pg_version to the default v14
        info!("setting pg_version to 140005");
        oldstate.server.pg_version = 140005;

        return Ok(oldstate);
    }
    bail!("unsupported safekeeper control file version {}", version)
}
