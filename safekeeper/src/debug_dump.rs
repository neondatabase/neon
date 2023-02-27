//! Utils for dumping full state of the safekeeper.

use std::fmt::Display;

use anyhow::Result;
use chrono::{DateTime, Utc};
use postgres_ffi::XLogSegNo;
use serde::Serialize;
use serde::Serializer;

use utils::id::{TenantId, TimelineId};
use utils::lsn::Lsn;

use crate::safekeeper::SafeKeeperState;
use crate::safekeeper::SafekeeperMemState;
use crate::safekeeper::TermHistory;

use crate::timeline::ReplicaState;
use crate::GlobalTimelines;

/// Serialize through Display trait.
fn display_serialize<S, F>(z: &F, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    F: Display,
{
    s.serialize_str(&format!("{}", z))
}

/// Various filters that influence the resulting JSON output.
#[derive(Debug, Serialize)]
pub struct Args {
    /// Dump all available safekeeper state. False by default.
    pub dump_all: bool,

    /// Dump control_file content. Uses value of `dump_all` by default.
    pub dump_control_file: bool,

    /// Dump in-memory state. Uses value of `dump_all` by default.
    pub dump_memory: bool,

    /// Dump all disk files in a timeline directory. Uses value of `dump_all` by default.
    pub dump_disk_content: bool,

    /// Dump full term history. True by default.
    pub dump_term_history: bool,

    /// Filter timelines by tenant_id.
    pub tenant_id: Option<TenantId>,

    /// Filter timelines by timeline_id.
    pub timeline_id: Option<TimelineId>,
}

/// Response for debug dump request.
#[derive(Debug, Serialize)]
pub struct Response {
    pub start_time: DateTime<Utc>,
    pub finish_time: DateTime<Utc>,
    pub timelines: Vec<Timeline>,
}

#[derive(Debug, Serialize)]
pub struct Timeline {
    #[serde(serialize_with = "display_serialize")]
    pub tenant_id: TenantId,
    #[serde(serialize_with = "display_serialize")]
    pub timeline_id: TimelineId,
    pub control_file: Option<SafeKeeperState>,
    pub memory: Option<Memory>,
    pub disk_content: Option<DiskContent>,
}

#[derive(Debug, Serialize)]
pub struct Memory {
    pub is_cancelled: bool,
    pub peers_info_len: usize,
    pub replicas: Vec<Option<ReplicaState>>,
    pub wal_backup_active: bool,
    pub active: bool,
    pub num_computes: u32,
    pub last_removed_segno: XLogSegNo,
    pub epoch_start_lsn: Lsn,
    pub mem_state: SafekeeperMemState,

    // PhysicalStorage state.
    pub write_lsn: Lsn,
    pub write_record_lsn: Lsn,
    pub flush_record_lsn: Lsn,
    pub file_open: bool,
}

#[derive(Debug, Serialize)]
pub struct DiskContent {}

pub fn build(args: Args) -> Result<Response> {
    let start_time = Utc::now();
    let ptrs_snapshot = GlobalTimelines::get_all();

    let mut timelines = Vec::new();
    for tli in ptrs_snapshot {
        let ttid = tli.ttid;
        if let Some(tenant_id) = args.tenant_id {
            if tenant_id != ttid.tenant_id {
                continue;
            }
        }
        if let Some(timeline_id) = args.timeline_id {
            if timeline_id != ttid.timeline_id {
                continue;
            }
        }

        let control_file = if args.dump_control_file {
            let mut state = tli.get_state().1;
            if !args.dump_term_history {
                state.acceptor_state.term_history = TermHistory(vec![]);
            }
            Some(state)
        } else {
            None
        };

        let memory = if args.dump_memory {
            Some(tli.memory_dump())
        } else {
            None
        };

        let timeline = Timeline {
            tenant_id: ttid.tenant_id,
            timeline_id: ttid.timeline_id,
            control_file,
            memory,
            disk_content: None,
        };
        timelines.push(timeline);
    }

    Ok(Response {
        start_time,
        finish_time: Utc::now(),
        timelines,
    })
}
