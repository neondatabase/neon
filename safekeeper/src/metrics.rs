//! This module exports metrics for all active timelines.

use std::time::{Instant, SystemTime};

use metrics::{
    core::{AtomicU64, Collector, Desc, GenericGaugeVec, Opts},
    proto::MetricFamily,
    Gauge, IntGaugeVec,
};
use postgres_ffi::xlog_utils::XLogSegNo;
use utils::{lsn::Lsn, zid::ZTenantTimelineId};

use crate::{
    safekeeper::{SafeKeeperState, SafekeeperMemState},
    timeline::{GlobalTimelines, ReplicaState},
};

pub struct FullTimelineInfo {
    pub zttid: ZTenantTimelineId,
    pub replicas: Vec<ReplicaState>,
    pub wal_backup_active: bool,
    pub timeline_is_active: bool,
    pub num_computes: u32,
    pub last_removed_segno: XLogSegNo,

    pub epoch_start_lsn: Lsn,
    pub mem_state: SafekeeperMemState,
    pub persisted_state: SafeKeeperState,

    pub flush_lsn: Lsn,
}

pub struct TimelineCollector {
    descs: Vec<Desc>,
    commit_lsn: GenericGaugeVec<AtomicU64>,
    backup_lsn: GenericGaugeVec<AtomicU64>,
    flush_lsn: GenericGaugeVec<AtomicU64>,
    epoch_start_lsn: GenericGaugeVec<AtomicU64>,
    peer_horizon_lsn: GenericGaugeVec<AtomicU64>,
    remote_consistent_lsn: GenericGaugeVec<AtomicU64>,
    feedback_ps_write_lsn: GenericGaugeVec<AtomicU64>,
    feedback_last_time_seconds: GenericGaugeVec<AtomicU64>,
    timeline_active: GenericGaugeVec<AtomicU64>,
    wal_backup_active: GenericGaugeVec<AtomicU64>,
    connected_computes: IntGaugeVec,
    disk_usage: GenericGaugeVec<AtomicU64>,
    acceptor_term: GenericGaugeVec<AtomicU64>,
    collect_timeline_metrics: Gauge,
}

impl Default for TimelineCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl TimelineCollector {
    pub fn new() -> TimelineCollector {
        let mut descs = Vec::new();

        let commit_lsn = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_commit_lsn",
                "Current commit_lsn (not necessarily persisted to disk), grouped by timeline",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(commit_lsn.desc().into_iter().cloned());

        let backup_lsn = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_backup_lsn",
                "Current backup_lsn, up to which WAL is backed up, grouped by timeline",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(backup_lsn.desc().into_iter().cloned());

        let flush_lsn = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_flush_lsn",
                "Current flush_lsn, grouped by timeline",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(flush_lsn.desc().into_iter().cloned());

        let epoch_start_lsn = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_epoch_start_lsn",
                "Point since which compute generates new WAL in the current consensus term",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(epoch_start_lsn.desc().into_iter().cloned());

        let peer_horizon_lsn = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_peer_horizon_lsn",
                "LSN of the most lagging safekeeper",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(peer_horizon_lsn.desc().into_iter().cloned());

        let remote_consistent_lsn = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_remote_consistent_lsn",
                "LSN which is persisted to the remote storage in pageserver",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(remote_consistent_lsn.desc().into_iter().cloned());

        let feedback_ps_write_lsn = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_feedback_ps_write_lsn",
                "Last LSN received by the pageserver, acknowledged in the feedback",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(feedback_ps_write_lsn.desc().into_iter().cloned());

        let feedback_last_time_seconds = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_feedback_last_time_seconds",
                "Timestamp of the last feedback from the pageserver",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(feedback_last_time_seconds.desc().into_iter().cloned());

        let timeline_active = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_timeline_active",
                "Reports 1 for active timelines, 0 for inactive",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(timeline_active.desc().into_iter().cloned());

        let wal_backup_active = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_wal_backup_active",
                "Reports 1 for timelines with active WAL backup, 0 otherwise",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(wal_backup_active.desc().into_iter().cloned());

        let connected_computes = IntGaugeVec::new(
            Opts::new(
                "safekeeper_connected_computes",
                "Number of active compute connections",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(connected_computes.desc().into_iter().cloned());

        let disk_usage = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_disk_usage_bytes",
                "Estimated disk space used to store WAL segments",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(disk_usage.desc().into_iter().cloned());

        let acceptor_term = GenericGaugeVec::new(
            Opts::new("safekeeper_acceptor_term", "Current consensus term"),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(acceptor_term.desc().into_iter().cloned());

        let collect_timeline_metrics = Gauge::new(
            "safekeeper_collect_timeline_metrics_seconds",
            "Time spent collecting timeline metrics, including obtaining mutex lock for all timelines",
        )
        .unwrap();
        descs.extend(collect_timeline_metrics.desc().into_iter().cloned());

        TimelineCollector {
            descs,
            commit_lsn,
            backup_lsn,
            flush_lsn,
            epoch_start_lsn,
            peer_horizon_lsn,
            remote_consistent_lsn,
            feedback_ps_write_lsn,
            feedback_last_time_seconds,
            timeline_active,
            wal_backup_active,
            connected_computes,
            disk_usage,
            acceptor_term,
            collect_timeline_metrics,
        }
    }
}

impl Collector for TimelineCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let start_collecting = Instant::now();

        // reset all metrics to clean up inactive timelines
        self.commit_lsn.reset();
        self.backup_lsn.reset();
        self.flush_lsn.reset();
        self.epoch_start_lsn.reset();
        self.peer_horizon_lsn.reset();
        self.remote_consistent_lsn.reset();
        self.feedback_ps_write_lsn.reset();
        self.feedback_last_time_seconds.reset();
        self.timeline_active.reset();
        self.wal_backup_active.reset();
        self.connected_computes.reset();
        self.disk_usage.reset();
        self.acceptor_term.reset();

        let timelines = GlobalTimelines::active_timelines_metrics();

        for tli in timelines {
            let tenant_id = tli.zttid.tenant_id.to_string();
            let timeline_id = tli.zttid.timeline_id.to_string();
            let labels = &[tenant_id.as_str(), timeline_id.as_str()];

            let mut most_advanced: Option<utils::pq_proto::ReplicationFeedback> = None;
            for replica in tli.replicas.iter() {
                if let Some(replica_feedback) = replica.pageserver_feedback {
                    if let Some(current) = most_advanced {
                        if current.ps_writelsn < replica_feedback.ps_writelsn {
                            most_advanced = Some(replica_feedback);
                        }
                    } else {
                        most_advanced = Some(replica_feedback);
                    }
                }
            }

            self.commit_lsn
                .with_label_values(labels)
                .set(tli.mem_state.commit_lsn.into());
            self.backup_lsn
                .with_label_values(labels)
                .set(tli.mem_state.backup_lsn.into());
            self.flush_lsn
                .with_label_values(labels)
                .set(tli.flush_lsn.into());
            self.epoch_start_lsn
                .with_label_values(labels)
                .set(tli.epoch_start_lsn.into());
            self.peer_horizon_lsn
                .with_label_values(labels)
                .set(tli.mem_state.peer_horizon_lsn.into());
            self.remote_consistent_lsn
                .with_label_values(labels)
                .set(tli.mem_state.remote_consistent_lsn.into());
            self.timeline_active
                .with_label_values(labels)
                .set(tli.timeline_is_active as u64);
            self.wal_backup_active
                .with_label_values(labels)
                .set(tli.wal_backup_active as u64);
            self.connected_computes
                .with_label_values(labels)
                .set(tli.num_computes as i64);
            self.acceptor_term
                .with_label_values(labels)
                .set(tli.persisted_state.acceptor_state.term as u64);

            if let Some(feedback) = most_advanced {
                self.feedback_ps_write_lsn
                    .with_label_values(labels)
                    .set(feedback.ps_writelsn);
                if let Ok(unix_time) = feedback.ps_replytime.duration_since(SystemTime::UNIX_EPOCH)
                {
                    self.feedback_last_time_seconds
                        .with_label_values(labels)
                        .set(unix_time.as_secs());
                }
            }

            if tli.last_removed_segno != 0 {
                let segno_count = tli
                    .flush_lsn
                    .segment_number(tli.persisted_state.server.wal_seg_size as usize)
                    - tli.last_removed_segno;
                let disk_usage_bytes = segno_count * tli.persisted_state.server.wal_seg_size as u64;
                self.disk_usage
                    .with_label_values(labels)
                    .set(disk_usage_bytes);
            }
        }

        // collect MetricFamilys.
        let mut mfs = Vec::new();
        mfs.extend(self.commit_lsn.collect());
        mfs.extend(self.backup_lsn.collect());
        mfs.extend(self.flush_lsn.collect());
        mfs.extend(self.epoch_start_lsn.collect());
        mfs.extend(self.peer_horizon_lsn.collect());
        mfs.extend(self.remote_consistent_lsn.collect());
        mfs.extend(self.feedback_ps_write_lsn.collect());
        mfs.extend(self.feedback_last_time_seconds.collect());
        mfs.extend(self.timeline_active.collect());
        mfs.extend(self.wal_backup_active.collect());
        mfs.extend(self.connected_computes.collect());
        mfs.extend(self.disk_usage.collect());
        mfs.extend(self.acceptor_term.collect());

        // report time it took to collect all info
        let elapsed = start_collecting.elapsed().as_secs_f64();
        self.collect_timeline_metrics.set(elapsed);
        mfs.extend(self.collect_timeline_metrics.collect());

        mfs
    }
}
