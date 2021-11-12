use zenith_utils::zid::{ZTenantId, ZTimelineId};

use lazy_static::lazy_static;
use zenith_metrics::{register_int_gauge_vec, IntGaugeVec};

pub mod basebackup;
pub mod branches;
pub mod config;
pub mod http;
pub mod layered_repository;
pub mod page_service;
pub mod relish;
pub mod remote_storage;
pub mod repository;
pub mod restore_local_repo;
pub mod tenant_mgr;
pub mod tenant_threads;
pub mod virtual_file;
pub mod waldecoder;
pub mod walreceiver;
pub mod walredo;

lazy_static! {
    static ref LIVE_CONNECTIONS_COUNT: IntGaugeVec = register_int_gauge_vec!(
        "pageserver_live_connections_count",
        "Number of live network connections",
        &["pageserver_connection_kind"]
    )
    .expect("failed to define a metric");
}

pub const LOG_FILE_NAME: &str = "pageserver.log";

/// Config for the Repository checkpointer
#[derive(Debug, Clone, Copy)]
pub enum CheckpointConfig {
    // Flush in-memory data that is older than this
    Distance(u64),
    // Flush all in-memory data
    Forced,
}
