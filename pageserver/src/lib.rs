pub mod basebackup;
pub mod config;
pub mod http;
pub mod import_datadir;
pub mod keyspace;
pub mod layered_repository;
pub mod page_cache;
pub mod page_service;
pub mod pgdatadir_mapping;
pub mod relish;
pub mod remote_storage;
pub mod repository;
pub mod tenant_mgr;
pub mod tenant_threads;
pub mod thread_mgr;
pub mod timelines;
pub mod virtual_file;
pub mod walingest;
pub mod walreceiver;
pub mod walrecord;
pub mod walredo;

use lazy_static::lazy_static;
use zenith_metrics::{register_int_gauge_vec, IntGaugeVec};
use zenith_utils::zid::{ZTenantId, ZTimelineId};

use layered_repository::LayeredRepository;
use pgdatadir_mapping::DatadirTimeline;

/// Current storage format version
///
/// This is embedded in the metadata file, and also in the header of all the
/// layer files. If you make any backwards-incompatible changes to the storage
/// format, bump this!
pub const STORAGE_FORMAT_VERSION: u16 = 1;

// Magic constants used to identify different kinds of files
pub const IMAGE_FILE_MAGIC: u32 = 0x5A60_0000 | STORAGE_FORMAT_VERSION as u32;
pub const DELTA_FILE_MAGIC: u32 = 0x5A61_0000 | STORAGE_FORMAT_VERSION as u32;

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
    // Flush all in-memory data
    Flush,
    // Flush all in-memory data and reconstruct all page images
    Forced,
}

pub type RepositoryImpl = LayeredRepository;

pub type DatadirTimelineImpl = DatadirTimeline<RepositoryImpl>;
