mod auth;
pub mod basebackup;
pub mod config;
pub mod consumption_metrics;
pub mod context;
pub mod disk_usage_eviction_task;
pub mod http;
pub mod import_datadir;
pub mod keyspace;
pub(crate) mod metrics;
pub mod page_cache;
pub mod page_service;
pub mod pgdatadir_mapping;
pub mod repository;
pub(crate) mod statvfs;
pub mod task_mgr;
pub mod tenant;
pub mod trace;
pub mod virtual_file;
pub mod walingest;
pub mod walrecord;
pub mod walredo;

use std::path::Path;

use crate::task_mgr::TaskKind;
use tracing::info;

/// Current storage format version
///
/// This is embedded in the header of all the layer files.
/// If you make any backwards-incompatible changes to the storage
/// format, bump this!
/// Note that TimelineMetadata uses its own version number to track
/// backwards-compatible changes to the metadata format.
pub const STORAGE_FORMAT_VERSION: u16 = 3;

pub const DEFAULT_PG_VERSION: u32 = 15;

// Magic constants used to identify different kinds of files
pub const IMAGE_FILE_MAGIC: u16 = 0x5A60;
pub const DELTA_FILE_MAGIC: u16 = 0x5A61;

static ZERO_PAGE: bytes::Bytes = bytes::Bytes::from_static(&[0u8; 8192]);

pub use crate::metrics::preinitialize_metrics;

#[tracing::instrument]
pub async fn shutdown_pageserver(exit_code: i32) {
    use std::time::Duration;
    // Shut down the libpq endpoint task. This prevents new connections from
    // being accepted.
    timed(
        task_mgr::shutdown_tasks(Some(TaskKind::LibpqEndpointListener), None, None),
        "shutdown LibpqEndpointListener",
        Duration::from_secs(1),
    )
    .await;

    // Shut down any page service tasks.
    timed(
        task_mgr::shutdown_tasks(Some(TaskKind::PageRequestHandler), None, None),
        "shutdown PageRequestHandlers",
        Duration::from_secs(1),
    )
    .await;

    // Shut down all the tenants. This flushes everything to disk and kills
    // the checkpoint and GC tasks.
    timed(
        tenant::mgr::shutdown_all_tenants(),
        "shutdown all tenants",
        Duration::from_secs(5),
    )
    .await;

    // Shut down the HTTP endpoint last, so that you can still check the server's
    // status while it's shutting down.
    // FIXME: We should probably stop accepting commands like attach/detach earlier.
    timed(
        task_mgr::shutdown_tasks(Some(TaskKind::HttpEndpointListener), None, None),
        "shutdown http",
        Duration::from_secs(1),
    )
    .await;

    // There should be nothing left, but let's be sure
    timed(
        task_mgr::shutdown_tasks(None, None, None),
        "shutdown leftovers",
        Duration::from_secs(1),
    )
    .await;
    info!("Shut down successfully completed");
    std::process::exit(exit_code);
}

const DEFAULT_BASE_BACKOFF_SECONDS: f64 = 0.1;
const DEFAULT_MAX_BACKOFF_SECONDS: f64 = 3.0;

async fn exponential_backoff(n: u32, base_increment: f64, max_seconds: f64) {
    let backoff_duration_seconds =
        exponential_backoff_duration_seconds(n, base_increment, max_seconds);
    if backoff_duration_seconds > 0.0 {
        info!(
            "Backoff: waiting {backoff_duration_seconds} seconds before processing with the task",
        );
        tokio::time::sleep(std::time::Duration::from_secs_f64(backoff_duration_seconds)).await;
    }
}

pub fn exponential_backoff_duration_seconds(n: u32, base_increment: f64, max_seconds: f64) -> f64 {
    if n == 0 {
        0.0
    } else {
        (1.0 + base_increment).powf(f64::from(n)).min(max_seconds)
    }
}

/// The name of the metadata file pageserver creates per timeline.
/// Full path: `tenants/<tenant_id>/timelines/<timeline_id>/metadata`.
pub const METADATA_FILE_NAME: &str = "metadata";

/// Per-tenant configuration file.
/// Full path: `tenants/<tenant_id>/config`.
pub const TENANT_CONFIG_NAME: &str = "config";

/// A suffix used for various temporary files. Any temporary files found in the
/// data directory at pageserver startup can be automatically removed.
pub const TEMP_FILE_SUFFIX: &str = "___temp";

/// A marker file to mark that a timeline directory was not fully initialized.
/// If a timeline directory with this marker is encountered at pageserver startup,
/// the timeline directory and the marker file are both removed.
/// Full path: `tenants/<tenant_id>/timelines/<timeline_id>___uninit`.
pub const TIMELINE_UNINIT_MARK_SUFFIX: &str = "___uninit";

pub const TIMELINE_DELETE_MARK_SUFFIX: &str = "___delete";

/// A marker file to prevent pageserver from loading a certain tenant on restart.
/// Different from [`TIMELINE_UNINIT_MARK_SUFFIX`] due to semantics of the corresponding
/// `ignore` management API command, that expects the ignored tenant to be properly loaded
/// into pageserver's memory before being ignored.
/// Full path: `tenants/<tenant_id>/___ignored_tenant`.
pub const IGNORED_TENANT_FILE_NAME: &str = "___ignored_tenant";

pub fn is_temporary(path: &Path) -> bool {
    match path.file_name() {
        Some(name) => name.to_string_lossy().ends_with(TEMP_FILE_SUFFIX),
        None => false,
    }
}

fn ends_with_suffix(path: &Path, suffix: &str) -> bool {
    match path.file_name() {
        Some(name) => name.to_string_lossy().ends_with(suffix),
        None => false,
    }
}

pub fn is_uninit_mark(path: &Path) -> bool {
    ends_with_suffix(path, TIMELINE_UNINIT_MARK_SUFFIX)
}

pub fn is_delete_mark(path: &Path) -> bool {
    ends_with_suffix(path, TIMELINE_DELETE_MARK_SUFFIX)
}

fn is_walkdir_io_not_found(e: &walkdir::Error) -> bool {
    if let Some(e) = e.io_error() {
        if e.kind() == std::io::ErrorKind::NotFound {
            return true;
        }
    }
    false
}

/// During pageserver startup, we need to order operations not to exhaust tokio worker threads by
/// blocking.
///
/// The instances of this value exist only during startup, otherwise `None` is provided, meaning no
/// delaying is needed.
#[derive(Clone)]
pub struct InitializationOrder {
    /// Each initial tenant load task carries this until completion.
    pub initial_tenant_load: Option<utils::completion::Completion>,

    /// Barrier for when we can start initial logical size calculations.
    pub initial_logical_size_can_start: utils::completion::Barrier,

    /// Each timeline owns a clone of this to be consumed on the initial logical size calculation
    /// attempt. It is important to drop this once the attempt has completed.
    pub initial_logical_size_attempt: utils::completion::Completion,

    /// Barrier for when we can start any background jobs.
    ///
    /// This can be broken up later on, but right now there is just one class of a background job.
    pub background_jobs_can_start: utils::completion::Barrier,
}

/// Time the future with a warning when it exceeds a threshold.
async fn timed<Fut: std::future::Future>(
    fut: Fut,
    name: &str,
    warn_at: std::time::Duration,
) -> <Fut as std::future::Future>::Output {
    let started = std::time::Instant::now();

    let mut fut = std::pin::pin!(fut);

    match tokio::time::timeout(warn_at, &mut fut).await {
        Ok(ret) => {
            tracing::info!(
                task = name,
                elapsed_ms = started.elapsed().as_millis(),
                "completed"
            );
            ret
        }
        Err(_) => {
            tracing::info!(
                task = name,
                elapsed_ms = started.elapsed().as_millis(),
                "still waiting, taking longer than expected..."
            );

            let ret = fut.await;

            tracing::warn!(
                task = name,
                elapsed_ms = started.elapsed().as_millis(),
                "completed, took longer than expected"
            );

            ret
        }
    }
}

#[cfg(test)]
mod backoff_defaults_tests {
    use super::*;

    #[test]
    fn backoff_defaults_produce_growing_backoff_sequence() {
        let mut current_backoff_value = None;

        for i in 0..10_000 {
            let new_backoff_value = exponential_backoff_duration_seconds(
                i,
                DEFAULT_BASE_BACKOFF_SECONDS,
                DEFAULT_MAX_BACKOFF_SECONDS,
            );

            if let Some(old_backoff_value) = current_backoff_value.replace(new_backoff_value) {
                assert!(
                    old_backoff_value <= new_backoff_value,
                    "{i}th backoff value {new_backoff_value} is smaller than the previous one {old_backoff_value}"
                )
            }
        }

        assert_eq!(
            current_backoff_value.expect("Should have produced backoff values to compare"),
            DEFAULT_MAX_BACKOFF_SECONDS,
            "Given big enough of retries, backoff should reach its allowed max value"
        );
    }
}

#[cfg(test)]
mod timed_tests {
    use super::timed;
    use std::time::Duration;

    #[tokio::test]
    async fn timed_completes_when_inner_future_completes() {
        // A future that completes on time should have its result returned
        let r1 = timed(
            async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                123
            },
            "test 1",
            Duration::from_millis(50),
        )
        .await;
        assert_eq!(r1, 123);

        // A future that completes too slowly should also have its result returned
        let r1 = timed(
            async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                456
            },
            "test 1",
            Duration::from_millis(10),
        )
        .await;
        assert_eq!(r1, 456);
    }
}
