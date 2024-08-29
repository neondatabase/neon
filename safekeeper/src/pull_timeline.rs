use std::sync::Arc;

use camino::Utf8PathBuf;
use camino_tempfile::Utf8TempDir;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use anyhow::{bail, Context, Result};
use tokio::io::AsyncWriteExt;
use tracing::info;
use utils::{
    id::{TenantId, TenantTimelineId, TimelineId},
    lsn::Lsn,
    pausable_failpoint,
};

use crate::{
    control_file, debug_dump,
    http::routes::TimelineStatus,
    timeline::{get_tenant_dir, get_timeline_dir, Timeline, TimelineError},
    wal_storage::{self, Storage},
    GlobalTimelines, SafeKeeperConf,
};

/// Info about timeline on safekeeper ready for reporting.
#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub http_hosts: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct Response {
    // Donor safekeeper host
    pub safekeeper_host: String,
    // TODO: add more fields?
}

/// Response for debug dump request.
#[derive(Debug, Serialize, Deserialize)]
pub struct DebugDumpResponse {
    pub start_time: DateTime<Utc>,
    pub finish_time: DateTime<Utc>,
    pub timelines: Vec<debug_dump::Timeline>,
    pub timelines_count: usize,
    pub config: debug_dump::Config,
}

/// Find the most advanced safekeeper and pull timeline from it.
pub async fn handle_request(request: Request) -> Result<Response> {
    let existing_tli = GlobalTimelines::get(TenantTimelineId::new(
        request.tenant_id,
        request.timeline_id,
    ));
    if existing_tli.is_ok() {
        bail!("Timeline {} already exists", request.timeline_id);
    }

    let client = reqwest::Client::new();
    let http_hosts = request.http_hosts.clone();

    // Send request to /v1/tenant/:tenant_id/timeline/:timeline_id
    let responses = futures::future::join_all(http_hosts.iter().map(|url| {
        let url = format!(
            "{}/v1/tenant/{}/timeline/{}",
            url, request.tenant_id, request.timeline_id
        );
        client.get(url).send()
    }))
    .await;

    let mut statuses = Vec::new();
    for (i, response) in responses.into_iter().enumerate() {
        let response = response.context(format!("Failed to get status from {}", http_hosts[i]))?;
        let status: crate::http::routes::TimelineStatus = response.json().await?;
        statuses.push((status, i));
    }

    // Find the most advanced safekeeper
    // TODO: current logic may be wrong, fix it later
    let (status, i) = statuses
        .into_iter()
        .max_by_key(|(status, _)| {
            (
                status.acceptor_state.epoch,
                status.flush_lsn,
                status.commit_lsn,
            )
        })
        .unwrap();
    let safekeeper_host = http_hosts[i].clone();

    assert!(status.tenant_id == request.tenant_id);
    assert!(status.timeline_id == request.timeline_id);

    pull_timeline(status, safekeeper_host).await
}

async fn pull_timeline(status: TimelineStatus, host: String) -> Result<Response> {
    let ttid = TenantTimelineId::new(status.tenant_id, status.timeline_id);
    info!(
        "pulling timeline {} from safekeeper {}, commit_lsn={}, flush_lsn={}, term={}, epoch={}",
        ttid,
        host,
        status.commit_lsn,
        status.flush_lsn,
        status.acceptor_state.term,
        status.acceptor_state.epoch
    );

    let conf = &GlobalTimelines::get_global_config();

    let client = reqwest::Client::new();
    // TODO: don't use debug dump, it should be used only in tests.
    //      This is a proof of concept, we should figure out a way
    //      to use scp without implementing it manually.

    // Implementing our own scp over HTTP.
    // At first, we need to fetch list of files from safekeeper.
    let dump: DebugDumpResponse = client
        .get(format!(
            "{}/v1/debug_dump?dump_all=true&tenant_id={}&timeline_id={}",
            host, status.tenant_id, status.timeline_id
        ))
        .send()
        .await?
        .json()
        .await?;

    if dump.timelines.len() != 1 {
        bail!(
            "expected to fetch single timeline, got {} timelines",
            dump.timelines.len()
        );
    }

    let timeline = dump.timelines.into_iter().next().unwrap();
    let disk_content = timeline.disk_content.ok_or(anyhow::anyhow!(
        "timeline {} doesn't have disk content",
        ttid
    ))?;

    let mut filenames = disk_content
        .files
        .iter()
        .map(|file| file.name.clone())
        .collect::<Vec<_>>();

    // Sort filenames to make sure we pull files in correct order
    // After sorting, we should have:
    // - 000000010000000000000001
    // - ...
    // - 000000010000000000000002.partial
    // - safekeeper.control
    filenames.sort();

    // safekeeper.control should be the first file, so we need to move it to the beginning
    let control_file_index = filenames
        .iter()
        .position(|name| name == "safekeeper.control")
        .ok_or(anyhow::anyhow!("safekeeper.control not found"))?;
    filenames.remove(control_file_index);
    filenames.insert(0, "safekeeper.control".to_string());

    pausable_failpoint!("sk-pull-timeline-after-list-pausable");

    info!(
        "downloading {} files from safekeeper {}",
        filenames.len(),
        host
    );

    let (_tmp_dir, tli_dir_path) = create_temp_timeline_dir(conf, ttid).await?;

    // Note: some time happens between fetching list of files and fetching files themselves.
    //       It's possible that some files will be removed from safekeeper and we will fail to fetch them.
    //       This function will fail in this case, should be retried by the caller.
    for filename in filenames {
        let file_path = tli_dir_path.join(&filename);
        // /v1/tenant/:tenant_id/timeline/:timeline_id/file/:filename
        let http_url = format!(
            "{}/v1/tenant/{}/timeline/{}/file/{}",
            host, status.tenant_id, status.timeline_id, filename
        );

        let mut file = tokio::fs::File::create(&file_path).await?;
        let mut response = client.get(&http_url).send().await?;
        if response.status() != reqwest::StatusCode::OK {
            bail!(
                "pulling file {} failed: status is {}",
                filename,
                response.status()
            );
        }
        while let Some(chunk) = response.chunk().await? {
            file.write_all(&chunk).await?;
            file.flush().await?;
        }
    }

    // TODO: fsync?

    // Let's create timeline from temp directory and verify that it's correct
    let (commit_lsn, flush_lsn) = validate_temp_timeline(conf, ttid, &tli_dir_path).await?;
    info!(
        "finished downloading timeline {}, commit_lsn={}, flush_lsn={}",
        ttid, commit_lsn, flush_lsn
    );
    assert!(status.commit_lsn <= status.flush_lsn);

    // Finally, load the timeline.
    let _tli = load_temp_timeline(conf, ttid, &tli_dir_path).await?;

    Ok(Response {
        safekeeper_host: host,
    })
}

/// Create temp directory for a new timeline. It needs to be located on the same
/// filesystem as the rest of the timelines. It will be automatically deleted when
/// Utf8TempDir goes out of scope.
pub async fn create_temp_timeline_dir(
    conf: &SafeKeeperConf,
    ttid: TenantTimelineId,
) -> Result<(Utf8TempDir, Utf8PathBuf)> {
    // conf.workdir is usually /storage/safekeeper/data
    // will try to transform it into /storage/safekeeper/tmp
    let temp_base = conf
        .workdir
        .parent()
        .ok_or(anyhow::anyhow!("workdir has no parent"))?
        .join("tmp");

    tokio::fs::create_dir_all(&temp_base).await?;

    let tli_dir = camino_tempfile::Builder::new()
        .suffix("_temptli")
        .prefix(&format!("{}_{}_", ttid.tenant_id, ttid.timeline_id))
        .tempdir_in(temp_base)?;

    let tli_dir_path = tli_dir.path().to_path_buf();

    Ok((tli_dir, tli_dir_path))
}

/// Do basic validation of a temp timeline, before moving it to the global map.
pub async fn validate_temp_timeline(
    conf: &SafeKeeperConf,
    ttid: TenantTimelineId,
    path: &Utf8PathBuf,
) -> Result<(Lsn, Lsn)> {
    let control_path = path.join("safekeeper.control");

    let control_store = control_file::FileStorage::load_control_file(control_path)?;
    if control_store.server.wal_seg_size == 0 {
        bail!("wal_seg_size is not set");
    }

    let wal_store = wal_storage::PhysicalStorage::new(&ttid, path.clone(), conf, &control_store)?;

    let commit_lsn = control_store.commit_lsn;
    let flush_lsn = wal_store.flush_lsn();

    Ok((commit_lsn, flush_lsn))
}

/// Move timeline from a temp directory to the main storage, and load it to the global map.
/// This operation is done under a lock to prevent bugs if several concurrent requests are
/// trying to load the same timeline. Note that it doesn't guard against creating the
/// timeline with the same ttid, but no one should be doing this anyway.
pub async fn load_temp_timeline(
    conf: &SafeKeeperConf,
    ttid: TenantTimelineId,
    tmp_path: &Utf8PathBuf,
) -> Result<Arc<Timeline>> {
    // Take a lock to prevent concurrent loadings
    let load_lock = GlobalTimelines::loading_lock().await;
    let guard = load_lock.lock().await;

    if !matches!(GlobalTimelines::get(ttid), Err(TimelineError::NotFound(_))) {
        bail!("timeline already exists, cannot overwrite it")
    }

    // Move timeline dir to the correct location
    let timeline_path = get_timeline_dir(conf, &ttid);

    info!(
        "moving timeline {} from {} to {}",
        ttid, tmp_path, timeline_path
    );
    tokio::fs::create_dir_all(get_tenant_dir(conf, &ttid.tenant_id)).await?;
    tokio::fs::rename(tmp_path, &timeline_path).await?;

    let tli = GlobalTimelines::load_timeline(&guard, ttid)
        .await
        .context("Failed to load timeline after copy")?;

    info!(
        "loaded timeline {}, flush_lsn={}",
        ttid,
        tli.get_flush_lsn().await
    );

    Ok(tli)
}
