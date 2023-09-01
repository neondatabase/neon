use serde::{Deserialize, Serialize};

use anyhow::{bail, Context, Result};
use tokio::io::AsyncWriteExt;
use tracing::info;
use utils::id::{TenantId, TenantTimelineId, TimelineId};

use serde_with::{serde_as, DisplayFromStr};

use crate::{
    control_file, debug_dump,
    http::routes::TimelineStatus,
    wal_storage::{self, Storage},
    GlobalTimelines,
};

/// Info about timeline on safekeeper ready for reporting.
#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    #[serde_as(as = "DisplayFromStr")]
    pub tenant_id: TenantId,
    #[serde_as(as = "DisplayFromStr")]
    pub timeline_id: TimelineId,
    pub http_hosts: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct Response {
    // Donor safekeeper host
    pub safekeeper_host: String,
    // TODO: add more fields?
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
        "Pulling timeline {} from safekeeper {}, commit_lsn={}, flush_lsn={}, term={}, epoch={}",
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
    let dump: debug_dump::Response = client
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
            "Expected to fetch single timeline, got {} timelines",
            dump.timelines.len()
        );
    }

    let timeline = dump.timelines.into_iter().next().unwrap();
    let disk_content = timeline.disk_content.ok_or(anyhow::anyhow!(
        "Timeline {} doesn't have disk content",
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

    info!(
        "Downloading {} files from safekeeper {}",
        filenames.len(),
        host
    );

    // Creating temp directory for a new timeline. It needs to be
    // located on the same filesystem as the rest of the timelines.

    // conf.workdir is usually /storage/safekeeper/data
    // will try to transform it into /storage/safekeeper/tmp
    let temp_base = conf
        .workdir
        .parent()
        .ok_or(anyhow::anyhow!("workdir has no parent"))?
        .join("tmp");

    tokio::fs::create_dir_all(&temp_base).await?;

    let tli_dir = tempfile::Builder::new()
        .suffix("_temptli")
        .prefix(&format!("{}_{}_", ttid.tenant_id, ttid.timeline_id))
        .tempdir_in(temp_base)?;
    let tli_dir_path = tli_dir.path().to_owned();

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
        while let Some(chunk) = response.chunk().await? {
            file.write_all(&chunk).await?;
            file.flush().await?;
        }
    }

    // TODO: fsync?

    // Let's create timeline from temp directory and verify that it's correct

    let control_path = tli_dir_path.join("safekeeper.control");

    let control_store = control_file::FileStorage::load_control_file(control_path)?;
    if control_store.server.wal_seg_size == 0 {
        bail!("wal_seg_size is not set");
    }

    let wal_store =
        wal_storage::PhysicalStorage::new(&ttid, tli_dir_path.clone(), conf, &control_store)?;

    let commit_lsn = status.commit_lsn;
    let flush_lsn = wal_store.flush_lsn();

    info!(
        "Finished downloading timeline {}, commit_lsn={}, flush_lsn={}",
        ttid, commit_lsn, flush_lsn
    );
    assert!(status.commit_lsn <= status.flush_lsn);

    // Move timeline dir to the correct location
    let timeline_path = conf.timeline_dir(&ttid);

    info!(
        "Moving timeline {} from {} to {}",
        ttid,
        tli_dir_path.display(),
        timeline_path.display()
    );
    tokio::fs::create_dir_all(conf.tenant_dir(&ttid.tenant_id)).await?;
    tokio::fs::rename(tli_dir_path, &timeline_path).await?;

    let tli = GlobalTimelines::load_timeline(ttid)
        .await
        .context("Failed to load timeline after copy")?;

    info!(
        "Loaded timeline {}, flush_lsn={}",
        ttid,
        tli.get_flush_lsn().await
    );

    Ok(Response {
        safekeeper_host: host,
    })
}
