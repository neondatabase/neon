use std::cmp::min;
use std::io::{self, ErrorKind};
use std::ops::RangeInclusive;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use futures::{SinkExt, StreamExt, TryStreamExt};
use http::StatusCode;
use http_utils::error::ApiError;
use postgres_ffi::{PG_TLI, XLogFileName, XLogSegNo};
use remote_storage::GenericRemoteStorage;
use reqwest::Certificate;
use safekeeper_api::models::{PullTimelineRequest, PullTimelineResponse, TimelineStatus};
use safekeeper_api::{Term, membership};
use safekeeper_client::mgmt_api;
use safekeeper_client::mgmt_api::Client;
use serde::Deserialize;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;
use tokio::task;
use tokio::time::sleep;
use tokio_tar::{Archive, Builder, Header};
use tokio_util::io::{CopyToBytes, SinkWriter};
use tokio_util::sync::PollSender;
use tracing::{error, info, instrument, warn};
use utils::crashsafe::fsync_async_opt;
use utils::id::{NodeId, TenantTimelineId};
use utils::logging::SecretString;
use utils::lsn::Lsn;
use utils::pausable_failpoint;

use crate::control_file::CONTROL_FILE_NAME;
use crate::state::{EvictionState, TimelinePersistentState};
use crate::timeline::{Timeline, TimelineError, WalResidentTimeline};
use crate::timelines_global_map::{create_temp_timeline_dir, validate_temp_timeline};
use crate::wal_storage::{open_wal_file, wal_file_paths};
use crate::{GlobalTimelines, debug_dump, wal_backup};

/// Stream tar archive of timeline to tx.
#[instrument(name = "snapshot", skip_all, fields(ttid = %tli.ttid))]
pub async fn stream_snapshot(
    tli: Arc<Timeline>,
    source: NodeId,
    destination: NodeId,
    tx: mpsc::Sender<Result<Bytes>>,
    storage: Option<Arc<GenericRemoteStorage>>,
) {
    match tli.try_wal_residence_guard().await {
        Err(e) => {
            tx.send(Err(anyhow!("Error checking residence: {:#}", e)))
                .await
                .ok();
        }
        Ok(maybe_resident_tli) => {
            if let Err(e) = match maybe_resident_tli {
                Some(resident_tli) => {
                    stream_snapshot_resident_guts(
                        resident_tli,
                        source,
                        destination,
                        tx.clone(),
                        storage,
                    )
                    .await
                }
                None => {
                    if let Some(storage) = storage {
                        stream_snapshot_offloaded_guts(
                            tli,
                            source,
                            destination,
                            tx.clone(),
                            &storage,
                        )
                        .await
                    } else {
                        tx.send(Err(anyhow!("remote storage not configured")))
                            .await
                            .ok();
                        return;
                    }
                }
            } {
                // Error type/contents don't matter as they won't can't reach the client
                // (hyper likely doesn't do anything with it), but http stream will be
                // prematurely terminated. It would be nice to try to send the error in
                // trailers though.
                tx.send(Err(anyhow!("snapshot failed"))).await.ok();
                error!("snapshot failed: {:#}", e);
            }
        }
    }
}

/// State needed while streaming the snapshot.
pub struct SnapshotContext {
    /// The interval of segment numbers. If None, the timeline hasn't had writes yet, so only send the control file
    pub from_to_segno: Option<RangeInclusive<XLogSegNo>>,
    pub term: Term,
    pub last_log_term: Term,
    pub flush_lsn: Lsn,
    pub wal_seg_size: usize,
    // used to remove WAL hold off in Drop.
    pub tli: WalResidentTimeline,
}

impl Drop for SnapshotContext {
    fn drop(&mut self) {
        let tli = self.tli.clone();
        task::spawn(async move {
            let mut shared_state = tli.write_shared_state().await;
            shared_state.wal_removal_on_hold = false;
        });
    }
}

/// Build a tokio_tar stream that sends encoded bytes into a Bytes channel.
fn prepare_tar_stream(
    tx: mpsc::Sender<Result<Bytes>>,
) -> tokio_tar::Builder<impl AsyncWrite + Unpin + Send> {
    // tokio-tar wants Write implementor, but we have mpsc tx <Result<Bytes>>;
    // use SinkWriter as a Write impl. That is,
    // - create Sink from the tx. It returns PollSendError if chan is closed.
    let sink = PollSender::new(tx);
    // - SinkWriter needs sink error to be io one, map it.
    let sink_io_err = sink.sink_map_err(|_| io::Error::from(ErrorKind::BrokenPipe));
    // - SinkWriter wants sink type to be just Bytes, not Result<Bytes>, so map
    //   it with with(). Note that with() accepts async function which we don't
    //   need and allows the map to fail, which we don't need either, but hence
    //   two Oks.
    let oksink = sink_io_err.with(|b: Bytes| async { io::Result::Ok(Result::Ok(b)) });
    // - SinkWriter (not surprisingly) wants sink of &[u8], not bytes, so wrap
    // into CopyToBytes. This is a data copy.
    let copy_to_bytes = CopyToBytes::new(oksink);
    let writer = SinkWriter::new(copy_to_bytes);
    let pinned_writer = Box::pin(writer);

    // Note that tokio_tar append_* funcs use tokio::io::copy with 8KB buffer
    // which is also likely suboptimal.
    Builder::new_non_terminated(pinned_writer)
}

/// Implementation of snapshot for an offloaded timeline, only reads control file
pub(crate) async fn stream_snapshot_offloaded_guts(
    tli: Arc<Timeline>,
    source: NodeId,
    destination: NodeId,
    tx: mpsc::Sender<Result<Bytes>>,
    storage: &GenericRemoteStorage,
) -> Result<()> {
    let mut ar = prepare_tar_stream(tx);

    tli.snapshot_offloaded(&mut ar, source, destination, storage)
        .await?;

    ar.finish().await?;

    Ok(())
}

/// Implementation of snapshot for a timeline which is resident (includes some segment data)
pub async fn stream_snapshot_resident_guts(
    tli: WalResidentTimeline,
    source: NodeId,
    destination: NodeId,
    tx: mpsc::Sender<Result<Bytes>>,
    storage: Option<Arc<GenericRemoteStorage>>,
) -> Result<()> {
    let mut ar = prepare_tar_stream(tx);

    let bctx = tli
        .start_snapshot(&mut ar, source, destination, storage)
        .await?;
    pausable_failpoint!("sk-snapshot-after-list-pausable");

    if let Some(from_to_segno) = &bctx.from_to_segno {
        let tli_dir = tli.get_timeline_dir();
        info!(
            "sending {} segments [{:#X}-{:#X}], term={}, last_log_term={}, flush_lsn={}",
            from_to_segno.end() - from_to_segno.start() + 1,
            from_to_segno.start(),
            from_to_segno.end(),
            bctx.term,
            bctx.last_log_term,
            bctx.flush_lsn,
        );
        for segno in from_to_segno.clone() {
            let Some((mut sf, is_partial)) =
                open_wal_file(&tli_dir, segno, bctx.wal_seg_size).await?
            else {
                // File is not found
                let (wal_file_path, _wal_file_partial_path) =
                    wal_file_paths(&tli_dir, segno, bctx.wal_seg_size);
                tracing::warn!("couldn't find WAL segment file {wal_file_path}");
                bail!("couldn't find WAL segment file {wal_file_path}")
            };
            let mut wal_file_name = XLogFileName(PG_TLI, segno, bctx.wal_seg_size);
            if is_partial {
                wal_file_name.push_str(".partial");
            }
            ar.append_file(&wal_file_name, &mut sf).await?;
        }
    } else {
        info!("Not including any segments into the snapshot");
    }

    // Do the term check before ar.finish to make archive corrupted in case of
    // term change. Client shouldn't ignore abrupt stream end, but to be sure.
    tli.finish_snapshot(&bctx).await?;

    ar.finish().await?;

    Ok(())
}

impl Timeline {
    /// Simple snapshot for an offloaded timeline: we will only upload a renamed partial segment and
    /// pass a modified control file into the provided tar stream (nothing with data segments on disk, since
    /// we are offloaded and there aren't any)
    async fn snapshot_offloaded<W: AsyncWrite + Unpin + Send>(
        self: &Arc<Timeline>,
        ar: &mut tokio_tar::Builder<W>,
        source: NodeId,
        destination: NodeId,
        storage: &GenericRemoteStorage,
    ) -> Result<()> {
        // Take initial copy of control file, then release state lock
        let mut control_file = {
            let shared_state = self.write_shared_state().await;

            let control_file = TimelinePersistentState::clone(shared_state.sk.state());

            // Rare race: we got unevicted between entering function and reading control file.
            // We error out and let API caller retry.
            if !matches!(control_file.eviction_state, EvictionState::Offloaded(_)) {
                bail!("Timeline was un-evicted during snapshot, please retry");
            }

            control_file
        };

        // Modify the partial segment of the in-memory copy for the control file to
        // point to the destination safekeeper.
        let replace = control_file
            .partial_backup
            .replace_uploaded_segment(source, destination)?;

        let Some(replace) = replace else {
            // In Manager:: ready_for_eviction, we do not permit eviction unless the timeline
            // has a partial segment.  It is unexpected that
            anyhow::bail!("Timeline has no partial segment, cannot generate snapshot");
        };

        tracing::info!("Replacing uploaded partial segment in in-mem control file: {replace:?}");

        // Optimistically try to copy the partial segment to the destination's path: this
        // can fail if the timeline was un-evicted and modified in the background.
        let remote_timeline_path = &self.remote_path;
        wal_backup::copy_partial_segment(
            storage,
            &replace.previous.remote_path(remote_timeline_path),
            &replace.current.remote_path(remote_timeline_path),
        )
        .await?;

        // Since the S3 copy succeeded with the path given in our control file snapshot, and
        // we are sending that snapshot in our response, we are giving the caller a consistent
        // snapshot even if our local Timeline was unevicted or otherwise modified in the meantime.
        let buf = control_file
            .write_to_buf()
            .with_context(|| "failed to serialize control store")?;
        let mut header = Header::new_gnu();
        header.set_size(buf.len().try_into().expect("never breaches u64"));
        ar.append_data(&mut header, CONTROL_FILE_NAME, buf.as_slice())
            .await
            .with_context(|| "failed to append to archive")?;

        Ok(())
    }
}

impl WalResidentTimeline {
    /// Start streaming tar archive with timeline:
    /// 1) stream control file under lock;
    /// 2) hold off WAL removal;
    /// 3) collect SnapshotContext to understand which WAL segments should be
    ///    streamed.
    ///
    /// Snapshot streams data up to flush_lsn. To make this safe, we must check
    /// that term doesn't change during the procedure, or we risk sending mix of
    /// WAL from different histories. Term is remembered in the SnapshotContext
    /// and checked in finish_snapshot. Note that in the last segment some WAL
    /// higher than flush_lsn set here might be streamed; that's fine as long as
    /// terms doesn't change.
    ///
    /// Alternatively we could send only up to commit_lsn to get some valid
    /// state which later will be recovered by compute, in this case term check
    /// is not needed, but we likely don't want that as there might be no
    /// compute which could perform the recovery.
    ///
    /// When returned SnapshotContext is dropped WAL hold is removed.
    async fn start_snapshot<W: AsyncWrite + Unpin + Send>(
        &self,
        ar: &mut tokio_tar::Builder<W>,
        source: NodeId,
        destination: NodeId,
        storage: Option<Arc<GenericRemoteStorage>>,
    ) -> Result<SnapshotContext> {
        let mut shared_state = self.write_shared_state().await;
        let wal_seg_size = shared_state.get_wal_seg_size();

        let mut control_store = TimelinePersistentState::clone(shared_state.sk.state());
        // Modify the partial segment of the in-memory copy for the control file to
        // point to the destination safekeeper.
        let replace = control_store
            .partial_backup
            .replace_uploaded_segment(source, destination)?;

        if let Some(replace) = replace {
            // The deserialized control file has an uploaded partial. We upload a copy
            // of it to object storage for the destination safekeeper and send an updated
            // control file in the snapshot.
            tracing::info!(
                "Replacing uploaded partial segment in in-mem control file: {replace:?}"
            );

            let remote_timeline_path = &self.tli.remote_path;
            wal_backup::copy_partial_segment(
                &*storage.context("remote storage not configured")?,
                &replace.previous.remote_path(remote_timeline_path),
                &replace.current.remote_path(remote_timeline_path),
            )
            .await?;
        }

        let buf = control_store
            .write_to_buf()
            .with_context(|| "failed to serialize control store")?;
        let mut header = Header::new_gnu();
        header.set_size(buf.len().try_into().expect("never breaches u64"));
        ar.append_data(&mut header, CONTROL_FILE_NAME, buf.as_slice())
            .await
            .with_context(|| "failed to append to archive")?;

        // We need to stream since the oldest segment someone (s3 or pageserver)
        // still needs. This duplicates calc_horizon_lsn logic.
        //
        // We know that WAL wasn't removed up to this point because it cannot be
        // removed further than `backup_lsn`. Since we're holding shared_state
        // lock and setting `wal_removal_on_hold` later, it guarantees that WAL
        // won't be removed until we're done.
        let timeline_state = shared_state.sk.state();
        let from_lsn = min(
            timeline_state.remote_consistent_lsn,
            timeline_state.backup_lsn,
        );
        let flush_lsn = shared_state.sk.flush_lsn();
        let (send_segments, msg) = if from_lsn == Lsn::INVALID {
            (false, "snapshot is called on uninitialized timeline")
        } else {
            (true, "timeline is initialized")
        };
        tracing::info!(
            remote_consistent_lsn=%timeline_state.remote_consistent_lsn,
            backup_lsn=%timeline_state.backup_lsn,
            %flush_lsn,
            "{msg}"
        );
        let from_segno = from_lsn.segment_number(wal_seg_size);
        let term = shared_state.sk.state().acceptor_state.term;
        let last_log_term = shared_state.sk.last_log_term();
        let upto_segno = flush_lsn.segment_number(wal_seg_size);
        // have some limit on max number of segments as a sanity check
        const MAX_ALLOWED_SEGS: u64 = 1000;
        let num_segs = upto_segno - from_segno + 1;
        if num_segs > MAX_ALLOWED_SEGS {
            bail!(
                "snapshot is called on timeline with {} segments, but the limit is {}",
                num_segs,
                MAX_ALLOWED_SEGS
            );
        }

        // Prevent WAL removal while we're streaming data.
        //
        // Since this a flag, not a counter just bail out if already set; we
        // shouldn't need concurrent snapshotting.
        if shared_state.wal_removal_on_hold {
            bail!("wal_removal_on_hold is already true");
        }
        shared_state.wal_removal_on_hold = true;

        // Drop shared_state to release the lock, before calling wal_residence_guard().
        drop(shared_state);

        let tli_copy = self.wal_residence_guard().await?;
        let from_to_segno = send_segments.then_some(from_segno..=upto_segno);
        let bctx = SnapshotContext {
            from_to_segno,
            term,
            last_log_term,
            flush_lsn,
            wal_seg_size,
            tli: tli_copy,
        };

        Ok(bctx)
    }

    /// Finish snapshotting: check that term(s) hasn't changed.
    ///
    /// Note that WAL gc hold off is removed in Drop of SnapshotContext to not
    /// forget this if snapshotting fails mid the way.
    pub async fn finish_snapshot(&self, bctx: &SnapshotContext) -> Result<()> {
        let shared_state = self.read_shared_state().await;
        let term = shared_state.sk.state().acceptor_state.term;
        let last_log_term = shared_state.sk.last_log_term();
        // There are some cases to relax this check (e.g. last_log_term might
        // change, but as long as older history is strictly part of new that's
        // fine), but there is no need to do it.
        if bctx.term != term || bctx.last_log_term != last_log_term {
            bail!(
                "term(s) changed during snapshot: were term={}, last_log_term={}, now term={}, last_log_term={}",
                bctx.term,
                bctx.last_log_term,
                term,
                last_log_term
            );
        }
        Ok(())
    }
}

/// Response for debug dump request.
#[derive(Debug, Deserialize)]
pub struct DebugDumpResponse {
    pub start_time: DateTime<Utc>,
    pub finish_time: DateTime<Utc>,
    pub timelines: Vec<debug_dump::Timeline>,
    pub timelines_count: usize,
    pub config: debug_dump::Config,
}

/// Find the most advanced safekeeper and pull timeline from it.
pub async fn handle_request(
    request: PullTimelineRequest,
    sk_auth_token: Option<SecretString>,
    ssl_ca_certs: Vec<Certificate>,
    global_timelines: Arc<GlobalTimelines>,
    wait_for_peer_timeline_status: bool,
) -> Result<PullTimelineResponse, ApiError> {
    if let Some(mconf) = &request.mconf {
        let sk_id = global_timelines.get_sk_id();
        if !mconf.contains(sk_id) {
            return Err(ApiError::BadRequest(anyhow!(
                "refused to pull timeline with {mconf}, node {sk_id} is not member of it",
            )));
        }
    }

    let existing_tli = global_timelines.get(TenantTimelineId::new(
        request.tenant_id,
        request.timeline_id,
    ));
    if let Ok(timeline) = existing_tli {
        let cur_generation = timeline
            .read_shared_state()
            .await
            .sk
            .state()
            .mconf
            .generation;

        info!(
            "Timeline {} already exists with generation {cur_generation}",
            request.timeline_id,
        );

        if let Some(mconf) = request.mconf {
            timeline
                .membership_switch(mconf)
                .await
                .map_err(|e| ApiError::InternalServerError(anyhow::anyhow!(e)))?;
        }

        return Ok(PullTimelineResponse {
            safekeeper_host: None,
        });
    }

    let mut http_client = reqwest::Client::builder();
    for ssl_ca_cert in ssl_ca_certs {
        http_client = http_client.add_root_certificate(ssl_ca_cert);
    }
    let http_client = http_client
        .build()
        .map_err(|e| ApiError::InternalServerError(e.into()))?;

    let http_hosts = request.http_hosts.clone();

    // Figure out statuses of potential donors.
    let mut statuses = Vec::new();
    if !wait_for_peer_timeline_status {
        let responses: Vec<Result<TimelineStatus, mgmt_api::Error>> =
            futures::future::join_all(http_hosts.iter().map(|url| async {
                let cclient = Client::new(http_client.clone(), url.clone(), sk_auth_token.clone());
                let resp = cclient
                    .timeline_status(request.tenant_id, request.timeline_id)
                    .await?;
                let info: TimelineStatus = resp
                    .json()
                    .await
                    .context("Failed to deserialize timeline status")
                    .map_err(|e| mgmt_api::Error::ReceiveErrorBody(e.to_string()))?;
                Ok(info)
            }))
            .await;

        for (i, response) in responses.into_iter().enumerate() {
            match response {
                Ok(status) => {
                    if let Some(mconf) = &request.mconf {
                        if status.mconf.generation > mconf.generation {
                            // We probably raced with another timeline membership change with higher generation.
                            // Ignore this request.
                            return Err(ApiError::Conflict(format!(
                                "cannot pull timeline with generation {}: timeline {} already exists with generation {} on {}",
                                mconf.generation,
                                request.timeline_id,
                                status.mconf.generation,
                                http_hosts[i],
                            )));
                        }
                    }
                    statuses.push((status, i));
                }
                Err(e) => {
                    info!("error fetching status from {}: {e}", http_hosts[i]);
                }
            }
        }

        // Allow missing responses from up to one safekeeper (say due to downtime)
        // e.g. if we created a timeline on PS A and B, with C being offline. Then B goes
        // offline and C comes online. Then we want a pull on C with A and B as hosts to work.
        let min_required_successful = (http_hosts.len() - 1).max(1);
        if statuses.len() < min_required_successful {
            return Err(ApiError::InternalServerError(anyhow::anyhow!(
                "only got {} successful status responses. required: {min_required_successful}",
                statuses.len()
            )));
        }
    } else {
        let mut retry = true;
        // We must get status from all other peers.
        // Otherwise, we may run into split-brain scenario.
        while retry {
            statuses.clear();
            retry = false;
            for (i, url) in http_hosts.iter().enumerate() {
                let cclient = Client::new(http_client.clone(), url.clone(), sk_auth_token.clone());
                match cclient
                    .timeline_status(request.tenant_id, request.timeline_id)
                    .await
                {
                    Ok(resp) => {
                        if resp.status() == StatusCode::NOT_FOUND {
                            warn!(
                                "Timeline {} not found on peer SK {}, no need to pull it",
                                TenantTimelineId::new(request.tenant_id, request.timeline_id),
                                url
                            );
                            return Ok(PullTimelineResponse {
                                safekeeper_host: None,
                            });
                        }
                        let info: TimelineStatus = resp
                            .json()
                            .await
                            .context("Failed to deserialize timeline status")
                            .map_err(ApiError::InternalServerError)?;
                        statuses.push((info, i));
                    }
                    Err(e) => {
                        match e {
                            // If we get a 404, it means the timeline doesn't exist on this safekeeper.
                            // We can ignore this error.
                            mgmt_api::Error::ApiError(status, _)
                                if status == StatusCode::NOT_FOUND =>
                            {
                                warn!(
                                    "Timeline {} not found on peer SK {}, no need to pull it",
                                    TenantTimelineId::new(request.tenant_id, request.timeline_id),
                                    url
                                );
                                return Ok(PullTimelineResponse {
                                    safekeeper_host: None,
                                });
                            }
                            _ => {}
                        }
                        retry = true;
                        error!("Failed to get timeline status from {}: {:#}", url, e);
                    }
                }
            }
            sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    let max_term = statuses
        .iter()
        .map(|(status, _)| status.acceptor_state.term)
        .max()
        .unwrap();

    // Find the most advanced safekeeper
    let (status, i) = statuses
        .into_iter()
        .max_by_key(|(status, _)| {
            (
                status.acceptor_state.epoch,
                status.flush_lsn,
                /* BEGIN_HADRON */
                // We need to pull from the SK with the highest term.
                // This is because another compute may come online and vote the same highest term again on the other two SKs.
                // Then, there will be 2 computes running on the same term.
                status.acceptor_state.term,
                /* END_HADRON */
                status.commit_lsn,
            )
        })
        .unwrap();
    let safekeeper_host = http_hosts[i].clone();

    assert!(status.tenant_id == request.tenant_id);
    assert!(status.timeline_id == request.timeline_id);

    // TODO(diko): This is hadron only check to make sure that we pull the timeline
    // from the safekeeper with the highest term during timeline restore.
    // We could avoid returning the error by calling bump_term after pull_timeline.
    // However, this is not a big deal because we retry the pull_timeline requests.
    // The check should be removed together with removing custom hadron logic for
    // safekeeper restore.
    if wait_for_peer_timeline_status && status.acceptor_state.term != max_term {
        return Err(ApiError::PreconditionFailed(
            format!(
                "choosen safekeeper {} has term {}, but the most advanced term is {}",
                safekeeper_host, status.acceptor_state.term, max_term
            )
            .into(),
        ));
    }

    match pull_timeline(
        status,
        safekeeper_host,
        sk_auth_token,
        http_client,
        global_timelines,
        request.mconf,
    )
    .await
    {
        Ok(resp) => Ok(resp),
        Err(e) => {
            match e.downcast_ref::<TimelineError>() {
                Some(TimelineError::AlreadyExists(_)) => Ok(PullTimelineResponse {
                    safekeeper_host: None,
                }),
                Some(TimelineError::Deleted(_)) => Err(ApiError::Conflict(format!(
                    "Timeline {}/{} deleted",
                    request.tenant_id, request.timeline_id
                ))),
                Some(TimelineError::CreationInProgress(_)) => {
                    // We don't return success here because creation might still fail.
                    Err(ApiError::Conflict("Creation in progress".to_owned()))
                }
                _ => Err(ApiError::InternalServerError(e)),
            }
        }
    }
}

async fn pull_timeline(
    status: TimelineStatus,
    host: String,
    sk_auth_token: Option<SecretString>,
    http_client: reqwest::Client,
    global_timelines: Arc<GlobalTimelines>,
    mconf: Option<membership::Configuration>,
) -> Result<PullTimelineResponse> {
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

    let conf = &global_timelines.get_global_config();

    let (_tmp_dir, tli_dir_path) = create_temp_timeline_dir(conf, ttid).await?;
    let client = Client::new(http_client, host.clone(), sk_auth_token.clone());
    // Request stream with basebackup archive.
    let bb_resp = client
        .snapshot(status.tenant_id, status.timeline_id, conf.my_id)
        .await?;

    // Make Stream of Bytes from it...
    let bb_stream = bb_resp.bytes_stream().map_err(std::io::Error::other);
    // and turn it into StreamReader implementing AsyncRead.
    let bb_reader = tokio_util::io::StreamReader::new(bb_stream);

    // Extract it on the fly to the disk. We don't use simple unpack() to fsync
    // files.
    let mut entries = Archive::new(bb_reader).entries()?;
    while let Some(base_tar_entry) = entries.next().await {
        let mut entry = base_tar_entry?;
        let header = entry.header();
        let file_path = header.path()?.into_owned();
        match header.entry_type() {
            tokio_tar::EntryType::Regular => {
                let utf8_file_path =
                    Utf8PathBuf::from_path_buf(file_path).expect("non-Unicode path");
                let dst_path = tli_dir_path.join(utf8_file_path);
                let mut f = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&dst_path)
                    .await?;
                tokio::io::copy(&mut entry, &mut f).await?;
                // fsync the file
                f.sync_all().await?;
            }
            _ => {
                bail!(
                    "entry {} in backup tar archive is of unexpected type: {:?}",
                    file_path.display(),
                    header.entry_type()
                );
            }
        }
    }

    // fsync temp timeline directory to remember its contents.
    fsync_async_opt(&tli_dir_path, !conf.no_sync).await?;

    let generation = mconf.as_ref().map(|c| c.generation);

    // Let's create timeline from temp directory and verify that it's correct
    let (commit_lsn, flush_lsn) =
        validate_temp_timeline(conf, ttid, &tli_dir_path, generation).await?;
    info!(
        "finished downloading timeline {}, commit_lsn={}, flush_lsn={}",
        ttid, commit_lsn, flush_lsn
    );
    assert!(status.commit_lsn <= status.flush_lsn);

    // Finally, load the timeline.
    let timeline = global_timelines
        .load_temp_timeline(ttid, &tli_dir_path, generation)
        .await?;

    if let Some(mconf) = mconf {
        // Switch to provided mconf to guarantee that the timeline will not
        // be deleted by request with older generation.
        // The generation might already be higer than the one in mconf, e.g.
        // if another membership_switch request was executed between `load_temp_timeline`
        // and `membership_switch`, but that's totaly fine. `membership_switch` will
        // ignore switch to older generation.
        timeline.membership_switch(mconf).await?;
    }

    Ok(PullTimelineResponse {
        safekeeper_host: Some(host),
    })
}
