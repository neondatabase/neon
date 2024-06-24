//! Code related to evicting WAL files to remote storage. The actual upload is done by the
//! partial WAL backup code. This file has code to delete and re-download WAL files,
//! cross-validate with partial WAL backup if local file is still present.

use anyhow::Context;
use camino::Utf8PathBuf;
use remote_storage::RemotePath;
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncWriteExt},
};
use tracing::{info, instrument, warn};
use utils::crashsafe::durable_rename;

use crate::{
    timeline_manager::{Manager, StateSnapshot},
    wal_backup,
    wal_backup_partial::{self, PartialRemoteSegment},
    wal_storage::wal_file_paths,
};

impl Manager {
    pub(crate) fn ready_for_eviction(
        &self,
        next_cfile_save: &Option<tokio::time::Instant>,
        state: &StateSnapshot,
    ) -> bool {
        self.backup_task.is_none()
            && self.recovery_task.is_none()
            && self.wal_removal_task.is_none()
            && self.partial_backup_task.is_none()
            && self.partial_backup_uploaded.is_some()
            && next_cfile_save.is_none()
            && self.access_service.is_empty()
            && !self.tli_broker_active.get()
            && !wal_backup_partial::needs_uploading(state, &self.partial_backup_uploaded)
            && self
                .partial_backup_uploaded
                .as_ref()
                .unwrap()
                .flush_lsn
                .segment_number(self.wal_seg_size)
                == self.last_removed_segno + 1
    }

    #[instrument(name = "evict_timeline", skip_all)]
    pub(crate) async fn evict_timeline(&mut self) {
        assert!(!self.is_offloaded);
        let partial_backup_uploaded = match &self.partial_backup_uploaded {
            Some(p) => p.clone(),
            None => {
                warn!("no partial backup uploaded, skipping eviction");
                return;
            }
        };

        info!(
            "starting eviction with backup {:?}",
            partial_backup_uploaded
        );

        if let Err(e) = do_eviction(self, &partial_backup_uploaded).await {
            warn!("failed to evict timeline: {:?}", e);
            return;
        }

        info!("successfully evicted timeline");
    }

    #[instrument(name = "unevict_timeline", skip_all)]
    pub(crate) async fn unevict_timeline(&mut self) {
        assert!(self.is_offloaded);
        let partial_backup_uploaded = match &self.partial_backup_uploaded {
            Some(p) => p.clone(),
            None => {
                warn!("no partial backup uploaded, skipping eviction");
                return;
            }
        };

        info!(
            "starting uneviction with backup {:?}",
            partial_backup_uploaded
        );

        if let Err(e) = do_uneviction(self, &partial_backup_uploaded).await {
            warn!("failed to unevict timeline: {:?}", e);
            return;
        }

        info!("successfully restored evicted timeline");
    }
}

async fn do_eviction(mgr: &mut Manager, partial: &PartialRemoteSegment) -> anyhow::Result<()> {
    validate_local_segment(mgr, partial).await?;

    mgr.tli.switch_to_offloaded(partial).await?;
    // switch manager state as soon as possible
    mgr.is_offloaded = true;

    if mgr.conf.delete_offloaded_wal {
        delete_local_segment(mgr, partial).await?;
    }

    Ok(())
}

async fn do_uneviction(mgr: &mut Manager, partial: &PartialRemoteSegment) -> anyhow::Result<()> {
    // if the local segment is present, validate it
    validate_local_segment(mgr, partial).await?;

    // TODO: file can be invalid because previous download failed, we shouldn't
    // prevent uneviction in that case

    redownload_partial_segment(mgr, partial).await?;

    mgr.tli.switch_to_present().await?;
    // switch manager state as soon as possible
    mgr.is_offloaded = false;

    Ok(())
}

async fn delete_local_segment(mgr: &Manager, partial: &PartialRemoteSegment) -> anyhow::Result<()> {
    let local_path = local_segment_path(mgr, partial);

    info!("deleting WAL file to evict: {}", local_path,);
    tokio::fs::remove_file(&local_path).await?;
    Ok(())
}

async fn redownload_partial_segment(
    mgr: &Manager,
    partial: &PartialRemoteSegment,
) -> anyhow::Result<()> {
    let tmp_file = mgr.tli.timeline_dir().join("remote_partial.tmp");
    let remote_segfile = remote_segment_path(mgr, partial)?;

    info!(
        "redownloading partial segment: {} -> {}",
        remote_segfile, tmp_file
    );

    let mut reader = wal_backup::read_object(&remote_segfile, 0).await?;
    let mut file = File::create(&tmp_file).await?;

    let actual_len = tokio::io::copy(&mut reader, &mut file).await?;
    let expected_len = partial.flush_lsn.segment_offset(mgr.wal_seg_size);

    if actual_len != expected_len as u64 {
        anyhow::bail!(
            "partial downloaded {} bytes, expected {}",
            actual_len,
            expected_len
        );
    }

    info!(
        "downloaded {} bytes, resizing the file to wal_seg_size = {}",
        actual_len, mgr.wal_seg_size
    );
    assert!(actual_len <= mgr.wal_seg_size as u64);
    file.set_len(mgr.wal_seg_size as u64).await?;
    file.flush().await?;

    let final_path = local_segment_path(mgr, partial);
    if let Err(e) = durable_rename(&tmp_file, &final_path, !mgr.conf.no_sync).await {
        // Probably rename succeeded, but fsync of it failed. Remove
        // the file then to avoid using it.
        tokio::fs::remove_file(tmp_file)
            .await
            .or_else(utils::fs_ext::ignore_not_found)?;
        return Err(e.into());
    }

    Ok(())
}

async fn validate_local_segment(
    mgr: &Manager,
    partial: &PartialRemoteSegment,
) -> anyhow::Result<()> {
    let local_path = local_segment_path(mgr, partial);

    match File::open(&local_path).await {
        Ok(mut local_file) => do_validation(mgr, &mut local_file, mgr.wal_seg_size, partial)
            .await
            .context("validation failed"),
        Err(_) => {
            info!(
                "local WAL file {} is not present, skipping validation",
                local_path
            );
            Ok(())
        }
    }
}

async fn do_validation(
    mgr: &Manager,
    file: &mut File,
    wal_seg_size: usize,
    partial: &PartialRemoteSegment,
) -> anyhow::Result<()> {
    let local_size = file.metadata().await?.len() as usize;
    if local_size != wal_seg_size {
        anyhow::bail!(
            "local segment size is invalid: found {}, expected {}",
            local_size,
            wal_seg_size
        );
    }

    let remote_segfile = remote_segment_path(mgr, partial)?;
    let mut remote_reader: std::pin::Pin<Box<dyn AsyncRead + Send + Sync>> =
        wal_backup::read_object(&remote_segfile, 0).await?;

    let expected_remote_size = partial.flush_lsn.segment_offset(mgr.wal_seg_size);
    compare_n_bytes(&mut remote_reader, file, expected_remote_size).await?;
    check_end(&mut remote_reader).await?;

    read_n_zeroes(file, mgr.wal_seg_size - expected_remote_size).await?;
    check_end(file).await?;

    Ok(())
}

fn local_segment_path(mgr: &Manager, partial: &PartialRemoteSegment) -> Utf8PathBuf {
    let flush_lsn = partial.flush_lsn;
    let segno = flush_lsn.segment_number(mgr.wal_seg_size);
    let (_, local_partial_segfile) =
        wal_file_paths(mgr.tli.timeline_dir(), segno, mgr.wal_seg_size);
    local_partial_segfile
}

fn remote_segment_path(
    mgr: &Manager,
    partial: &PartialRemoteSegment,
) -> anyhow::Result<RemotePath> {
    let remote_timeline_path = wal_backup::remote_timeline_path(&mgr.tli.ttid)?;
    Ok(remote_timeline_path.join(&partial.name))
}

async fn compare_n_bytes<R1, R2>(reader1: &mut R1, reader2: &mut R2, n: usize) -> anyhow::Result<()>
where
    R1: AsyncRead + Unpin,
    R2: AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;

    const BUF_SIZE: usize = 32 * 1024;

    let mut buffer1 = [0u8; BUF_SIZE];
    let mut buffer2 = [0u8; BUF_SIZE];

    let mut offset = 0;

    while offset < n {
        let bytes_to_read = std::cmp::min(BUF_SIZE, n - offset);

        let bytes_read1 = reader1
            .read(&mut buffer1[..bytes_to_read])
            .await
            .with_context(|| format!("failed to read from reader1 at offset {}", offset))?;
        if bytes_read1 == 0 {
            anyhow::bail!("unexpected EOF from reader1 at offset {}", offset);
        }

        let bytes_read2 = reader2
            .read_exact(&mut buffer2[..bytes_read1])
            .await
            .with_context(|| {
                format!(
                    "failed to read {} bytes from reader2 at offset {}",
                    bytes_read1, offset
                )
            })?;
        assert!(bytes_read2 == bytes_read1);

        if buffer1[..bytes_read1] != buffer2[..bytes_read2] {
            let diff_offset = buffer1[..bytes_read1]
                .iter()
                .zip(buffer2[..bytes_read2].iter())
                .position(|(a, b)| a != b)
                .expect("mismatched buffers, but no difference found");
            anyhow::bail!("mismatch at offset {}", offset + diff_offset);
        }

        offset += bytes_read1;
    }

    Ok(())
}

async fn check_end<R>(mut reader: R) -> anyhow::Result<()>
where
    R: AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;

    let mut buffer = [0u8; 1];
    let bytes_read = reader.read(&mut buffer).await?;
    if bytes_read != 0 {
        anyhow::bail!("expected EOF, found bytes");
    }
    Ok(())
}

async fn read_n_zeroes<R>(reader: &mut R, n: usize) -> anyhow::Result<()>
where
    R: AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;

    const BUF_SIZE: usize = 32 * 1024;
    let mut buffer = [0u8; BUF_SIZE];
    let mut offset = 0;

    while offset < n {
        let bytes_to_read = std::cmp::min(BUF_SIZE, n - offset);

        let bytes_read = reader
            .read(&mut buffer[..bytes_to_read])
            .await
            .context("expected zeroes, got read error")?;
        if bytes_read == 0 {
            anyhow::bail!("expected zeroes, got EOF");
        }

        if buffer[..bytes_read].iter().all(|&b| b == 0) {
            offset += bytes_read;
        } else {
            anyhow::bail!("non-zero byte found");
        }
    }

    Ok(())
}
