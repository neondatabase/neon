use std::sync::Arc;

use anyhow::{bail, Result};
use camino::Utf8PathBuf;

use postgres_ffi::{MAX_SEND_SIZE};
use tokio::{
    fs::{OpenOptions},
    io::{AsyncSeekExt, AsyncWriteExt},
};
use tracing::{info, warn};
use utils::{id::TenantTimelineId, lsn::Lsn};

use crate::{
    control_file::{FileStorage, Storage},
    pull_timeline::{create_temp_timeline_dir, load_temp_timeline, validate_temp_timeline},
    safekeeper::{SafeKeeperState},
    timeline::{Timeline, TimelineError},
    wal_storage::{wal_file_paths, WalReader},
    GlobalTimelines,
};

pub struct Request {
    pub source: Arc<Timeline>,
    pub until_lsn: Lsn,
    pub destination_ttid: TenantTimelineId,
}

pub async fn handle_request(request: Request) -> Result<()> {
    // TODO: request.until_lsn MUST be a valid LSN, and we cannot check it :(

    match GlobalTimelines::get(request.destination_ttid) {
        // timeline already exists. would be good to check that this timeline is the copy
        // of the source timeline, but it isn't obvious how to do that
        Ok(_) => return Ok(()),
        // timeline not found, we are going to create it
        Err(TimelineError::NotFound(_)) => {}
        // error, probably timeline was already cancelled
        res => {
            res?;
        }
    }

    let conf = &GlobalTimelines::get_global_config();
    let ttid = request.destination_ttid;

    let (_tmp_dir, tli_dir_path) = create_temp_timeline_dir(conf, ttid).await?;

    let (mem_state, state) = request.source.get_state().await;
    let start_lsn = state.timeline_start_lsn;
    if start_lsn == Lsn::INVALID {
        bail!("timeline is not initialized");
    }

    {
        let commit_lsn = mem_state.commit_lsn;
        let flush_lsn = request.source.get_flush_lsn().await;

        info!(
            "collected info about source timeline: start_lsn={}, commit_lsn={}, flush_lsn={}",
            start_lsn, commit_lsn, flush_lsn
        );

        assert!(commit_lsn >= start_lsn);
        assert!(flush_lsn >= start_lsn);

        if request.until_lsn > flush_lsn {
            bail!("requested LSN is beyond the end of the timeline");
        }
        if request.until_lsn < start_lsn {
            bail!("requested LSN is before the start of the timeline");
        }

        if request.until_lsn > commit_lsn {
            warn!("copy_timeline WAL is not fully committed");
        }
    }

    let wal_seg_size = state.server.wal_seg_size as usize;
    if wal_seg_size == 0 {
        bail!("wal_seg_size is not set");
    }

    let first_segment = start_lsn.segment_number(wal_seg_size);
    let last_segment = request.until_lsn.segment_number(wal_seg_size);

    let mut wal_reader = WalReader::new(
        conf.workdir.clone(),
        conf.timeline_dir(&request.source.ttid),
        &state,
        start_lsn,
        true,
    )?;

    let mut buf = [0u8; MAX_SEND_SIZE];

    for segment in first_segment..=last_segment {
        let segment_start = segment * wal_seg_size as u64;
        let segment_end = segment_start + wal_seg_size as u64;

        let copy_start = segment_start.max(start_lsn.0);
        let copy_end = segment_end.min(request.until_lsn.0);

        let copy_start = copy_start - segment_start;
        let copy_end = copy_end - segment_start;

        let wal_file_path = {
            let (normal, partial) = wal_file_paths(&tli_dir_path, segment, wal_seg_size)?;

            if segment == last_segment {
                partial
            } else {
                normal
            }
        };

        write_segment(
            &mut buf,
            &wal_file_path,
            wal_seg_size as u64,
            copy_start,
            copy_end,
            &mut wal_reader,
        )
        .await?;
    }

    let mut new_state = SafeKeeperState::new(
        &request.destination_ttid,
        state.server.clone(),
        vec![],
        request.until_lsn,
        start_lsn,
    );
    new_state.timeline_start_lsn = start_lsn;

    let mut file_storage = FileStorage::create_new(tli_dir_path.clone(), conf, new_state.clone())?;
    file_storage.persist(&new_state).await?;

    // now we have a ready timeline in a temp directory
    validate_temp_timeline(conf, request.destination_ttid, &tli_dir_path).await?;
    load_temp_timeline(conf, request.destination_ttid, &tli_dir_path).await?;

    Ok(())
}

async fn write_segment(
    buf: &mut [u8],
    file_path: &Utf8PathBuf,
    wal_seg_size: u64,
    from: u64,
    to: u64,
    reader: &mut WalReader,
) -> Result<()> {
    assert!(from <= to);
    assert!(to <= wal_seg_size);

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(&file_path)
        .await?;

    // maybe fill with zeros, as in wal_storage.rs?
    file.set_len(wal_seg_size).await?;
    file.seek(std::io::SeekFrom::Start(from)).await?;

    let mut bytes_left = to - from;
    while bytes_left > 0 {
        let len = bytes_left as usize;
        let len = len.min(buf.len());
        let len = reader.read(&mut buf[..len]).await?;
        file.write_all(&buf[..len]).await?;
        bytes_left -= len as u64;
    }

    file.flush().await?;
    file.sync_all().await?;
    Ok(())
}
