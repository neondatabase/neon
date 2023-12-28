use anyhow::{bail, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::{ArgAction, Parser};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use postgres_ffi::{WAL_SEGMENT_SIZE, dispatch_pgversion};
use postgres_ffi::v16::xlog_utils::{XLogSegNoOffsetToRecPtr, find_end_of_wal};
use remote_storage::RemoteStorageConfig;
use safekeeper::control_file::FileStorage;
use safekeeper::safekeeper::SafeKeeperState;
use safekeeper::wal_storage::wal_file_paths;
use sd_notify::NotifyState;
use tokio::runtime::Handle;
use tokio::signal::unix::{signal, SignalKind};
use tokio::task::JoinError;
use toml_edit::Document;
use utils::id::{TenantId, TimelineId, TenantTimelineId};
use utils::lsn::Lsn;

use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write, Read, Seek};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use storage_broker::Uri;
use tokio::sync::mpsc;

use tracing::*;
use utils::pid_file;

use metrics::set_build_info_metric;
use safekeeper::defaults::{
    DEFAULT_HEARTBEAT_TIMEOUT, DEFAULT_HTTP_LISTEN_ADDR, DEFAULT_MAX_OFFLOADER_LAG_BYTES,
    DEFAULT_PG_LISTEN_ADDR,
};
use safekeeper::wal_service;
use safekeeper::GlobalTimelines;
use safekeeper::SafeKeeperConf;
use safekeeper::{broker, WAL_SERVICE_RUNTIME};
use safekeeper::{control_file, BROKER_RUNTIME};
use safekeeper::{http, WAL_REMOVER_RUNTIME};
use safekeeper::{remove_wal, WAL_BACKUP_RUNTIME};
use safekeeper::{wal_backup, HTTP_RUNTIME};
use storage_broker::DEFAULT_ENDPOINT;
use utils::auth::{JwtAuth, Scope, SwappableJwtAuth};
use utils::{
    id::NodeId,
    logging::{self, LogFormat},
    project_build_tag, project_git_version,
    sentry_init::init_sentry,
    tcp_listener,
};

const PID_FILE_NAME: &str = "safekeeper.pid";
const ID_FILE_NAME: &str = "safekeeper.id";

const CONTROL_FILE_NAME: &str = "safekeeper.control";

project_git_version!(GIT_VERSION);
project_build_tag!(BUILD_TAG);

const ABOUT: &str = r#"
Fixing the issue of some WAL files missing the prefix bytes.
"#;

#[derive(Parser)]
#[command(name = "Neon safekeeper", version = GIT_VERSION, about = ABOUT, long_about = None)]
struct Args {
    /// Path to the data2 directory.
    datafrom: Utf8PathBuf,
    /// Path to the data directory.
    datato: Utf8PathBuf,
    #[arg(long, default_value = "false", action=ArgAction::Set)]
    dryrun: bool,
}

struct TimelineDirInfo {
    ttid: TenantTimelineId,
    timeline_dir: Utf8PathBuf,
    control_file: SafeKeeperState,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    // We want to allow multiple occurences of the same arg (taking the last) so
    // that neon_local could generate command with defaults + overrides without
    // getting 'argument cannot be used multiple times' error. This seems to be
    // impossible with pure Derive API, so convert struct to Command, modify it,
    // parse arguments, and then fill the struct back.
    let cmd = <Args as clap::CommandFactory>::command().args_override_self(true);
    let mut matches = cmd.get_matches();
    let mut args = <Args as clap::FromArgMatches>::from_arg_matches_mut(&mut matches)?;

    logging::init(
        LogFormat::from_config("plain")?,
        logging::TracingErrorLayerEnablement::Disabled,
        logging::Output::Stdout,
    )?;

    let all_timelines = read_all_timelines(&args.datafrom).await?;

    let wal_seg_size = WAL_SEGMENT_SIZE;

    for tli in all_timelines {
        assert!(tli.control_file.local_start_lsn == tli.control_file.timeline_start_lsn);
        info!("Found timeline {}, start_lsn={}, commit_lsn={}", tli.ttid, tli.control_file.local_start_lsn, tli.control_file.commit_lsn);
    
        let new_tli_dir = args.datato.join(tli.ttid.tenant_id.to_string()).join(tli.ttid.timeline_id.to_string());
        
        // check existence
        if !new_tli_dir.exists() {
            info!("Timeline {} does not exist in the target directory {}", tli.ttid, new_tli_dir);
            if args.dryrun {
                continue;
            }
            copy_directory(&tli, &new_tli_dir).await?;
            continue;
        }

        let new_tli = read_timeline(tli.ttid.clone(), new_tli_dir.as_path().as_std_path()).await?;
        if new_tli.control_file.local_start_lsn == tli.control_file.timeline_start_lsn {
            info!("Timeline {} is already fixed in the target directory {}", tli.ttid, new_tli_dir);
            continue;
        }

        let segnum = new_tli.control_file.local_start_lsn.segment_number(wal_seg_size);
        let valid_segnames = wal_file_paths(&tli.timeline_dir, segnum, wal_seg_size)?;
        let new_segnames = wal_file_paths(&new_tli.timeline_dir, segnum, wal_seg_size)?;

        info!(
            "Timeline {} has local_start_lsn={}, timeline_start_lsn={}, commit_lsn={} //// can be fixed with bytes from {} up to commit_lsn={}",
            new_tli.ttid,
            new_tli.control_file.local_start_lsn,
            new_tli.control_file.timeline_start_lsn,
            new_tli.control_file.commit_lsn,
            valid_segnames.0,
            tli.control_file.commit_lsn,
        );
        assert!(new_tli.control_file.timeline_start_lsn == tli.control_file.timeline_start_lsn);

        let new_segname = if new_segnames.0.exists() {
            new_segnames.0
        } else if new_segnames.1.exists() {
            new_segnames.1
        } else {
            info!("Segment {} was already deleted, nothing to backfill", new_segnames.0);
            continue;
        };

        let valid_segname = if valid_segnames.0.exists() {
            valid_segnames.0
        } else if valid_segnames.1.exists() {
            valid_segnames.1
        } else {
            panic!("Cannot find valid segment for timeline {}, this file doesn't exist {}", tli.ttid, valid_segnames.0);
        };

        let mut copy_start_lsn = XLogSegNoOffsetToRecPtr(segnum, 0, wal_seg_size);
        if tli.control_file.timeline_start_lsn.0 > copy_start_lsn {
            copy_start_lsn = tli.control_file.timeline_start_lsn.0;
        }

        let copy_start_lsn = Lsn(copy_start_lsn);
        let copy_end_lsn = new_tli.control_file.local_start_lsn;

        assert!(copy_end_lsn >= copy_start_lsn);

        if args.dryrun {
            continue;
        }

        let pg_version = tli.control_file.server.pg_version / 10000;
        // find the flush_lsn from data2
        let flush_lsn = dispatch_pgversion!(
            pg_version,
            pgv::xlog_utils::find_end_of_wal(
                tli.timeline_dir.as_path().as_std_path(),
                wal_seg_size,
                tli.control_file.commit_lsn,
            )?,
            bail!("unsupported postgres version: {}", pg_version)
        );

        info!("ACTION: Copying bytes {} - {} (commit_lsn={}, flush_lsn={}) from {} to {}", copy_start_lsn, copy_end_lsn, tli.control_file.commit_lsn, flush_lsn, valid_segname, new_segname);

        assert!(copy_end_lsn <= flush_lsn);
        assert!(new_tli.control_file.commit_lsn >= new_tli.control_file.local_start_lsn);

        if flush_lsn > copy_end_lsn {
            // check intersection from two segments
            let valid_bytes = read_slice(&valid_segname, copy_end_lsn, flush_lsn, wal_seg_size)?;
            let new_bytes = read_slice(&new_segname, copy_end_lsn, flush_lsn, wal_seg_size)?;
            info!("Checking bytes intersection, from {} up to {}", copy_end_lsn, flush_lsn);
            assert!(valid_bytes == new_bytes);
        }

        if copy_end_lsn > tli.control_file.commit_lsn {
            info!("Missing some committed bytes");
        }

        let valid_slice = read_slice(&valid_segname, copy_start_lsn, copy_end_lsn, wal_seg_size)?;

        info!("Read {} bytes, going to write", valid_slice.len());
        assert!(valid_slice.len() == (copy_end_lsn.0 - copy_start_lsn.0) as usize);

        write_slice(&new_segname, copy_start_lsn, wal_seg_size, &valid_slice)?;
        info!("Success, timeline {} WAL is fixed", new_tli.ttid);
    }

    Ok(())
}

async fn read_all_timelines(dir: &Utf8Path) -> Result<Vec<TimelineDirInfo>> {
    info!("Reading all timelines from {:?}", dir);

    let mut timelines = Vec::new();
    for tenant_entry in fs::read_dir(dir).with_context(|| format!("Failed to read {:?}", dir))? {
        let tenant_entry = tenant_entry.with_context(|| format!("Failed to read {:?}", dir))?;
        let path = tenant_entry.path();
        if !path.is_dir() {
            info!("Skipping non-directory {:?}", path);
            continue;
        }
        let dirname = path.file_name().unwrap().to_str().unwrap();
        let tenant_id = TenantId::from_str(dirname);
        if tenant_id.is_err() {
            info!("Skipping non-tenant directory {:?}", path);
            continue;
        }
        let tenant_id = tenant_id.unwrap();

        for timeline_entry in fs::read_dir(&path).with_context(|| format!("Failed to read {:?}", path))?
        {
            let timeline_entry =
                timeline_entry.with_context(|| format!("Failed to read {:?}", path))?;
            let path = timeline_entry.path();
            if !path.is_dir() {
                info!("Skipping non-directory {:?}", path);
                continue;
            }
            let dirname = path.file_name().unwrap().to_str().unwrap();
            let timeline_id = TimelineId::from_str(dirname);
            if timeline_id.is_err() {
                info!("Skipping non-timeline directory {:?}", path);
                continue;
            }
            let timeline_id = timeline_id.unwrap();
            let ttid = TenantTimelineId::new(tenant_id, timeline_id);

            let tliinfo = read_timeline(ttid, &path).await?;
            timelines.push(tliinfo);
        }
    }
    Ok(timelines)
}

async fn read_timeline(ttid: TenantTimelineId, dir: &Path) -> Result<TimelineDirInfo> {
    let control_file_path = dir.join(CONTROL_FILE_NAME);
    let control_file = FileStorage::load_control_file(control_file_path)?;
    Ok(TimelineDirInfo {
        ttid,
        timeline_dir: Utf8PathBuf::from_path_buf(dir.to_path_buf()).expect("valid utf8"),
        control_file,
    })
}

async fn copy_directory(tli: &TimelineDirInfo, new_tli_dir: &Utf8Path) -> Result<()> {
    info!("ACTION: Copying timeline {} to {}", tli.ttid, new_tli_dir);
    // TODO: 
    Ok(())
}

fn read_slice(path: &Utf8Path, start: Lsn, end: Lsn, wal_seg_size: usize) -> Result<Vec<u8>> {
    assert!(end >= start);
    let start = start.segment_offset(wal_seg_size);
    let end = end.segment_offset(wal_seg_size);
    assert!(end >= start);

    let mut buf = Vec::new();
    let mut file = File::open(path)?;
    file.seek(std::io::SeekFrom::Start(start as u64))?;
    file.take((end - start) as u64).read_to_end(&mut buf)?;
    Ok(buf)
}

fn write_slice(path: &Utf8Path, start: Lsn, wal_seg_size: usize, buf: &[u8]) -> Result<()> {
    let start = start.segment_offset(wal_seg_size);
    let mut file = OpenOptions::new().write(true).open(path)?;
    file.seek(std::io::SeekFrom::Start(start as u64))?;
    file.write_all(buf)?;
    Ok(())
}
