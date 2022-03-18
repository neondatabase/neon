//!
//! Timeline management code
//

use anyhow::{anyhow, bail, Context, Result};
use postgres_ffi::ControlFileData;
use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
    sync::Arc,
};
use tracing::*;

use zenith_utils::lsn::Lsn;
use zenith_utils::zid::{ZTenantId, ZTimelineId};
use zenith_utils::{crashsafe_dir, logging};

use crate::DatadirTimeline;
use crate::RepositoryImpl;
use crate::{config::PageServerConf, repository::Repository};
use crate::{import_datadir, LOG_FILE_NAME};
use crate::{layered_repository::LayeredRepository, walredo::WalRedoManager};
use crate::{repository::RepositoryTimeline, tenant_mgr};
use crate::{repository::Timeline, CheckpointConfig};

#[derive(Clone)]
pub enum TimelineInfo {
    Local {
        timeline_id: ZTimelineId,
        tenant_id: ZTenantId,
        last_record_lsn: Lsn,
        prev_record_lsn: Lsn,
        ancestor_timeline_id: Option<ZTimelineId>,
        ancestor_lsn: Option<Lsn>,
        disk_consistent_lsn: Lsn,
        current_logical_size: usize,
        current_logical_size_non_incremental: Option<usize>,
    },
    Remote {
        timeline_id: ZTimelineId,
        tenant_id: ZTenantId,
        disk_consistent_lsn: Lsn,
    },
}

impl TimelineInfo {
    pub fn from_ids(
        tenant_id: ZTenantId,
        timeline_id: ZTimelineId,
        include_non_incremental_logical_size: bool,
    ) -> Result<Self> {
        let repo = tenant_mgr::get_repository_for_tenant(tenant_id)?;
        let result = match repo.get_timeline(timeline_id)? {
            RepositoryTimeline::Local { id, timeline } => {
                let ancestor_timeline_id = timeline.get_ancestor_timeline_id();
                let ancestor_lsn = if ancestor_timeline_id.is_some() {
                    Some(timeline.get_ancestor_lsn())
                } else {
                    None
                };

                let tline = tenant_mgr::get_timeline_for_tenant(tenant_id, timeline_id)?;
                let current_logical_size = tline.get_current_logical_size();
                let current_logical_size_non_incremental = get_current_logical_size_non_incremental(
                    include_non_incremental_logical_size,
                    tline.as_ref(),
                );

                Self::Local {
                    timeline_id: id,
                    tenant_id,
                    last_record_lsn: timeline.get_last_record_lsn(),
                    prev_record_lsn: timeline.get_prev_record_lsn(),
                    ancestor_timeline_id,
                    ancestor_lsn,
                    disk_consistent_lsn: timeline.get_disk_consistent_lsn(),
                    current_logical_size,
                    current_logical_size_non_incremental,
                }
            }
            RepositoryTimeline::Remote {
                id,
                disk_consistent_lsn,
            } => Self::Remote {
                timeline_id: id,
                tenant_id,
                disk_consistent_lsn,
            },
        };
        Ok(result)
    }

    pub fn timeline_id(&self) -> ZTimelineId {
        match *self {
            TimelineInfo::Local { timeline_id, .. } => timeline_id,
            TimelineInfo::Remote { timeline_id, .. } => timeline_id,
        }
    }

    pub fn tenant_id(&self) -> ZTenantId {
        match *self {
            TimelineInfo::Local { tenant_id, .. } => tenant_id,
            TimelineInfo::Remote { tenant_id, .. } => tenant_id,
        }
    }
}

fn get_current_logical_size_non_incremental<R: Repository>(
    include_non_incremental_logical_size: bool,
    timeline: &DatadirTimeline<R>,
) -> Option<usize> {
    if !include_non_incremental_logical_size {
        return None;
    }
    match timeline.get_current_logical_size_non_incremental(timeline.get_last_record_lsn()) {
        Ok(size) => Some(size),
        Err(e) => {
            error!("Failed to get non-incremental logical size: {:?}", e);
            None
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PointInTime {
    pub timeline_id: ZTimelineId,
    pub lsn: Lsn,
}

pub fn init_pageserver(
    conf: &'static PageServerConf,
    create_tenant: Option<ZTenantId>,
    initial_timeline_id: Option<ZTimelineId>,
) -> anyhow::Result<()> {
    // Initialize logger
    // use true as daemonize parameter because otherwise we pollute zenith cli output with a few pages long output of info messages
    let _log_file = logging::init(LOG_FILE_NAME, true)?;

    // We don't use the real WAL redo manager, because we don't want to spawn the WAL redo
    // process during repository initialization.
    //
    // FIXME: That caused trouble, because the WAL redo manager spawned a thread that launched
    // initdb in the background, and it kept running even after the "zenith init" had exited.
    // In tests, we started the  page server immediately after that, so that initdb was still
    // running in the background, and we failed to run initdb again in the same directory. This
    // has been solved for the rapid init+start case now, but the general race condition remains
    // if you restart the server quickly. The WAL redo manager doesn't use a separate thread
    // anymore, but I think that could still happen.
    let dummy_redo_mgr = Arc::new(crate::walredo::DummyRedoManager {});

    crashsafe_dir::create_dir_all(conf.tenants_path())?;

    if let Some(tenant_id) = create_tenant {
        println!("initializing tenantid {}", tenant_id);
        let repo = create_repo(conf, tenant_id, dummy_redo_mgr)
            .context("failed to create repo")?
            .ok_or_else(|| anyhow!("For newely created pageserver, found already existing repository for tenant {}", tenant_id))?;
        let new_timeline_id = initial_timeline_id.unwrap_or_else(ZTimelineId::generate);
        bootstrap_timeline(conf, tenant_id, new_timeline_id, repo.as_ref())
            .context("failed to create initial timeline")?;
        println!("initial timeline {} created", new_timeline_id)
    } else if initial_timeline_id.is_some() {
        println!("Ignoring initial timeline parameter, due to no tenant id to create given");
    }

    println!("pageserver init succeeded");
    Ok(())
}

pub fn create_repo(
    conf: &'static PageServerConf,
    tenant_id: ZTenantId,
    wal_redo_manager: Arc<dyn WalRedoManager + Send + Sync>,
) -> Result<Option<Arc<RepositoryImpl>>> {
    let repo_dir = conf.tenant_path(&tenant_id);
    if repo_dir.exists() {
        debug!("repo for {} already exists", tenant_id);
        return Ok(None);
    }

    // top-level dir may exist if we are creating it through CLI
    crashsafe_dir::create_dir_all(&repo_dir)
        .with_context(|| format!("could not create directory {}", repo_dir.display()))?;
    crashsafe_dir::create_dir(conf.timelines_path(&tenant_id))?;
    info!("created directory structure in {}", repo_dir.display());

    Ok(Some(Arc::new(LayeredRepository::new(
        conf,
        wal_redo_manager,
        tenant_id,
        conf.remote_storage_config.is_some(),
    ))))
}

// Returns checkpoint LSN from controlfile
fn get_lsn_from_controlfile(path: &Path) -> Result<Lsn> {
    // Read control file to extract the LSN
    let controlfile_path = path.join("global").join("pg_control");
    let controlfile = ControlFileData::decode(&fs::read(controlfile_path)?)?;
    let lsn = controlfile.checkPoint;

    Ok(Lsn(lsn))
}

// Create the cluster temporarily in 'initdbpath' directory inside the repository
// to get bootstrap data for timeline initialization.
//
fn run_initdb(conf: &'static PageServerConf, initdbpath: &Path) -> Result<()> {
    info!("running initdb in {}... ", initdbpath.display());

    let initdb_path = conf.pg_bin_dir().join("initdb");
    let initdb_output = Command::new(initdb_path)
        .args(&["-D", initdbpath.to_str().unwrap()])
        .args(&["-U", &conf.superuser])
        .args(&["-E", "utf8"])
        .arg("--no-instructions")
        // This is only used for a temporary installation that is deleted shortly after,
        // so no need to fsync it
        .arg("--no-sync")
        .env_clear()
        .env("LD_LIBRARY_PATH", conf.pg_lib_dir().to_str().unwrap())
        .env("DYLD_LIBRARY_PATH", conf.pg_lib_dir().to_str().unwrap())
        .stdout(Stdio::null())
        .output()
        .context("failed to execute initdb")?;
    if !initdb_output.status.success() {
        bail!(
            "initdb failed: '{}'",
            String::from_utf8_lossy(&initdb_output.stderr)
        );
    }

    Ok(())
}

//
// - run initdb to init temporary instance and get bootstrap data
// - after initialization complete, remove the temp dir.
//
fn bootstrap_timeline<R: Repository>(
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
    tli: ZTimelineId,
    repo: &R,
) -> Result<()> {
    let _enter = info_span!("bootstrapping", timeline = %tli, tenant = %tenantid).entered();

    let initdb_path = conf.tenant_path(&tenantid).join("tmp");

    // Init temporarily repo to get bootstrap data
    run_initdb(conf, &initdb_path)?;
    let pgdata_path = initdb_path;

    let lsn = get_lsn_from_controlfile(&pgdata_path)?.align();

    // Import the contents of the data directory at the initial checkpoint
    // LSN, and any WAL after that.
    // Initdb lsn will be equal to last_record_lsn which will be set after import.
    // Because we know it upfront avoid having an option or dummy zero value by passing it to create_empty_timeline.
    let timeline = repo.create_empty_timeline(tli, lsn)?;
    let mut page_tline: DatadirTimeline<R> = DatadirTimeline::new(timeline, u64::MAX);
    import_datadir::import_timeline_from_postgres_datadir(&pgdata_path, &mut page_tline, lsn)?;
    page_tline.tline.checkpoint(CheckpointConfig::Forced)?;

    println!(
        "created initial timeline {} timeline.lsn {}",
        tli,
        page_tline.tline.get_last_record_lsn()
    );

    // Remove temp dir. We don't need it anymore
    fs::remove_dir_all(pgdata_path)?;

    Ok(())
}

pub(crate) fn get_timelines(
    tenant_id: ZTenantId,
    include_non_incremental_logical_size: bool,
) -> Result<Vec<TimelineInfo>> {
    let repo = tenant_mgr::get_repository_for_tenant(tenant_id)
        .with_context(|| format!("Failed to get repo for tenant {}", tenant_id))?;

    let mut result = Vec::new();
    for timeline in repo
        .list_timelines()
        .with_context(|| format!("Failed to list timelines for tenant {}", tenant_id))?
    {
        match timeline {
            RepositoryTimeline::Local {
                timeline: _,
                id: timeline_id,
            } => {
                result.push(TimelineInfo::from_ids(
                    tenant_id,
                    timeline_id,
                    include_non_incremental_logical_size,
                )?);
            }
            RepositoryTimeline::Remote { .. } => continue,
        }
    }
    Ok(result)
}

pub(crate) fn create_timeline(
    conf: &'static PageServerConf,
    tenant_id: ZTenantId,
    new_timeline_id: Option<ZTimelineId>,
    ancestor_timeline_id: Option<ZTimelineId>,
    ancestor_start_lsn: Option<Lsn>,
) -> Result<Option<TimelineInfo>> {
    let new_timeline_id = new_timeline_id.unwrap_or_else(ZTimelineId::generate);
    let repo = tenant_mgr::get_repository_for_tenant(tenant_id)?;

    if conf.timeline_path(&new_timeline_id, &tenant_id).exists() {
        match repo.get_timeline(new_timeline_id)? {
            RepositoryTimeline::Local { id, .. } => {
                debug!("timeline {} already exists", id);
                return Ok(None);
            }
            RepositoryTimeline::Remote { id, .. } => bail!(
                "timeline {} already exists in pageserver's remote storage",
                id
            ),
        }
    }

    let mut start_lsn = ancestor_start_lsn.unwrap_or(Lsn(0));

    match ancestor_timeline_id {
        Some(ancestor_timeline_id) => {
            let ancestor_timeline = repo
                .get_timeline(ancestor_timeline_id)
                .with_context(|| format!("Cannot get ancestor timeline {}", ancestor_timeline_id))?
                .local_timeline()
                .with_context(|| {
                    format!(
                        "Cannot branch off the timeline {} that's not present locally",
                        ancestor_timeline_id
                    )
                })?;

            if start_lsn == Lsn(0) {
                // Find end of WAL on the old timeline
                let end_of_wal = ancestor_timeline.get_last_record_lsn();
                info!("branching at end of WAL: {}", end_of_wal);
                start_lsn = end_of_wal;
            } else {
                // Wait for the WAL to arrive and be processed on the parent branch up
                // to the requested branch point. The repository code itself doesn't
                // require it, but if we start to receive WAL on the new timeline,
                // decoding the new WAL might need to look up previous pages, relation
                // sizes etc. and that would get confused if the previous page versions
                // are not in the repository yet.
                ancestor_timeline.wait_lsn(start_lsn)?;
            }
            start_lsn = start_lsn.align();

            let ancestor_ancestor_lsn = ancestor_timeline.get_ancestor_lsn();
            if ancestor_ancestor_lsn > start_lsn {
                // can we safely just branch from the ancestor instead?
                anyhow::bail!(
                    "invalid start lsn {} for ancestor timeline {}: less than timeline ancestor lsn {}",
                    start_lsn,
                    ancestor_timeline_id,
                    ancestor_ancestor_lsn,
                );
            }
            repo.branch_timeline(ancestor_timeline_id, new_timeline_id, start_lsn)?;
        }
        None => {
            bootstrap_timeline(conf, tenant_id, new_timeline_id, repo.as_ref())?;
        }
    }

    let new_timeline_info = TimelineInfo::from_ids(tenant_id, new_timeline_id, false)?;

    Ok(Some(new_timeline_info))
}
