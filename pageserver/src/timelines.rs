//!
//! Timeline management code
//!
// TODO: move all paths construction to conf impl
//

use anyhow::{bail, Context, Result};
use postgres_ffi::ControlFileData;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
    str::FromStr,
    sync::Arc,
};
use tracing::*;

use zenith_utils::lsn::Lsn;
use zenith_utils::zid::{ZTenantId, ZTimelineId};
use zenith_utils::{crashsafe_dir, logging};

use crate::walredo::WalRedoManager;
use crate::CheckpointConfig;
use crate::{config::PageServerConf, repository::Repository};
use crate::{import_datadir, LOG_FILE_NAME};
use crate::{repository::RepositoryTimeline, tenant_mgr};

#[derive(Serialize, Deserialize, Clone)]
pub struct TimelineInfo {
    #[serde(with = "hex")]
    pub timeline_id: ZTimelineId,
    pub latest_valid_lsn: Lsn,
    pub ancestor_id: Option<String>,
    pub ancestor_lsn: Option<String>,
    pub current_logical_size: usize,
    pub current_logical_size_non_incremental: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
pub struct PointInTime {
    pub timeline_id: ZTimelineId,
    pub lsn: Lsn,
}

pub fn init_pageserver(conf: &'static PageServerConf, create_tenant: Option<&str>) -> Result<()> {
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

    if let Some(tenantid) = create_tenant {
        let tenantid = ZTenantId::from_str(tenantid)?;
        println!("initializing tenantid {}", tenantid);
        create_repo(conf, tenantid, dummy_redo_mgr).context("failed to create repo")?;
    }
    crashsafe_dir::create_dir_all(conf.tenants_path())?;

    println!("pageserver init succeeded");
    Ok(())
}

pub fn create_repo(
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
    wal_redo_manager: Arc<dyn WalRedoManager + Send + Sync>,
) -> Result<Arc<dyn Repository>> {
    let repo_dir = conf.tenant_path(&tenantid);
    if repo_dir.exists() {
        bail!("repo for {} already exists", tenantid)
    }

    // top-level dir may exist if we are creating it through CLI
    crashsafe_dir::create_dir_all(&repo_dir)
        .with_context(|| format!("could not create directory {}", repo_dir.display()))?;

    crashsafe_dir::create_dir(conf.timelines_path(&tenantid))?;

    info!("created directory structure in {}", repo_dir.display());

    // create a new timeline directory
    let timeline_id = ZTimelineId::generate();
    let timelinedir = conf.timeline_path(&timeline_id, &tenantid);

    crashsafe_dir::create_dir(&timelinedir)?;

    let repo = Arc::new(crate::layered_repository::LayeredRepository::new(
        conf,
        wal_redo_manager,
        tenantid,
        conf.remote_storage_config.is_some(),
    ));

    // Load data into pageserver
    // TODO To implement zenith import we need to
    //      move data loading out of create_repo()
    bootstrap_timeline(conf, tenantid, timeline_id, repo.as_ref())?;

    Ok(repo)
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
fn bootstrap_timeline(
    conf: &'static PageServerConf,
    tenantid: ZTenantId,
    tli: ZTimelineId,
    repo: &dyn Repository,
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
    import_datadir::import_timeline_from_postgres_datadir(
        &pgdata_path,
        timeline.writer().as_ref(),
        lsn,
    )?;
    timeline.checkpoint(CheckpointConfig::Forced)?;

    println!(
        "created initial timeline {} timeline.lsn {}",
        tli,
        timeline.get_last_record_lsn()
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

    Ok(repo
        .list_timelines()
        .with_context(|| format!("Failed to list timelines for tenant {}", tenant_id))?
        .into_iter()
        .filter_map(|timeline| match timeline {
            RepositoryTimeline::Local { timeline, id } => Some((id, timeline)),
            RepositoryTimeline::Remote { .. } => None,
        })
        .map(|(timeline_id, timeline)| {
            let (ancestor_id, ancestor_lsn) = match timeline.get_ancestor_timeline_id() {
                Some(ancestor_id) => (
                    Some(ancestor_id.to_string()),
                    Some(timeline.get_ancestor_lsn().to_string()),
                ),
                None => (None, None),
            };

            let current_logical_size_non_incremental = if include_non_incremental_logical_size {
                match timeline
                    .get_current_logical_size_non_incremental(timeline.get_last_record_lsn())
                {
                    Ok(size) => Some(size),
                    Err(e) => {
                        error!(
                            "Failed to get current logical size for timeline {}: {:?}",
                            timeline_id, e
                        );
                        None
                    }
                }
            } else {
                None
            };

            TimelineInfo {
                timeline_id,
                latest_valid_lsn: timeline.get_last_record_lsn(),
                ancestor_id,
                ancestor_lsn,
                current_logical_size: timeline.get_current_logical_size(),
                // non incremental size calculation can be heavy, so let it be optional
                // needed for tests to check size calculation
                current_logical_size_non_incremental,
            }
        })
        .collect())
}

pub(crate) fn create_timeline(
    conf: &PageServerConf,
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
    start_lsn: Option<Lsn>,
) -> Result<TimelineInfo> {
    if conf.timeline_path(&timeline_id, &tenant_id).exists() {
        bail!("timeline {} already exists", timeline_id);
    }

    let repo = tenant_mgr::get_repository_for_tenant(tenant_id)?;

    let mut startpoint = PointInTime {
        timeline_id,
        lsn: start_lsn.unwrap_or(Lsn(0)),
    };

    let timeline = repo
        .get_timeline(startpoint.timeline_id)?
        .local_timeline()
        .context("Cannot branch off the timeline that's not present locally")?;
    if startpoint.lsn == Lsn(0) {
        // Find end of WAL on the old timeline
        let end_of_wal = timeline.get_last_record_lsn();
        info!("branching at end of WAL: {}", end_of_wal);
        startpoint.lsn = end_of_wal;
    } else {
        // Wait for the WAL to arrive and be processed on the parent branch up
        // to the requested branch point. The repository code itself doesn't
        // require it, but if we start to receive WAL on the new timeline,
        // decoding the new WAL might need to look up previous pages, relation
        // sizes etc. and that would get confused if the previous page versions
        // are not in the repository yet.
        timeline.wait_lsn(startpoint.lsn)?;
    }
    startpoint.lsn = startpoint.lsn.align();
    if timeline.get_ancestor_lsn() > startpoint.lsn {
        // can we safely just branch from the ancestor instead?
        bail!(
            "invalid startpoint {} for the timeline {}: less than timeline ancestor lsn {:?}",
            startpoint.lsn,
            timeline_id,
            timeline.get_ancestor_lsn()
        );
    }

    let new_timeline_id = ZTimelineId::generate();

    // Forward entire timeline creation routine to repository
    // backend, so it can do all needed initialization
    repo.branch_timeline(startpoint.timeline_id, new_timeline_id, startpoint.lsn)?;

    // Remember the human-readable branch name for the new timeline.
    // FIXME: there's a race condition, if you create a branch with the same
    // name concurrently.
    // TODO kb timeline creation needs more
    let data = new_timeline_id.to_string();
    fs::write(conf.timeline_path(&timeline_id, &tenant_id), data)?;

    Ok(TimelineInfo {
        timeline_id: new_timeline_id,
        latest_valid_lsn: startpoint.lsn,
        ancestor_id: Some(startpoint.timeline_id.to_string()),
        ancestor_lsn: Some(startpoint.lsn.to_string()),
        current_logical_size: 0,
        current_logical_size_non_incremental: Some(0),
    })
}
