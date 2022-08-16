//!
//! Misc bootstrapping code
//!

use anyhow::{bail, Context, Result};
use postgres_ffi::ControlFileData;
use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
    sync::Arc,
};
use tracing::*;

use utils::{
    crashsafe_dir, logging,
    lsn::Lsn,
    zid::{ZTenantId, ZTimelineId},
};

use crate::tenant_mgr;
use crate::{config::PageServerConf, repository::Repository, tenant_config::TenantConfOpt};
use crate::{import_datadir, LOG_FILE_NAME};
use crate::{repository::Timeline, CheckpointConfig};
use crate::{RepositoryImpl, TimelineImpl};

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

    crashsafe_dir::create_dir_all(conf.tenants_path())?;

    if let Some(tenant_id) = create_tenant {
        println!("initializing tenantid {}", tenant_id);

        // FIXME: initialize storage sync, otherwise we panic
        // Do we want this bootstrapping to really upload stuff to remote storage?
        crate::storage_sync::init_storage_sync(conf)?;

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
        let wal_redo_manager = std::sync::Arc::new(crate::walredo::DummyRedoManager {});

        let repo =
            RepositoryImpl::create(conf, TenantConfOpt::default(), tenant_id, wal_redo_manager)
                .context("failed to create repo")?;
        let new_timeline_id = initial_timeline_id.unwrap_or_else(ZTimelineId::generate);
        bootstrap_timeline(conf, tenant_id, new_timeline_id, &repo)
            .context("failed to create initial timeline")?;
        println!("initial timeline {} created", new_timeline_id)
    } else if initial_timeline_id.is_some() {
        println!("Ignoring initial timeline parameter, due to no tenant id to create given");
    }

    println!("pageserver init succeeded");
    Ok(())
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
        .args(&["-D", &initdbpath.to_string_lossy()])
        .args(&["-U", &conf.superuser])
        .args(&["-E", "utf8"])
        .arg("--no-instructions")
        // This is only used for a temporary installation that is deleted shortly after,
        // so no need to fsync it
        .arg("--no-sync")
        .env_clear()
        .env("LD_LIBRARY_PATH", conf.pg_lib_dir())
        .env("DYLD_LIBRARY_PATH", conf.pg_lib_dir())
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
    repo: &RepositoryImpl,
) -> Result<Arc<TimelineImpl>> {
    let initdb_path = conf
        .tenant_path(&tenantid)
        .join(format!("tmp-timeline-{}", tli));

    // Init temporarily repo to get bootstrap data
    run_initdb(conf, &initdb_path)?;
    let pgdata_path = initdb_path;

    let lsn = get_lsn_from_controlfile(&pgdata_path)?.align();

    // Import the contents of the data directory at the initial checkpoint
    // LSN, and any WAL after that.
    // Initdb lsn will be equal to last_record_lsn which will be set after import.
    // Because we know it upfront avoid having an option or dummy zero value by passing it to create_empty_timeline.
    let timeline = repo.create_empty_timeline(tli, lsn)?;
    import_datadir::import_timeline_from_postgres_datadir(&pgdata_path, &*timeline, lsn)?;

    fail::fail_point!("before-checkpoint-new-timeline", |_| {
        bail!("failpoint before-checkpoint-new-timeline");
    });

    timeline.checkpoint(CheckpointConfig::Forced)?;

    info!(
        "created root timeline {} timeline.lsn {}",
        tli,
        timeline.get_last_record_lsn()
    );

    // Remove temp dir. We don't need it anymore
    fs::remove_dir_all(pgdata_path)?;

    Ok(timeline)
}

///
/// Create a new timeline.
///
/// Returns the new timeline ID and reference to its Timeline object.
///
/// If the caller specified the timeline ID to use (`new_timeline_id`), and timeline with
/// the same timeline ID already exists, returns None. If `new_timeline_id` is not given,
/// a new unique ID is generated.
///
pub(crate) fn create_timeline(
    conf: &'static PageServerConf,
    tenant_id: ZTenantId,
    new_timeline_id: Option<ZTimelineId>,
    ancestor_timeline_id: Option<ZTimelineId>,
    mut ancestor_start_lsn: Option<Lsn>,
) -> Result<Option<Arc<TimelineImpl>>> {
    let new_timeline_id = new_timeline_id.unwrap_or_else(ZTimelineId::generate);
    let repo = tenant_mgr::get_tenant(tenant_id)?;

    if conf.timeline_path(&new_timeline_id, &tenant_id).exists() {
        debug!("timeline {} already exists", new_timeline_id);
        return Ok(None);
    }

    let new_timeline = match ancestor_timeline_id {
        Some(ancestor_timeline_id) => {
            let ancestor_timeline = repo
                .get_timeline(ancestor_timeline_id)
                .context("Cannot branch off the timeline that's not present locally")?;

            if let Some(lsn) = ancestor_start_lsn.as_mut() {
                // Wait for the WAL to arrive and be processed on the parent branch up
                // to the requested branch point. The repository code itself doesn't
                // require it, but if we start to receive WAL on the new timeline,
                // decoding the new WAL might need to look up previous pages, relation
                // sizes etc. and that would get confused if the previous page versions
                // are not in the repository yet.
                *lsn = lsn.align();
                ancestor_timeline.wait_lsn(*lsn)?;

                let ancestor_ancestor_lsn = ancestor_timeline.get_ancestor_lsn();
                if ancestor_ancestor_lsn > *lsn {
                    // can we safely just branch from the ancestor instead?
                    anyhow::bail!(
                    "invalid start lsn {} for ancestor timeline {}: less than timeline ancestor lsn {}",
                    lsn,
                    ancestor_timeline_id,
                    ancestor_ancestor_lsn,
                );
                }
            }

            let timeline =
                repo.branch_timeline(ancestor_timeline_id, new_timeline_id, ancestor_start_lsn)?;
            timeline.launch_wal_receiver()?;
            timeline
        }
        None => {
            let timeline = bootstrap_timeline(conf, tenant_id, new_timeline_id, repo.as_ref())?;
            timeline.launch_wal_receiver()?;
            timeline
        }
    };

    Ok(Some(new_timeline))
}
