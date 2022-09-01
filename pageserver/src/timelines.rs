//!
//! Timeline management code
//

use anyhow::{bail, ensure, Context, Result};

use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
    sync::Arc,
};
use tracing::*;

use utils::{
    crashsafe_dir,
    lsn::Lsn,
    zid::{ZTenantId, ZTimelineId},
};

use crate::import_datadir;
use crate::tenant_mgr;
use crate::CheckpointConfig;
use crate::{
    config::PageServerConf, storage_sync::index::RemoteIndex, tenant_config::TenantConfOpt,
};
use crate::{
    layered_repository::{Repository, Timeline},
    walredo::WalRedoManager,
};

#[derive(Debug, Clone, Copy)]
pub struct PointInTime {
    pub timeline_id: ZTimelineId,
    pub lsn: Lsn,
}

pub fn create_repo(
    conf: &'static PageServerConf,
    tenant_conf: TenantConfOpt,
    tenant_id: ZTenantId,
    wal_redo_manager: Arc<dyn WalRedoManager + Send + Sync>,
    remote_index: RemoteIndex,
) -> Result<Arc<Repository>> {
    let repo_dir = conf.tenant_path(&tenant_id);
    ensure!(
        !repo_dir.exists(),
        "cannot create new tenant repo: '{}' directory already exists",
        tenant_id
    );

    // top-level dir may exist if we are creating it through CLI
    crashsafe_dir::create_dir_all(&repo_dir)
        .with_context(|| format!("could not create directory {}", repo_dir.display()))?;
    crashsafe_dir::create_dir(conf.timelines_path(&tenant_id))?;
    info!("created directory structure in {}", repo_dir.display());

    // Save tenant's config
    Repository::persist_tenant_config(conf, tenant_id, tenant_conf)?;

    Ok(Arc::new(Repository::new(
        conf,
        tenant_conf,
        wal_redo_manager,
        tenant_id,
        remote_index,
        conf.remote_storage_config.is_some(),
    )))
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
    repo: &Repository,
) -> Result<Arc<Timeline>> {
    let initdb_path = conf
        .tenant_path(&tenantid)
        .join(format!("tmp-timeline-{}", tli));

    // Init temporarily repo to get bootstrap data
    run_initdb(conf, &initdb_path)?;
    let pgdata_path = initdb_path;

    let lsn = import_datadir::get_lsn_from_controlfile(&pgdata_path)?.align();

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
) -> Result<Option<Arc<Timeline>>> {
    let new_timeline_id = new_timeline_id.unwrap_or_else(ZTimelineId::generate);
    let repo = tenant_mgr::get_repository_for_tenant(tenant_id)?;

    if conf.timeline_path(&new_timeline_id, &tenant_id).exists() {
        debug!("timeline {} already exists", new_timeline_id);
        return Ok(None);
    }

    let loaded_timeline = match ancestor_timeline_id {
        Some(ancestor_timeline_id) => {
            let ancestor_timeline = repo
                .get_timeline(ancestor_timeline_id)
                .context("Cannot branch off the timeline that's not present in pageserver")?;

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

            repo.branch_timeline(ancestor_timeline_id, new_timeline_id, ancestor_start_lsn)?
        }
        None => bootstrap_timeline(conf, tenant_id, new_timeline_id, repo.as_ref())?,
    };

    Ok(Some(loaded_timeline))
}
