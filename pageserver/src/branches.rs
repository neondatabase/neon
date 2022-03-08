//!
//! Branch management code
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

use crate::config::PageServerConf;
use crate::pgdatadir_mapping::DatadirTimeline;
use crate::repository::{Repository, Timeline};
use crate::walredo::WalRedoManager;
use crate::CheckpointConfig;
use crate::RepositoryImpl;
use crate::{import_datadir, LOG_FILE_NAME};
use crate::{repository::RepositoryTimeline, tenant_mgr};

#[derive(Serialize, Deserialize, Clone)]
pub struct BranchInfo {
    pub name: String,
    #[serde(with = "hex")]
    pub timeline_id: ZTimelineId,
    pub latest_valid_lsn: Lsn,
    pub ancestor_id: Option<String>,
    pub ancestor_lsn: Option<String>,
    pub current_logical_size: usize,
    pub current_logical_size_non_incremental: Option<usize>,
}

impl BranchInfo {
    pub fn from_path<R: Repository, T: AsRef<Path>>(
        path: T,
        repo: &R,
        _include_non_incremental_logical_size: bool,
    ) -> Result<Self> {
        let path = path.as_ref();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let timeline_id = std::fs::read_to_string(path)
            .with_context(|| {
                format!(
                    "Failed to read branch file contents at path '{}'",
                    path.display()
                )
            })?
            .parse::<ZTimelineId>()?;

        let timeline = match repo.get_timeline(timeline_id)? {
            RepositoryTimeline::Local(local_entry) => local_entry,
            RepositoryTimeline::Remote { .. } => {
                bail!("Timeline {} is remote, no branches to display", timeline_id)
            }
        };

        // we use ancestor lsn zero if we don't have an ancestor, so turn this into an option based on timeline id
        let (ancestor_id, ancestor_lsn) = match timeline.get_ancestor_timeline_id() {
            Some(ancestor_id) => (
                Some(ancestor_id.to_string()),
                Some(timeline.get_ancestor_lsn().to_string()),
            ),
            None => (None, None),
        };

        // non incremental size calculation can be heavy, so let it be optional
        // needed for tests to check size calculation
        //
        // FIXME
        /*
        let current_logical_size_non_incremental = include_non_incremental_logical_size
            .then(|| {
                timeline.get_current_logical_size_non_incremental(timeline.get_last_record_lsn())
            })
            .transpose()?;
         */
        let current_logical_size_non_incremental = Some(0);
        let current_logical_size = 0;

        Ok(BranchInfo {
            name,
            timeline_id,
            latest_valid_lsn: timeline.get_last_record_lsn(),
            ancestor_id,
            ancestor_lsn,
            current_logical_size, // : timeline.get_current_logical_size(),
            current_logical_size_non_incremental,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PointInTime {
    pub timelineid: ZTimelineId,
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
) -> Result<Arc<RepositoryImpl>> {
    let repo_dir = conf.tenant_path(&tenantid);
    if repo_dir.exists() {
        bail!("repo for {} already exists", tenantid)
    }

    // top-level dir may exist if we are creating it through CLI
    crashsafe_dir::create_dir_all(&repo_dir)
        .with_context(|| format!("could not create directory {}", repo_dir.display()))?;

    crashsafe_dir::create_dir(conf.timelines_path(&tenantid))?;
    crashsafe_dir::create_dir_all(conf.branches_path(&tenantid))?;
    crashsafe_dir::create_dir_all(conf.tags_path(&tenantid))?;

    info!("created directory structure in {}", repo_dir.display());

    // create a new timeline directory
    let timeline_id = ZTimelineId::generate();
    let timelinedir = conf.timeline_path(&timeline_id, &tenantid);

    crashsafe_dir::create_dir(&timelinedir)?;

    let repo = crate::layered_repository::LayeredRepository::new(
        conf,
        wal_redo_manager,
        tenantid,
        conf.remote_storage_config.is_some(),
    );

    // Load data into pageserver
    // TODO To implement zenith import we need to
    //      move data loading out of create_repo()
    bootstrap_timeline(conf, tenantid, timeline_id, &repo)?;

    Ok(Arc::new(repo))
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
        anyhow::bail!(
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

    let mut page_tline: DatadirTimeline<R> = DatadirTimeline::new(timeline);

    import_datadir::import_timeline_from_postgres_datadir(&pgdata_path, &mut page_tline, lsn)?;
    page_tline.tline.checkpoint(CheckpointConfig::Forced)?;

    println!(
        "created initial timeline {} timeline.lsn {}",
        tli,
        page_tline.tline.get_last_record_lsn()
    );

    let data = tli.to_string();
    fs::write(conf.branch_path("main", &tenantid), data)?;
    println!("created main branch");

    // Remove temp dir. We don't need it anymore
    fs::remove_dir_all(pgdata_path)?;

    Ok(())
}

pub(crate) fn get_branches(
    conf: &PageServerConf,
    tenantid: &ZTenantId,
    include_non_incremental_logical_size: bool,
) -> Result<Vec<BranchInfo>> {
    let repo = tenant_mgr::get_repository_for_tenant(*tenantid)?;

    // Each branch has a corresponding record (text file) in the refs/branches
    // with timeline_id.
    let branches_dir = conf.branches_path(tenantid);

    std::fs::read_dir(&branches_dir)
        .with_context(|| {
            format!(
                "Found no branches directory '{}' for tenant {}",
                branches_dir.display(),
                tenantid
            )
        })?
        .map(|dir_entry_res| {
            let dir_entry = dir_entry_res.with_context(|| {
                format!(
                    "Failed to list branches directory '{}' content for tenant {}",
                    branches_dir.display(),
                    tenantid
                )
            })?;
            BranchInfo::from_path(
                dir_entry.path(),
                repo.as_ref(),
                include_non_incremental_logical_size,
            )
        })
        .collect()
}

pub(crate) fn create_branch(
    conf: &PageServerConf,
    branchname: &str,
    startpoint_str: &str,
    tenantid: &ZTenantId,
) -> Result<BranchInfo> {
    let repo = tenant_mgr::get_repository_for_tenant(*tenantid)?;

    if conf.branch_path(branchname, tenantid).exists() {
        anyhow::bail!("branch {} already exists", branchname);
    }

    let mut startpoint = parse_point_in_time(conf, startpoint_str, tenantid)?;
    let timeline = repo
        .get_timeline(startpoint.timelineid)?
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
        anyhow::bail!(
            "invalid startpoint {} for the branch {}: less than timeline ancestor lsn {:?}",
            startpoint.lsn,
            branchname,
            timeline.get_ancestor_lsn()
        );
    }

    let new_timeline_id = ZTimelineId::generate();

    // Forward entire timeline creation routine to repository
    // backend, so it can do all needed initialization
    repo.branch_timeline(startpoint.timelineid, new_timeline_id, startpoint.lsn)?;

    // Remember the human-readable branch name for the new timeline.
    // FIXME: there's a race condition, if you create a branch with the same
    // name concurrently.
    let data = new_timeline_id.to_string();
    fs::write(conf.branch_path(branchname, tenantid), data)?;

    Ok(BranchInfo {
        name: branchname.to_string(),
        timeline_id: new_timeline_id,
        latest_valid_lsn: startpoint.lsn,
        ancestor_id: Some(startpoint.timelineid.to_string()),
        ancestor_lsn: Some(startpoint.lsn.to_string()),
        current_logical_size: 0,
        current_logical_size_non_incremental: Some(0),
    })
}

//
// Parse user-given string that represents a point-in-time.
//
// We support multiple variants:
//
// Raw timeline id in hex, meaning the end of that timeline:
//    bc62e7d612d0e6fe8f99a6dd2f281f9d
//
// A specific LSN on a timeline:
//    bc62e7d612d0e6fe8f99a6dd2f281f9d@2/15D3DD8
//
// Same, with a human-friendly branch name:
//    main
//    main@2/15D3DD8
//
// Human-friendly tag name:
//    mytag
//
//
fn parse_point_in_time(
    conf: &PageServerConf,
    s: &str,
    tenantid: &ZTenantId,
) -> Result<PointInTime> {
    let mut strings = s.split('@');
    let name = strings.next().unwrap();

    let lsn = strings
        .next()
        .map(Lsn::from_str)
        .transpose()
        .context("invalid LSN in point-in-time specification")?;

    // Check if it's a tag
    if lsn.is_none() {
        let tagpath = conf.tag_path(name, tenantid);
        if tagpath.exists() {
            let pointstr = fs::read_to_string(tagpath)?;

            return parse_point_in_time(conf, &pointstr, tenantid);
        }
    }

    // Check if it's a branch
    // Check if it's branch @ LSN
    let branchpath = conf.branch_path(name, tenantid);
    if branchpath.exists() {
        let pointstr = fs::read_to_string(branchpath)?;

        let mut result = parse_point_in_time(conf, &pointstr, tenantid)?;

        result.lsn = lsn.unwrap_or(Lsn(0));
        return Ok(result);
    }

    // Check if it's a timelineid
    // Check if it's timelineid @ LSN
    if let Ok(timelineid) = ZTimelineId::from_str(name) {
        let tlipath = conf.timeline_path(&timelineid, tenantid);
        if tlipath.exists() {
            return Ok(PointInTime {
                timelineid,
                lsn: lsn.unwrap_or(Lsn(0)),
            });
        }
    }

    bail!("could not parse point-in-time {}", s);
}
