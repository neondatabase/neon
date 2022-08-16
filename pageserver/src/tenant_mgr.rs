//! This module acts as a switchboard to access different tenants managed by this
//! page server.

use crate::config::PageServerConf;
use crate::layered_repository::TenantState;
use crate::repository::Repository;
use crate::tenant_config::TenantConfOpt;
use crate::thread_mgr;
use crate::thread_mgr::ThreadKind;
use crate::walredo::PostgresRedoManager;
use crate::RepositoryImpl;
use anyhow::{bail, Context, Result};
use once_cell::sync::Lazy;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::*;

use utils::zid::ZTenantId;

static TENANTS: Lazy<RwLock<HashMap<ZTenantId, Arc<RepositoryImpl>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

fn read_tenants() -> RwLockReadGuard<'static, HashMap<ZTenantId, Arc<RepositoryImpl>>> {
    TENANTS
        .read()
        .expect("Failed to read() tenants lock, it got poisoned")
}
fn write_tenants() -> RwLockWriteGuard<'static, HashMap<ZTenantId, Arc<RepositoryImpl>>> {
    TENANTS
        .write()
        .expect("Failed to write() tenants lock, it got poisoned")
}

/// Initialize repositories with locally available timelines.
/// Timelines that are only partially available locally (remote storage has more data than this pageserver)
/// are scheduled for download and added to the repository once download is completed.
/// TODO

pub fn init_tenant_mgr(conf: &'static PageServerConf) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let rt_guard = runtime.enter();

    let mut handles = Vec::new();

    // Scan local filesystem for attached tenants
    let tenants_dir = conf.tenants_path();
    for dir_entry in std::fs::read_dir(&tenants_dir)
        .with_context(|| format!("Failed to list tenants dir {}", tenants_dir.display()))?
    {
        match &dir_entry {
            Ok(dir_entry) => {
                let tenant_id: ZTenantId = dir_entry
                    .path()
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .parse()
                    .unwrap();

                // Start loading the tenant into memory. It will initially be in Loading
                // state.
                let (repo, handle) = RepositoryImpl::spawn_load(conf, tenant_id)?;
                handles.push(handle);
                write_tenants().insert(tenant_id, repo);
            }
            Err(e) => {
                // On error, print it, but continue with the other tenants. If we error out
                // here, the pageserver startup fails altogether, causing outage for *all*
                // tenants. That seems worse.
                error!(
                    "Failed to list tenants dir entry {:?} in directory {}, reason: {:?}",
                    dir_entry,
                    tenants_dir.display(),
                    e,
                );
            }
        }
    }
    drop(rt_guard);

    thread_mgr::spawn(
        ThreadKind::InitialLoad,
        None,
        None,
        "initial tenant load",
        false,
        move || {
            info!("in initial load thread");
            for h in handles {
                let _ = runtime.block_on(h);
            }
            info!("initial load done!");
            Ok(())
        },
    )?;

    Ok(())
}

///
/// Shut down all tenants. This runs as part of pageserver shutdown.
///
pub fn shutdown_all_tenants() {
    let mut m = write_tenants();
    let mut tenantids = Vec::new();
    for (tenantid, tenant) in m.iter_mut() {
        tenant.state.send_modify(|state_guard| match *state_guard {
            TenantState::Loading
            | TenantState::Attaching
            | TenantState::Active
            | TenantState::Stopping => {
                *state_guard = TenantState::Stopping;
                tenantids.push(*tenantid)
            }
            TenantState::Broken => {}
        });
    }
    drop(m);

    thread_mgr::shutdown_threads(Some(ThreadKind::WalReceiverManager), None, None);

    // Ok, no background threads running anymore. Flush any remaining data in
    // memory to disk.
    //
    // We assume that any incoming connections that might request pages from
    // the repository have already been terminated by the caller, so there
    // should be no more activity in any of the repositories.
    //
    // On error, log it but continue with the shutdown for other tenants.
    for tenant_id in tenantids {
        debug!("shutdown tenant {tenant_id}");
        match get_tenant(tenant_id) {
            Ok(repo) => {
                if let Err(err) = repo.checkpoint() {
                    error!("Could not checkpoint tenant {tenant_id} during shutdown: {err:?}");
                }
            }
            Err(err) => {
                error!("Could not get repository for tenant {tenant_id} during shutdown: {err:?}");
            }
        }
    }
}

pub fn create_tenant(
    conf: &'static PageServerConf,
    tenant_conf: TenantConfOpt,
    tenant_id: ZTenantId,
) -> anyhow::Result<Option<ZTenantId>> {
    match write_tenants().entry(tenant_id) {
        Entry::Occupied(_) => {
            debug!("tenant {tenant_id} already exists");
            Ok(None)
        }
        Entry::Vacant(v) => {
            let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenant_id));
            let repo = RepositoryImpl::create(conf, tenant_conf, tenant_id, wal_redo_manager)?;
            v.insert(Arc::new(repo));
            Ok(Some(tenant_id))
        }
    }
}

pub fn update_tenant_config(
    tenant_conf: TenantConfOpt,
    tenant_id: ZTenantId,
) -> anyhow::Result<()> {
    info!("configuring tenant {tenant_id}");
    let repo = get_tenant(tenant_id)?;

    repo.update_tenant_config(tenant_conf)?;
    Ok(())
}

pub fn get_tenant(tenant_id: ZTenantId) -> anyhow::Result<Arc<RepositoryImpl>> {
    let m = read_tenants();
    let tenant = m
        .get(&tenant_id)
        .with_context(|| format!("Tenant {tenant_id} not found"))?;

    Ok(Arc::clone(tenant))
}

///
/// Get reference to a tenant's Repository object. If the tenant is
/// not in active state yet, we will wait for it to become active,
/// with a 30 s timeout. Returns an error if the tenant does not
/// exist, or it's not active yet and the wait times out,
///
pub fn get_active_tenant(tenant_id: ZTenantId) -> anyhow::Result<Arc<RepositoryImpl>> {
    let tenant = get_tenant(tenant_id)?;

    tokio::runtime::Handle::current().block_on(async {
        match tokio::time::timeout(
            std::time::Duration::from_secs(30),
            tenant.wait_until_active(),
        )
        .await
        {
            Ok(Ok(())) => Ok(tenant),
            Ok(Err(e)) => Err(e),
            Err(_) => bail!("timeout waiting for tenant {} to become active", tenant_id),
        }
    })
}

pub fn detach_tenant(tenant_id: ZTenantId) -> anyhow::Result<()> {
    let repo = get_tenant(tenant_id)?;
    let task = repo.spawn_detach()?;
    drop(repo);

    // FIXME: Should we go ahead and remove the tenant anyway, if detaching fails? It's a bit
    // annoying if a tenant gets wedged so that you can't even detach it. OTOH, it's scary
    // to delete files if we're not sure what's wrong.
    tokio::spawn(async move {
        match task.await {
            Ok(_) => {
                write_tenants().remove(&tenant_id);
            }
            Err(err) => {
                error!("detaching tenant {} failed: {:?}", tenant_id, err);
            }
        };
    });
    Ok(())
}

///
/// Get list of tenants, for the mgmt API
///
pub fn list_tenants() -> Vec<(ZTenantId, TenantState)> {
    read_tenants()
        .iter()
        .map(|(id, tenant)| (*id, tenant.get_state()))
        .collect()
}

///
/// Execute Attach mgmt API command.
///
/// Downloading all the tenant data is performed in the background,
/// this awn the background task and returns quickly.
///
pub fn attach_tenant(conf: &'static PageServerConf, tenant_id: ZTenantId) -> Result<()> {
    match write_tenants().entry(tenant_id) {
        Entry::Occupied(e) => {
            // Cannot attach a tenant that already exists. The error message depends on
            // the state it's in.
            match e.get().get_state() {
                TenantState::Loading | TenantState::Active => {
                    bail!("tenant {tenant_id} already exists")
                }
                TenantState::Attaching => bail!("tenant {tenant_id} attach is already in progress"),
                TenantState::Stopping => bail!("tenant {tenant_id} is being shut down"),
                TenantState::Broken => bail!("tenant {tenant_id} is marked as broken"),
            }
        }
        Entry::Vacant(v) => {
            let repo = RepositoryImpl::spawn_attach(conf, tenant_id)?;
            v.insert(repo);
            Ok(())
        }
    }
}
