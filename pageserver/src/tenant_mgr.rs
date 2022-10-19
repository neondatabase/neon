//! This module acts as a switchboard to access different tenants managed by this
//! page server. The code to handle tenant-related mgmt API commands like Attach,
//! Detach or Create tenant is here.

use std::collections::hash_map;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use tracing::*;

use remote_storage::GenericRemoteStorage;

use crate::config::PageServerConf;
use crate::tenant::{Tenant, TenantState};
use crate::tenant_config::TenantConfOpt;

use utils::id::TenantId;

mod tenants_state {
    use once_cell::sync::Lazy;
    use std::{
        collections::HashMap,
        sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
    };
    use utils::id::TenantId;

    use crate::tenant::Tenant;

    static TENANTS: Lazy<RwLock<HashMap<TenantId, Arc<Tenant>>>> =
        Lazy::new(|| RwLock::new(HashMap::new()));

    pub(super) fn read_tenants() -> RwLockReadGuard<'static, HashMap<TenantId, Arc<Tenant>>> {
        TENANTS
            .read()
            .expect("Failed to read() tenants lock, it got poisoned")
    }

    pub(super) fn write_tenants() -> RwLockWriteGuard<'static, HashMap<TenantId, Arc<Tenant>>> {
        TENANTS
            .write()
            .expect("Failed to write() tenants lock, it got poisoned")
    }
}

///
/// Initialize Tenant structs for tenants that are found on local disk. This is
/// called once at pageserver startup.
///
pub fn init_tenant_mgr(
    conf: &'static PageServerConf,
    remote_storage: Option<&GenericRemoteStorage>,
) -> anyhow::Result<()> {
    let _entered = info_span!("init_tenant_mgr").entered();

    // Scan local filesystem for attached tenants
    let mut number_of_tenants = 0;
    let tenants_dir = conf.tenants_path();
    for dir_entry in std::fs::read_dir(&tenants_dir)
        .with_context(|| format!("Failed to list tenants dir {}", tenants_dir.display()))?
    {
        match &dir_entry {
            Ok(dir_entry) => {
                let tenant_dir_path = dir_entry.path();
                if crate::is_temporary(&tenant_dir_path) {
                    info!(
                        "Found temporary tenant directory, removing: {}",
                        tenant_dir_path.display()
                    );
                    if let Err(e) = std::fs::remove_dir_all(&tenant_dir_path) {
                        error!(
                            "Failed to remove temporary directory '{}': {:?}",
                            tenant_dir_path.display(),
                            e
                        );
                    }
                } else {
                    number_of_tenants += 1;
                    if let Err(e) = load_local_tenant(conf, &tenant_dir_path, remote_storage) {
                        error!(
                            "Failed to collect tenant files from dir '{}' for entry {:?}, reason: {:#}",
                            tenants_dir.display(),
                            dir_entry,
                            e
                        );
                    }
                }
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

    info!("Processed {number_of_tenants} local tenants at startup");
    Ok(())
}

fn load_local_tenant(
    conf: &'static PageServerConf,
    tenant_path: &Path,
    remote_storage: Option<&GenericRemoteStorage>,
) -> anyhow::Result<()> {
    let tenant_id = tenant_path
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or_default()
        .parse::<TenantId>()
        .context("Could not parse tenant id out of the tenant dir name")?;

    // Start loading the tenant into memory. It will initially be in Loading
    // state.
    let tenant = Tenant::spawn_load(conf, tenant_id, remote_storage)?;
    tenants_state::write_tenants().insert(tenant_id, tenant);
    Ok(())
}

///
/// Shut down all tenants. This runs as part of pageserver shutdown.
///
pub async fn shutdown_all_tenants() {
    let tenants_to_shut_down = {
        let m = tenants_state::read_tenants();
        let mut tenants_to_shut_down = Vec::with_capacity(m.len());
        for (_, tenant) in m.iter() {
            tenants_to_shut_down.push(Arc::clone(tenant))
        }
        tenants_to_shut_down
    };

    // FIXME: stop accepting create/attach tenant calls before this
    let mut shutdown_tasks = Vec::with_capacity(tenants_to_shut_down.len());
    for tenant in tenants_to_shut_down.iter() {
        shutdown_tasks.push(tenant.shutdown());
    }

    futures::future::join_all(shutdown_tasks).await;
}

pub fn create_tenant(
    conf: &'static PageServerConf,
    tenant_conf: TenantConfOpt,
    tenant_id: TenantId,
    remote_storage: Option<&GenericRemoteStorage>,
) -> anyhow::Result<Option<TenantId>> {
    match tenants_state::write_tenants().entry(tenant_id) {
        hash_map::Entry::Occupied(_) => {
            debug!("tenant {tenant_id} already exists");
            Ok(None)
        }
        hash_map::Entry::Vacant(v) => {
            let tenant = Tenant::create_empty_tenant(conf, tenant_conf, tenant_id, remote_storage)?;
            v.insert(tenant);
            Ok(Some(tenant_id))
        }
    }
}

pub fn update_tenant_config(tenant_conf: TenantConfOpt, tenant_id: TenantId) -> anyhow::Result<()> {
    info!("configuring tenant {tenant_id}");
    let tenant = get_tenant(tenant_id)?;
    tenant.update_tenant_config(tenant_conf)?;
    Ok(())
}

/// Get reference to the Tenant object of given tenant. Note that the tenant can
/// be in any state, including Broken or Loading. If you are going to access the
/// timelines or data in the tenant, you need to ensure that it is in Active
/// state. See use get_active_tenant().
pub fn get_tenant(tenant_id: TenantId) -> anyhow::Result<Arc<Tenant>> {
    let m = tenants_state::read_tenants();
    let tenant = m
        .get(&tenant_id)
        .with_context(|| format!("Tenant {tenant_id} not found"))?;
    Ok(Arc::clone(tenant))
}

/// Get reference to a tenant's Tenant object. If the tenant is not in active
/// state yet, we will wait for it to become active, with a 30 s timeout.
/// Returns an error if the tenant does not exist, or it's not active yet and
/// the wait times out,
pub async fn get_active_tenant(tenant_id: TenantId) -> anyhow::Result<Arc<Tenant>> {
    let tenant = get_tenant(tenant_id)?;
    match tokio::time::timeout(Duration::from_secs(30), tenant.wait_until_active()).await {
        Ok(Ok(())) => Ok(tenant),
        Ok(Err(err)) => Err(err),
        Err(_) => Err(anyhow!(
            "timed out waiting for tenant to become active (it is {:?})",
            tenant.get_state()
        )),
    }
}

pub async fn detach_tenant(tenant_id: TenantId) -> anyhow::Result<()> {
    let tenant = get_tenant(tenant_id)?;
    let task = tenant.detach_tenant();

    // FIXME: Should we go ahead and remove the tenant anyway, if detaching fails? It's a bit
    // annoying if a tenant gets wedged so that you can't even detach it. OTOH, it's scary
    // to delete files if we're not sure what's wrong.
    match task.await {
        Ok(_) => {
            tenants_state::write_tenants().remove(&tenant_id);
        }
        Err(err) => {
            error!("detaching tenant {} failed: {:?}", tenant_id, err);
        }
    };
    Ok(())
}

/// Get list of tenants, for the mgmt API
pub fn list_tenants() -> Vec<(TenantId, TenantState)> {
    tenants_state::read_tenants()
        .iter()
        .map(|(id, tenant)| (*id, tenant.get_state()))
        .collect()
}

/// Execute Attach mgmt API command.
///
/// Downloading all the tenant data is performed in the background, this merely
/// spawns the background task and returns quickly.
pub async fn attach_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    remote_storage: &GenericRemoteStorage,
) -> anyhow::Result<()> {
    match tenants_state::write_tenants().entry(tenant_id) {
        hash_map::Entry::Occupied(e) => {
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
        hash_map::Entry::Vacant(v) => {
            let tenant = Tenant::spawn_attach(conf, tenant_id, remote_storage)?;
            v.insert(tenant);
            Ok(())
        }
    }
}
