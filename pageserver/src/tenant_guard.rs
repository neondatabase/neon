use crate::{
    task_mgr::{self, spawn, TaskKind, BACKGROUND_RUNTIME},
    tenant::{Tenant, TenantState},
};
use anyhow::bail;
use std::future::Future;
use std::{
    ops::Deref,
    sync::{Arc, Weak},
};
use tokio::sync::watch;
use tracing::{error, info};

/// A tenant guard structure, which is used to control the tenant's lifetime.
/// It's only being held by the runtime of the pageserver.
///
/// Holding a guard do not gives a guarrantie that tenant will be alive.
///
/// This works as follows: `TenantAccessor` is a `Weak<TenantAccessorInner>` pointer
/// with the actual tenant we're controlling. At the same time, runtime has
/// a fiber with `Arc<TenantAccessorInner>`, which is subscribed for the tenant
/// state changes and finishes when state is changed to `TenantState::Stopping`.
#[derive(Clone)]
pub struct TenantAccessor(Weak<TenantAccessorInner>);

impl TenantAccessor {
    /// Returns a new guard around the tenant
    pub fn new(tenant: Tenant) -> Self {
        let mut recv = tenant.subscribe_for_state_updates();
        let (drop_watcher, _) = watch::channel(());
        let loop_arc = Arc::new(TenantAccessorInner {
            tenant: Some(tenant),
            drop_watcher,
        });
        let weak_inner = Arc::downgrade(&loop_arc);

        // This is the closure which waits for state update to `TenantState::Stopping`.
        // After that, `TenantAccessorInner` is dropped and the tenant stops.
        let future = async move {
            while recv.changed().await.is_ok() {
                match *recv.borrow_and_update() {
                    TenantState::Active { .. } | TenantState::Paused => continue,
                    TenantState::Broken | TenantState::Stopping => break,
                    TenantState::Stopped => {
                        panic!("Tenant is forbidden to have 'Stopped' state directly")
                    }
                }
            }
            drop(loop_arc)
        };
        BACKGROUND_RUNTIME.spawn(future);

        Self(weak_inner)
    }

    // Returns the current state of tenant. If the tenant is already dropped,
    // returns `TenantState::Stopped`.
    pub fn current_state(&self) -> TenantState {
        match self.0.upgrade() {
            Some(inner) => inner.tenant.as_ref().unwrap().current_state(),
            None => TenantState::Stopped,
        }
    }

    // Subscribe for tenant shutdown. Returns the receiver of this notification.
    // If the tenant is already dropped at the moment of the call, returns `None`.
    pub fn subscribe_for_tenant_shutdown(&self) -> Option<watch::Receiver<()>> {
        Some(self.0.upgrade()?.drop_watcher.subscribe())
    }

    // Runs a closure with access to `&Tenant`, which prevents tenant to have an
    // uncontrollable lifetime. Returns either the result of closure launch or
    // `None` if tenant is already dropped.
    pub fn with_upgrade<F, O>(&self, func: F) -> anyhow::Result<O>
    where
        F: FnOnce(&Tenant) -> O,
    {
        if let Some(arc) = self.0.upgrade() {
            Ok(func(arc.tenant.as_ref().unwrap()))
        } else {
            bail!("tenant is already stopped")
        }
    }

    // Runs an async closure with access to `TenantRef`, which acts like `&Tenant`
    // to prevent tenant to have an uncontrollable lifetime. Returns either the
    // result of closure launch or `None` if tenant is already dropped.
    //
    // Important: NEVER MOVE the `TenantRef` out of the closure to prevent tenant
    // immortality!
    //
    // TODO: Change `TenantRef` to just `&Tenant` when `async closures` feature lands.
    pub async fn with_upgrade_async<F, T, O>(&self, func: F) -> anyhow::Result<O>
    where
        F: FnOnce(TenantRef) -> T,
        T: Future<Output = O>,
    {
        if let Some(arc) = self.0.upgrade() {
            Ok(func(TenantRef::new(arc)).await)
        } else {
            bail!("tenant is already stopped")
        }
    }
}

/// The actual tenant. Since we also need to have some mechanism to subscribe
/// to Tenant drop finish, we also have a `drop_watcher`, which sends a signal
/// to receivers when the tenant is dropped.
struct TenantAccessorInner {
    tenant: Option<Tenant>,
    drop_watcher: watch::Sender<()>,
}

impl Drop for TenantAccessorInner {
    // Schedules the task to drop the tenant.
    //
    // TODO: Use `async drop` feature when it lands.
    fn drop(&mut self) {
        let tenant = self.tenant.take().unwrap();
        let tenant_id = tenant.tenant_id();
        let future = async move {
            info!("shutdown tenant {tenant_id}");

            // Shutdown all tenant background tasks
            task_mgr::shutdown_tasks(None, Some(tenant_id), None).await;

            // Flush any remaining data in memory to disk.
            //
            // We assume that any incoming connections that might request pages from
            // the tenant have already been terminated by the caller, so there
            // should be no more activity in any of the repositories.
            if let Err(err) = tenant.checkpoint().await {
                error!("Could not checkpoint tenant {tenant_id} during shutdown: {err:?}");
            }

            Ok(())
        };

        // Spawn the task inside our runtime
        spawn(
            BACKGROUND_RUNTIME.handle(),
            TaskKind::TenantDrop,
            Some(tenant_id),
            None,
            format!("tenant {tenant_id} drop").as_str(),
            false,
            future,
        );

        // Notify the waiters of shutdown
        self.drop_watcher.send_replace(());
    }
}

/// A wrapper around `Arc` which acts like `&Tenant`.
pub struct TenantRef(Arc<TenantAccessorInner>);

impl TenantRef {
    fn new(tenant: Arc<TenantAccessorInner>) -> Self {
        Self(tenant)
    }
}

impl Deref for TenantRef {
    type Target = Tenant;

    fn deref(&self) -> &Self::Target {
        self.0.tenant.as_ref().unwrap()
    }
}
