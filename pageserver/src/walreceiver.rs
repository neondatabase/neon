//! WAL receiver manages an open connection to safekeeper, to get the WAL it streams into.
//! To do so, a current implementation needs to do the following:
//!
//! * acknowledge the timelines that it needs to stream WAL into.
//! Pageserver is able to dynamically (un)load tenants on attach and detach,
//! hence WAL receiver needs to react on such events.
//!
//! * get a broker subscription, stream data from it to determine that a timeline needs WAL streaming.
//! For that, it watches specific keys in etcd broker and pulls the relevant data periodically.
//! The data is produced by safekeepers, that push it periodically and pull it to synchronize between each other.
//! Without this data, no WAL streaming is possible currently.
//!
//! Only one active WAL streaming connection is allowed at a time.
//! The connection is supposed to be updated periodically, based on safekeeper timeline data.
//!
//! * handle the actual connection and WAL streaming
//!
//! Handling happens dynamically, by portions of WAL being processed and registered in the server.
//! Along with the registration, certain metadata is written to show WAL streaming progress and rely on that when considering safekeepers for connection.
//!
//! The current module contains high-level primitives used in the submodules; general synchronization, timeline acknowledgement and shutdown logic.

mod connection_manager;
mod walreceiver_connection;

use anyhow::{ensure, Context};
use etcd_broker::Client;
use itertools::Itertools;
use once_cell::sync::Lazy;
use std::cell::Cell;
use std::collections::{hash_map, HashMap, HashSet};
use std::future::Future;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::thread_local;
use std::time::Duration;
use tokio::{
    select,
    sync::{mpsc, watch, RwLock},
    task::JoinHandle,
};
use tracing::*;
use url::Url;

use crate::config::PageServerConf;
use crate::http::models::WalReceiverEntry;
use crate::tenant_mgr::{self, LocalTimelineUpdate, TenantState};
use crate::thread_mgr::{self, ThreadKind};
use utils::zid::{ZTenantId, ZTenantTimelineId, ZTimelineId};

thread_local! {
    // Boolean that is true only for WAL receiver threads
    //
    // This is used in `wait_lsn` to guard against usage that might lead to a deadlock.
    pub(crate) static IS_WAL_RECEIVER: Cell<bool> = Cell::new(false);
}

/// WAL receiver state for sharing with the outside world.
/// Only entries for timelines currently available in pageserver are stored.
static WAL_RECEIVER_ENTRIES: Lazy<RwLock<HashMap<ZTenantTimelineId, WalReceiverEntry>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Gets the public WAL streaming entry for a certain timeline.
pub async fn get_wal_receiver_entry(
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
) -> Option<WalReceiverEntry> {
    WAL_RECEIVER_ENTRIES
        .read()
        .await
        .get(&ZTenantTimelineId::new(tenant_id, timeline_id))
        .cloned()
}

/// Sets up the main WAL receiver thread that manages the rest of the subtasks inside of it, per timeline.
/// See comments in [`wal_receiver_main_thread_loop_step`] for more details on per timeline activities.
pub fn init_wal_receiver_main_thread(
    conf: &'static PageServerConf,
    mut timeline_updates_receiver: mpsc::UnboundedReceiver<LocalTimelineUpdate>,
) -> anyhow::Result<()> {
    let etcd_endpoints = conf.broker_endpoints.clone();
    ensure!(
        !etcd_endpoints.is_empty(),
        "Cannot start wal receiver: etcd endpoints are empty"
    );
    let broker_prefix = &conf.broker_etcd_prefix;
    info!(
        "Starting wal receiver main thread, etdc endpoints: {}",
        etcd_endpoints.iter().map(Url::to_string).join(", ")
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("wal-receiver-runtime-thread")
        .worker_threads(40)
        .enable_all()
        .on_thread_start(|| IS_WAL_RECEIVER.with(|c| c.set(true)))
        .build()
        .context("Failed to create storage sync runtime")?;
    let etcd_client = runtime
        .block_on(Client::connect(etcd_endpoints, None))
        .context("Failed to connect to etcd")?;

    thread_mgr::spawn(
        ThreadKind::WalReceiverManager,
        None,
        None,
        "WAL receiver manager main thread",
        true,
        move || {
            runtime.block_on(async move {
                let mut local_timeline_wal_receivers = HashMap::new();
                loop {
                    select! {
                        _ = thread_mgr::shutdown_watcher() => {
                            info!("Shutdown signal received");
                            shutdown_all_wal_connections(&mut local_timeline_wal_receivers).await;
                            break;
                        },
                        _ = wal_receiver_main_thread_loop_step(
                            broker_prefix,
                            &etcd_client,
                            &mut timeline_updates_receiver,
                            &mut local_timeline_wal_receivers,
                        ) => {},
                    }
                }
            }.instrument(info_span!("wal_receiver_main")));

            info!("Wal receiver main thread stopped");
            Ok(())
        },
    )
    .map(|_thread_id| ())
    .context("Failed to spawn wal receiver main thread")
}

async fn shutdown_all_wal_connections(
    local_timeline_wal_receivers: &mut HashMap<ZTenantId, HashMap<ZTimelineId, TaskHandle<()>>>,
) {
    info!("Shutting down all WAL connections");
    let mut broker_join_handles = Vec::new();
    for (tenant_id, timelines) in local_timeline_wal_receivers.drain() {
        for (timeline_id, handles) in timelines {
            handles.cancellation.send(()).ok();
            broker_join_handles.push((
                ZTenantTimelineId::new(tenant_id, timeline_id),
                handles.handle,
            ));
        }
    }

    let mut tenants = HashSet::with_capacity(broker_join_handles.len());
    for (id, broker_join_handle) in broker_join_handles {
        tenants.insert(id.tenant_id);
        debug!("Waiting for wal broker for timeline {id} to finish");
        if let Err(e) = broker_join_handle.await {
            error!("Failed to join on wal broker for timeline {id}: {e}");
        }
    }
    if let Err(e) = tokio::task::spawn_blocking(move || {
        for tenant_id in tenants {
            if let Err(e) = tenant_mgr::set_tenant_state(tenant_id, TenantState::Idle) {
                error!("Failed to make tenant {tenant_id} idle: {e:?}");
            }
        }
    })
    .await
    {
        error!("Failed to await a task to make all tenants idle: {e:?}");
    }
}

/// A handle of an asynchronous task.
/// The task has a channel that it can use to communicate its lifecycle events in a certain form, see [`TaskEvent`]
/// and a cancellation channel that it can listen to for earlier interrupts.
///
/// Note that the communication happens via the `watch` channel, that does not accumulate the events, replacing the old one with the never one on submission.
/// That may lead to certain events not being observed by the listener.
#[derive(Debug)]
struct TaskHandle<E> {
    handle: JoinHandle<()>,
    events_receiver: watch::Receiver<TaskEvent<E>>,
    cancellation: watch::Sender<()>,
}

#[derive(Debug, Clone)]
pub enum TaskEvent<E> {
    Started,
    NewEvent(E),
    End(Result<(), String>),
    Cancelled,
}

impl<E: Clone> TaskHandle<E> {
    /// Initializes the task, starting it immediately after the creation.
    pub fn spawn<Fut>(
        task: impl FnOnce(Arc<watch::Sender<TaskEvent<E>>>, watch::Receiver<()>) -> Fut + Send + 'static,
    ) -> Self
    where
        Fut: Future<Output = Result<(), String>> + Send,
        E: Sync + Send + 'static,
    {
        let (cancellation, cancellation_receiver) = watch::channel(());
        let (events_sender, events_receiver) = watch::channel(TaskEvent::Started);
        let events_sender = Arc::new(events_sender);

        let sender = Arc::clone(&events_sender);
        let handle = tokio::task::spawn(async move {
            let task_result = task(sender, cancellation_receiver).await;
            events_sender.send(TaskEvent::End(task_result)).ok();
        });

        TaskHandle {
            handle,
            events_receiver,
            cancellation,
        }
    }

    async fn next_task_event(&mut self) -> TaskEvent<E> {
        macro_rules! join_on_task_handle {
            ($handle:expr) => {
                async {
                    match $handle.await {
                        Ok(()) => TaskEvent::End(Ok(())),
                        Err(e) => {
                            if e.is_panic() {
                                TaskEvent::End(Err(format!("WAL receiver task panicked: {e}")))
                            } else {
                                TaskEvent::Cancelled
                            }
                        }
                    }
                }
            };
        }

        select! {
            next_task_event = self.events_receiver.changed() => match next_task_event {
                Ok(()) => self.events_receiver.borrow().clone(),
                Err(_task_channel_part_dropped) => join_on_task_handle!(&mut self.handle).await,
            },
            task_completion_result = join_on_task_handle!(&mut self.handle) => task_completion_result,
        }
    }

    /// Aborts current task, waiting for it to finish.
    async fn shutdown(self) {
        self.cancellation.send(()).ok();
        if let Err(e) = self.handle.await {
            error!("Task failed to shut down: {e}")
        }
    }
}

/// A step to process timeline attach/detach events to enable/disable the corresponding WAL receiver machinery.
/// In addition to WAL streaming management, the step ensures that corresponding tenant has its service threads enabled or disabled.
/// This is done here, since only walreceiver knows when a certain tenant has no streaming enabled.
///
/// Cannot fail, should always try to process the next timeline event even if the other one was not processed properly.
async fn wal_receiver_main_thread_loop_step<'a>(
    broker_prefix: &'a str,
    etcd_client: &'a Client,
    timeline_updates_receiver: &'a mut mpsc::UnboundedReceiver<LocalTimelineUpdate>,
    local_timeline_wal_receivers: &'a mut HashMap<ZTenantId, HashMap<ZTimelineId, TaskHandle<()>>>,
) {
    // Only react on updates from [`tenant_mgr`] on local timeline attach/detach.
    match timeline_updates_receiver.recv().await {
        Some(update) => {
            info!("Processing timeline update: {update:?}");
            match update {
                // Timeline got detached, stop all related tasks and remove public timeline data.
                LocalTimelineUpdate::Detach(id) => {
                    match local_timeline_wal_receivers.get_mut(&id.tenant_id) {
                        Some(wal_receivers) => {
                            if let hash_map::Entry::Occupied(o) = wal_receivers.entry(id.timeline_id) {
                                o.remove().shutdown().await
                            }
                            if wal_receivers.is_empty() {
                                if let Err(e) = change_tenant_state(id.tenant_id, TenantState::Idle).await {
                                    error!("Failed to make tenant idle for id {id}: {e:#}");
                                }
                            }
                        }
                        None => warn!("Timeline {id} does not have a tenant entry in wal receiver main thread"),
                    };
                    {
                        WAL_RECEIVER_ENTRIES.write().await.remove(&id);
                    }
                }
                // Timeline got attached, retrieve all necessary information to start its broker loop and maintain this loop endlessly.
                LocalTimelineUpdate::Attach(new_id, new_timeline) => {
                    let timeline_connection_managers = local_timeline_wal_receivers
                        .entry(new_id.tenant_id)
                        .or_default();

                    if timeline_connection_managers.is_empty() {
                        if let Err(e) =
                            change_tenant_state(new_id.tenant_id, TenantState::Active).await
                        {
                            error!("Failed to make tenant active for id {new_id}: {e:#}");
                            return;
                        }
                    }

                    let vacant_connection_manager_entry =
                        match timeline_connection_managers.entry(new_id.timeline_id) {
                            hash_map::Entry::Occupied(_) => {
                                debug!("Attepted to readd an existing timeline {new_id}, ignoring");
                                return;
                            }
                            hash_map::Entry::Vacant(v) => v,
                        };

                    let (wal_connect_timeout, lagging_wal_timeout, max_lsn_wal_lag) =
                        match fetch_tenant_settings(new_id.tenant_id).await {
                            Ok(settings) => settings,
                            Err(e) => {
                                error!("Failed to fetch tenant settings for id {new_id}: {e:#}");
                                return;
                            }
                        };

                    {
                        WAL_RECEIVER_ENTRIES.write().await.insert(
                            new_id,
                            WalReceiverEntry {
                                wal_producer_connstr: None,
                                last_received_msg_lsn: None,
                                last_received_msg_ts: None,
                            },
                        );
                    }

                    vacant_connection_manager_entry.insert(
                        connection_manager::spawn_connection_manager_task(
                            new_id,
                            broker_prefix.to_owned(),
                            etcd_client.clone(),
                            new_timeline,
                            wal_connect_timeout,
                            lagging_wal_timeout,
                            max_lsn_wal_lag,
                        ),
                    );
                }
            }
        }
        None => {
            info!("Local timeline update channel closed");
            shutdown_all_wal_connections(local_timeline_wal_receivers).await;
        }
    }
}

async fn fetch_tenant_settings(
    tenant_id: ZTenantId,
) -> anyhow::Result<(Duration, Duration, NonZeroU64)> {
    tokio::task::spawn_blocking(move || {
        let repo = tenant_mgr::get_repository_for_tenant(tenant_id)
            .with_context(|| format!("no repository found for tenant {tenant_id}"))?;
        Ok::<_, anyhow::Error>((
            repo.get_wal_receiver_connect_timeout(),
            repo.get_lagging_wal_timeout(),
            repo.get_max_lsn_wal_lag(),
        ))
    })
    .await
    .with_context(|| format!("Failed to join on tenant {tenant_id} settings fetch task"))?
}

async fn change_tenant_state(tenant_id: ZTenantId, new_state: TenantState) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || {
        tenant_mgr::set_tenant_state(tenant_id, new_state)
            .with_context(|| format!("Failed to activate tenant {tenant_id}"))
    })
    .await
    .with_context(|| format!("Failed to spawn activation task for tenant {tenant_id}"))?
}
