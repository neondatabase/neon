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

use crate::config::PageServerConf;
use crate::task_mgr::WALRECEIVER_RUNTIME;

use anyhow::{ensure, Context};
use etcd_broker::Client;
use itertools::Itertools;
use once_cell::sync::OnceCell;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::*;
use url::Url;

pub use connection_manager::spawn_connection_manager_task;

static ETCD_CLIENT: OnceCell<Client> = OnceCell::new();

///
/// Initialize the etcd client. This must be called once at page server startup.
///
pub async fn init_etcd_client(conf: &'static PageServerConf) -> anyhow::Result<()> {
    let etcd_endpoints = conf.broker_endpoints.clone();
    ensure!(
        !etcd_endpoints.is_empty(),
        "Cannot start wal receiver: etcd endpoints are empty"
    );

    let etcd_client = Client::connect(etcd_endpoints.clone(), None)
        .await
        .context("Failed to connect to etcd")?;

    // FIXME: Should we still allow the pageserver to start, if etcd
    // doesn't work? It could still serve GetPage requests, with the
    // data it has locally and from what it can download from remote
    // storage
    if ETCD_CLIENT.set(etcd_client).is_err() {
        panic!("etcd already initialized");
    }

    info!(
        "Initialized etcd client with endpoints: {}",
        etcd_endpoints.iter().map(Url::to_string).join(", ")
    );
    Ok(())
}

///
/// Get a handle to the etcd client
///
pub fn get_etcd_client() -> &'static etcd_broker::Client {
    ETCD_CLIENT.get().expect("etcd client not initialized")
}

pub fn is_etcd_client_initialized() -> bool {
    ETCD_CLIENT.get().is_some()
}

/// A handle of an asynchronous task.
/// The task has a channel that it can use to communicate its lifecycle events in a certain form, see [`TaskEvent`]
/// and a cancellation channel that it can listen to for earlier interrupts.
///
/// Note that the communication happens via the `watch` channel, that does not accumulate the events, replacing the old one with the never one on submission.
/// That may lead to certain events not being observed by the listener.
#[derive(Debug)]
pub struct TaskHandle<E> {
    events_receiver: watch::Receiver<TaskEvent<E>>,
    cancellation: watch::Sender<()>,
}

#[derive(Debug, Clone)]
pub enum TaskEvent<E> {
    Started,
    NewEvent(E),
    End,
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
        let _ = WALRECEIVER_RUNTIME.spawn(async move {
            events_sender.send(TaskEvent::Started).ok();
            task(sender, cancellation_receiver).await
        });

        TaskHandle {
            events_receiver,
            cancellation,
        }
    }

    async fn next_task_event(&mut self) -> TaskEvent<E> {
        match self.events_receiver.changed().await {
            Ok(()) => self.events_receiver.borrow().clone(),
            Err(_task_channel_part_dropped) => TaskEvent::End,
        }
    }

    /// Aborts current task, waiting for it to finish.
    pub async fn shutdown(mut self) {
        self.cancellation.send(()).ok();
        // wait until the sender is dropped
        while self.events_receiver.changed().await.is_ok() {}
    }
}
