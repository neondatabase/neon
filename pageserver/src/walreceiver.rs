//! FIXME: update comment
//!
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

use anyhow::{ensure, Context};
use etcd_broker::Client;
use itertools::Itertools;
use once_cell::sync::OnceCell;
use std::cell::Cell;
use std::future::Future;
use std::sync::Arc;
use std::thread_local;
use tokio::{runtime::Handle, select, sync::watch, task::JoinHandle};
use tracing::*;
use url::Url;

pub use connection_manager::spawn_connection_manager_task;

thread_local! {
    // Boolean that is true only for WAL receiver threads
    //
    // This is used in `wait_lsn` to guard against usage that might lead to a deadlock.
    //
    // FIXME: check if this still works. The code that used to set this is gone..
    pub(crate) static IS_WAL_RECEIVER: Cell<bool> = Cell::new(false);
}

static ETCD_CLIENT: OnceCell<Client> = OnceCell::new();

static WALRECEIVER_RUNTIME: OnceCell<tokio::runtime::Runtime> = OnceCell::new();
static WALRECEIVER_RUNTIME_HANDLE: OnceCell<Handle> = OnceCell::new();

///
/// Get a handle to the WAL receiver runtime
///
pub fn get_walreceiver_runtime() -> Handle {
    //
    // In unit tests, page server startup doesn't happen and no one calls
    // init_etcd_client. Use the current runtime instead.
    //
    if cfg!(test) {
        WALRECEIVER_RUNTIME_HANDLE
            .get_or_init(|| Handle::current().clone())
            .clone()
    } else {
        WALRECEIVER_RUNTIME_HANDLE
            .get()
            .expect("walreceiver runtime not initialized")
            .clone()
    }
}

///
/// Initialize the etcd client. This must be called once at page server startup.
///
pub fn init_etcd_client(conf: &'static PageServerConf) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("wal-receiver-runtime-thread")
        .enable_all()
        .on_thread_start(|| IS_WAL_RECEIVER.with(|c| c.set(true)))
        .build()
        .context("Failed to create storage sync runtime")?;

    let etcd_endpoints = conf.broker_endpoints.clone();
    ensure!(
        !etcd_endpoints.is_empty(),
        "Cannot start wal receiver: etcd endpoints are empty"
    );

    let etcd_client = runtime
        .block_on(Client::connect(etcd_endpoints.clone(), None))
        .context("Failed to connect to etcd")?;

    // FIXME: Should we still allow the pageserver to start, if etcd
    // doesn't work? It could still serve GetPage requests, with the
    // data it has locally and from what it can download from remote
    // storage

    if WALRECEIVER_RUNTIME_HANDLE
        .set(runtime.handle().clone())
        .is_err()
    {
        panic!("walreceiver runtime already initialized");
    }
    // XXX: It's not enough to hold on to the handle. If the Runtime is
    // dropped, the runtime is gone and all the threads are killed, even if
    // you still hold a reference to its handle. So we stash the Runtime
    // itself in WALRECEIVER_RUNTIME, to prevent it from being dropped.
    if WALRECEIVER_RUNTIME.set(runtime).is_err() {
        panic!("walreceiver runtime already initialized");
    }

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

/// A handle of an asynchronous task.
/// The task has a channel that it can use to communicate its lifecycle events in a certain form, see [`TaskEvent`]
/// and a cancellation channel that it can listen to for earlier interrupts.
///
/// Note that the communication happens via the `watch` channel, that does not accumulate the events, replacing the old one with the never one on submission.
/// That may lead to certain events not being observed by the listener.
#[derive(Debug)]
pub struct TaskHandle<E> {
    handle: JoinHandle<Result<(), String>>,
    events_receiver: watch::Receiver<TaskEvent<E>>,
    cancellation: watch::Sender<()>,
}

#[derive(Debug, Clone)]
pub enum TaskEvent<E> {
    Started,
    NewEvent(E),
    End(Result<(), String>),
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
        let handle = get_walreceiver_runtime().spawn(async move {
            events_sender.send(TaskEvent::Started).ok();
            task(sender, cancellation_receiver).await
        });

        TaskHandle {
            handle,
            events_receiver,
            cancellation,
        }
    }

    async fn next_task_event(&mut self) -> TaskEvent<E> {
        select! {
            next_task_event = self.events_receiver.changed() => match next_task_event {
                Ok(()) => self.events_receiver.borrow().clone(),
                Err(_task_channel_part_dropped) => join_on_handle(&mut self.handle).await,
            },
            task_completion_result = join_on_handle(&mut self.handle) => task_completion_result,
        }
    }

    /// Aborts current task, waiting for it to finish.
    pub async fn shutdown(self) {
        self.cancellation.send(()).ok();
        if let Err(e) = self.handle.await {
            error!("Task failed to shut down: {e}")
        }
    }
}

async fn join_on_handle<E>(handle: &mut JoinHandle<Result<(), String>>) -> TaskEvent<E> {
    match handle.await {
        Ok(task_result) => TaskEvent::End(task_result),
        Err(e) => {
            if e.is_cancelled() {
                TaskEvent::End(Ok(()))
            } else {
                TaskEvent::End(Err(format!("WAL receiver task panicked: {e}")))
            }
        }
    }
}
