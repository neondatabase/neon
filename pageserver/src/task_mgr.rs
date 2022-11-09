//!
//! This module provides centralized handling of tokio tasks in the Page Server.
//!
//! We provide a few basic facilities:
//! - A global registry of tasks that lists what kind of tasks they are, and
//!   which tenant or timeline they are working on
//!
//! - The ability to request a task to shut down.
//!
//!
//! # How it works?
//!
//! There is a global hashmap of all the tasks (`TASKS`). Whenever a new
//! task is spawned, a PageServerTask entry is added there, and when a
//! task dies, it removes itself from the hashmap. If you want to kill a
//! task, you can scan the hashmap to find it.
//!
//! # Task shutdown
//!
//! To kill a task, we rely on co-operation from the victim. Each task is
//! expected to periodically call the `is_shutdown_requested()` function, and
//! if it returns true, exit gracefully. In addition to that, when waiting for
//! the network or other long-running operation, you can use
//! `shutdown_watcher()` function to get a Future that will become ready if
//! the current task has been requested to shut down. You can use that with
//! Tokio select!().
//!
//!
//! TODO: This would be a good place to also handle panics in a somewhat sane way.
//! Depending on what task panics, we might want to kill the whole server, or
//! only a single tenant or timeline.
//!

// Clippy 1.60 incorrectly complains about the tokio::task_local!() macro.
// Silence it. See https://github.com/rust-lang/rust-clippy/issues/9224.
#![allow(clippy::declare_interior_mutable_const)]

use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures::FutureExt;
use tokio::runtime::Runtime;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::task_local;

use tracing::{debug, error, info, warn};

use once_cell::sync::Lazy;

use utils::id::{TenantId, TimelineId};

use crate::shutdown_pageserver;

//
// There are four runtimes:
//
// Compute request runtime
//  - used to handle connections from compute nodes. Any tasks related to satisfying
//    GetPage requests, base backups, import, and other such compute node operations
//    are handled by the Compute request runtime
//  - page_service.rs
//  - this includes layer downloads from remote storage, if a layer is needed to
//    satisfy a GetPage request
//
// Management request runtime
//  - used to handle HTTP API requests
//
// WAL receiver runtime:
//  - used to handle WAL receiver connections.
//  - and to receiver updates from etcd
//
// Background runtime
//  - layer flushing
//  - garbage collection
//  - compaction
//  - remote storage uploads
//  - initial tenant loading
//
// Everything runs in a tokio task. If you spawn new tasks, spawn it using the correct
// runtime.
//
// There might be situations when one task needs to wait for a task running in another
// Runtime to finish. For example, if a background operation needs a layer from remote
// storage, it will start to download it. If a background operation needs a remote layer,
// and the download was already initiated by a GetPage request, the background task
// will wait for the download - running in the Page server runtime - to finish.
// Another example: the initial tenant loading tasks are launched in the background ops
// runtime. If a GetPage request comes in before the load of a tenant has finished, the
// GetPage request will wait for the tenant load to finish.
//
// The core Timeline code is synchronous, and uses a bunch of std Mutexes and RWLocks to
// protect data structures. Let's keep it that way. Synchronous code is easier to debug
// and analyze, and there's a lot of hairy, low-level, performance critical code there.
//
// It's nice to have different runtimes, so that you can quickly eyeball how much CPU
// time each class of operations is taking, with 'top -H' or similar.
//
// It's also good to avoid hogging all threads that would be needed to process
// other operations, if the upload tasks e.g. get blocked on locks. It shouldn't
// happen, but still.
//
pub static COMPUTE_REQUEST_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name("compute request worker")
        .enable_all()
        .build()
        .expect("Failed to create compute request runtime")
});

pub static MGMT_REQUEST_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name("mgmt request worker")
        .enable_all()
        .build()
        .expect("Failed to create mgmt request runtime")
});

pub static WALRECEIVER_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name("walreceiver worker")
        .enable_all()
        .build()
        .expect("Failed to create walreceiver runtime")
});

pub static BACKGROUND_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name("background op worker")
        .enable_all()
        .build()
        .expect("Failed to create background op runtime")
});

pub struct PageserverTaskId(u64);

/// Each task that we track is associated with a "task ID". It's just an
/// increasing number that we assign. Note that it is different from tokio::task::Id.
static NEXT_TASK_ID: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(1));

/// Global registry of tasks
static TASKS: Lazy<Mutex<HashMap<u64, Arc<PageServerTask>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

task_local! {
    // There is a Tokio watch channel for each task, which can be used to signal the
    // task that it needs to shut down. This task local variable holds the receiving
    // end of the channel. The sender is kept in the global registry, so that anyone
    // can send the signal to request task shutdown.
    static SHUTDOWN_RX: watch::Receiver<bool>;

    // Each task holds reference to its own PageServerTask here.
    static CURRENT_TASK: Arc<PageServerTask>;
}

///
/// There are many kinds of tasks in the system. Some are associated with a particular
/// tenant or timeline, while others are global.
///
/// Note that we don't try to limit how many task of a certain kind can be running
/// at the same time.
///
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TaskKind {
    // libpq listener task. It just accepts connection and spawns a
    // PageRequestHandler task for each connection.
    LibpqEndpointListener,

    // HTTP endpoint listener.
    HttpEndpointListener,

    // Task that handles a single connection. A PageRequestHandler task
    // starts detached from any particular tenant or timeline, but it can be
    // associated with one later, after receiving a command from the client.
    PageRequestHandler,

    // Manages the WAL receiver connection for one timeline. It subscribes to
    // events from etcd, decides which safekeeper to connect to. It spawns a
    // separate WalReceiverConnection task to handle each connection.
    WalReceiverManager,

    // Handles a connection to a safekeeper, to stream WAL to a timeline.
    WalReceiverConnection,

    // Garbage collection worker. One per tenant
    GarbageCollector,

    // Compaction. One per tenant.
    Compaction,

    // Initial logical size calculation
    InitialLogicalSizeCalculation,

    // Task that flushes frozen in-memory layers to disk
    LayerFlushTask,

    // Task that manages the remote upload queue
    StorageSync,

    // task that handles the initial downloading of all tenants
    InitialLoad,

    // task that handles attaching a tenant
    Attach,

    // Task that manages tenant drop
    TenantDrop,
}

#[derive(Default)]
struct MutableTaskState {
    /// Tenant and timeline that this task is associated with.
    tenant_id: Option<TenantId>,
    timeline_id: Option<TimelineId>,

    /// Handle for waiting for the task to exit. It can be None, if the
    /// the task has already exited.
    join_handle: Option<JoinHandle<()>>,
}

struct PageServerTask {
    #[allow(dead_code)] // unused currently
    task_id: PageserverTaskId,

    kind: TaskKind,

    name: String,

    // To request task shutdown, send 'true' to the channel to notify the task.
    shutdown_tx: watch::Sender<bool>,

    mutable: Mutex<MutableTaskState>,
}

/// Launch a new task
/// Note: if shutdown_process_on_error is set to true failure
///   of the task will lead to shutdown of entire process
pub fn spawn<F>(
    runtime: &tokio::runtime::Handle,
    kind: TaskKind,
    tenant_id: Option<TenantId>,
    timeline_id: Option<TimelineId>,
    name: &str,
    shutdown_process_on_error: bool,
    future: F,
) -> PageserverTaskId
where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task_id = NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed);
    let task = Arc::new(PageServerTask {
        task_id: PageserverTaskId(task_id),
        kind,
        name: name.to_string(),
        shutdown_tx,
        mutable: Mutex::new(MutableTaskState {
            tenant_id,
            timeline_id,
            join_handle: None,
        }),
    });

    TASKS.lock().unwrap().insert(task_id, Arc::clone(&task));

    let mut task_mut = task.mutable.lock().unwrap();

    let task_name = name.to_string();
    let task_cloned = Arc::clone(&task);
    let join_handle = runtime.spawn(task_wrapper(
        task_name,
        task_id,
        task_cloned,
        shutdown_rx,
        shutdown_process_on_error,
        future,
    ));
    task_mut.join_handle = Some(join_handle);
    drop(task_mut);

    // The task is now running. Nothing more to do here
    PageserverTaskId(task_id)
}

/// This wrapper function runs in a newly-spawned task. It initializes the
/// task-local variables and calls the payload function.
async fn task_wrapper<F>(
    task_name: String,
    task_id: u64,
    task: Arc<PageServerTask>,
    shutdown_rx: watch::Receiver<bool>,
    shutdown_process_on_error: bool,
    future: F,
) where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    debug!("Starting task '{}'", task_name);

    let result = SHUTDOWN_RX
        .scope(
            shutdown_rx,
            CURRENT_TASK.scope(task, {
                // We use AssertUnwindSafe here so that the payload function
                // doesn't need to be UnwindSafe. We don't do anything after the
                // unwinding that would expose us to unwind-unsafe behavior.
                AssertUnwindSafe(future).catch_unwind()
            }),
        )
        .await;
    task_finish(result, task_name, task_id, shutdown_process_on_error).await;
}

async fn task_finish(
    result: std::result::Result<
        anyhow::Result<()>,
        std::boxed::Box<dyn std::any::Any + std::marker::Send>,
    >,
    task_name: String,
    task_id: u64,
    shutdown_process_on_error: bool,
) {
    // Remove our entry from the global hashmap.
    let task = TASKS
        .lock()
        .unwrap()
        .remove(&task_id)
        .expect("no task in registry");

    let mut shutdown_process = false;
    {
        let task_mut = task.mutable.lock().unwrap();

        match result {
            Ok(Ok(())) => {
                debug!("Task '{}' exited normally", task_name);
            }
            Ok(Err(err)) => {
                if shutdown_process_on_error {
                    error!(
                        "Shutting down: task '{}' tenant_id: {:?}, timeline_id: {:?} exited with error: {:?}",
                        task_name, task_mut.tenant_id, task_mut.timeline_id, err
                    );
                    shutdown_process = true;
                } else {
                    error!(
                        "Task '{}' tenant_id: {:?}, timeline_id: {:?} exited with error: {:?}",
                        task_name, task_mut.tenant_id, task_mut.timeline_id, err
                    );
                }
            }
            Err(err) => {
                if shutdown_process_on_error {
                    error!(
                        "Shutting down: task '{}' tenant_id: {:?}, timeline_id: {:?} panicked: {:?}",
                        task_name, task_mut.tenant_id, task_mut.timeline_id, err
                    );
                    shutdown_process = true;
                } else {
                    error!(
                        "Task '{}' tenant_id: {:?}, timeline_id: {:?} panicked: {:?}",
                        task_name, task_mut.tenant_id, task_mut.timeline_id, err
                    );
                }
            }
        }
    }

    if shutdown_process {
        shutdown_pageserver(1).await;
    }
}

// expected to be called from the task of the given id.
pub fn associate_with(tenant_id: Option<TenantId>, timeline_id: Option<TimelineId>) {
    CURRENT_TASK.with(|ct| {
        let mut task_mut = ct.mutable.lock().unwrap();
        task_mut.tenant_id = tenant_id;
        task_mut.timeline_id = timeline_id;
    });
}

/// Is there a task running that matches the criteria

/// Signal and wait for tasks to shut down.
///
///
/// The arguments are used to select the tasks to kill. Any None arguments are
/// ignored. For example, to shut down all WalReceiver tasks:
///
///   shutdown_tasks(Some(TaskKind::WalReceiver), None, None)
///
/// Or to shut down all tasks for given timeline:
///
///   shutdown_tasks(None, Some(tenant_id), Some(timeline_id))
///
pub async fn shutdown_tasks(
    kind: Option<TaskKind>,
    tenant_id: Option<TenantId>,
    timeline_id: Option<TimelineId>,
) {
    let mut victim_tasks = Vec::new();

    {
        let tasks = TASKS.lock().unwrap();
        for task in tasks.values() {
            let task_mut = task.mutable.lock().unwrap();
            if (kind.is_none() || Some(task.kind) == kind)
                && (tenant_id.is_none() || task_mut.tenant_id == tenant_id)
                && (timeline_id.is_none() || task_mut.timeline_id == timeline_id)
            {
                let _ = task.shutdown_tx.send_replace(true);
                victim_tasks.push(Arc::clone(task));
            }
        }
    }

    for task in victim_tasks {
        let join_handle = {
            let mut task_mut = task.mutable.lock().unwrap();
            info!("waiting for {} to shut down", task.name);
            let join_handle = task_mut.join_handle.take();
            drop(task_mut);
            join_handle
        };
        if let Some(join_handle) = join_handle {
            let _ = join_handle.await;
        } else {
            // Possibly one of:
            //  * The task had not even fully started yet.
            //  * It was shut down concurrently and already exited
        }
    }
}

pub fn current_task_kind() -> Option<TaskKind> {
    CURRENT_TASK.try_with(|ct| ct.kind).ok()
}

/// A Future that can be used to check if the current task has been requested to
/// shut down.
pub async fn shutdown_watcher() {
    let mut shutdown_rx = SHUTDOWN_RX
        .try_with(|rx| rx.clone())
        .expect("shutdown_requested() called in an unexpected task or thread");

    while !*shutdown_rx.borrow() {
        if shutdown_rx.changed().await.is_err() {
            break;
        }
    }
}

/// Has the current task been requested to shut down?
pub fn is_shutdown_requested() -> bool {
    if let Ok(shutdown_rx) = SHUTDOWN_RX.try_with(|rx| rx.clone()) {
        *shutdown_rx.borrow()
    } else {
        if !cfg!(test) {
            warn!("is_shutdown_requested() called in an unexpected task or thread");
        }
        false
    }
}
