//!
//! This module provides centralized handling of threads in the Page Server.
//!
//! We provide a few basic facilities:
//! - A global registry of threads that lists what kind of threads they are, and
//!   which tenant or timeline they are working on
//!
//! - The ability to request a thread to shut down.
//!
//!
//! # How it works?
//!
//! There is a global hashmap of all the threads (`THREADS`). Whenever a new
//! thread is spawned, a PageServerThread entry is added there, and when a
//! thread dies, it removes itself from the hashmap. If you want to kill a
//! thread, you can scan the hashmap to find it.
//!
//! # Thread shutdown
//!
//! To kill a thread, we rely on co-operation from the victim. Each thread is
//! expected to periodically call the `is_shutdown_requested()` function, and
//! if it returns true, exit gracefully. In addition to that, when waiting for
//! the network or other long-running operation, you can use
//! `shutdown_watcher()` function to get a Future that will become ready if
//! the current thread has been requested to shut down. You can use that with
//! Tokio select!(), but note that it relies on thread-local storage, so it
//! will only work with the "current-thread" Tokio runtime!
//!
//!
//! TODO: This would be a good place to also handle panics in a somewhat sane way.
//! Depending on what thread panics, we might want to kill the whole server, or
//! only a single tenant or timeline.
//!

use std::cell::RefCell;
use std::collections::HashMap;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

use tokio::sync::watch;

use tracing::{debug, error, info, warn};

use lazy_static::lazy_static;

use utils::zid::{ZTenantId, ZTimelineId};

use crate::shutdown_pageserver;

lazy_static! {
    /// Each thread that we track is associated with a "thread ID". It's just
    /// an increasing number that we assign, not related to any system thread
    /// id.
    static ref NEXT_THREAD_ID: AtomicU64 = AtomicU64::new(1);

    /// Global registry of threads
    static ref THREADS: Mutex<HashMap<u64, Arc<PageServerThread>>> = Mutex::new(HashMap::new());
}

// There is a Tokio watch channel for each thread, which can be used to signal the
// thread that it needs to shut down. This thread local variable holds the receiving
// end of the channel. The sender is kept in the global registry, so that anyone
// can send the signal to request thread shutdown.
thread_local!(static SHUTDOWN_RX: RefCell<Option<watch::Receiver<()>>> = RefCell::new(None));

// Each thread holds reference to its own PageServerThread here.
thread_local!(static CURRENT_THREAD: RefCell<Option<Arc<PageServerThread>>> = RefCell::new(None));

///
/// There are many kinds of threads in the system. Some are associated with a particular
/// tenant or timeline, while others are global.
///
/// Note that we don't try to limit how may threads of a certain kind can be running
/// at the same time.
///
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ThreadKind {
    // libpq listener thread. It just accepts connection and spawns a
    // PageRequestHandler thread for each connection.
    LibpqEndpointListener,

    // HTTP endpoint listener.
    HttpEndpointListener,

    // Thread that handles a single connection. A PageRequestHandler thread
    // starts detached from any particular tenant or timeline, but it can be
    // associated with one later, after receiving a command from the client.
    PageRequestHandler,

    // Thread that connects to a safekeeper to fetch WAL for one timeline.
    WalReceiver,

    // Thread that handles compaction of all timelines for a tenant.
    Compactor,

    // Thread that handles GC of a tenant
    GarbageCollector,

    // Thread that flushes frozen in-memory layers to disk
    LayerFlushThread,

    // Thread for synchronizing pageserver layer files with the remote storage.
    // Shared by all tenants.
    StorageSync,
}

struct PageServerThread {
    _thread_id: u64,

    kind: ThreadKind,

    /// Tenant and timeline that this thread is associated with.
    tenant_id: Option<ZTenantId>,
    timeline_id: Option<ZTimelineId>,

    name: String,

    // To request thread shutdown, set the flag, and send a dummy message to the
    // channel to notify it.
    shutdown_requested: AtomicBool,
    shutdown_tx: watch::Sender<()>,

    /// Handle for waiting for the thread to exit. It can be None, if the
    /// the thread has already exited.
    join_handle: Mutex<Option<JoinHandle<()>>>,
}

/// Launch a new thread
/// Note: if shutdown_process_on_error is set to true failure
///   of the thread will lead to shutdown of entire process
pub fn spawn<F>(
    kind: ThreadKind,
    tenant_id: Option<ZTenantId>,
    timeline_id: Option<ZTimelineId>,
    name: &str,
    shutdown_process_on_error: bool,
    f: F,
) -> std::io::Result<()>
where
    F: FnOnce() -> anyhow::Result<()> + Send + 'static,
{
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let thread_id = NEXT_THREAD_ID.fetch_add(1, Ordering::Relaxed);
    let thread = PageServerThread {
        _thread_id: thread_id,
        kind,
        tenant_id,
        timeline_id,
        name: name.to_string(),

        shutdown_requested: AtomicBool::new(false),
        shutdown_tx,

        join_handle: Mutex::new(None),
    };

    let thread_rc = Arc::new(thread);

    let mut jh_guard = thread_rc.join_handle.lock().unwrap();

    THREADS
        .lock()
        .unwrap()
        .insert(thread_id, Arc::clone(&thread_rc));

    let thread_rc2 = Arc::clone(&thread_rc);
    let thread_name = name.to_string();
    let join_handle = match thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            thread_wrapper(
                thread_name,
                thread_id,
                thread_rc2,
                shutdown_rx,
                shutdown_process_on_error,
                f,
            )
        }) {
        Ok(handle) => handle,
        Err(err) => {
            error!("Failed to spawn thread '{}': {}", name, err);
            // Could not spawn the thread. Remove the entry
            THREADS.lock().unwrap().remove(&thread_id);
            return Err(err);
        }
    };
    *jh_guard = Some(join_handle);
    drop(jh_guard);

    // The thread is now running. Nothing more to do here
    Ok(())
}

/// This wrapper function runs in a newly-spawned thread. It initializes the
/// thread-local variables and calls the payload function
fn thread_wrapper<F>(
    thread_name: String,
    thread_id: u64,
    thread: Arc<PageServerThread>,
    shutdown_rx: watch::Receiver<()>,
    shutdown_process_on_error: bool,
    f: F,
) where
    F: FnOnce() -> anyhow::Result<()> + Send + 'static,
{
    SHUTDOWN_RX.with(|rx| {
        *rx.borrow_mut() = Some(shutdown_rx);
    });
    CURRENT_THREAD.with(|ct| {
        *ct.borrow_mut() = Some(thread);
    });

    debug!("Starting thread '{}'", thread_name);

    // We use AssertUnwindSafe here so that the payload function
    // doesn't need to be UnwindSafe. We don't do anything after the
    // unwinding that would expose us to unwind-unsafe behavior.
    let result = panic::catch_unwind(AssertUnwindSafe(f));

    // Remove our entry from the global hashmap.
    let thread = THREADS
        .lock()
        .unwrap()
        .remove(&thread_id)
        .expect("no thread in registry");

    match result {
        Ok(Ok(())) => debug!("Thread '{}' exited normally", thread_name),
        Ok(Err(err)) => {
            if shutdown_process_on_error {
                error!(
                    "Shutting down: thread '{}' tenant_id: {:?}, timeline_id: {:?} exited with error: {:?}",
                    thread_name, thread.tenant_id, thread.timeline_id, err
                );
                shutdown_pageserver(1);
            } else {
                error!(
                    "Thread '{}' tenant_id: {:?}, timeline_id: {:?} exited with error: {:?}",
                    thread_name, thread.tenant_id, thread.timeline_id, err
                );
            }
        }
        Err(err) => {
            if shutdown_process_on_error {
                error!(
                    "Shutting down: thread '{}' tenant_id: {:?}, timeline_id: {:?} panicked: {:?}",
                    thread_name, thread.tenant_id, thread.timeline_id, err
                );
                shutdown_pageserver(1);
            } else {
                error!(
                    "Thread '{}' tenant_id: {:?}, timeline_id: {:?} panicked: {:?}",
                    thread_name, thread.tenant_id, thread.timeline_id, err
                );
            }
        }
    }
}

/// Is there a thread running that matches the criteria

/// Signal and wait for threads to shut down.
///
///
/// The arguments are used to select the threads to kill. Any None arguments are
/// ignored. For example, to shut down all WalReceiver threads:
///
///   shutdown_threads(Some(ThreadKind::WalReceiver), None, None)
///
/// Or to shut down all threads for given timeline:
///
///   shutdown_threads(None, Some(timelineid), None)
///
pub fn shutdown_threads(
    kind: Option<ThreadKind>,
    tenant_id: Option<ZTenantId>,
    timeline_id: Option<ZTimelineId>,
) {
    let mut victim_threads = Vec::new();

    let threads = THREADS.lock().unwrap();
    for thread in threads.values() {
        if (kind.is_none() || Some(thread.kind) == kind)
            && (tenant_id.is_none() || thread.tenant_id == tenant_id)
            && (timeline_id.is_none() || thread.timeline_id == timeline_id)
        {
            thread.shutdown_requested.store(true, Ordering::Relaxed);
            // FIXME: handle error?
            let _ = thread.shutdown_tx.send(());
            victim_threads.push(Arc::clone(thread));
        }
    }
    drop(threads);

    for thread in victim_threads {
        info!("waiting for {} to shut down", thread.name);
        if let Some(join_handle) = thread.join_handle.lock().unwrap().take() {
            let _ = join_handle.join();
        } else {
            // The thread had not even fully started yet. Or it was shut down
            // concurrently and already exited
        }
    }
}

/// A Future that can be used to check if the current thread has been requested to
/// shut down.
pub async fn shutdown_watcher() {
    let _ = SHUTDOWN_RX
        .with(|rx| {
            rx.borrow()
                .as_ref()
                .expect("shutdown_requested() called in an unexpected thread")
                .clone()
        })
        .changed()
        .await;
}

/// Has the current thread been requested to shut down?
pub fn is_shutdown_requested() -> bool {
    CURRENT_THREAD.with(|ct| {
        if let Some(ct) = ct.borrow().as_ref() {
            ct.shutdown_requested.load(Ordering::Relaxed)
        } else {
            if !cfg!(test) {
                warn!("is_shutdown_requested() called in an unexpected thread");
            }
            false
        }
    })
}
