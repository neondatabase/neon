//!
//! Most async functions throughout the pageserver take a `ctx: &RequestContext`
//! argument. It is used to control desired behaviour of the operation, and to
//! allow cancelling the operation gracefully.
//!
//! # Context hierarchy
//!
//! RequestContext's form a hierarchy. For example:
//!
//!  listener context (LibpqEndpointListener)
//!    connection context (PageRequestHandler)
//!      per-request context (PageRequestHandler)
//!
//! The top "listener context" is created at pageserver startup. The tokio
//! task that listens on the libpq protocol TCP port holds that context. When
//! it accepts a connection, it spawns a new task to handle that connection
//! and creates a new per-connection context for it. The mgmt API listener,
//! background jobs, and other things form separate but similar hierarchies.
//!
//! Usually, each tokio task has its own context, but it's not a strict
//! requirement and some tasks can hold multiple contexts, and converesely,
//! some contexts are shared by multiple tasks that work together to perform
//! some operation.
//!
//! The hierarchy is not explictly tracked in the RequestContext struct
//! itself, but only by their cancellation tokens. It's entirely possible for
//! the parent context to be dropped before its children.
//!
//! # Tenant and Timeline registration
//!
//! Most operations are performed on a particular Tenant or Timeline. When
//! operating on a Tenant or Timeline, it's important that the Tenant/Timeline
//! isn't detached or deleted while there are tasks working on it. To ensure
//! that, a RequestContext can be registered with a Tenant or Timeline. See
//! `Tenant::register_context` and `Timeline::register_context` When
//! shutting down a Tenant or Timeline, the shutdown routine cancels all the
//! registered contexts, and waits for them to be dropped before completing
//! the shutdown.
//!
//! To enforce that you hold a registered context when operating on a Tenant
//! or Timeline, most functions take a TimelineRequestContext or
//! TenantRequestContext reference as argument.
//!
//! NOTE: The Tenant / Timeline registration is separate from the context
//! hierarchy. You can create a new RequestContext with TimelineRequestContext
//! as the parent, and register it with a different timeline, for example.
//!
//! # Notes
//!
//! All RequestContexts in the system have a unique ID, and are also tracked
//! in a global hash table, CONTEXTS.
//!
//! - Futures are normally not assumed to be async cancellation-safe. Pass a
//!   RequestContext as argument and use cancel() on it instead.
//!
//! - If you perform an operation that depends on some external actor or the
//!   network, use the cancellation token to check for cancellation
//!
//! - By convention, the appropriate context for current operation is carried in
//!   a variable called 'ctx'. If a function handles multiple contexts, it's
//!   best to *not* have a variable called 'ctx', to force you to think which
//!   one to use in each call.
//!
//! # TODO
//! - include a unique request ID for tracing
//!

use once_cell::sync::Lazy;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Each RequestContext has a unique context ID. It's just an increasing
/// number that we assign.
static NEXT_CONTEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Global registry of contexts
static CONTEXTS: Lazy<Mutex<HashMap<RequestContextId, (TaskKind, CancellationToken)>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct RequestContextId(u64);

///
pub struct RequestContext {
    context_id: RequestContextId,
    task_kind: TaskKind,

    download_behavior: DownloadBehavior,
    cancellation_token: CancellationToken,
}

/// DownloadBehavior option specifies the behavior if completing the operation
/// would require downloading a layer file from remote storage.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DownloadBehavior {
    /// Download the layer file. It can take a while.
    Download,

    /// Download the layer file, but print a warning to the log. This should be used
    /// in code where the layer file is expected to already exist locally.
    Warn,

    /// Return a PageReconstructError::NeedsDownload error
    Error,
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

    // Context for one management API request
    MgmtRequest,

    // Manages the WAL receiver connection for one timeline. It subscribes to
    // events from storage_broker, decides which safekeeper to connect to. It spawns a
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
    LayerFlush,

    // Task that uploads a file to remote storage
    RemoteUploadTask,

    // Task that downloads a file from remote storage
    RemoteDownloadTask,

    // task that handles the initial downloading of all tenants
    InitialLoad,

    // task that handles attaching a tenant
    Attach,

    // task that handles metrics collection
    MetricsCollection,

    // task that drives downloading layers
    DownloadAllRemoteLayers,

    // Only used in unit tests
    UnitTest,
}

impl Drop for RequestContext {
    fn drop(&mut self) {
        CONTEXTS
            .lock()
            .unwrap()
            .remove(&self.context_id)
            .expect("context is not in global registry");
    }
}

impl RequestContext {
    /// Create a new RequestContext
    pub fn new(task_kind: TaskKind, download_behavior: DownloadBehavior) -> Self {
        let cancellation_token = CancellationToken::new();
        let context_id = RequestContextId(NEXT_CONTEXT_ID.fetch_add(1, Ordering::Relaxed));
        CONTEXTS
            .lock()
            .unwrap()
            .insert(context_id, (task_kind, cancellation_token.clone()));

        RequestContext {
            task_kind,
            context_id,
            download_behavior,
            cancellation_token,
        }
    }

    /// Create a new RequestContext, as a child of 'parent'.
    pub fn with_parent(
        task_kind: TaskKind,
        download_behavior: DownloadBehavior,
        parent: &RequestContext,
    ) -> Self {
        let cancellation_token = parent.cancellation_token.child_token();
        let context_id = RequestContextId(NEXT_CONTEXT_ID.fetch_add(1, Ordering::Relaxed));
        CONTEXTS
            .lock()
            .unwrap()
            .insert(context_id, (task_kind, cancellation_token.clone()));

        RequestContext {
            task_kind,
            context_id,
            download_behavior,
            cancellation_token,
        }
    }

    pub fn context_id(&self) -> RequestContextId {
        self.context_id
    }

    pub fn task_kind(&self) -> TaskKind {
        self.task_kind
    }

    pub fn download_behavior(&self) -> DownloadBehavior {
        self.download_behavior
    }

    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }

    pub async fn cancelled(&self) {
        self.cancellation_token.cancelled().await
    }
}

///
/// Cancel all the contexts in 'context_ids' and wait for them to finish.
///
/// Whenever we notice that one of the contexts has finished, it is removed
/// from 'context_ids'. On return, it is empty.
///
pub async fn cancel_and_wait(context_ids: &mut Vec<RequestContextId>) {
    {
        let contexts = CONTEXTS.lock().unwrap();
        context_ids.retain(|context_id| {
            if let Some((task_kind, cancellation_token)) = contexts.get(context_id) {
                info!("cancelling task {task_kind:?} with ID {context_id:?}");
                cancellation_token.cancel();
                true
            } else {
                // Already gone
                false
            }
        });
    }
    wait_contexts_to_finish(context_ids).await
}

async fn wait_contexts_to_finish(context_ids: &mut Vec<RequestContextId>) {
    let mut n = 0;
    while !context_ids.is_empty() {
        {
            let contexts = CONTEXTS.lock().unwrap();
            while let Some(context_id) = context_ids.last() {
                if let Some((task_kind, _cancellation_token)) = contexts.get(context_id) {
                    info!("waiting for task {task_kind:?} with ID {context_id:?} to finish");
                    break;
                } else {
                    context_ids.pop();
                }
            }
        }
        if !context_ids.is_empty() {
            crate::exponential_backoff(
                n,
                crate::DEFAULT_BASE_BACKOFF_SECONDS,
                crate::DEFAULT_MAX_BACKOFF_SECONDS,
            )
            .await;
            n += 1;
        }
    }
}

/// Cancel and wait for all tasks of given 'kind' to finish
pub async fn shutdown_tasks(kind: TaskKind) {
    let mut context_ids = Vec::new();
    {
        let contexts = CONTEXTS.lock().unwrap();
        for (&context_id, (task_kind, cancellation_token)) in contexts.iter() {
            if *task_kind == kind {
                cancellation_token.cancel();
                context_ids.push(context_id);
            }
        }
    }
    wait_contexts_to_finish(&mut context_ids).await
}

/// Cancel all remaining contexts.
///
/// This is used as part of pageserver shutdown. We have already shut down all
/// tasks / contexts, this is just a backstop or sanity check to make sure we
/// didn't miss anything. Hence, also print a warning for any remaining tasks.
pub async fn shutdown_all_tasks() {
    loop {
        let mut context_ids = Vec::new();
        {
            let contexts = CONTEXTS.lock().unwrap();

            if contexts.is_empty() {
                return;
            }

            for (&context_id, (task_kind, cancellation_token)) in contexts.iter() {
                cancellation_token.cancel();
                context_ids.push(context_id);
                warn!(
                    "unexpected task of kind {:?} with ID {:?} still running",
                    *task_kind, context_id
                );
            }
        }
        wait_contexts_to_finish(&mut context_ids).await
    }
}
