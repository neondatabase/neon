//! WAL receiver manages an open connection to safekeeper, to get the WAL it streams into.
//! To do so, a current implementation needs to do the following:
//!
//! * acknowledge the timelines that it needs to stream WAL into.
//! Pageserver is able to dynamically (un)load tenants on attach and detach,
//! hence WAL receiver needs to react on such events.
//!
//! * get a broker subscription, stream data from it to determine that a timeline needs WAL streaming.
//! For that, it watches specific keys in storage_broker and pulls the relevant data periodically.
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

use crate::task_mgr::WALRECEIVER_RUNTIME;

use std::future::Future;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::*;

pub use connection_manager::spawn_connection_manager_task;

/// A handle of an asynchronous task.
/// The task has a channel that it can use to communicate its lifecycle events in a certain form, see [`TaskEvent`]
/// and a cancellation token that it can listen to for earlier interrupts.
///
/// Note that the communication happens via the `watch` channel, that does not accumulate the events, replacing the old one with the never one on submission.
/// That may lead to certain events not being observed by the listener.
#[derive(Debug)]
pub struct TaskHandle<E> {
    join_handle: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
    events_receiver: watch::Receiver<TaskStateUpdate<E>>,
    cancellation: CancellationToken,
}

pub enum TaskEvent<E> {
    Update(TaskStateUpdate<E>),
    End(anyhow::Result<()>),
}

#[derive(Debug, Clone)]
pub enum TaskStateUpdate<E> {
    Started,
    Progress(E),
}

impl<E: Clone> TaskHandle<E> {
    /// Initializes the task, starting it immediately after the creation.
    pub fn spawn<Fut>(
        task: impl FnOnce(watch::Sender<TaskStateUpdate<E>>, CancellationToken) -> Fut + Send + 'static,
    ) -> Self
    where
        Fut: Future<Output = anyhow::Result<()>> + Send,
        E: Send + Sync + 'static,
    {
        let cancellation = CancellationToken::new();
        let (events_sender, events_receiver) = watch::channel(TaskStateUpdate::Started);

        let cancellation_clone = cancellation.clone();
        let join_handle = WALRECEIVER_RUNTIME.spawn(async move {
            events_sender.send(TaskStateUpdate::Started).ok();
            task(events_sender, cancellation_clone).await
            // events_sender is dropped at some point during the .await above.
            // But the task is still running on WALRECEIVER_RUNTIME.
            // That is the window when `!jh.is_finished()`
            // is true inside `fn next_task_event()` below.
        });

        TaskHandle {
            join_handle: Some(join_handle),
            events_receiver,
            cancellation,
        }
    }

    async fn next_task_event(&mut self) -> TaskEvent<E> {
        match self.events_receiver.changed().await {
            Ok(()) => TaskEvent::Update((self.events_receiver.borrow()).clone()),
            Err(_task_channel_part_dropped) => {
                TaskEvent::End(match self.join_handle.as_mut() {
                    Some(jh) => {
                        if !jh.is_finished() {
                            // Barring any implementation errors in this module, we can
                            // only arrive here while the task that executes the future
                            // passed to `Self::spawn()` is still execution. Cf the comment
                            // in Self::spawn().
                            //
                            // This was logging at warning level in earlier versions, presumably
                            // to leave some breadcrumbs in case we had an implementation
                            // error that would would make us get stuck in `jh.await`.
                            //
                            // There hasn't been such a bug so far.
                            // But in a busy system, e.g., during pageserver restart,
                            // we arrive here often enough that the warning-level logs
                            // became a distraction.
                            // So, tone them down to info-level.
                            //
                            // XXX: rewrite this module to eliminate the race condition.
                            info!("sender is dropped while join handle is still alive");
                        }

                        let res = jh
                            .await
                            .map_err(|e| anyhow::anyhow!("Failed to join task: {e}"))
                            .and_then(|x| x);

                        // For cancellation-safety, drop join_handle only after successful .await.
                        self.join_handle = None;

                        res
                    }
                    None => {
                        // Another option is to have an enum, join handle or result and give away the reference to it
                        Err(anyhow::anyhow!("Task was joined more than once"))
                    }
                })
            }
        }
    }

    /// Aborts current task, waiting for it to finish.
    pub async fn shutdown(self) {
        if let Some(jh) = self.join_handle {
            self.cancellation.cancel();
            match jh.await {
                Ok(Ok(())) => debug!("Shutdown success"),
                Ok(Err(e)) => error!("Shutdown task error: {e:?}"),
                Err(join_error) => {
                    if join_error.is_cancelled() {
                        error!("Shutdown task was cancelled");
                    } else {
                        error!("Shutdown task join error: {join_error}")
                    }
                }
            }
        }
    }
}
