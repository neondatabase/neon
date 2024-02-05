use std::sync::Arc;

use tokio::sync::{mpsc, OwnedSemaphorePermit};
use tracing::{debug, instrument};

use crate::backoff;

pub struct Client<T> {
    cmds_tx: mpsc::UnboundedSender<Command>,
    items_rx: async_channel::Receiver<CreatedItem<T>>,
}

pub trait Launcher<T> {
    fn what() -> &'static str;
    fn create(&self) -> anyhow::Result<T>;
}

#[derive(Debug)]
enum Command {
    SetSlotCount(usize),
}

#[derive(thiserror::Error, Debug)]
pub enum GetError {
    #[error("shutting down")]
    ShuttingDown,
}

impl<T> Client<T> {
    pub async fn get(&self) -> Result<T, GetError> {
        self.items_rx
            .recv()
            .await
            .map_err(|_| GetError::ShuttingDown)
            .map(|CreatedItem { permit, item }| {
                drop(permit); // allow a new one to be pre-spanwed
                item
            })
    }

    pub fn set_slot_count_nowait(&self, count: usize) {
        self.cmds_tx
            .send(Command::SetSlotCount(count))
            .expect("while cmds_tx is open, the pool task doesn't exit");
    }
}

pub struct Pool<T, L>
where
    T: Send + 'static,
    L: Send + Launcher<T> + 'static,
{
    launcher: L,
    cmds_rx: mpsc::UnboundedReceiver<Command>,
    items_tx: async_channel::Sender<CreatedItem<T>>,
}

struct CreatedItem<T> {
    permit: OwnedSemaphorePermit,
    item: T,
}

impl<T, L> Pool<T, L>
where
    T: Send + 'static,
    L: Send + Launcher<T> + 'static,
{
    pub async fn launch(launcher: L) -> Client<T> {
        let (cmds_tx, cmds_rx) = mpsc::unbounded_channel(); // callers are limited to mgmt api
        let (items_tx, items_rx) = async_channel::unbounded(); // task() limits pending items itself

        // task gets cancelled by dropping the last Client
        tokio::spawn(
            Self {
                launcher,
                cmds_rx,
                items_tx,
            }
            .task(),
        );
        Client { cmds_tx, items_rx }
    }

    #[instrument(skip_all)]
    async fn task(mut self) {
        let initial = 0;
        let mut configured = initial;
        let pending_items = Arc::new(tokio::sync::Semaphore::new(initial));
        let mut need_forget = 0;
        let mut last_launcher_failure_at = None;
        loop {
            debug!(
                configured,
                need_forget,
                available = pending_items.available_permits(),
                last_launcher_failure_secs_ago =
                    last_launcher_failure_at.map(|at| at.elapsed().as_secs_f64()),
                "iteration"
            );
            let try_launch_once = || async {
                let permit = Arc::clone(&pending_items)
                    .acquire_owned()
                    .expect("we never close this semaphore");
                if need_forget > 0 {
                    debug!("fogetting permit to reduce semaphore count");
                    need_forget -= 1;
                    permit.forget();
                    continue;
                }
                debug!("creating item");
                let item = match self.launcher.create() {
                    Ok(item) => item,
                    Err(e) => {
                        error!(
                            "launcher failed to create item: {}",
                            report_compact_sources(&e)
                        );
                    }
                };
            };
            let try_launch_retrying = backoff::retry(
                try_launch_once,
                |_| false,
                0,
                u32::MAX,
                L::what(),
                CancellationToken::new(),
            );
            let cmd = tokio::select! {
                res = self.cmds_rx.recv() => {
                    match res {
                        Some(cmd) => cmd,
                        None => return, // dropping tx acts as cancellation
                    }
                }
                item = try_launch => {
                    match self.items_tx.send(CreatedItem { permit, item }).await {
                        Ok(()) => continue,
                        Err(_) => {
                            debug!("stopping, client has gone away");
                            return;
                        }
                    }
                }
            };
            debug!(?cmd, "received command");
            match cmd {
                Command::SetSlotCount(desired) => {
                    if desired > configured {
                        pending_items.add_permits(desired - configured);
                    } else if desired < configured {
                        need_forget += configured - desired;
                    }
                    configured = desired;
                }
            }
        }
    }
}
