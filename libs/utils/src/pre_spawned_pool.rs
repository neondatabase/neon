use std::{
    collections::VecDeque,
    num::NonZeroUsize,
    sync::{Arc, RwLock},
    thread::JoinHandle,
};

use tokio::sync::{mpsc, OwnedSemaphorePermit};
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument};

pub struct Client<T> {
    cmds_tx: mpsc::UnboundedSender<Command>,
    items_rx: async_channel::Receiver<CreatedItem<T>>,
}

pub trait Launcher<T> {
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
        self.cmds_tx.send(Command::SetSlotCount(count));
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
        let mut pending_items = Arc::new(tokio::sync::Semaphore::new(initial));
        let mut need_forget = 0;
        loop {
            debug!(
                configured,
                need_forget,
                available = pending_items.available_permits(),
                "iteration"
            );
            let cmd = tokio::select! {
                res = self.cmds_rx.recv() => {
                    match res {
                        Some(cmd) => cmd,
                        None => return, // dropping tx acts as cancellation
                    }
                }
                permit = Arc::clone(&pending_items).acquire_owned() => {
                    let permit = permit.expect("we never close this semaphore");
                    if need_forget > 0 {
                        debug!("fogetting permit to reduce semaphore count");
                        need_forget  -= 1;
                        permit.forget();
                        continue;
                    }
                    debug!("creating item");
                    let item = match self.launcher.create() {
                        Ok(item) => item,
                        Err(e) => todo!(),
                    };
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
