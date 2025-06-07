//! Like [`tokio::sync::watch`] but the Sender gets an error when the Receiver is gone.
//!
//! TODO: an actually more efficient implementation

use tokio_util::sync::CancellationToken;

pub fn channel<T>(init: T) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = tokio::sync::watch::channel(init);
    let cancel = CancellationToken::new();
    (
        Sender {
            tx,
            cancel: cancel.clone().drop_guard(),
        },
        Receiver { rx, cancel },
    )
}

pub struct Sender<T> {
    tx: tokio::sync::watch::Sender<T>,
    cancel: tokio_util::sync::DropGuard,
}

pub struct Receiver<T> {
    rx: tokio::sync::watch::Receiver<T>,
    cancel: CancellationToken,
}

pub enum RecvError {
    SenderGone,
}
pub enum SendError {
    ReceiverGone,
}

impl<T> Sender<T> {
    pub fn send_replace(&mut self, value: T) -> Result<(), (T, SendError)> {
        if self.tx.receiver_count() == 0 {
            // we don't provide outside access to tx, so, we know the only
            // rx that is ever going to exist is gone
            return Err((value, SendError::ReceiverGone));
        }
        self.tx.send_replace(value);
        Ok(())
    }
}

impl<T> Receiver<T> {
    pub async fn changed(&mut self) -> Result<(), RecvError> {
        match self.rx.changed().await {
            Ok(()) => Ok(self.rx.borrow()),
            Err(e) => Err(RecvError::SenderGone),
        }
    }
    pub fn borrow(&self) -> impl Deref<Target = T> {
        self.rx.borrow()
    }
    pub async fn closed(&mut self) {
        self.cancel.cancelled().await
    }
}
