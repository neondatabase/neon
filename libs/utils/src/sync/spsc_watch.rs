//! watch is probably not the right word, because we do take out

use tokio_util::sync::CancellationToken;

use crate::sync::spsc_fold;

pub fn channel<T: Send>() -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = spsc_fold::channel();
    let cancel = CancellationToken::new();
    (
        Sender {
            tx,
            _cancel: cancel.clone().drop_guard(),
        },
        Receiver { rx, cancel },
    )
}

pub struct Sender<T> {
    tx: spsc_fold::Sender<T>,
    _cancel: tokio_util::sync::DropGuard,
}

pub struct Receiver<T> {
    rx: spsc_fold::Receiver<T>,
    cancel: CancellationToken,
}

impl<T: Send> Sender<T> {
    pub fn send_replace(&mut self, value: T) -> Result<(), (T, spsc_fold::SendError)> {
        poll_ready(self.tx.send(value, |old, new| {
            *old = new;
            Ok(())
        }))
    }
}

impl<T: Send> Receiver<T> {
    pub async fn recv(&mut self) -> Result<T, spsc_fold::RecvError> {
        self.rx.recv().await
    }
    pub async fn cancelled(&mut self) {
        self.cancel.cancelled().await
    }
}

fn poll_ready<F: Future<Output = O>, O>(f: F) -> O {
    futures::executor::block_on(async move {
        let f = std::pin::pin!(f);
        match futures::poll!(f) {
            std::task::Poll::Ready(r) => r,
            std::task::Poll::Pending => unreachable!("expecting future to always return Ready"),
        }
    })
}
