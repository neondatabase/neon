use tokio::sync::mpsc;

/// A bi-directional channel.
pub struct Duplex<S, R> {
    tx: mpsc::Sender<S>,
    rx: mpsc::Receiver<R>,
}

/// Creates a bi-directional channel.
///
/// The channel will buffer up to the provided number of messages. Once the buffer is full,
/// attempts to send new messages will wait until a message is received from the channel.
/// The provided buffer capacity must be at least 1.
pub fn channel<A: Send, B: Send>(buffer: usize) -> (Duplex<A, B>, Duplex<B, A>) {
    let (tx_a, rx_a) = mpsc::channel::<A>(buffer);
    let (tx_b, rx_b) = mpsc::channel::<B>(buffer);

    (Duplex { tx: tx_a, rx: rx_b }, Duplex { tx: tx_b, rx: rx_a })
}

impl<S: Send, R: Send> Duplex<S, R> {
    /// Sends a value, waiting until there is capacity.
    ///
    /// A successful send occurs when it is determined that the other end of the channel has not hung up already.
    pub async fn send(&self, x: S) -> Result<(), mpsc::error::SendError<S>> {
        self.tx.send(x).await
    }

    /// Receives the next value for this receiver.
    ///
    /// This method returns `None` if the channel has been closed and there are
    /// no remaining messages in the channel's buffer.
    pub async fn recv(&mut self) -> Option<R> {
        self.rx.recv().await
    }

    pub fn close(self) {
        let Self { tx: _, rx } = { self };
        drop(rx); // this makes the other Duplex's tx resolve on tx.closed()
    }

    /// Future
    pub async fn closed(&self) {
        self.tx.closed().await
    }

    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    const FOREVER: Duration = Duration::from_secs(100 * 365 * 24 * 60 * 60);

    #[tokio::test(start_paused = true)]
    async fn test_await_close() {
        let (a, mut b) = super::channel::<i32, i32>(1);

        let mut recv_fut = Box::pin(b.recv());

        tokio::select! {
            _ = &mut recv_fut => unreachable!("nothing was sent"),
            _ = tokio::time::sleep(FOREVER) => (),
        }

        a.close();

        tokio::select! {
            res = &mut recv_fut => {
                assert!(res.is_none());
            },
            _ = tokio::time::sleep(FOREVER) => (),
        }

        drop(recv_fut);

        assert!(b.is_closed());
    }
}
