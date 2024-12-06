use tokio::sync::mpsc;

/// A bi-directional channel.
pub struct Duplex<S, R> {
    pub tx: mpsc::Sender<S>,
    pub rx: mpsc::Receiver<R>,
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
}
