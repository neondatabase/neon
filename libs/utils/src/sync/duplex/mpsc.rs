use tokio::sync::mpsc;
use tokio::sync::oneshot;

/// Sends values to the associated `Receiver`.
///
/// Instances are created by the [`channel`] function.
pub struct Sender<S, R> {
    pub tx: mpsc::Sender<Request<S, R>>,
}

/// Receives values from the associated `Sender`.
///
/// Instances are created by the [`channel`] function.
pub struct Receiver<S, R> {
    pub rx: mpsc::Receiver<Request<S, R>>,
}

/// Request type that [`Sender`] sends to [`Receiver`]
pub struct Request<S, R> {
    /// Actual payload
    pub payload: S,
    /// Sends associated response back to the associated `Receiver` on the sender side.
    /// Instances are created by the [`Sender::send`] function.
    pub response_tx: oneshot::Sender<R>,
}

impl<S, R> Request<S, R> {
    /// Creates a new request that can send back response.
    pub fn new(data: S, response_tx: oneshot::Sender<R>) -> Self {
        Request {
            payload: data,
            response_tx,
        }
    }
}

pub mod error {
    pub type SendError<S, R> = tokio::sync::mpsc::error::SendError<super::Request<S, R>>;
    pub type RecvError = tokio::sync::oneshot::error::RecvError;
}

/// Creates a bounded mpsc channel that enables bi-directional communication between asynchronous tasks
/// with backpressure.
///
/// The channel will buffer up to the provided number of messages.  Once the
/// buffer is full, attempts to send new messages will wait until a message is
/// received from the channel. The provided buffer capacity must be at least 1.
///
/// # Panics
///
/// Panics if the buffer capacity is 0.
pub fn channel<S: Send, R: Send>(buffer: usize) -> (Sender<S, R>, Receiver<S, R>) {
    let (tx, rx) = mpsc::channel::<Request<S, R>>(buffer);
    (Sender { tx }, Receiver { rx })
}

impl<S: Send, R: Send> Sender<S, R> {
    /// Sends a value, waiting until there is capacity. On success, returns a one-shot channel receiver that
    /// gets the associated response back.
    pub async fn send(&self, x: S) -> Result<oneshot::Receiver<R>, error::SendError<S, R>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Request::new(x, tx)).await?;
        Ok(rx)
    }
}

impl<S: Send, R: Send> Receiver<S, R> {
    /// Receives the next value for the receiver.
    ///
    /// This method returns `None` if the channel has been closed and there are
    /// no remaining messages in the channel's buffer.
    pub async fn recv(&mut self) -> Option<Request<S, R>> {
        self.rx.recv().await
    }
}
