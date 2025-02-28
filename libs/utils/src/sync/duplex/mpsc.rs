use tokio::sync::mpsc;

/// A bi-directional channel.
pub struct Duplex<S, R> {
    tx: mpsc::Sender<S>,
    rx: RecvOnly<R>,
}

/// Creates a bi-directional channel.
///
/// The channel will buffer up to the provided number of messages. Once the buffer is full,
/// attempts to send new messages will wait until a message is received from the channel.
/// The provided buffer capacity must be at least 1.
pub fn channel<A: Send, B: Send>(buffer: usize) -> (Duplex<A, B>, Duplex<B, A>) {
    let (tx_a, rx_a) = mpsc::channel::<A>(buffer);
    let (tx_b, rx_b) = mpsc::channel::<B>(buffer);

    (
        Duplex {
            tx: tx_a,
            rx: RecvOnly { rx: rx_b },
        },
        Duplex {
            tx: tx_b,
            rx: RecvOnly { rx: rx_a },
        },
    )
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

    pub fn shutdown_write_half(self) -> RecvOnly<R> {
        let Self { tx: _, rx } = { self };
        rx
    }

    /// Future
    pub async fn wait_for_peer_gone(&self) {
        self.tx.closed().await
    }

    pub fn is_peer_gone(&self) -> bool {
        self.tx.is_closed()
    }
}

pub struct RecvOnly<R> {
    rx: mpsc::Receiver<R>,
}

impl<R: Send> RecvOnly<R> {
    pub async fn recv(&mut self) -> Option<R> {
        self.rx.recv().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    #[tokio::test]
    async fn test_orderly_shutdown_buffered_writer_use_case() {
        let (a, mut b) = super::channel::<i32, i32>(1);

        let written = Arc::new(std::sync::Mutex::new(Vec::new()));

        let flush_task = tokio::spawn({
            let written = Arc::clone(&written);
            async move {
                loop {
                    match b.recv().await {
                        Some(slice) => {
                            dbg!(slice);
                            // retry flush until sucess, with exponential back-off
                            for attempt in 1.. {
                                println!("attempt {}", attempt);
                                if b.is_peer_gone() {
                                    anyhow::bail!("peer is gone");
                                }
                                if slice % 2 == 0 {
                                    // simulate success
                                    written.lock().unwrap().push(slice.clone());
                                    break;
                                } else {
                                    println!("simulated IO error");
                                    crate::backoff::exponential_backoff2(
                                        attempt,
                                        0.1,
                                        0.5,
                                        b.wait_for_peer_gone(),
                                    )
                                    .await;
                                }
                            }
                            // return the slice for reuse; if there is no interest, still process remaining flushes
                            let _ = b.send(slice).await;
                        }
                        None => {
                            println!("peer hung up");
                            return Ok(());
                        }
                    }
                }
            }
        });

        a.send(2).await.unwrap();
        a.send(4).await.unwrap();

        // orderly shutdown looks like this
        let mut a = a.shutdown_write_half();
        let recv = a.recv().await.unwrap();
        assert_eq!(recv, 2);
        let recv = a.recv().await.unwrap();
        assert_eq!(recv, 4);
        flush_task
            .await
            .expect("flush task to not panic")
            .expect("flush task to exit with Ok()");

        assert_eq!(written.lock().unwrap().as_slice(), &[2, 4]);
    }
}
