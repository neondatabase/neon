use core::{future::poll_fn, task::Poll};
use std::sync::{Arc, Mutex};

use diatomic_waker::DiatomicWaker;

pub struct Sender<T> {
    state: Arc<Inner<T>>,
}

pub struct Receiver<T> {
    state: Arc<Inner<T>>,
}

struct Inner<T> {
    wake_receiver: DiatomicWaker,
    wake_sender: DiatomicWaker,
    value: Mutex<State<T>>,
}

enum State<T> {
    NoData,
    HasData(T),
    TryFoldFailed, // transient state
    SenderWaitsForReceiverToConsume(T),
    SenderGone(Option<T>),
    ReceiverGone,
    AllGone,
    SenderDropping,   // transient state
    ReceiverDropping, // transient state
}

pub fn channel<T: Send>() -> (Sender<T>, Receiver<T>) {
    let inner = Inner {
        wake_receiver: DiatomicWaker::new(),
        wake_sender: DiatomicWaker::new(),
        value: Mutex::new(State::NoData),
    };

    let state = Arc::new(inner);
    (
        Sender {
            state: state.clone(),
        },
        Receiver { state },
    )
}

#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("receiver is gone")]
    ReceiverGone,
}

impl<T: Send> Sender<T> {
    /// # Panics
    ///
    /// If `try_fold` panics,  any subsequent call to `send` panic.
    pub async fn send<F>(&mut self, value: T, try_fold: F) -> Result<(), SendError>
    where
        F: Fn(&mut T, T) -> Result<(), T>,
    {
        let mut value = Some(value);
        poll_fn(|cx| {
            let mut guard = self.state.value.lock().unwrap();
            match &mut *guard {
                State::NoData => {
                    *guard = State::HasData(value.take().unwrap());
                    self.state.wake_receiver.notify();
                    Poll::Ready(Ok(()))
                }
                State::HasData(_) => {
                    let State::HasData(acc_mut) = &mut *guard else {
                        unreachable!("this match arm guarantees that the guard is HasData");
                    };
                    match try_fold(acc_mut, value.take().unwrap()) {
                        Ok(()) => {
                            // no need to wake receiver, if it was waiting it already
                            // got a wake-up when we transitioned from NoData to HasData
                            Poll::Ready(Ok(()))
                        }
                        Err(unfoldable_value) => {
                            value = Some(unfoldable_value);
                            let State::HasData(acc) =
                                std::mem::replace(&mut *guard, State::TryFoldFailed)
                            else {
                                unreachable!("this match arm guarantees that the guard is HasData");
                            };
                            *guard = State::SenderWaitsForReceiverToConsume(acc);
                            // SAFETY: send is single threaded due to `&mut self` requirement,
                            // therefore register is not concurrent.
                            unsafe {
                                self.state.wake_sender.register(cx.waker());
                            }
                            Poll::Pending
                        }
                    }
                }
                State::TryFoldFailed => {
                    unreachable!();
                }
                State::SenderWaitsForReceiverToConsume(_data) => {
                    // Really, we shouldn't be polled until receiver has consumed and wakes us.
                    Poll::Pending
                }
                State::ReceiverGone => Poll::Ready(Err(SendError::ReceiverGone)),
                State::SenderGone(_) => {
                    unreachable!();
                }
                State::AllGone => {
                    unreachable!();
                }
                State::SenderDropping => {
                    unreachable!();
                }
                State::ReceiverDropping => {
                    unreachable!();
                }
            }
        })
        .await
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let Ok(mut guard) = self.state.value.lock() else {
            return;
        };
        *guard = match std::mem::replace(&mut *guard, State::SenderDropping) {
            State::NoData => State::SenderGone(None),
            State::HasData(data) | State::SenderWaitsForReceiverToConsume(data) => {
                State::SenderGone(Some(data))
            }
            State::TryFoldFailed => unreachable!(),
            State::SenderGone(_) => unreachable!(),
            State::ReceiverGone => State::AllGone,
            State::AllGone => unreachable!(),
            State::SenderDropping => unreachable!(),
            State::ReceiverDropping => unreachable!(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RecvError {
    #[error("sender is gone")]
    SenderGone,
}

impl<T: Send> Receiver<T> {
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        poll_fn(|cx| {
            let mut guard = self.state.value.lock().unwrap();
            match &mut *guard {
                State::NoData => {
                    // SAFETY: recv is single threaded due to `&mut self` requirement,
                    // therefore register is not concurrent.
                    unsafe {
                        self.state.wake_receiver.register(cx.waker());
                    }
                    Poll::Pending
                }
                guard @ State::HasData(_) => {
                    let State::HasData(data) = std::mem::replace(guard, State::NoData) else {
                        unreachable!("this match arm guarantees that the guard is HasData");
                    };
                    self.state.wake_sender.notify();
                    Poll::Ready(Ok(data))
                }
                State::TryFoldFailed => {
                    unreachable!();
                }
                guard @ State::SenderWaitsForReceiverToConsume(_) => {
                    let State::SenderWaitsForReceiverToConsume(data) =
                        std::mem::replace(guard, State::NoData) else {
                        unreachable!(
                            "this match arm guarantees that the guard is SenderWaitsForReceiverToConsume"
                        );
                    };
                    self.state.wake_sender.notify();
                    Poll::Ready(Ok(data))
                }
                guard @ State::SenderGone(Some(_)) => {
                    let State::SenderGone(Some(data)) =
                        std::mem::replace(guard, State::SenderGone(None))
                    else {
                        unreachable!(
                            "this match arm guarantees that the guard is SenderGone(Some(_))"
                        );
                    };
                    Poll::Ready(Ok(data))
                }
                State::SenderGone(None) => Poll::Ready(Err(RecvError::SenderGone)),
                State::ReceiverGone => {
                    unreachable!();
                }
                State::AllGone => {
                    unreachable!();
                }
                State::SenderDropping => {
                    unreachable!();
                }
                State::ReceiverDropping => {
                    unreachable!();
                }
            }
        })
        .await
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let Ok(mut guard) = self.state.value.lock() else {
            return;
        };
        *guard = match std::mem::replace(&mut *guard, State::ReceiverDropping) {
            State::NoData => State::ReceiverGone,
            State::HasData(_) | State::SenderWaitsForReceiverToConsume(_) => State::ReceiverGone,
            State::TryFoldFailed => unreachable!(),
            State::SenderGone(_) => State::AllGone,
            State::ReceiverGone => unreachable!(),
            State::AllGone => unreachable!(),
            State::SenderDropping => unreachable!(),
            State::ReceiverDropping => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    const FOREVER: std::time::Duration = std::time::Duration::from_secs(u64::MAX);

    #[tokio::test]
    async fn test_send_recv() {
        let (mut sender, mut receiver) = channel();

        sender
            .send(42, |acc, val| {
                *acc += val;
                Ok(())
            })
            .await
            .unwrap();

        let received = receiver.recv().await.unwrap();
        assert_eq!(received, 42);
    }

    #[tokio::test]
    async fn test_send_recv_with_fold() {
        let (mut sender, mut receiver) = channel();

        sender
            .send(1, |acc, val| {
                *acc += val;
                Ok(())
            })
            .await
            .unwrap();
        sender
            .send(2, |acc, val| {
                *acc += val;
                Ok(())
            })
            .await
            .unwrap();

        let received = receiver.recv().await.unwrap();
        assert_eq!(received, 3);
    }

    #[tokio::test(start_paused = true)]
    async fn test_sender_waits_for_receiver_if_try_fold_fails() {
        let (mut sender, mut receiver) = channel();

        sender.send(23, |_, _| panic!("first send")).await.unwrap();

        let send_fut = sender.send(42, |_, val| Err(val));
        let mut send_fut = std::pin::pin!(send_fut);

        tokio::select! {
            _ = tokio::time::sleep(FOREVER) => {},
            _ = &mut send_fut => {
                panic!("send should not complete");
            },
        }

        let val = receiver.recv().await.unwrap();
        assert_eq!(val, 23);

        tokio::select! {
            _ = tokio::time::sleep(FOREVER) => {
                panic!("receiver should have consumed the value");
            },
            _ = &mut send_fut => { },
        }

        let val = receiver.recv().await.unwrap();
        assert_eq!(val, 42);
    }

    #[tokio::test(start_paused = true)]
    async fn test_sender_errors_if_waits_for_receiver_and_receiver_drops() {
        let (mut sender, receiver) = channel();

        sender.send(23, |_, _| unreachable!()).await.unwrap();

        let send_fut = sender.send(42, |_, val| Err(val));
        let send_fut = std::pin::pin!(send_fut);

        drop(receiver);

        let result = send_fut.await;
        assert!(matches!(result, Err(SendError::ReceiverGone)));
    }

    #[tokio::test(start_paused = true)]
    async fn test_receiver_errors_if_waits_for_sender_and_sender_drops() {
        let (sender, mut receiver) = channel::<()>();

        let recv_fut = receiver.recv();
        let recv_fut = std::pin::pin!(recv_fut);

        drop(sender);

        let result = recv_fut.await;
        assert!(matches!(result, Err(RecvError::SenderGone)));
    }

    #[tokio::test(start_paused = true)]
    async fn test_receiver_errors_if_waits_for_sender_and_sender_drops_with_data() {
        let (mut sender, mut receiver) = channel();

        sender.send(42, |_, _| unreachable!()).await.unwrap();

        {
            let recv_fut = receiver.recv();
            let recv_fut = std::pin::pin!(recv_fut);

            drop(sender);

            let val = recv_fut.await.unwrap();
            assert_eq!(val, 42);
        }

        let result = receiver.recv().await;
        assert!(matches!(result, Err(RecvError::SenderGone)));
    }

    #[tokio::test(start_paused = true)]
    async fn test_receiver_waits_for_sender_if_no_data() {
        let (mut sender, mut receiver) = channel();

        let recv_fut = receiver.recv();
        let mut recv_fut = std::pin::pin!(recv_fut);

        tokio::select! {
            _ = tokio::time::sleep(FOREVER) => {},
            _ = &mut recv_fut => {
                panic!("recv should not complete");
            },
        }

        sender.send(42, |_, _| Ok(())).await.unwrap();

        let val = recv_fut.await.unwrap();
        assert_eq!(val, 42);
    }

    #[tokio::test]
    async fn test_receiver_gone_while_nodata() {
        let (mut sender, receiver) = channel();
        drop(receiver);

        let result = sender.send(42, |_, _| Ok(())).await;
        assert!(matches!(result, Err(SendError::ReceiverGone)));
    }

    #[tokio::test]
    async fn test_sender_gone_while_nodata() {
        let (sender, mut receiver) = super::channel::<usize>();
        drop(sender);

        let result = receiver.recv().await;
        assert!(matches!(result, Err(RecvError::SenderGone)));
    }
}