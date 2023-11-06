use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    task::{self, Poll},
    time::Duration,
};

use pin_list::PinList;
use pin_project_lite::pin_project;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{timeout, Instant};

use super::limit_algorithm::{LimitAlgorithm, Sample};

/// Limits the number of concurrent jobs.
///
/// Concurrency is limited through the use of [Token]s. Acquire a token to run a job, and release the
/// token once the job is finished.
///
/// The limit will be automatically adjusted based on observed latency (delay) and/or failures
/// caused by overload (loss).
#[derive(Debug)]
pub struct Limiter<T> {
    limit_algo: AsyncMutex<T>,
    inner: LimiterInner,
}

type SemaphoreTypes = dyn pin_list::Types<
    Id = pin_list::id::Checked,
    Protected = std::task::Waker,
    Removed = (),
    Unprotected = (),
>;

#[derive(Debug)]
struct LimiterInner {
    semaphore2: Mutex<PinList<SemaphoreTypes>>,

    // ONLY WRITE WHEN LIMIT_ALGO IS LOCKED
    limits: AtomicUsize,
    // ONLY USE ATOMIC ADD/SUB
    in_flight: AtomicUsize,

    #[cfg(test)]
    notifier: Option<std::sync::Arc<tokio::sync::Notify>>,
}

/// A concurrency token, required to run a job.
///
/// Release the token back to the [Limiter] after the job is complete.
#[derive(Debug)]
pub struct Token<'t> {
    limiter: &'t LimiterInner,
    start: Instant,
}

/// A snapshot of the state of the [Limiter].
///
/// Not guaranteed to be consistent under high concurrency.
#[derive(Debug, Clone, Copy)]
pub struct LimiterState {
    limit: usize,
    available: usize,
    in_flight: usize,
}

/// Whether a job succeeded or failed as a result of congestion/overload.
///
/// Errors not considered to be caused by overload should be ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The job succeeded, or failed in a way unrelated to overload.
    Success,
    /// The job failed because of overload, e.g. it timed out or an explicit backpressure signal
    /// was observed.
    Overload,
}

impl Outcome {
    fn from_reqwest_error(error: &reqwest_middleware::Error) -> Self {
        match error {
            reqwest_middleware::Error::Middleware(_) => Outcome::Success,
            reqwest_middleware::Error::Reqwest(e) => {
                if let Some(status) = e.status() {
                    if status.is_server_error()
                        || reqwest::StatusCode::TOO_MANY_REQUESTS.as_u16() == status
                    {
                        Outcome::Overload
                    } else {
                        Outcome::Success
                    }
                } else {
                    Outcome::Success
                }
            }
        }
    }
}

impl<T> Limiter<T>
where
    T: LimitAlgorithm,
{
    /// Create a limiter with a given limit control algorithm.
    pub fn new(limit_algorithm: T, initial_limit: usize) -> Self {
        assert!(initial_limit > 0);
        Self {
            limit_algo: AsyncMutex::new(limit_algorithm),
            inner: LimiterInner {
                semaphore2: Mutex::new(PinList::new(pin_list::id::Checked::new())),
                limits: AtomicUsize::new(initial_limit),
                in_flight: AtomicUsize::new(0),

                #[cfg(test)]
                notifier: None,
            },
        }
    }

    /// In some cases [Token]s are acquired asynchronously when updating the limit.
    #[cfg(test)]
    pub fn with_release_notifier(mut self, n: std::sync::Arc<tokio::sync::Notify>) -> Self {
        self.inner.notifier = Some(n);
        self
    }

    /// Try to immediately acquire a concurrency [Token].
    ///
    /// Returns `None` if there are none available.
    pub fn try_acquire(&self) -> Option<Token<'_>> {
        let limit = self.inner.limits.load(Ordering::Acquire);
        self.inner
            .in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |in_flight| {
                if in_flight >= limit {
                    None
                } else {
                    Some(in_flight + 1)
                }
            })
            .map(|_| Token::new(&self.inner))
            .ok()
    }

    /// Try to acquire a concurrency [Token], waiting for `duration` if there are none available.
    ///
    /// Returns `None` if there are none available after `duration`.
    pub async fn acquire_timeout(&self, duration: Duration) -> Option<Token<'_>> {
        match timeout(
            duration,
            Acquire {
                semaphore: &self.inner,
                node: pin_list::Node::new(),
            },
        )
        .await
        {
            Ok(permit) => Some(permit),
            Err(_) => None,
        }
    }

    /// Return the concurrency [Token], along with the outcome of the job.
    ///
    /// The [Outcome] of the job, and the time taken to perform it, may be used
    /// to update the concurrency limit.
    ///
    /// Set the outcome to `None` to ignore the job.
    pub async fn release(&self, token: Token<'_>, outcome: Option<Outcome>) {
        let in_flight = self.inner.in_flight.load(Ordering::Acquire);
        let old_limit = self.inner.limits.load(Ordering::Acquire);

        let new_limit = if let Some(outcome) = outcome {
            let mut algo = self.limit_algo.lock().await;

            let sample = Sample {
                latency: token.start.elapsed(),
                in_flight,
                outcome,
            };

            let new_limit = algo.update(old_limit, sample).await;
            // update limit while locked
            self.inner.limits.store(new_limit, Ordering::Release);
            new_limit
        } else {
            old_limit
        };
        std::mem::forget(token);

        self.inner.release(in_flight, new_limit, 1);

        #[cfg(test)]
        if let Some(n) = &self.inner.notifier {
            n.notify_one();
        }
    }

    /// The current state of the limiter.
    pub fn state(&self) -> LimiterState {
        let limit = self.inner.limits.load(Ordering::Relaxed);
        let in_flight = self.inner.in_flight.load(Ordering::Relaxed);
        LimiterState {
            limit,
            available: limit.saturating_sub(in_flight),
            in_flight,
        }
    }
}

impl LimiterInner {
    // release some permits to the wake list
    fn release(&self, mut in_flight: usize, mut new_limit: usize, mut extra: usize) {
        // TODO: use array vec like tokio
        let mut wakers = Vec::<task::Waker>::new();
        loop {
            for waker in wakers.drain(..) {
                waker.wake()
            }

            let to_wake = new_limit.saturating_sub(in_flight) + extra;
            if to_wake == 0 {
                return;
            }

            let mut semaphore = self.semaphore2.lock().unwrap();
            if semaphore.is_empty() {
                // release the extra tokens
                if extra > 0 {
                    self.in_flight.fetch_sub(extra, Ordering::Release);
                }
                return;
            }

            let mut cursor = semaphore.cursor_front_mut();
            while wakers.len() < usize::min(to_wake, 32) {
                if let Ok(waker) = cursor.remove_current(()) {
                    wakers.push(waker);
                } else {
                    break;
                }
            }

            let to_acquire = wakers.len().saturating_sub(extra);
            extra = extra.saturating_sub(wakers.len());

            // update in_flight. this can race, but it's not important to be correct. just good enough
            in_flight = self.in_flight.fetch_add(to_acquire, Ordering::Release) + wakers.len();
            new_limit = self.limits.load(Ordering::Acquire);
        }
    }
}

impl<'t> Token<'t> {
    fn new(limiter: &'t LimiterInner) -> Self {
        Self {
            limiter,
            start: Instant::now(),
        }
    }

    #[cfg(test)]
    pub fn set_latency(&mut self, latency: Duration) {
        use std::ops::Sub;

        self.start = Instant::now().sub(latency);
    }
}

impl Drop for Token<'_> {
    /// Reduces the number of jobs in flight.
    fn drop(&mut self) {
        let in_flight = self.limiter.in_flight.load(Ordering::Acquire);
        let limit = self.limiter.limits.load(Ordering::Acquire);
        self.limiter.release(in_flight, limit, 1);
    }
}

impl LimiterState {
    /// The current concurrency limit.
    pub fn limit(&self) -> usize {
        self.limit
    }
    /// The amount of concurrency available to use.
    pub fn available(&self) -> usize {
        self.available
    }
    /// The number of jobs in flight.
    pub fn in_flight(&self) -> usize {
        self.in_flight
    }
}

pin_project! {
    struct Acquire<'s> {
        semaphore: &'s LimiterInner,
        #[pin]
        node: pin_list::Node<SemaphoreTypes>,
    }

    impl PinnedDrop for Acquire<'_> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            let node = match this.node.initialized_mut() {
                // The future was cancelled before it could complete.
                Some(initialized) => initialized,
                // The future has completed already (or hasn't started); we don't have to do
                // anything.
                None => return,
            };

            let node = {
                node.reset(&mut this.semaphore.semaphore2.lock().unwrap()) };

            match node {
                // If we've cancelled the future like usual, just do that.
                (pin_list::NodeData::Linked(_waker), ()) => {}

                // Otherwise, we have been woken but aren't around to take the lock. To
                // prevent deadlocks, pass the notification on to someone else.
                (pin_list::NodeData::Removed(()), ()) => {
                    let _ = Token::new(this.semaphore);
                }
            }
        }
    }
}

impl<'s> Future for Acquire<'s> {
    type Output = Token<'s>;
    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        let mut inner = this.semaphore.semaphore2.lock().unwrap();

        if let Some(node) = this.node.as_mut().initialized_mut() {
            // Check whether we've been woken up, only continuing if so.
            if let Err(node) = node.take_removed(&inner) {
                // If we haven't been woken, re-register our waker and pend.
                *node.protected_mut(&mut inner).unwrap() = cx.waker().clone();
                return Poll::Pending;
            }
            return Poll::Ready(Token::new(this.semaphore));
        }

        // Otherwise, re-register ourselves to be woken when the mutex is unlocked again
        inner.push_back(this.node, cx.waker().clone(), ());

        Poll::Pending
    }
}

#[async_trait::async_trait]
impl<T: LimitAlgorithm + Send + Sync + 'static> reqwest_middleware::Middleware for Limiter<T> {
    async fn handle(
        &self,
        req: reqwest::Request,
        extensions: &mut task_local_extensions::Extensions,
        next: reqwest_middleware::Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        let token = self
            .acquire_timeout(Duration::from_secs(1)) // TODO
            .await
            .ok_or_else(|| anyhow::Error::msg("Too many concurrent requests"))?;
        match next.run(req, extensions).await {
            Ok(result) => {
                self.release(token, Some(Outcome::Success)).await;
                Ok(result)
            }
            Err(e) => {
                self.release(token, Some(Outcome::from_reqwest_error(&e)))
                    .await;
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{pin::pin, task::Context, time::Duration};

    use futures::{task::noop_waker_ref, Future};

    use super::{Limiter, Outcome};
    use crate::rate_limiter::limit_algorithm::Fixed;

    #[tokio::test]
    async fn it_works() {
        let limiter = Limiter::new(Fixed, 10);

        let token = limiter.try_acquire().unwrap();

        limiter.release(token, Some(Outcome::Success)).await;

        assert_eq!(limiter.state().limit(), 10);
    }

    #[tokio::test]
    async fn is_fair() {
        let limiter = Limiter::new(Fixed, 1);

        // === TOKEN 1 ===
        let token1 = limiter.try_acquire().unwrap();

        let mut token2_fut = pin!(limiter.acquire_timeout(Duration::from_secs(1)));
        assert!(
            token2_fut
                .as_mut()
                .poll(&mut Context::from_waker(noop_waker_ref()))
                .is_pending(),
            "token is acquired by token1"
        );

        let mut token3_fut = pin!(limiter.acquire_timeout(Duration::from_secs(1)));
        assert!(
            token3_fut
                .as_mut()
                .poll(&mut Context::from_waker(noop_waker_ref()))
                .is_pending(),
            "token is acquired by token1"
        );

        limiter.release(token1, Some(Outcome::Success)).await;
        // === END TOKEN 1 ===

        // === TOKEN 2 ===
        assert!(
            limiter.try_acquire().is_none(),
            "token is acquired by token2"
        );

        assert!(
            token3_fut
                .as_mut()
                .poll(&mut Context::from_waker(noop_waker_ref()))
                .is_pending(),
            "token is acquired by token2"
        );

        let token2 = token2_fut.await.unwrap();

        limiter.release(token2, Some(Outcome::Success)).await;
        // === END TOKEN 2 ===

        // === TOKEN 3 ===
        assert!(
            limiter.try_acquire().is_none(),
            "token is acquired by token3"
        );

        let token3 = token3_fut.await.unwrap();
        limiter.release(token3, Some(Outcome::Success)).await;
        // === END TOKEN 3 ===

        // === TOKEN 4 ===
        let token4 = limiter.try_acquire().unwrap();
        limiter.release(token4, Some(Outcome::Success)).await;
    }
}
