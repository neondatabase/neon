use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::sync::{Mutex as AsyncMutex, Semaphore, SemaphorePermit};
use tokio::time::{timeout, Instant};
use tracing::info;

use crate::console::errors::ApiError;

use super::limit_algorithm::{LimitAlgorithm, Sample};

/// Limits the number of concurrent jobs.
///
/// Concurrency is limited through the use of [Token]s. Acquire a token to run a job, and release the
/// token once the job is finished.
///
/// The limit will be automatically adjusted based on observed latency (delay) and/or failures
/// caused by overload (loss).
pub struct Limiter<T> {
    limit_algo: AsyncMutex<T>,
    semaphore: std::sync::Arc<Semaphore>,
    timeout: Duration,

    // ONLY WRITE WHEN LIMIT_ALGO IS LOCKED
    limits: AtomicUsize,

    // ONLY USE ATOMIC ADD/SUB
    in_flight: Arc<AtomicUsize>,

    api_locks: Option<&'static crate::console::locks::ApiLocks>,

    #[cfg(test)]
    notifier: Option<std::sync::Arc<tokio::sync::Notify>>,
}

/// A concurrency token, required to run a job.
///
/// Release the token back to the [Limiter] after the job is complete.
#[derive(Debug)]
pub struct Token<'t> {
    permit: Option<tokio::sync::SemaphorePermit<'t>>,
    start: Instant,
    in_flight: Arc<AtomicUsize>,
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
    fn from_reqwest_response(response: &reqwest::Response) -> Self {
        if response.status().is_server_error()
            || response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
        {
            Outcome::Overload
        } else {
            Outcome::Success
        }
    }
}

impl<T> Limiter<T>
where
    T: LimitAlgorithm,
{
    /// Create a limiter with a given limit control algorithm.
    pub fn new(
        limit_algorithm: T,
        timeout: Duration,
        initial_limit: usize,
        api_locks: Option<&'static crate::console::locks::ApiLocks>,
    ) -> Self {
        assert!(initial_limit > 0);
        Self {
            limit_algo: AsyncMutex::new(limit_algorithm),
            semaphore: Arc::new(Semaphore::new(initial_limit)),
            timeout,
            limits: AtomicUsize::new(initial_limit),
            in_flight: Arc::new(AtomicUsize::new(0)),
            api_locks,
            #[cfg(test)]
            notifier: None,
        }
    }

    /// In some cases [Token]s are acquired asynchronously when updating the limit.
    #[cfg(test)]
    pub fn with_release_notifier(mut self, n: std::sync::Arc<tokio::sync::Notify>) -> Self {
        self.notifier = Some(n);
        self
    }

    /// Try to immediately acquire a concurrency [Token].
    ///
    /// Returns `None` if there are none available.
    pub fn try_acquire(&self) -> Option<Token> {
        let result = self
            .semaphore
            .try_acquire()
            .map(|permit| Token::new(permit, self.in_flight.clone()))
            .ok();
        if result.is_some() {
            self.in_flight.fetch_add(1, Ordering::AcqRel);
        }
        result
    }

    /// Try to acquire a concurrency [Token], waiting for `duration` if there are none available.
    ///
    /// Returns `None` if there are none available after `duration`.
    pub async fn acquire_timeout(&self, duration: Duration) -> Option<Token<'_>> {
        info!("acquiring token: {:?}", self.semaphore.available_permits());
        let result = match timeout(duration, self.semaphore.acquire()).await {
            Ok(maybe_permit) => maybe_permit
                .map(|permit| Token::new(permit, self.in_flight.clone()))
                .ok(),
            Err(_) => None,
        };
        if result.is_some() {
            self.in_flight.fetch_add(1, Ordering::AcqRel);
        }
        result
    }

    /// Return the concurrency [Token], along with the outcome of the job.
    ///
    /// The [Outcome] of the job, and the time taken to perform it, may be used
    /// to update the concurrency limit.
    ///
    /// Set the outcome to `None` to ignore the job.
    pub async fn release(&self, mut token: Token<'_>, outcome: Option<Outcome>) {
        tracing::info!("outcome is {:?}", outcome);
        let in_flight = self.in_flight.load(Ordering::Acquire);
        let available = self.semaphore.available_permits();
        let old_limit = self.limits.load(Ordering::Acquire);
        let total = in_flight + available;

        let mut algo = self.limit_algo.lock().await;

        let new_limit = if let Some(outcome) = outcome {
            let sample = Sample {
                latency: token.start.elapsed(),
                in_flight,
                outcome,
            };
            algo.update(old_limit, sample).await
        } else {
            old_limit
        };
        tracing::info!("new limit is {}", new_limit);
        if new_limit < total {
            token.forget();
        } else {
            self.semaphore.add_permits(new_limit - total);
        }
        self.limits.store(new_limit, Ordering::Release);
        #[cfg(test)]
        if let Some(n) = &self.notifier {
            n.notify_one();
        }
    }

    /// The current state of the limiter.
    pub fn state(&self) -> LimiterState {
        let limit = self.limits.load(Ordering::Relaxed);
        let available = self.semaphore.available_permits();
        LimiterState {
            limit,
            available,
            in_flight: limit.saturating_sub(available),
        }
    }
}

impl<'t> Token<'t> {
    fn new(permit: SemaphorePermit<'t>, in_flight: Arc<AtomicUsize>) -> Self {
        Self {
            permit: Some(permit),
            start: Instant::now(),
            in_flight,
        }
    }

    #[cfg(test)]
    pub fn set_latency(&mut self, latency: Duration) {
        use std::ops::Sub;

        self.start = Instant::now().sub(latency);
    }

    pub fn forget(&mut self) {
        if let Some(permit) = self.permit.take() {
            permit.forget();
        }
    }
}

impl Drop for Token<'_> {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
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

#[async_trait::async_trait]
impl<T: LimitAlgorithm + Send + Sync + 'static> reqwest_middleware::Middleware for Limiter<T> {
    async fn handle(
        &self,
        req: reqwest::Request,
        extensions: &mut task_local_extensions::Extensions,
        next: reqwest_middleware::Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        let start = Instant::now();
        let token = self.acquire_timeout(self.timeout).await.ok_or_else(|| {
            reqwest_middleware::Error::Middleware(
                // TODO: Should we map it into user facing errors?
                ApiError::Console {
                    status: crate::http::StatusCode::TOO_MANY_REQUESTS,
                    text: "Too many requests".into(),
                }
                .into(),
            )
        })?;
        info!(duration = ?start.elapsed(), "waiting for token to connect to the control plane");
        if let Some(locks) = self.api_locks {
            locks.observe_control_plane_acquire(start.elapsed());
        }
        match next.run(req, extensions).await {
            Ok(response) => {
                self.release(token, Some(Outcome::from_reqwest_response(&response)))
                    .await;
                Ok(response)
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
        let limiter = Limiter::new(Fixed, Duration::from_secs(1), 10, None);

        let token = limiter.try_acquire().unwrap();

        limiter.release(token, Some(Outcome::Success)).await;

        assert_eq!(limiter.state().limit(), 10);
    }

    #[tokio::test]
    async fn is_fair() {
        let limiter = Limiter::new(Fixed, Duration::from_secs(1), 1, None);

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
