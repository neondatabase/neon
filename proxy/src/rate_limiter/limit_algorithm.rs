//! Algorithms for controlling concurrency limits.
use std::sync::Arc;
use std::time::Duration;

use flag_bearer::{Semaphore, SemaphoreState, Uncloseable};
use tokio::time::Instant;
use tokio::time::error::Elapsed;

use self::aimd::Aimd;

pub(crate) mod aimd;

/// Whether a job succeeded or failed as a result of congestion/overload.
///
/// Errors not considered to be caused by overload should be ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The job succeeded, or failed in a way unrelated to overload.
    Success,
    /// The job failed because of overload, e.g. it timed out or an explicit backpressure signal
    /// was observed.
    Overload,
}

/// An algorithm for controlling a concurrency limit.
pub(crate) trait LimitAlgorithm: Send + Sync + 'static {
    /// Update the concurrency limit in response to a new job completion.
    fn update(&self, old_limit: usize, sample: Sample) -> usize;
}

/// The result of a job (or jobs), including the [`Outcome`] (loss) and latency (delay).
#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub(crate) struct Sample {
    pub(crate) latency: Duration,
    /// Jobs in flight when the sample was taken.
    pub(crate) in_flight: usize,
    pub(crate) outcome: Outcome,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RateLimitAlgorithm {
    #[default]
    Fixed,
    Aimd {
        #[serde(flatten)]
        conf: Aimd,
    },
}

pub(crate) struct Fixed;

impl LimitAlgorithm for Fixed {
    fn update(&self, old_limit: usize, _sample: Sample) -> usize {
        old_limit
    }
}

#[derive(Clone, Copy, Debug, serde::Deserialize, PartialEq)]
pub struct RateLimiterConfig {
    #[serde(flatten)]
    pub(crate) algorithm: RateLimitAlgorithm,
    pub(crate) initial_limit: usize,
}

pub(crate) struct LimiterInner<L: ?Sized> {
    limit: usize,
    in_flight: usize,
    alg: L,
}

impl<L: ?Sized + LimitAlgorithm> LimiterInner<L> {
    fn update_limit(&mut self, latency: Duration, outcome: Option<Outcome>) {
        if let Some(outcome) = outcome {
            let sample = Sample {
                latency,
                in_flight: self.in_flight,
                outcome,
            };
            self.limit = self.alg.update(self.limit, sample);
        }
    }
}

impl<L: ?Sized> SemaphoreState for LimiterInner<L> {
    type Params = ();
    type Permit = ();
    type Closeable = Uncloseable;

    fn acquire(&mut self, p: Self::Params) -> Result<Self::Permit, Self::Params> {
        if self.in_flight < self.limit {
            self.in_flight += 1;

            Ok(p)
        } else {
            Err(p)
        }
    }

    fn release(&mut self, _p: Self::Permit) {
        self.in_flight -= 1;
    }
}

/// Limits the number of concurrent jobs.
///
/// Concurrency is limited through the use of [`Token`]s. Acquire a token to run a job, and release the
/// token once the job is finished.
///
/// The limit will be automatically adjusted based on observed latency (delay) and/or failures
/// caused by overload (loss).
pub(crate) struct DynamicLimiter<L: ?Sized = dyn LimitAlgorithm> {
    disabled: bool,
    sem: Semaphore<LimiterInner<L>>,
}

/// A concurrency token, required to run a job.
///
/// Release the token back to the [`DynamicLimiter`] after the job is complete.
pub(crate) struct Token {
    start: Instant,
    limiter: Option<Arc<DynamicLimiter>>,
}

/// A snapshot of the state of the [`DynamicLimiter`].
///
/// Not guaranteed to be consistent under high concurrency.
#[derive(Debug, Clone, Copy)]
#[cfg(test)]
struct LimiterState {
    limit: usize,
}

impl<L> DynamicLimiter<L> {
    pub(crate) fn new_inner(initial_limit: usize, alg: L) -> Arc<Self> {
        Arc::new(Self {
            disabled: initial_limit == 0,
            sem: Semaphore::new_fifo(LimiterInner {
                limit: initial_limit,
                in_flight: 0,
                alg,
            }),
        })
    }
}

impl DynamicLimiter {
    /// Create a limiter with a given limit control algorithm.
    pub(crate) fn new(config: RateLimiterConfig) -> Arc<Self> {
        let initial_limit = config.initial_limit;
        match config.algorithm {
            RateLimitAlgorithm::Fixed => DynamicLimiter::new_inner(initial_limit, Fixed),
            RateLimitAlgorithm::Aimd { conf } => DynamicLimiter::new_inner(initial_limit, conf),
        }
    }

    /// Try to acquire a concurrency [Token], waiting for `duration` if there are none available.
    pub(crate) async fn acquire_timeout(
        self: &Arc<Self>,
        duration: Duration,
    ) -> Result<Token, Elapsed> {
        tokio::time::timeout(duration, self.acquire()).await
    }

    /// Try to acquire a concurrency [Token].
    async fn acquire(self: &Arc<Self>) -> Token {
        if self.disabled {
            // If the rate limiter is disabled, we can always acquire a token.
            Token::disabled()
        } else {
            match self.sem.acquire(()).await {
                Err(close) => close.never(),
                Ok(permit) => {
                    permit.take();
                    Token::new(self.clone())
                }
            }
        }
    }

    /// Return the concurrency [Token], along with the outcome of the job.
    ///
    /// The [Outcome] of the job, and the time taken to perform it, may be used
    /// to update the concurrency limit.
    ///
    /// Set the outcome to `None` to ignore the job.
    fn release_inner(&self, start: Instant, outcome: Option<Outcome>) {
        if outcome.is_none() {
            tracing::warn!("outcome is {:?}", outcome);
        } else {
            tracing::debug!("outcome is {:?}", outcome);
        }
        if self.disabled {
            return;
        }

        self.sem.with_state(|s| {
            s.update_limit(start.elapsed(), outcome);
            s.release(());
        });
    }

    /// The current state of the limiter.
    #[cfg(test)]
    fn state(&self) -> LimiterState {
        self.sem.with_state(|s| LimiterState { limit: s.limit })
    }
}

impl Token {
    fn new(limiter: Arc<DynamicLimiter>) -> Self {
        Self {
            start: Instant::now(),
            limiter: Some(limiter),
        }
    }
    pub(crate) fn disabled() -> Self {
        Self {
            start: Instant::now(),
            limiter: None,
        }
    }

    pub(crate) fn is_disabled(&self) -> bool {
        self.limiter.is_none()
    }

    pub(crate) fn release(mut self, outcome: Outcome) {
        self.release_mut(Some(outcome));
    }

    pub(crate) fn release_mut(&mut self, outcome: Option<Outcome>) {
        if let Some(limiter) = self.limiter.take() {
            limiter.release_inner(self.start, outcome);
        }
    }
}

impl Drop for Token {
    fn drop(&mut self) {
        self.release_mut(None);
    }
}

#[cfg(test)]
impl LimiterState {
    /// The current concurrency limit.
    fn limit(self) -> usize {
        self.limit
    }
}
