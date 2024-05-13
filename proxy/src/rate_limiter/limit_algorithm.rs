//! Algorithms for controlling concurrency limits.
use parking_lot::Mutex;
use std::{pin::pin, sync::Arc, time::Duration};
use tokio::{
    sync::Notify,
    time::{timeout_at, Instant},
};

use self::aimd::{Aimd, AimdConfig};

mod aimd;

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

/// An algorithm for controlling a concurrency limit.
pub trait LimitAlgorithm: Send + Sync + 'static {
    /// Update the concurrency limit in response to a new job completion.
    fn update(&self, old_limit: usize, sample: Sample) -> usize;
}

/// The result of a job (or jobs), including the [Outcome] (loss) and latency (delay).
#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub struct Sample {
    pub(crate) latency: Duration,
    /// Jobs in flight when the sample was taken.
    pub(crate) in_flight: usize,
    pub(crate) outcome: Outcome,
}

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum)]
pub enum RateLimitAlgorithm {
    Fixed,
    #[default]
    Aimd,
}

pub struct Fixed;

impl LimitAlgorithm for Fixed {
    fn update(&self, old_limit: usize, _sample: Sample) -> usize {
        old_limit
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RateLimiterConfig {
    pub disable: bool,
    pub algorithm: RateLimitAlgorithm,
    pub timeout: Duration,
    pub initial_limit: usize,
    pub aimd_config: Option<AimdConfig>,
}

impl RateLimiterConfig {
    pub fn create_rate_limit_algorithm(self) -> Box<dyn LimitAlgorithm> {
        match self.algorithm {
            RateLimitAlgorithm::Fixed => Box::new(Fixed),
            RateLimitAlgorithm::Aimd => Box::new(Aimd::new(self.aimd_config.unwrap())), // For aimd algorithm config is mandatory.
        }
    }
}

impl Default for RateLimiterConfig {
    fn default() -> Self {
        Self {
            disable: true,
            algorithm: RateLimitAlgorithm::Aimd,
            timeout: Duration::from_secs(1),
            initial_limit: 100,
            aimd_config: Some(AimdConfig::default()),
        }
    }
}

pub struct LimiterInner {
    alg: Box<dyn LimitAlgorithm>,
    available: usize,
    limit: usize,
    in_flight: usize,
}

impl LimiterInner {
    fn update(&mut self, latency: Duration, outcome: Option<Outcome>) {
        match outcome {
            Some(outcome) => {
                let sample = Sample {
                    latency,
                    in_flight: self.in_flight,
                    outcome,
                };
                self.limit = self.alg.update(self.limit, sample);
            }
            None => {}
        }
    }

    fn take(&mut self, ready: &Notify) -> Option<()> {
        if self.available > 1 {
            self.available -= 1;
            self.in_flight += 1;

            // tell the next in the queue that there is a permit ready
            if self.available > 1 {
                ready.notify_one();
            }
            Some(())
        } else {
            None
        }
    }
}

/// Limits the number of concurrent jobs.
///
/// Concurrency is limited through the use of [Token]s. Acquire a token to run a job, and release the
/// token once the job is finished.
///
/// The limit will be automatically adjusted based on observed latency (delay) and/or failures
/// caused by overload (loss).
pub struct Limiter {
    config: RateLimiterConfig,
    inner: Mutex<LimiterInner>,
    // to notify when a token is available
    ready: Notify,
}

/// A concurrency token, required to run a job.
///
/// Release the token back to the [Limiter] after the job is complete.
pub struct Token {
    start: Instant,
    limiter: Option<Arc<Limiter>>,
}

/// A snapshot of the state of the [Limiter].
///
/// Not guaranteed to be consistent under high concurrency.
#[derive(Debug, Clone, Copy)]
pub struct LimiterState {
    limit: usize,
    in_flight: usize,
}

impl Limiter {
    /// Create a limiter with a given limit control algorithm.
    pub fn new(config: RateLimiterConfig) -> Arc<Self> {
        assert!(config.initial_limit > 0);
        let ready = Notify::new();
        ready.notify_one();

        Arc::new(Self {
            inner: Mutex::new(LimiterInner {
                alg: config.create_rate_limit_algorithm(),
                available: config.initial_limit,
                limit: config.initial_limit,
                in_flight: 0,
            }),
            ready,
            config,
        })
    }

    // /// Try to immediately acquire a concurrency [Token].
    // ///
    // /// Returns `None` if there are none available.
    // pub fn try_acquire(self: &Arc<Self>) -> Option<Token> {
    //     if self.config.disable {
    //         // If the rate limiter is disabled, we can always acquire a token.
    //         Some(Token::new(None, self.clone()))
    //     } else {
    //         let mut inner = self.inner.lock();
    //         if inner.available > 1 {
    //             inner.available -= 1;
    //             Some(Token::new(Some(()), self.clone()))
    //         } else {
    //             None
    //         }
    //     }
    // }

    /// Try to acquire a concurrency [Token], waiting for `duration` if there are none available.
    ///
    /// Returns `None` if there are none available after `duration`.
    pub async fn acquire_timeout(self: &Arc<Self>, duration: Duration) -> Option<Token> {
        if self.config.disable {
            // If the rate limiter is disabled, we can always acquire a token.
            Some(Token::disabled())
        } else {
            let deadline = Instant::now() + duration;

            let mut notified = pin!(self.ready.notified());
            let mut ready = notified.as_mut().enable();
            loop {
                if ready {
                    let mut inner = self.inner.lock();
                    if inner.take(&self.ready).is_some() {
                        break Some(Token::new(self.clone()));
                    }
                }
                match timeout_at(deadline, notified.as_mut()).await {
                    Ok(()) => ready = true,
                    Err(_) => break None,
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
        tracing::info!("outcome is {:?}", outcome);
        if self.config.disable {
            return;
        }

        let mut inner = self.inner.lock();

        inner.update(start.elapsed(), outcome);
        if inner.in_flight < inner.limit {
            inner.available = inner.limit - inner.in_flight;
            // At least 1 permit is now available
            self.ready.notify_one();
        }

        inner.in_flight -= 1;
    }

    /// The current state of the limiter.
    pub fn state(&self) -> LimiterState {
        let inner = self.inner.lock();
        LimiterState {
            limit: inner.limit,
            in_flight: inner.in_flight,
        }
    }
}

impl Token {
    fn new(limiter: Arc<Limiter>) -> Self {
        Self {
            start: Instant::now(),
            limiter: Some(limiter),
        }
    }
    fn disabled() -> Self {
        Self {
            start: Instant::now(),
            limiter: None,
        }
    }

    pub fn release(mut self, outcome: Option<Outcome>) {
        if let Some(limiter) = self.limiter.take() {
            limiter.release_inner(self.start, outcome);
        }
    }
}

impl Drop for Token {
    fn drop(&mut self) {
        if let Some(limiter) = self.limiter.take() {
            limiter.release_inner(self.start, None);
        }
    }
}

impl LimiterState {
    /// The current concurrency limit.
    pub fn limit(&self) -> usize {
        self.limit
    }
    /// The number of jobs in flight.
    pub fn in_flight(&self) -> usize {
        self.in_flight
    }
}
