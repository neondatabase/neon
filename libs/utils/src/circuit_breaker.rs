use std::{
    fmt::Display,
    time::{Duration, Instant},
};

use metrics::IntCounter;

/// Circuit breakers are for operations that are expensive and fallible.
///
/// If a circuit breaker fails repeatedly, we will stop attempting it for some
/// period of time, to avoid denial-of-service from retries, and
/// to mitigate the log spam from repeated failures.
pub struct CircuitBreaker {
    /// An identifier that enables us to log useful errors when a circuit is broken
    name: String,

    /// Consecutive failures since last success
    fail_count: usize,

    /// How many consecutive failures before we break the circuit
    fail_threshold: usize,

    /// If circuit is broken, when was it broken?
    broken_at: Option<Instant>,

    /// If set, we will auto-reset the circuit this long after it was broken.  If None, broken
    /// circuits stay broken forever, or until success() is called.
    reset_period: Option<Duration>,

    /// If this is true, no actual circuit-breaking happens.  This is for overriding a circuit breaker
    /// to permit something to keep running even if it would otherwise have tripped it.
    short_circuit: bool,
}

impl CircuitBreaker {
    pub fn new(name: String, fail_threshold: usize, reset_period: Option<Duration>) -> Self {
        Self {
            name,
            fail_count: 0,
            fail_threshold,
            broken_at: None,
            reset_period,
            short_circuit: false,
        }
    }

    /// Construct an unbreakable circuit breaker, for use in unit tests etc.
    pub fn short_circuit() -> Self {
        Self {
            name: String::new(),
            fail_threshold: 0,
            fail_count: 0,
            broken_at: None,
            reset_period: None,
            short_circuit: true,
        }
    }

    pub fn fail<E>(&mut self, metric: &IntCounter, error: E)
    where
        E: Display,
    {
        if self.short_circuit {
            return;
        }

        self.fail_count += 1;
        if self.broken_at.is_none() && self.fail_count >= self.fail_threshold {
            self.break_circuit(metric, error);
        }
    }

    /// Call this after successfully executing an operation
    pub fn success(&mut self, metric: &IntCounter) {
        self.fail_count = 0;
        if let Some(broken_at) = &self.broken_at {
            tracing::info!(breaker=%self.name, "Circuit breaker failure ended (was broken for {})",
                humantime::format_duration(broken_at.elapsed()));
            self.broken_at = None;
            metric.inc();
        }
    }

    /// Call this before attempting an operation, and skip the operation if we are currently broken.
    pub fn is_broken(&mut self) -> bool {
        if self.short_circuit {
            return false;
        }

        if let Some(broken_at) = self.broken_at {
            match self.reset_period {
                Some(reset_period) if broken_at.elapsed() > reset_period => {
                    self.reset_circuit();
                    false
                }
                _ => true,
            }
        } else {
            false
        }
    }

    fn break_circuit<E>(&mut self, metric: &IntCounter, error: E)
    where
        E: Display,
    {
        self.broken_at = Some(Instant::now());
        tracing::error!(breaker=%self.name, "Circuit breaker broken!  Last error: {error}");
        metric.inc();
    }

    fn reset_circuit(&mut self) {
        self.broken_at = None;
        self.fail_count = 0;
    }
}
