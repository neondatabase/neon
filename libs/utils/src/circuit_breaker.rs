use std::time::{Duration, Instant};

/// Circuit breakers are for operations that are expensive and fallible: if they fail repeatedly,
/// we will stop attempting them for some period of time, to avoid denial-of-service from retries, and
/// to mitigate the log spam from repeated failures.
pub struct CircuitBreaker {
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
    pub fn new(fail_threshold: usize, reset_period: Option<Duration>) -> Self {
        Self {
            fail_count: 0,
            fail_threshold,
            broken_at: None,
            reset_period,
            short_circuit: false,
        }
    }

    pub fn short_circuit() -> Self {
        Self {
            fail_threshold: 0,
            fail_count: 0,
            broken_at: None,
            reset_period: None,
            short_circuit: true,
        }
    }

    pub fn fail(&mut self) {
        if self.short_circuit {
            return;
        }

        self.fail_count += 1;
        if self.broken_at.is_none() && self.fail_count >= self.fail_threshold {
            self.break_circuit();
        }
    }

    /// Call this after successfully executing an operation
    pub fn success(&mut self) {
        self.fail_count = 0;
        self.broken_at = None;
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

    fn break_circuit(&mut self) {
        self.broken_at = Some(Instant::now())
    }

    fn reset_circuit(&mut self) {
        self.broken_at = None;
        self.fail_count = 0;
    }
}
