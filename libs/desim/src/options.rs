use rand::{rngs::StdRng, Rng};

/// Describes random delays and failures. Delay will be uniformly distributed in [min, max].
/// Connection failure will occur with the probablity fail_prob.
#[derive(Clone, Debug)]
pub struct Delay {
    pub min: u64,
    pub max: u64,
    pub fail_prob: f64, // [0; 1]
}

impl Delay {
    /// Create a struct with no delay, no failures.
    pub fn empty() -> Delay {
        Delay {
            min: 0,
            max: 0,
            fail_prob: 0.0,
        }
    }

    /// Create a struct with a fixed delay.
    pub fn fixed(ms: u64) -> Delay {
        Delay {
            min: ms,
            max: ms,
            fail_prob: 0.0,
        }
    }

    /// Generate a random delay in range [min, max]. Return None if the
    /// message should be dropped.
    pub fn delay(&self, rng: &mut StdRng) -> Option<u64> {
        if rng.gen_bool(self.fail_prob) {
            return None;
        }
        Some(rng.gen_range(self.min..=self.max))
    }
}

/// Describes network settings. All network packets will be subjected to the same delays and failures.
#[derive(Clone, Debug)]
pub struct NetworkOptions {
    /// Connection will be automatically closed after this timeout if no data is received.
    pub keepalive_timeout: Option<u64>,
    /// New connections will be delayed by this amount of time.
    pub connect_delay: Delay,
    /// Each message will be delayed by this amount of time.
    pub send_delay: Delay,
}
