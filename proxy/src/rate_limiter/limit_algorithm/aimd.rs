use std::usize;

use super::{LimitAlgorithm, Outcome, Sample};

/// Loss-based congestion avoidance.
///
/// Additive-increase, multiplicative decrease.
///
/// Adds available currency when:
/// 1. no load-based errors are observed, and
/// 2. the utilisation of the current limit is high.
///
/// Reduces available concurrency by a factor when load-based errors are detected.
#[derive(Clone, Copy, Debug, serde::Deserialize, PartialEq)]
pub struct Aimd {
    /// Minimum limit for AIMD algorithm.
    pub min: usize,
    /// Maximum limit for AIMD algorithm.
    pub max: usize,
    /// Decrease AIMD decrease by value in case of error.
    pub dec: f32,
    /// Increase AIMD increase by value in case of success.
    pub inc: usize,
    /// A threshold below which the limit won't be increased.
    pub utilisation: f32,
}

impl Default for Aimd {
    fn default() -> Self {
        Self {
            min: 1,
            max: 1500,
            inc: 10,
            dec: 0.9,
            utilisation: 0.8,
        }
    }
}

impl LimitAlgorithm for Aimd {
    fn update(&self, old_limit: usize, sample: Sample) -> usize {
        dbg!(sample);
        use Outcome::*;
        match sample.outcome {
            Success => {
                let utilisation = sample.in_flight as f32 / old_limit as f32;

                if utilisation > self.utilisation {
                    let limit = old_limit + self.inc;
                    limit.clamp(self.min, self.max)
                } else {
                    old_limit
                }
            }
            Overload => {
                let limit = old_limit as f32 * self.dec;

                // Floor instead of round, so the limit reduces even with small numbers.
                // E.g. round(2 * 0.9) = 2, but floor(2 * 0.9) = 1
                let limit = limit.floor() as usize;

                limit.clamp(self.min, self.max)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::rate_limiter::limit_algorithm::{
        DynamicLimiter, RateLimitAlgorithm, RateLimiterConfig,
    };

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn should_decrease_limit_on_overload() {
        let config = RateLimiterConfig {
            initial_limit: 10,
            algorithm: RateLimitAlgorithm::Aimd {
                conf: Aimd {
                    dec: 0.5,
                    ..Default::default()
                },
            },
        };

        let limiter = DynamicLimiter::new(config);

        let token = limiter
            .acquire_timeout(Duration::from_millis(1))
            .await
            .unwrap();
        token.release(Outcome::Overload);

        assert_eq!(limiter.state().limit(), 5, "overload: decrease");
    }

    #[tokio::test(start_paused = true)]
    async fn should_increase_limit_on_success_when_using_gt_util_threshold() {
        let config = RateLimiterConfig {
            initial_limit: 4,
            algorithm: RateLimitAlgorithm::Aimd {
                conf: Aimd {
                    dec: 0.5,
                    utilisation: 0.5,
                    inc: 1,
                    ..Default::default()
                },
            },
        };

        let limiter = DynamicLimiter::new(config);

        let token = limiter
            .acquire_timeout(Duration::from_millis(1))
            .await
            .unwrap();
        let _token = limiter
            .acquire_timeout(Duration::from_millis(1))
            .await
            .unwrap();
        let _token = limiter
            .acquire_timeout(Duration::from_millis(1))
            .await
            .unwrap();

        token.release(Outcome::Success);
        assert_eq!(limiter.state().limit(), 5, "success: increase");
    }

    #[tokio::test(start_paused = true)]
    async fn should_not_change_limit_on_success_when_using_lt_util_threshold() {
        let config = RateLimiterConfig {
            initial_limit: 4,
            algorithm: RateLimitAlgorithm::Aimd {
                conf: Aimd {
                    dec: 0.5,
                    utilisation: 0.5,
                    ..Default::default()
                },
            },
        };

        let limiter = DynamicLimiter::new(config);

        let token = limiter
            .acquire_timeout(Duration::from_millis(1))
            .await
            .unwrap();

        token.release(Outcome::Success);
        assert_eq!(
            limiter.state().limit(),
            4,
            "success: ignore when < half limit"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn should_not_change_limit_when_no_outcome() {
        let config = RateLimiterConfig {
            initial_limit: 10,
            algorithm: RateLimitAlgorithm::Aimd {
                conf: Aimd {
                    dec: 0.5,
                    utilisation: 0.5,
                    ..Default::default()
                },
            },
        };

        let limiter = DynamicLimiter::new(config);

        let token = limiter
            .acquire_timeout(Duration::from_millis(1))
            .await
            .unwrap();
        drop(token);
        assert_eq!(limiter.state().limit(), 10, "ignore");
    }
}
