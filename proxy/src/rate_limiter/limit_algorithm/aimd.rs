use std::usize;

use super::{LimitAlgorithm, Outcome, Sample};

#[derive(clap::Parser, Clone, Copy, Debug)]
pub struct AimdConfig {
    /// Minimum limit for AIMD algorithm. Makes sense only if `rate_limit_algorithm` is `Aimd`.
    #[clap(long, default_value_t = 1)]
    pub aimd_min_limit: usize,
    /// Maximum limit for AIMD algorithm. Makes sense only if `rate_limit_algorithm` is `Aimd`.
    #[clap(long, default_value_t = 1500)]
    pub aimd_max_limit: usize,
    /// Increase AIMD increase by value in case of success. Makes sense only if `rate_limit_algorithm` is `Aimd`.
    #[clap(long, default_value_t = 10)]
    pub aimd_increase_by: usize,
    /// Decrease AIMD decrease by value in case of timout/429. Makes sense only if `rate_limit_algorithm` is `Aimd`.
    #[clap(long, default_value_t = 0.9)]
    pub aimd_decrease_factor: f32,
    /// A threshold below which the limit won't be increased. Makes sense only if `rate_limit_algorithm` is `Aimd`.
    #[clap(long, default_value_t = 0.8)]
    pub aimd_min_utilisation_threshold: f32,
}

impl Default for AimdConfig {
    fn default() -> Self {
        Self {
            aimd_min_limit: 1,
            aimd_max_limit: 1500,
            aimd_increase_by: 10,
            aimd_decrease_factor: 0.9,
            aimd_min_utilisation_threshold: 0.8,
        }
    }
}

/// Loss-based congestion avoidance.
///
/// Additive-increase, multiplicative decrease.
///
/// Adds available currency when:
/// 1. no load-based errors are observed, and
/// 2. the utilisation of the current limit is high.
///
/// Reduces available concurrency by a factor when load-based errors are detected.
pub struct Aimd {
    min_limit: usize,
    max_limit: usize,
    decrease_factor: f32,
    increase_by: usize,
    min_utilisation_threshold: f32,
}

impl Aimd {
    pub fn new(config: AimdConfig) -> Self {
        Self {
            min_limit: config.aimd_min_limit,
            max_limit: config.aimd_max_limit,
            decrease_factor: config.aimd_decrease_factor,
            increase_by: config.aimd_increase_by,
            min_utilisation_threshold: config.aimd_min_utilisation_threshold,
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

                if utilisation > self.min_utilisation_threshold {
                    let limit = old_limit + self.increase_by;
                    limit.clamp(self.min_limit, self.max_limit)
                } else {
                    old_limit
                }
            }
            Overload => {
                let limit = old_limit as f32 * self.decrease_factor;

                // Floor instead of round, so the limit reduces even with small numbers.
                // E.g. round(2 * 0.9) = 2, but floor(2 * 0.9) = 1
                let limit = limit.floor() as usize;

                limit.clamp(self.min_limit, self.max_limit)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::rate_limiter::limit_algorithm::{Limiter, RateLimiterConfig};

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn should_decrease_limit_on_overload() {
        let config = RateLimiterConfig {
            initial_limit: 10,
            aimd_config: Some(AimdConfig {
                aimd_decrease_factor: 0.5,
                ..Default::default()
            }),
            disable: false,
            ..Default::default()
        };

        let limiter = Limiter::new(config);

        let token = limiter
            .acquire_timeout(Duration::from_millis(1))
            .await
            .unwrap();
        token.release(Some(Outcome::Overload));

        assert_eq!(limiter.state().limit(), 5, "overload: decrease");
    }

    #[tokio::test(start_paused = true)]
    async fn should_increase_limit_on_success_when_using_gt_util_threshold() {
        let config = RateLimiterConfig {
            initial_limit: 4,
            aimd_config: Some(AimdConfig {
                aimd_decrease_factor: 0.5,
                aimd_min_utilisation_threshold: 0.5,
                aimd_increase_by: 1,
                ..Default::default()
            }),
            disable: false,
            ..Default::default()
        };

        let limiter = Limiter::new(config);

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

        token.release(Some(Outcome::Success));
        assert_eq!(limiter.state().limit(), 5, "success: increase");
    }

    #[tokio::test(start_paused = true)]
    async fn should_not_change_limit_on_success_when_using_lt_util_threshold() {
        let config = RateLimiterConfig {
            initial_limit: 4,
            aimd_config: Some(AimdConfig {
                aimd_decrease_factor: 0.5,
                aimd_min_utilisation_threshold: 0.5,
                ..Default::default()
            }),
            disable: false,
            ..Default::default()
        };

        let limiter = Limiter::new(config);

        let token = limiter
            .acquire_timeout(Duration::from_millis(1))
            .await
            .unwrap();

        token.release(Some(Outcome::Success));
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
            aimd_config: Some(AimdConfig {
                aimd_decrease_factor: 0.5,
                aimd_min_utilisation_threshold: 0.5,
                ..Default::default()
            }),
            disable: false,
            ..Default::default()
        };

        let limiter = Limiter::new(config);

        let token = limiter
            .acquire_timeout(Duration::from_millis(1))
            .await
            .unwrap();
        token.release(None);
        assert_eq!(limiter.state().limit(), 10, "ignore");
    }
}
