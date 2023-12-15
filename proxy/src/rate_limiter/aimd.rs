use std::usize;

use async_trait::async_trait;

use super::limit_algorithm::{AimdConfig, LimitAlgorithm, Sample};

use super::limiter::Outcome;

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

#[async_trait]
impl LimitAlgorithm for Aimd {
    async fn update(&mut self, old_limit: usize, sample: Sample) -> usize {
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
    use std::sync::Arc;

    use tokio::sync::Notify;

    use super::*;

    use crate::rate_limiter::{Limiter, RateLimiterConfig};

    #[tokio::test]
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

        let release_notifier = Arc::new(Notify::new());

        let limiter = Limiter::new(config).with_release_notifier(release_notifier.clone());

        let token = limiter.try_acquire().unwrap();
        limiter.release(token, Some(Outcome::Overload)).await;
        release_notifier.notified().await;
        assert_eq!(limiter.state().limit(), 5, "overload: decrease");
    }

    #[tokio::test]
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

        let token = limiter.try_acquire().unwrap();
        let _token = limiter.try_acquire().unwrap();
        let _token = limiter.try_acquire().unwrap();

        limiter.release(token, Some(Outcome::Success)).await;
        assert_eq!(limiter.state().limit(), 5, "success: increase");
    }

    #[tokio::test]
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

        let token = limiter.try_acquire().unwrap();

        limiter.release(token, Some(Outcome::Success)).await;
        assert_eq!(
            limiter.state().limit(),
            4,
            "success: ignore when < half limit"
        );
    }

    #[tokio::test]
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

        let token = limiter.try_acquire().unwrap();
        limiter.release(token, None).await;
        assert_eq!(limiter.state().limit(), 10, "ignore");
    }
}
