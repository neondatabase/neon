use std::usize;

use async_trait::async_trait;

use super::limit_algorithm::{LimitAlgorithm, Sample};
use super::RateLimiterConfig;

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
    const DEFAULT_DECREASE_FACTOR: f32 = 0.9;
    const DEFAULT_INCREASE: usize = 1;
    const DEFAULT_MIN_LIMIT: usize = 1;
    const DEFAULT_MAX_LIMIT: usize = 1000;
    const DEFAULT_INCREASE_MIN_UTILISATION: f32 = 0.8;

    pub fn new(config: &RateLimiterConfig) -> Self {
        Self {
            min_limit: config.aimd_min_limit,
            max_limit: config.aimd_max_limit,
            decrease_factor: config.aimd_decrease_factor,
            increase_by: config.aimd_increase_by,
            min_utilisation_threshold: config.aimd_min_utilisation_threshold,
        }
    }

    pub fn decrease_factor(self, factor: f32) -> Self {
        assert!((0.5..1.0).contains(&factor));
        Self {
            decrease_factor: factor,
            ..self
        }
    }

    pub fn increase_by(self, increase: usize) -> Self {
        assert!(increase > 0);
        Self {
            increase_by: increase,
            ..self
        }
    }

    pub fn with_max_limit(self, max: usize) -> Self {
        assert!(max > 0);
        Self {
            max_limit: max,
            ..self
        }
    }

    /// A threshold below which the limit won't be increased. 0.5 = 50%.
    pub fn with_min_utilisation_threshold(self, min_util: f32) -> Self {
        assert!(min_util > 0. && min_util < 1.);
        Self {
            min_utilisation_threshold: min_util,
            ..self
        }
    }
}

impl Default for Aimd {
    fn default() -> Self {
        Self {
            min_limit: Self::DEFAULT_MIN_LIMIT,
            max_limit: Self::DEFAULT_MAX_LIMIT,
            decrease_factor: Self::DEFAULT_DECREASE_FACTOR,
            increase_by: Self::DEFAULT_INCREASE,
            min_utilisation_threshold: Self::DEFAULT_INCREASE_MIN_UTILISATION,
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
    use std::{sync::Arc, time::Duration};

    use tokio::sync::Notify;

    use super::*;

    use crate::rate_limiter::Limiter;

    #[tokio::test]
    async fn should_decrease_limit_on_overload() {
        let aimd = Aimd::default().decrease_factor(0.5).increase_by(1);

        let release_notifier = Arc::new(Notify::new());

        let limiter = Limiter::new(aimd, Duration::from_secs(1), 10, None)
            .with_release_notifier(release_notifier.clone());

        let token = limiter.try_acquire().unwrap();
        limiter.release(token, Some(Outcome::Overload)).await;
        release_notifier.notified().await;
        assert_eq!(limiter.state().limit(), 5, "overload: decrease");
    }

    #[tokio::test]
    async fn should_increase_limit_on_success_when_using_gt_util_threshold() {
        let aimd = Aimd::default()
            .decrease_factor(0.5)
            .increase_by(1)
            .with_min_utilisation_threshold(0.5);

        let limiter = Limiter::new(aimd, Duration::from_secs(1), 4, None);

        let token = limiter.try_acquire().unwrap();
        let _token = limiter.try_acquire().unwrap();
        let _token = limiter.try_acquire().unwrap();

        limiter.release(token, Some(Outcome::Success)).await;
        assert_eq!(limiter.state().limit(), 5, "success: increase");
    }

    #[tokio::test]
    async fn should_not_change_limit_on_success_when_using_lt_util_threshold() {
        let aimd = Aimd::default()
            .decrease_factor(0.5)
            .increase_by(1)
            .with_min_utilisation_threshold(0.5);

        let limiter = Limiter::new(aimd, Duration::from_secs(1), 4, None);

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
        let aimd = Aimd::default().decrease_factor(0.5).increase_by(1);

        let limiter = Limiter::new(aimd, Duration::from_secs(1), 10, None);

        let token = limiter.try_acquire().unwrap();
        limiter.release(token, None).await;
        assert_eq!(limiter.state().limit(), 10, "ignore");
    }
}
