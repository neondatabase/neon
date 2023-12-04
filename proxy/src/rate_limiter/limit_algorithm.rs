//! Algorithms for controlling concurrency limits.
use async_trait::async_trait;
use std::time::Duration;

use super::{limiter::Outcome, Aimd};

/// An algorithm for controlling a concurrency limit.
#[async_trait]
pub trait LimitAlgorithm: Send + Sync + 'static {
    /// Update the concurrency limit in response to a new job completion.
    async fn update(&mut self, old_limit: usize, sample: Sample) -> usize;
}

/// The result of a job (or jobs), including the [Outcome] (loss) and latency (delay).
#[derive(Debug, Clone, PartialEq, Eq)]
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

#[async_trait]
impl LimitAlgorithm for Fixed {
    async fn update(&mut self, old_limit: usize, _sample: Sample) -> usize {
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
