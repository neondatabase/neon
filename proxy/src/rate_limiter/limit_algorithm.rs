//! Algorithms for controlling concurrency limits.
use async_trait::async_trait;
use std::time::Duration;

use super::limiter::Outcome;

/// An algorithm for controlling a concurrency limit.
#[async_trait]
pub trait LimitAlgorithm {
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
    #[default]
    None,
    Fixed,
    Aimd,
}

pub struct Fixed;

#[async_trait]
impl LimitAlgorithm for Fixed {
    async fn update(&mut self, old_limit: usize, _sample: Sample) -> usize {
        old_limit
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RateLimiterConfig {
    pub algorithm: RateLimitAlgorithm,
    pub timeout: Duration,
    pub initial_limit: usize,
    pub aimd_min_limit: usize,
    pub aimd_max_limit: usize,
    pub aimd_increase_by: usize,
    pub aimd_decrease_factor: f32,
    pub aimd_min_utilisation_threshold: f32,
}
