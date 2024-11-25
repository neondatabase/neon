use std::fmt::{self, Debug, Formatter};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use cgroups_rs::{
    hierarchies::{self, is_cgroup2_unified_mode},
    memory::MemController,
    Subsystem,
};
use tokio::sync::watch;
use tracing::{info, warn};

/// Configuration for a `CgroupWatcher`
#[derive(Debug, Clone)]
pub struct Config {
    /// Interval at which we should be fetching memory statistics
    memory_poll_interval: Duration,

    /// The number of samples used in constructing aggregated memory statistics
    memory_history_len: usize,
    /// The number of most recent samples that will be periodically logged.
    ///
    /// Each sample is logged exactly once. Increasing this value means that recent samples will be
    /// logged less frequently, and vice versa.
    ///
    /// For simplicity, this value must be greater than or equal to `memory_history_len`.
    memory_history_log_interval: usize,
    /// The max number of iterations to skip before logging the next iteration
    memory_history_log_noskip_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            memory_poll_interval: Duration::from_millis(100),
            memory_history_len: 5, // use 500ms of history for decision-making
            memory_history_log_interval: 20, // but only log every ~2s (otherwise it's spammy)
            memory_history_log_noskip_interval: Duration::from_secs(15), // but only if it's changed, or 60 seconds have passed
        }
    }
}

/// Responds to `MonitorEvents` to manage the cgroup: preventing it from being
/// OOM killed or throttling.
///
/// The `CgroupWatcher` primarily achieves this by reading from a stream of
/// `MonitorEvent`s. See `main_signals_loop` for details on how to keep the
/// cgroup happy.
#[derive(Debug)]
pub struct CgroupWatcher {
    pub config: Config,

    /// The actual cgroup we are watching and managing.
    cgroup: cgroups_rs::Cgroup,
}

impl CgroupWatcher {
    /// Create a new `CgroupWatcher`.
    #[tracing::instrument(skip_all, fields(%name))]
    pub fn new(name: String) -> anyhow::Result<Self> {
        // TODO: clarify exactly why we need v2
        // Make sure cgroups v2 (aka unified) are supported
        if !is_cgroup2_unified_mode() {
            anyhow::bail!("cgroups v2 not supported");
        }
        let cgroup = cgroups_rs::Cgroup::load(hierarchies::auto(), &name);

        Ok(Self {
            cgroup,
            config: Default::default(),
        })
    }

    /// The entrypoint for the `CgroupWatcher`.
    #[tracing::instrument(skip_all)]
    pub async fn watch(
        &self,
        updates: watch::Sender<(Instant, MemoryHistory)>,
    ) -> anyhow::Result<()> {
        // this requirement makes the code a bit easier to work with; see the config for more.
        assert!(self.config.memory_history_len <= self.config.memory_history_log_interval);

        let mut ticker = tokio::time::interval(self.config.memory_poll_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // ticker.reset_immediately(); // FIXME: enable this once updating to tokio >= 1.30.0

        let mem_controller = self.memory()?;

        // buffer for samples that will be logged. once full, it remains so.
        let history_log_len = self.config.memory_history_log_interval;
        let max_skip = self.config.memory_history_log_noskip_interval;
        let mut history_log_buf = vec![MemoryStatus::zeroed(); history_log_len];
        let mut last_logged_memusage = MemoryStatus::zeroed();

        // Ensure that we're tracking a value that's definitely in the past, as Instant::now is only guaranteed to be non-decreasing on Rust's T1-supported systems.
        let mut can_skip_logs_until = Instant::now() - max_skip;

        for t in 0_u64.. {
            ticker.tick().await;

            let now = Instant::now();
            let mem = Self::memory_usage(mem_controller);

            let i = t as usize % history_log_len;
            history_log_buf[i] = mem;

            // We're taking *at most* memory_history_len values; we may be bounded by the total
            // number of samples that have come in so far.
            let samples_count = (t + 1).min(self.config.memory_history_len as u64) as usize;
            // NB: in `ring_buf_recent_values_iter`, `i` is *inclusive*, which matches the fact
            // that we just inserted a value there, so the end of the iterator will *include* the
            // value at i, rather than stopping just short of it.
            let samples = ring_buf_recent_values_iter(&history_log_buf, i, samples_count);

            let summary = MemoryHistory {
                avg_non_reclaimable: samples.map(|h| h.non_reclaimable).sum::<u64>()
                    / samples_count as u64,
                samples_count,
                samples_span: self.config.memory_poll_interval * (samples_count - 1) as u32,
            };

            // Log the current history if it's time to do so. Because `history_log_buf` has length
            // equal to the logging interval, we can just log the entire buffer every time we set
            // the last entry, which also means that for this log line, we can ignore that it's a
            // ring buffer (because all the entries are in order of increasing time).
            //
            // We skip logging the data if data hasn't meaningfully changed in a while, unless
            // we've already ignored previous iterations for the last max_skip period.
            if i == history_log_len - 1
                && (now > can_skip_logs_until
                    || !history_log_buf
                        .iter()
                        .all(|usage| last_logged_memusage.status_is_close_or_similar(usage)))
            {
                info!(
                    history = ?MemoryStatus::debug_slice(&history_log_buf),
                    summary = ?summary,
                    "Recent cgroup memory statistics history"
                );

                can_skip_logs_until = now + max_skip;

                last_logged_memusage = *history_log_buf.last().unwrap();
            }

            updates
                .send((now, summary))
                .context("failed to send MemoryHistory")?;
        }

        unreachable!()
    }

    /// Get a handle on the memory subsystem.
    fn memory(&self) -> anyhow::Result<&MemController> {
        self.cgroup
            .subsystems()
            .iter()
            .find_map(|sub| match sub {
                Subsystem::Mem(c) => Some(c),
                _ => None,
            })
            .ok_or_else(|| anyhow!("could not find memory subsystem"))
    }

    /// Given a handle on the memory subsystem, returns the current memory information
    fn memory_usage(mem_controller: &MemController) -> MemoryStatus {
        let stat = mem_controller.memory_stat().stat;
        MemoryStatus {
            non_reclaimable: stat.active_anon + stat.inactive_anon,
        }
    }
}

// Helper function for `CgroupWatcher::watch`
fn ring_buf_recent_values_iter<T>(
    buf: &[T],
    last_value_idx: usize,
    count: usize,
) -> impl '_ + Iterator<Item = &T> {
    // Assertion carried over from `CgroupWatcher::watch`, to make the logic in this function
    // easier (we only have to add `buf.len()` once, rather than a dynamic number of times).
    assert!(count <= buf.len());

    buf.iter()
        // 'cycle' because the values could wrap around
        .cycle()
        // with 'cycle', this skip is more like 'offset', and functionally this is
        // offsettting by 'last_value_idx - count (mod buf.len())', but we have to be
        // careful to avoid underflow, so we pre-add buf.len().
        // The '+ 1' is because `last_value_idx` is inclusive, rather than exclusive.
        .skip((buf.len() + last_value_idx + 1 - count) % buf.len())
        .take(count)
}

/// Summary of recent memory usage
#[derive(Debug, Copy, Clone)]
pub struct MemoryHistory {
    /// Rolling average of non-reclaimable memory usage samples over the last `history_period`
    pub avg_non_reclaimable: u64,

    /// The number of samples used to construct this summary
    pub samples_count: usize,
    /// Total timespan between the first and last sample used for this summary
    pub samples_span: Duration,
}

#[derive(Debug, Copy, Clone)]
pub struct MemoryStatus {
    non_reclaimable: u64,
}

impl MemoryStatus {
    fn zeroed() -> Self {
        MemoryStatus { non_reclaimable: 0 }
    }

    fn debug_slice(slice: &[Self]) -> impl '_ + Debug {
        struct DS<'a>(&'a [MemoryStatus]);

        impl Debug for DS<'_> {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                f.debug_struct("[MemoryStatus]")
                    .field(
                        "non_reclaimable[..]",
                        &Fields(self.0, |stat: &MemoryStatus| {
                            BytesToGB(stat.non_reclaimable)
                        }),
                    )
                    .finish()
            }
        }

        struct Fields<'a, F>(&'a [MemoryStatus], F);

        impl<F: Fn(&MemoryStatus) -> T, T: Debug> Debug for Fields<'_, F> {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                f.debug_list().entries(self.0.iter().map(&self.1)).finish()
            }
        }

        struct BytesToGB(u64);

        impl Debug for BytesToGB {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                f.write_fmt(format_args!(
                    "{:.3}Gi",
                    self.0 as f64 / (1_u64 << 30) as f64
                ))
            }
        }

        DS(slice)
    }

    /// Check if the other memory status is a close or similar result.
    /// Returns true if the larger value is not larger than the smaller value
    /// by 1/8 of the smaller value, and within 128MiB.
    /// See tests::check_similarity_behaviour for examples of behaviour
    fn status_is_close_or_similar(&self, other: &MemoryStatus) -> bool {
        let margin;
        let diff;
        if self.non_reclaimable >= other.non_reclaimable {
            margin = other.non_reclaimable / 8;
            diff = self.non_reclaimable - other.non_reclaimable;
        } else {
            margin = self.non_reclaimable / 8;
            diff = other.non_reclaimable - self.non_reclaimable;
        }

        diff < margin && diff < 128 * 1024 * 1024
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn ring_buf_iter() {
        let buf = vec![0_i32, 1, 2, 3, 4, 5, 6, 7, 8, 9];

        let values = |offset, count| {
            super::ring_buf_recent_values_iter(&buf, offset, count)
                .copied()
                .collect::<Vec<i32>>()
        };

        // Boundary conditions: start, end, and entire thing:
        assert_eq!(values(0, 1), [0]);
        assert_eq!(values(3, 4), [0, 1, 2, 3]);
        assert_eq!(values(9, 4), [6, 7, 8, 9]);
        assert_eq!(values(9, 10), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

        // "normal" operation: no wraparound
        assert_eq!(values(7, 4), [4, 5, 6, 7]);

        // wraparound:
        assert_eq!(values(0, 4), [7, 8, 9, 0]);
        assert_eq!(values(1, 4), [8, 9, 0, 1]);
        assert_eq!(values(2, 4), [9, 0, 1, 2]);
        assert_eq!(values(2, 10), [3, 4, 5, 6, 7, 8, 9, 0, 1, 2]);
    }

    #[test]
    fn check_similarity_behaviour() {
        // This all accesses private methods, so we can't actually run this
        // as doctests, because doctests run as an external crate.
        let mut small = super::MemoryStatus {
            non_reclaimable: 1024,
        };
        let mut large = super::MemoryStatus {
            non_reclaimable: 1024 * 1024 * 1024 * 1024,
        };

        // objects are self-similar, no matter the size
        assert!(small.status_is_close_or_similar(&small));
        assert!(large.status_is_close_or_similar(&large));

        // inequality is symmetric
        assert!(!small.status_is_close_or_similar(&large));
        assert!(!large.status_is_close_or_similar(&small));

        small.non_reclaimable = 64;
        large.non_reclaimable = (small.non_reclaimable / 8) * 9;

        // objects are self-similar, no matter the size
        assert!(small.status_is_close_or_similar(&small));
        assert!(large.status_is_close_or_similar(&large));

        // values are similar if the larger value is larger by less than
        // 12.5%, i.e. 1/8 of the smaller value.
        // In the example above, large is exactly 12.5% larger, so this doesn't
        // match.
        assert!(!small.status_is_close_or_similar(&large));
        assert!(!large.status_is_close_or_similar(&small));

        large.non_reclaimable -= 1;
        assert!(large.status_is_close_or_similar(&large));

        assert!(small.status_is_close_or_similar(&large));
        assert!(large.status_is_close_or_similar(&small));

        // The 1/8 rule only applies up to 128MiB of difference
        small.non_reclaimable = 1024 * 1024 * 1024 * 1024;
        large.non_reclaimable = small.non_reclaimable / 8 * 9;
        assert!(small.status_is_close_or_similar(&small));
        assert!(large.status_is_close_or_similar(&large));

        assert!(!small.status_is_close_or_similar(&large));
        assert!(!large.status_is_close_or_similar(&small));
        // the large value is put just above the threshold
        large.non_reclaimable = small.non_reclaimable + 128 * 1024 * 1024;
        assert!(large.status_is_close_or_similar(&large));

        assert!(!small.status_is_close_or_similar(&large));
        assert!(!large.status_is_close_or_similar(&small));
        // now below
        large.non_reclaimable -= 1;
        assert!(large.status_is_close_or_similar(&large));

        assert!(small.status_is_close_or_similar(&large));
        assert!(large.status_is_close_or_similar(&small));
    }
}
