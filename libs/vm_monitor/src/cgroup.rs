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

    memory_history_len: usize,
    memory_history_log_interval: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            memory_poll_interval: Duration::from_millis(100),
            memory_history_len: 8, // 800ms of history
            memory_history_log_interval: 5,
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
        let mut ticker = tokio::time::interval(self.config.memory_poll_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // ticker.reset_immediately(); // FIXME: enable this once updating to tokio >= 1.30.0

        let history_len = self.config.memory_history_len;
        let mut history_buf = vec![MemoryStatus::zeroed(); history_len];

        let mem_controller = self.memory()?;

        for t in 0_u64.. {
            ticker.tick().await;

            let now = Instant::now();
            let mem = Self::memory_usage(&mem_controller);

            let i = t as usize % history_len;
            history_buf[i] = mem;

            let initialized_history = &history_buf[0..(t + 1).min(history_len as u64) as usize];
            let samples_count = initialized_history.len();

            let summary = MemoryHistory {
                avg_non_reclaimable: initialized_history
                    .iter()
                    .map(|h| h.non_reclaimable)
                    .sum::<u64>()
                    / samples_count as u64,
                samples_count,
                samples_span: self.config.memory_poll_interval * (samples_count - 1) as u32,
            };

            // Log the current history if it's time to do so
            if t != 0 && t % self.config.memory_history_log_interval as u64 == 0 {
                info!(
                    history = ?MemoryStatus::debug_slice(initialized_history),
                    summary = ?summary,
                    "Recent cgroup memory statistics history"
                );
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

        impl<'a> Debug for DS<'a> {
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

        impl<'a, F: Fn(&MemoryStatus) -> T, T: Debug> Debug for Fields<'a, F> {
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
}
