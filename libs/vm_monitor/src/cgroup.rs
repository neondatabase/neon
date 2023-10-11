use std::{
    fmt::{Debug, Display},
    fs,
    pin::pin,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{anyhow, Context};
use cgroups_rs::{
    freezer::FreezerController,
    hierarchies::{self, is_cgroup2_unified_mode, UNIFIED_MOUNTPOINT},
    memory::MemController,
    MaxValue,
    Subsystem::{Freezer, Mem},
};
use inotify::{EventStream, Inotify, WatchMask};
use pin_project_lite::pin_project;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, Instant};
use tokio_stream::{Stream, StreamExt};
use tracing::{error, info, warn};

use crate::{bytes_to_mebibytes, MiB};

/// Monotonically increasing counter of the number of memory.high events
/// the cgroup has experienced.
///
/// We use this to determine if a modification to the `memory.events` file actually
/// changed the `high` field. If not, we don't care about the change. When we
/// read the file, we check the `high` field in the file against `MEMORY_EVENT_COUNT`
/// to see if it changed since last time.
pub static MEMORY_EVENT_COUNT: AtomicU64 = AtomicU64::new(0);

/// Monotonically increasing counter that gives each cgroup event a unique id.
///
/// This allows us to answer questions like "did this upscale arrive before this
/// memory.high?". This static is also used by the `Sequenced` type to "tag" values
/// with a sequence number. As such, prefer to used the `Sequenced` type rather
/// than this static directly.
static EVENT_SEQUENCE_NUMBER: AtomicU64 = AtomicU64::new(0);

/// A memory event type reported in memory.events.
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum MemoryEvent {
    Low,
    High,
    Max,
    Oom,
    OomKill,
    OomGroupKill,
}

impl MemoryEvent {
    fn as_str(&self) -> &str {
        match self {
            MemoryEvent::Low => "low",
            MemoryEvent::High => "high",
            MemoryEvent::Max => "max",
            MemoryEvent::Oom => "oom",
            MemoryEvent::OomKill => "oom_kill",
            MemoryEvent::OomGroupKill => "oom_group_kill",
        }
    }
}

impl Display for MemoryEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Configuration for a `CgroupWatcher`
#[derive(Debug, Clone)]
pub struct Config {
    // The target difference between the total memory reserved for the cgroup
    // and the value of the cgroup's memory.high.
    //
    // In other words, memory.high + oom_buffer_bytes will equal the total memory that the cgroup may
    // use (equal to system memory, minus whatever's taken out for the file cache).
    oom_buffer_bytes: u64,

    // The amount of memory, in bytes, below a proposed new value for
    // memory.high that the cgroup's memory usage must be for us to downscale
    //
    // In other words, we can downscale only when:
    //
    //   memory.current + memory_high_buffer_bytes < (proposed) memory.high
    //
    // TODO: there's some minor issues with this approach -- in particular, that we might have
    // memory in use by the kernel's page cache that we're actually ok with getting rid of.
    memory_high_buffer_bytes: u64,

    // The maximum duration, in milliseconds, that we're allowed to pause
    // the cgroup for while waiting for the autoscaler-agent to upscale us
    max_upscale_wait: Duration,

    // The required minimum time, in milliseconds, that we must wait before re-freezing
    // the cgroup while waiting for the autoscaler-agent to upscale us.
    do_not_freeze_more_often_than: Duration,

    // The amount of memory, in bytes, that we should periodically increase memory.high
    // by while waiting for the autoscaler-agent to upscale us.
    //
    // This exists to avoid the excessive throttling that happens when a cgroup is above its
    // memory.high for too long. See more here:
    // https://github.com/neondatabase/autoscaling/issues/44#issuecomment-1522487217
    memory_high_increase_by_bytes: u64,

    // The period, in milliseconds, at which we should repeatedly increase the value
    // of the cgroup's memory.high while we're waiting on upscaling and memory.high
    // is still being hit.
    //
    // Technically speaking, this actually serves as a rate limit to moderate responding to
    // memory.high events, but these are roughly equivalent if the process is still allocating
    // memory.
    memory_high_increase_every: Duration,
}

impl Config {
    /// Calculate the new value for the cgroups memory.high based on system memory
    pub fn calculate_memory_high_value(&self, total_system_mem: u64) -> u64 {
        total_system_mem.saturating_sub(self.oom_buffer_bytes)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            oom_buffer_bytes: 100 * MiB,
            memory_high_buffer_bytes: 100 * MiB,
            // while waiting for upscale, don't freeze for more than 20ms every 1s
            max_upscale_wait: Duration::from_millis(20),
            do_not_freeze_more_often_than: Duration::from_millis(1000),
            // while waiting for upscale, increase memory.high by 10MiB every 25ms
            memory_high_increase_by_bytes: 10 * MiB,
            memory_high_increase_every: Duration::from_millis(25),
        }
    }
}

/// Used to represent data that is associated with a certain point in time, such
/// as an upscale request or memory.high event.
///
/// Internally, creating a `Sequenced` uses a static atomic counter to obtain
/// a unique sequence number. Sequence numbers are monotonically increasing,
/// allowing us to answer questions like "did this upscale happen after this
/// memory.high event?" by comparing the sequence numbers of the two events.
#[derive(Debug, Copy, Clone)]
pub struct Sequenced<T> {
    seqnum: u64,
    data: T,
}

impl<T> Sequenced<T> {
    pub fn new(data: T) -> Self {
        Self {
            seqnum: EVENT_SEQUENCE_NUMBER.fetch_add(1, Ordering::AcqRel),
            data,
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
    config: Config,

    /// The actual cgroup we are watching and managing.
    cgroup: cgroups_rs::Cgroup,

    command_sender: mpsc::Sender<(CgroupCommand, oneshot::Sender<CgroupCommandResult>)>,
}

/// Read memory.events for the desired event type.
///
/// `path` specifies the path to the desired `memory.events` file.
/// For more info, see the `memory.events` section of the [kernel docs]
/// <https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-interface-files>
fn get_event_count(path: &str, event: MemoryEvent) -> anyhow::Result<u64> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read memory.events from {path}"))?;

    // Then contents of the file look like:
    // low 42
    // high 101
    // ...
    contents
        .lines()
        .filter_map(|s| s.split_once(' '))
        .find(|(e, _)| *e == event.as_str())
        .ok_or_else(|| anyhow!("failed to find entry for memory.{event} events in {path}"))
        .and_then(|(_, count)| {
            count
                .parse::<u64>()
                .with_context(|| format!("failed to parse memory.{event} as u64"))
        })
}

/// Create an event stream that produces events whenever the file at the provided
/// path is modified.
fn create_file_watcher(path: &str) -> anyhow::Result<EventStream<[u8; 1024]>> {
    info!("creating file watcher for {path}");
    let inotify = Inotify::init().context("failed to initialize file watcher")?;
    inotify
        .watches()
        .add(path, WatchMask::MODIFY)
        .with_context(|| format!("failed to start watching {path}"))?;
    inotify
        // The inotify docs use [0u8; 1024] so we'll just copy them. We only need
        // to store one event at a time - if the event gets written over, that's
        // ok. We still see that there is an event. For more information, see:
        // https://man7.org/linux/man-pages/man7/inotify.7.html
        .into_event_stream([0u8; 1024])
        .context("failed to start inotify event stream")
}

impl CgroupWatcher {
    /// Create a new `CgroupWatcher`.
    #[tracing::instrument(skip_all, fields(%name))]
    pub fn new(
        name: String,
    ) -> anyhow::Result<(
        Self,
        CgroupWatchController,
        impl Stream<Item = Sequenced<u64>>,
    )> {
        // TODO: clarify exactly why we need v2
        // Make sure cgroups v2 (aka unified) are supported
        if !is_cgroup2_unified_mode() {
            anyhow::bail!("cgroups v2 not supported");
        }
        let cgroup = cgroups_rs::Cgroup::load(hierarchies::auto(), &name);

        // Start monitoring the cgroup for memory events. In general, for
        // cgroups v2 (aka unified), metrics are reported in files like
        // > `/sys/fs/cgroup/{name}/{metric}`
        // We are looking for `memory.high` events, which are stored in the
        // file `memory.events`. For more info, see the `memory.events` section
        // of https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-interface-files
        let path = format!("{}/{}/memory.events", UNIFIED_MOUNTPOINT, &name);
        let memory_events = create_file_watcher(&path)
            .with_context(|| format!("failed to create event watcher for {path}"))?
            // This would be nice with with .inspect_err followed by .ok
            .filter_map(move |_| match get_event_count(&path, MemoryEvent::High) {
                Ok(high) => Some(high),
                Err(error) => {
                    // TODO: Might want to just panic here
                    warn!(?error, "failed to read high events count from {}", &path);
                    None
                }
            })
            // Only report the event if the memory.high count increased
            .filter_map(|high| {
                if MEMORY_EVENT_COUNT.fetch_max(high, Ordering::AcqRel) < high {
                    Some(high)
                } else {
                    None
                }
            })
            .map(Sequenced::new);

        let initial_count = get_event_count(
            &format!("{}/{}/memory.events", UNIFIED_MOUNTPOINT, &name),
            MemoryEvent::High,
        )?;

        info!(initial_count, "initial memory.high event count");

        // Hard update `MEMORY_EVENT_COUNT` since there could have been processes
        // running in the cgroup before that caused it to be non-zero.
        MEMORY_EVENT_COUNT.fetch_max(initial_count, Ordering::AcqRel);

        let (command_tx, command_rx) = mpsc::channel(1);

        Ok((
            Self {
                config: Default::default(),
                cgroup,
                command_sender: command_tx,
            },
            CgroupWatchController {
                commands: command_rx,
            },
            memory_events,
        ))
    }

    async fn do_command(&self, cmd: CgroupCommand) -> anyhow::Result<CgroupCommandResult> {
        let (tx_result, rx_result) = oneshot::channel();

        self.command_sender
            .send((cmd, tx_result))
            .await
            .map_err(|_| anyhow!("cgroup command channel receiver dropped"))
            .context("failed to send internal cgroup command")?;

        rx_result
            .await
            .context("failed to receive internal cgroup command response")
    }

    pub async fn unset_memory_high(&self) -> anyhow::Result<()> {
        match self.do_command(CgroupCommand::UnsetMemoryHigh).await? {
            CgroupCommandResult::Failure { .. } => {
                unreachable!("UnsetMemoryHigh command should never return graceful failure")
            }
            CgroupCommandResult::Success { message } => {
                info!(status = message, "cgroup unset_memory_high successful");
                Ok(())
            }
        }
    }

    pub async fn set_initial_memory_high(&self, mem_size: MemorySize) -> anyhow::Result<()> {
        match self
            .do_command(CgroupCommand::SetInitialMemoryHigh(mem_size))
            .await?
        {
            CgroupCommandResult::Failure { .. } => {
                unreachable!("SetInitialMemoryHigh command should never return graceful failure")
            }
            CgroupCommandResult::Success { message } => {
                info!(
                    status = message,
                    "cgroup set_initial_memory_high successful"
                );
                Ok(())
            }
        }
    }

    pub async fn downscale(
        &self,
        target_mem_size: MemorySize,
    ) -> anyhow::Result<Result<String, String>> {
        self.do_command(CgroupCommand::Downscale(Sequenced::new(target_mem_size)))
            .await
            .map(|res| match res {
                CgroupCommandResult::Failure { message } => Err(message),
                CgroupCommandResult::Success { message } => Ok(message),
            })
    }

    pub async fn upscale(&self, mem_size: MemorySize) -> anyhow::Result<()> {
        match self
            .do_command(CgroupCommand::Upscale(Sequenced::new(mem_size)))
            .await?
        {
            CgroupCommandResult::Failure { .. } => {
                unreachable!("Upscale command should never return graceful failure")
            }
            CgroupCommandResult::Success { message } => {
                info!(status = message, "cgroup upscale successful");
                Ok(())
            }
        }
    }
}

pub struct CgroupWatchController {
    commands: mpsc::Receiver<(CgroupCommand, oneshot::Sender<CgroupCommandResult>)>,
}

#[derive(Debug, Copy, Clone)]
pub struct MemorySize {
    pub bytes: u64,
}

#[derive(Debug, Copy, Clone)]
enum CgroupCommand {
    UnsetMemoryHigh,
    SetInitialMemoryHigh(MemorySize),
    Downscale(Sequenced<MemorySize>),
    Upscale(Sequenced<MemorySize>),
}

enum CgroupCommandResult {
    Success { message: String },
    Failure { message: String },
}

pin_project! {
    /// Object storing the state inside of [`CgroupWatcher::watch`]
    #[project = CgroupWatcherStateProjected]
    struct CgroupWatcherState {
        // If not `None`, the time at which we last increased `memory.high` in order to reduce the
        // risk of throttling while waiting for upscaling.
        //
        // We're not allowed to increase `memory.high` more often than
        // `Config.memory_high_increase_every`.
        last_memory_high_increase_at: Option<Instant>,

        // If not `None`, the time at which we last froze the cgroup. We're not allowed to freeze
        // the cgroup more often than `Config.do_not_freeze_more_often_than`.
        last_frozen_at: Option<Instant>,

        // Timer representing when we must unfreeze the cgroup, if we believe it's currently frozen
        //
        // This gets set on memory.high events when `wait_to_freeze` is elapsed, and is never more
        // than `Config.max_upscale_wait`.
        #[pin]
        must_unfreeze_at: Option<tokio::time::Sleep>,

        // True if we've requested upscaling that hasn't yet happened
        waiting_on_upscale: bool,

        last_upscale_seqnum: Option<u64>,
    }
}

// `CgroupWatcher::watch` and supporting methods:
impl CgroupWatcher {
    /// The entrypoint for the `CgroupWatcher`.
    #[tracing::instrument(skip_all)]
    pub async fn watch<E>(
        &self,
        mut controller: CgroupWatchController,
        upscale_requester: mpsc::Sender<()>,
        events: E,
    ) -> anyhow::Result<()>
    where
        E: Stream<Item = Sequenced<u64>>,
    {
        let mut events = pin!(events);

        let state = pin!(CgroupWatcherState {
            last_memory_high_increase_at: None,
            last_frozen_at: None,
            must_unfreeze_at: None,
            waiting_on_upscale: false,
            last_upscale_seqnum: None,
        });
        let mut state = state.project();

        loop {
            tokio::select! {
                // We want the select! to be biased so that we always unfreeze if time's up, rather
                // than e.g. spinning on incoming commands or memory.high events.
                biased;

                // convert &mut Pin<&mut Option<Sleep>> (needed so we don't move out of must_unfreeze_at)
                //   → Pin<&mut Option<Sleep>>
                //   → Option<Pin<&mut Sleep>> (so we can unwrap)
                //   → Pin<&mut Sleep>         (so we get a Future out of it)
                _ = state.must_unfreeze_at.as_mut().as_pin_mut().unwrap(), if state.must_unfreeze_at.is_some() => {
                    info!("cgroup freeze limit expired without getting upscaled, thawing cgroup");
                    self.thaw()?;
                    state.must_unfreeze_at.set(None); // No longer need to unfreeze
                },

                // If the `Runner` has issued a command, then we should process that.
                command_opt = controller.commands.recv() => {
                    let (command, result_sender) = command_opt.ok_or_else(|| anyhow!("commands event stream closed"))?;
                    if result_sender.is_closed() {
                        warn!(?command, "skipping command because result sender is closed");
                        continue;
                    }

                    let (ty, command_result) = match command {
                        CgroupCommand::UnsetMemoryHigh => {
                            ("'unset memory.high'", self.handle_unset_memory_high(&mut state)?)
                        },
                        CgroupCommand::SetInitialMemoryHigh(mem_size) => {
                            ("'set initial memory.high'", self.handle_set_initial_memory_high(&mut state, mem_size)?)
                        },
                        CgroupCommand::Upscale(Sequenced { seqnum, data: resources }) => {
                            ("upscale", self.handle_upscale_command(&mut state, seqnum, resources)?)
                        },
                        CgroupCommand::Downscale(Sequenced { seqnum, data: resources }) => {
                            ("downscale", self.handle_downscale_command(&mut state, seqnum, resources)?)
                        },
                    };

                    if let Err(_) = result_sender.send(command_result) {
                        error!("Failed to send {ty} command result for cgroup");
                    }
                },

                // Got a memory.high event, need to decide what to do
                event = events.next() => {
                    let event = event.ok_or_else(|| anyhow!("memory.high event stream closed"))?;
                    self.handle_memory_high_event(&mut state, event, &upscale_requester).await?;
                },
            };
        }
    }

    fn handle_unset_memory_high(
        &self,
        _state: &mut CgroupWatcherStateProjected,
    ) -> anyhow::Result<CgroupCommandResult> {
        // We don't *really* need to fetch memory.high here, but it's nice to do in order to
        // improve the quality of the logs, and it should be minimally expensive.
        let message = match self
            .get_memory_high()
            .context("failed to get memory.high")?
        {
            MaxValue::Max => {
                let msg = "no need to update memory.high (currently set to 'max')";
                info!("{msg}");
                msg.to_owned()
            }
            MaxValue::Value(current_memory_high_bytes) => {
                info!(current_memory_high_bytes, "updating memory.high to 'max'");
                self.set_memory_high(MaxValue::Max)
                    .context("failed to set memory.high")?;
                "memory.high set to 'max'".to_owned()
            }
        };
        Ok(CgroupCommandResult::Success { message })
    }

    fn handle_set_initial_memory_high(
        &self,
        _state: &mut CgroupWatcherStateProjected,
        mem_size: MemorySize,
    ) -> anyhow::Result<CgroupCommandResult> {
        let new_memory_high_bytes = self.config.calculate_memory_high_value(mem_size.bytes);

        match self
            .get_memory_high()
            .context("failed to get memory.high")?
        {
            MaxValue::Max => info!(
                new_memory_high_bytes,
                "updating memory.high (currently set to 'max')"
            ),
            MaxValue::Value(current_memory_high_bytes) => info!(
                current_memory_high_bytes,
                new_memory_high_bytes, "updating memory.high"
            ),
        }

        self.set_memory_high(MaxValue::Value(new_memory_high_bytes as i64))
            .context("failed to set memory.high")?;
        Ok(CgroupCommandResult::Success {
            message: format!(
                "set cgroup memory.high to {} MiB",
                bytes_to_mebibytes(new_memory_high_bytes),
            ),
        })
    }

    fn handle_upscale_command(
        &self,
        state: &mut CgroupWatcherStateProjected<'_>,
        seqnum: u64,
        mem_size: MemorySize,
    ) -> anyhow::Result<CgroupCommandResult> {
        info!(seqnum, ?mem_size, "received upscale command");

        // On upscaling, we want to set memory.high to the appropriate value and reset any other
        // temporary conditions that may have accumulated while waiting for upscale.

        *state.waiting_on_upscale = false;
        *state.last_upscale_seqnum = Some(seqnum);

        if self
            .is_frozen()
            .context("failed to check if cgroup is frozen")?
        {
            info!("thawing cgroup");
            self.thaw()?;
            state.must_unfreeze_at.set(None);
        }

        let new_memory_high_bytes = self.config.calculate_memory_high_value(mem_size.bytes);

        match self
            .get_memory_high()
            .context("failed to get memory.high")?
        {
            MaxValue::Max => info!(
                new_memory_high_bytes,
                "updating memory.high (currently set to 'max')"
            ),
            MaxValue::Value(current_memory_high_bytes) => info!(
                current_memory_high_bytes,
                new_memory_high_bytes, "updating memory.high"
            ),
        }

        self.set_memory_high(MaxValue::Value(new_memory_high_bytes as i64))
            .context("failed to set memory.high")?;

        Ok(CgroupCommandResult::Success {
            message: format!(
                "set cgroup memory.high to {} MiB",
                bytes_to_mebibytes(new_memory_high_bytes)
            ),
        })
    }

    fn handle_downscale_command(
        &self,
        _state: &mut CgroupWatcherStateProjected<'_>,
        seqnum: u64,
        mem_size: MemorySize,
    ) -> anyhow::Result<CgroupCommandResult> {
        info!(seqnum, ?mem_size, "received downscale command");

        // On downscaling, we want to set memory.high, but only if the current
        // memory usage is sufficiently below the target value.

        let new_memory_high_bytes = self.config.calculate_memory_high_value(mem_size.bytes);
        let current_memory_usage = self
            .current_memory_usage()
            .context("failed to fetch cgroup memory usage")?;

        if new_memory_high_bytes < current_memory_usage + self.config.memory_high_buffer_bytes {
            info!(
                new_memory_high_bytes,
                current_memory_usage,
                buffer_bytes = self.config.memory_high_buffer_bytes,
                "cgroup rejecting downscale because calculated memory.high is not sufficiently less than current usage",
            );
            return Ok(CgroupCommandResult::Failure {
                message: format!(
                    "calculated memory.high too low: {} MiB (new high) < {} (current usage) + {} (buffer)",
                    bytes_to_mebibytes(new_memory_high_bytes),
                    bytes_to_mebibytes(current_memory_usage),
                    bytes_to_mebibytes(self.config.memory_high_buffer_bytes),
                ),
            });
        }

        // Ok, memory usage is low enough, let's decrease memory.high:
        match self
            .get_memory_high()
            .context("failed to get memory.high")?
        {
            MaxValue::Max => info!(
                new_memory_high_bytes,
                "updating memory.high (currently set to 'max')"
            ),
            MaxValue::Value(current_memory_high_bytes) => info!(
                current_memory_high_bytes,
                new_memory_high_bytes, "updating memory.high"
            ),
        }

        self.set_memory_high(MaxValue::Value(new_memory_high_bytes as i64))
            .context("failed to set memory.high")?;
        Ok(CgroupCommandResult::Success {
            message: format!(
                "set cgroup memory.high to {} MiB",
                bytes_to_mebibytes(new_memory_high_bytes),
            ),
        })
    }

    async fn handle_memory_high_event(
        &self,
        state: &mut CgroupWatcherStateProjected<'_>,
        event: Sequenced<u64>,
        upscale_requester: &mpsc::Sender<()>,
    ) -> anyhow::Result<()> {
        // The memory.high came before our last upscale, so we consider
        // it resolved
        if *state.last_upscale_seqnum > Some(event.seqnum) {
            info!(
                seqnum = event.seqnum,
                last_upscale_seqnum = state
                    .last_upscale_seqnum
                    .expect("None should not be greater than Some"),
                "ignoring memory.high event because it happened before before last upscale",
            );
            return Ok(());
        }

        // Fetch the current time at the start, so that subsequent delays
        // are more likely to under-estimate than over-estimate, which
        // should help with reliability if the system is overloaded.
        let now = Instant::now();

        // The memory.high came after our latest upscale, now we need to
        // decide what to do.
        //
        // If it's been long enough since we last froze, freeze the
        // cgroup and request upscale
        let long_enough_since_last_freeze = state
            .last_frozen_at
            .map(|t| now > t + self.config.do_not_freeze_more_often_than)
            .unwrap_or(true);
        if long_enough_since_last_freeze {
            info!("received memory.high event, freezing cgroup and forwarding upscale request");

            self.freeze().context("failed to freeze cgroup")?;
            state.must_unfreeze_at.set(Some(tokio::time::sleep_until(
                now + self.config.max_upscale_wait,
            )));

            Self::request_upscale(upscale_requester).await?;
            *state.waiting_on_upscale = true;
            return Ok(());
        }

        // If we're already waiting on upscaling, then increase
        // memory.high (if able) to avoid throttling.
        //
        // In either case, we'll re-request upscaling afterwards, because
        // our `Runner::run` (and downstream, the autoscaler-agent) both
        // deduplicate incoming upscaling requests.
        let can_increase_memory_high = state
            .last_memory_high_increase_at
            .map(|t| now > t + self.config.memory_high_increase_every)
            .unwrap_or(true);
        if *state.waiting_on_upscale && can_increase_memory_high {
            info!("received memory.high event but too soon to refreeze, so increasing memory.high");

            match self
                .get_memory_high()
                .context("failed to get memory.high")?
            {
                MaxValue::Max => {
                    warn!("memory.high is already set to 'max', no further increases possible")
                }
                MaxValue::Value(current_memory_high_bytes) => {
                    let new_memory_high_bytes = current_memory_high_bytes
                        + self.config.memory_high_increase_by_bytes as i64;
                    info!(
                        current_memory_high_bytes,
                        new_memory_high_bytes, "updating memory.high"
                    );

                    self.set_memory_high(MaxValue::Value(new_memory_high_bytes))
                        .context("failed to set memory.high")?;
                    *state.last_memory_high_increase_at = Some(now);
                }
            }
        } else {
            info!("received memory.high event, but too soon to refreeze or bump memory.high");
        }

        Self::request_upscale(upscale_requester).await?;
        *state.waiting_on_upscale = true;
        Ok(())
    }

    // TODO: make this non-async, using something like `tokio::sync::broadcast`,
    // because it doesn't really matter *how many* times we request upscaling, just
    // that we've done it at some point.
    async fn request_upscale(upscale_requester: &mpsc::Sender<()>) -> anyhow::Result<()> {
        upscale_requester
            .send(())
            .await
            .context("failed to request upscale")
    }
}

// Methods for manipulating the actual cgroup
impl CgroupWatcher {
    /// Get a handle on the freezer subsystem.
    fn freezer(&self) -> anyhow::Result<&FreezerController> {
        if let Some(Freezer(freezer)) = self
            .cgroup
            .subsystems()
            .iter()
            .find(|sub| matches!(sub, Freezer(_)))
        {
            Ok(freezer)
        } else {
            anyhow::bail!("could not find freezer subsystem")
        }
    }

    fn is_frozen(&self) -> anyhow::Result<bool> {
        use cgroups_rs::freezer::FreezerState;

        let state = self.freezer()?.state()?;
        Ok(matches!(
            state,
            FreezerState::Freezing | FreezerState::Frozen
        ))
    }

    /// Attempt to freeze the cgroup.
    fn freeze(&self) -> anyhow::Result<()> {
        self.freezer()
            .context("failed to get freezer subsystem")?
            .freeze()
            .context("failed to freeze")
    }

    /// Attempt to thaw the cgroup.
    fn thaw(&self) -> anyhow::Result<()> {
        self.freezer()
            .context("failed to get freezer subsystem")?
            .thaw()
            .context("failed to thaw")
    }

    /// Get a handle on the memory subsystem.
    ///
    /// Note: this method does not require `self.memory_update_lock` because
    /// getting a handle to the subsystem does not access any of the files we
    /// care about, such as memory.high and memory.events
    fn memory(&self) -> anyhow::Result<&MemController> {
        if let Some(Mem(memory)) = self
            .cgroup
            .subsystems()
            .iter()
            .find(|sub| matches!(sub, Mem(_)))
        {
            Ok(memory)
        } else {
            anyhow::bail!("could not find memory subsystem")
        }
    }

    /// Get cgroup current memory usage.
    fn current_memory_usage(&self) -> anyhow::Result<u64> {
        Ok(self
            .memory()
            .context("failed to get memory subsystem")?
            .memory_stat()
            .usage_in_bytes)
    }

    /// Set cgroup memory.high threshold.
    fn set_memory_high(&self, value: MaxValue) -> anyhow::Result<()> {
        self.memory()
            .context("failed to get memory subsystem")?
            .set_mem(cgroups_rs::memory::SetMemory {
                low: None,
                high: Some(value),
                min: None,
                max: None,
            })
            .map_err(anyhow::Error::from)
    }

    /// Get memory.high threshold.
    fn get_memory_high(&self) -> anyhow::Result<MaxValue> {
        let high = self
            .memory()
            .context("failed to get memory subsystem while getting memory statistics")?
            .get_mem()
            .map(|mem| mem.high)
            .context("failed to get memory statistics from subsystem")?;
        high.ok_or_else(|| anyhow!("failed to read memory.high from memory subsystem"))
    }
}
