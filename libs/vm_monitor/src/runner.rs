//! Exposes the `Runner`, which handles messages received from agent and
//! sends upscale requests.
//!
//! This is the "Monitor" part of the monitor binary and is the main entrypoint for
//! all functionality.

use std::fmt::Debug;
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use axum::extract::ws::{Message, WebSocket};
use futures::StreamExt;
use sysinfo::System;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::dispatcher::Dispatcher;
use crate::filecache::{FileCacheConfig, FileCacheState};
use crate::protocol::{InboundMsg, InboundMsgKind, OutboundMsg, OutboundMsgKind, Resources};
use crate::sliding_window::SlidingMax;
use crate::{bytes_to_mebibytes, Args, MiB};

/// Central struct that tracks the desired scaling target, and interacts with the agent
/// and dispatcher to handle signals from the agent.
#[derive(Debug)]
pub struct Runner {
    config: Config,
    filecache: Option<FileCacheState>,
    dispatcher: Dispatcher,

    /// We "mint" new message ids by incrementing this counter and taking the value.
    ///
    /// **Note**: This counter is always odd, so that we avoid collisions between the IDs generated
    /// by us vs the autoscaler-agent.
    counter: usize,

    last_scale_request: Option<(Resources, Instant)>,

    cpu_window: SlidingMax<f64, Instant>,
    mem_window: SlidingMax<u64, Instant>,

    system: System,

    /// A signal to kill the main thread produced by `self.run()`. This is triggered
    /// when the server receives a new connection. When the thread receives the
    /// signal off this channel, it will gracefully shutdown.
    kill: broadcast::Receiver<()>,
}

/// Configuration for a `Runner`
#[derive(Debug)]
pub struct Config {
    /// `sys_buffer_bytes` gives the estimated amount of memory, in bytes, that the kernel uses before
    /// handing out the rest to userspace. This value is the estimated difference between the
    /// *actual* physical memory and the amount reported by `grep MemTotal /proc/meminfo`.
    ///
    /// For more information, refer to `man 5 proc`, which defines MemTotal as "Total usable RAM
    /// (i.e., physical RAM minus a few reserved bits and the kernel binary code)".
    ///
    /// We only use `sys_buffer_bytes` when calculating the system memory from the *external* memory
    /// size, rather than the self-reported memory size, according to the kernel.
    ///
    /// TODO: this field is only necessary while we still have to trust the autoscaler-agent's
    /// upscale resource amounts (because we might not *actually* have been upscaled yet). This field
    /// should be removed once we have a better solution there.
    sys_buffer_bytes: u64,

    /// Interval at which we poll memory and CPU statistics for scaling decisions.
    poll_interval: Duration,

    /// The resources requested from the agent are calculated based on the Max of memory
    /// usage and load average over a sliding window of the last X seconds. This controls
    /// the length of the window to consider.
    sliding_window_length: Duration,

    /// Desired fraction of current CPU that the load average should be. For example, with a value
    /// of 0.7, we'd want load average to sit at 0.7 × CPU, scaling CPU to make this happen.
    load_average_fraction_target: f64,

    /// Desired fraction of current memory that we would like to be using. For example, with a value
    /// of 0.7, on a 4GB VM we'd like to be using 2.8GB of memory.
    memory_usage_fraction_target: f64,

    /// When requesting scaling to a certain # of CPUs, the request is rounded up to the
    /// nearest multiple of 'cpu_quantum'. For example, if the desired # of CPUs based on the
    /// usage is 3.1, and cpu_quantum is 0.25, we'd request 3.25 CPUs.
    cpu_quantum: f64,

    /// Like 'cpu_quantum', but for memory. In bytes.
    mem_quantum: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sys_buffer_bytes: 100 * MiB,
            poll_interval: Duration::from_millis(100),
            sliding_window_length: Duration::from_secs(60),
            cpu_quantum: 0.25,
            mem_quantum: 512 * 1024 * 1024,
            load_average_fraction_target: 0.9,
            memory_usage_fraction_target: 0.75,
        }
    }
}

impl Runner {
    /// Create a new monitor.
    #[tracing::instrument(skip_all, fields(?config, ?args))]
    pub async fn new(
        config: Config,
        args: &Args,
        ws: WebSocket,
        kill: broadcast::Receiver<()>,
        token: CancellationToken,
    ) -> anyhow::Result<Runner> {
        anyhow::ensure!(
            config.sys_buffer_bytes != 0,
            "invalid monitor Config: sys_buffer_bytes cannot be 0"
        );

        let dispatcher = Dispatcher::new(ws)
            .await
            .context("error creating new dispatcher")?;

        let now = Instant::now();
        let mut state = Runner {
            config,
            filecache: None,
            dispatcher,
            counter: 1, // NB: must be odd, see the comment about the field for more.
            last_scale_request: None,
            cpu_window: SlidingMax::new(0.0, now),
            mem_window: SlidingMax::new(0, now),
            kill,
            system: System::new(),
        };

        state.system.refresh_specifics(
            sysinfo::RefreshKind::new().with_memory(sysinfo::MemoryRefreshKind::new().with_ram()),
        );

        let mem = state.system.total_memory();

        if let Some(connstr) = &args.pgconnstr {
            info!("initializing file cache");
            let config = FileCacheConfig::default();

            let mut file_cache = FileCacheState::new(connstr, config, token.clone())
                .await
                .context("failed to create file cache")?;

            let size = file_cache
                .get_file_cache_size()
                .await
                .context("error getting file cache size")?;

            let new_size = file_cache.config.calculate_cache_size(mem);
            info!(
                initial = bytes_to_mebibytes(size),
                new = bytes_to_mebibytes(new_size),
                "setting initial file cache size",
            );

            // note: even if size == new_size, we want to explicitly set it, just
            // to make sure that we have the permissions to do so
            let actual_size = file_cache
                .set_file_cache_size(new_size)
                .await
                .context("failed to set file cache size, possibly due to inadequate permissions")?;
            if actual_size != new_size {
                info!("file cache size actually got set to {actual_size}")
            }

            state.filecache = Some(file_cache);
        }

        Ok(state)
    }

    /// Attempt to downscale filecache
    #[tracing::instrument(skip_all, fields(?target))]
    pub async fn try_downscale(&mut self, target: Resources) -> anyhow::Result<(bool, String)> {
        // Nothing to adjust
        if self.filecache.is_none() {
            info!("no action needed for downscale (no file cache enabled)");
            return Ok((true, "monitor is not managing file cache".to_string()));
        }

        let requested_mem = target.mem;
        let usable_system_memory = requested_mem.saturating_sub(self.config.sys_buffer_bytes);
        let expected_file_cache_size = self
            .filecache
            .as_ref()
            .map(|file_cache| file_cache.config.calculate_cache_size(usable_system_memory))
            .unwrap_or(0);

        // The downscaling has been approved. Downscale the file cache.
        let mut status = vec![];
        if let Some(file_cache) = &mut self.filecache {
            let actual_usage = file_cache
                .set_file_cache_size(expected_file_cache_size)
                .await
                .context("failed to set file cache size")?;
            let message = format!(
                "set file cache size to {} MiB",
                bytes_to_mebibytes(actual_usage),
            );
            info!("downscale: {message}");
            status.push(message);
        }

        // TODO: make this status thing less jank
        let status = status.join("; ");
        Ok((true, status))
    }

    /// Handle new resources
    #[tracing::instrument(skip_all, fields(?resources))]
    pub async fn handle_upscale(&mut self, resources: Resources) -> anyhow::Result<()> {
        if self.filecache.is_none() {
            info!("no action needed for upscale (file cache is disabled)");
            return Ok(());
        }

        let new_mem = resources.mem;
        let usable_system_memory = new_mem.saturating_sub(self.config.sys_buffer_bytes);

        if let Some(file_cache) = &mut self.filecache {
            let expected_usage = file_cache.config.calculate_cache_size(usable_system_memory);
            info!(
                target = bytes_to_mebibytes(expected_usage),
                total = bytes_to_mebibytes(new_mem),
                "updating file cache size",
            );

            let actual_usage = file_cache
                .set_file_cache_size(expected_usage)
                .await
                .context("failed to set file cache size")?;

            if actual_usage != expected_usage {
                warn!(
                    "file cache was set to a different size that we wanted: target = {} Mib, actual= {} Mib",
                    bytes_to_mebibytes(expected_usage),
                    bytes_to_mebibytes(actual_usage)
                )
            }
        }

        Ok(())
    }

    /// Take in a message and perform some action, such as downscaling or upscaling,
    /// and return a message to be send back.
    #[tracing::instrument(skip_all, fields(%id, message = ?inner))]
    pub async fn process_message(
        &mut self,
        InboundMsg { inner, id }: InboundMsg,
    ) -> anyhow::Result<Option<OutboundMsg>> {
        match inner {
            InboundMsgKind::UpscaleNotification { granted } => {
                self.handle_upscale(granted)
                    .await
                    .context("failed to handle upscale")?;
                Ok(Some(OutboundMsg::new(
                    OutboundMsgKind::UpscaleConfirmation {},
                    id,
                )))
            }
            InboundMsgKind::DownscaleRequest { target } => self
                .try_downscale(target)
                .await
                .context("failed to downscale")
                .map(|(ok, status)| {
                    Some(OutboundMsg::new(
                        OutboundMsgKind::DownscaleResult { ok, status },
                        id,
                    ))
                }),
            InboundMsgKind::InvalidMessage { error } => {
                warn!(
                    %error, id, "received notification of an invalid message we sent"
                );
                Ok(None)
            }
            InboundMsgKind::InternalError { error } => {
                warn!(error, id, "agent experienced an internal error");
                Ok(None)
            }
            InboundMsgKind::HealthCheck {} => {
                Ok(Some(OutboundMsg::new(OutboundMsgKind::HealthCheck {}, id)))
            }
        }
    }

    /// Calculate the desired size of the VM, based on the CPU and memory usage right now.
    pub fn calculate_raw_target(&mut self) -> Resources {
        self.system.refresh_specifics(
            sysinfo::RefreshKind::new().with_memory(sysinfo::MemoryRefreshKind::new().with_ram()),
        );

        // For CPU:
        //
        // We use the 1 minute load average to measure "current" CPU usage. Target # of
        // CPUs is at the point where:
        //
        // (CPUs) * (LoadAverageFractionTarget) == (load average).
        let load_avg_1min = System::load_average().one;
        let goal_cpus = load_avg_1min / self.config.load_average_fraction_target;

        // For Memory:
        //
        // Target point is where (Mem) * (MemoryUsageFractionTarget) == (Mem Usage)
        let used_memory = self.system.used_memory();
        let goal_memory_bytes: u64 =
            (self.system.used_memory() as f64 / self.config.memory_usage_fraction_target) as u64;

        debug!("load avg: {load_avg_1min} used memory: {used_memory}");
        Resources {
            cpu: goal_cpus,
            mem: goal_memory_bytes,
        }
    }

    /// To avoid overly fine-grained requests to the agent, round up the request to a
    /// multiple of the CPU and memory size of one a Compute Unit.
    ///
    /// We still track CPU and memory separately though. The autoscaler agent will combine
    /// the CPU and memory requests to a single "# of Compute Units" measure.
    fn quantize_resources(&self, res: Resources) -> Resources {
        Resources {
            cpu: (res.cpu / self.config.cpu_quantum).ceil() * self.config.cpu_quantum,
            mem: ((res.mem as f64 / self.config.mem_quantum as f64).ceil()
                * self.config.mem_quantum as f64) as u64,
        }
    }

    // TODO: don't propagate errors, probably just warn!?
    #[tracing::instrument(skip_all)]
    pub async fn run(&mut self) -> anyhow::Result<()> {
        info!("starting dispatcher");

        let mut ticker = tokio::time::interval(self.config.poll_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // ticker.reset_immediately(); // FIXME: enable this once updating to tokio >= 1.30.0

        loop {
            tokio::select! {
                signal = self.kill.recv() => {
                    match signal {
                        Ok(()) => return Ok(()),
                        Err(e) => bail!("failed to receive kill signal: {e}")
                    }
                }

                // Time to re-evaluate the scaling target
                _ = ticker.tick() => {
                    let now = Instant::now();

                    // Calculate the desired resources based on current usage
                    let target_now = self.calculate_raw_target();

                    // Round it up to the nearest CU sizes, to avoid overly fine-grained
                    // requests.
                    let quantized_target_now = self.quantize_resources(target_now);

                    // Smoothen using sliding windows.
                    self.cpu_window.add_sample(quantized_target_now.cpu, now);
                    self.cpu_window.trim(now - self.config.sliding_window_length);
                    self.mem_window.add_sample(quantized_target_now.mem, now);
                    self.mem_window.trim(now - self.config.sliding_window_length);

                    let sliding_target = Resources {
                        cpu: *self.cpu_window.get_max(),
                        mem: *self.mem_window.get_max(),
                    };

                    // If no change, we're all ok.
                    //
                    // XXX: If the agent doesn't perform the scaling, should we retry after a while though?
                    if let Some((last_request_target, last_request_at)) = self.last_scale_request {
                        if last_request_target == sliding_target {
                            continue;
                        }

                        // If it's been less than 1 second since the last time we requested
                        // scaling, don't send a request to avoid spamming the agent.
                        let elapsed = now.duration_since(last_request_at);
                        if elapsed < Duration::from_secs(1) {
                            // *Ideally* we'd like to log here that we're ignoring the fact the
                            // memory stats are too high, but in practice this can result in
                            // spamming the logs with repetitive messages about ignoring the signal
                            //
                            // See https://github.com/neondatabase/neon/issues/5865 for more.
                            continue;
                        }

                        info!(
                            old_target_cpu = last_request_target.cpu,
                            old_target_mem = last_request_target.mem,
                            target_cpu = sliding_target.cpu,
                            target_mem = sliding_target.mem,
                            "scaling target changed, requesting scaling",
                        );
                    } else {
                        info!(
                            target_cpu = sliding_target.cpu,
                            target_mem = sliding_target.mem,
                            "no previous scaling request, requesting initial scale",
                        );
                    }

                    self.last_scale_request = Some((sliding_target, now));

                    self.counter += 2; // Increment, preserving parity (i.e. keep the
                                       // counter odd). See the field comment for more.
                    self.dispatcher
                        .send(OutboundMsg::new(OutboundMsgKind::ScaleRequest {target: sliding_target}, self.counter))
                        .await
                        .context("failed to send message")?;
                },

                // there is a message from the agent
                msg = self.dispatcher.source.next() => {
                    if let Some(msg) = msg {
                        match &msg {
                            Ok(msg) => {
                                let message: InboundMsg = match msg {
                                    Message::Text(text) => {
                                        serde_json::from_str(text).context("failed to deserialize text message")?
                                    }
                                    other => {
                                        warn!(
                                            // Don't use 'message' as a key as the
                                            // string also uses that for its key
                                            msg = ?other,
                                            "problem processing incoming message: agent should only send text messages but received different type"
                                        );
                                        continue
                                    },
                                };

                                if matches!(&message.inner, InboundMsgKind::HealthCheck { .. }) {
                                    debug!(?msg, "received message");
                                } else {
                                    info!(?msg, "received message");
                                }

                                let out = match self.process_message(message.clone()).await {
                                    Ok(Some(out)) => out,
                                    Ok(None) => continue,
                                    Err(e) => {
                                        // use {:#} for our logging because the display impl only
                                        // gives the outermost cause, and the debug impl
                                        // pretty-prints the error, whereas {:#} contains all the
                                        // causes, but is compact (no newlines).
                                        warn!(error = format!("{e:#}"), "error handling message");
                                        OutboundMsg::new(
                                            OutboundMsgKind::InternalError {
                                                error: e.to_string(),
                                            },
                                            message.id
                                        )
                                    }
                                };

                                self.dispatcher
                                    .send(out)
                                    .await
                                    .context("failed to send message")?;
                            }
                            Err(e) => warn!(
                                error = format!("{e}"),
                                msg = ?msg,
                                "received error message"
                            ),
                        }
                    } else {
                        anyhow::bail!("dispatcher connection closed")
                    }
                }
            }
        }
    }
}
