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
use tokio::sync::{broadcast, watch};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::cgroup::{self, CgroupWatcher};
use crate::dispatcher::Dispatcher;
use crate::filecache::{FileCacheConfig, FileCacheState};
use crate::protocol::{InboundMsg, InboundMsgKind, OutboundMsg, OutboundMsgKind, Resources};
use crate::{bytes_to_mebibytes, get_total_system_memory, spawn_with_cancel, Args, MiB};

/// Central struct that interacts with agent, dispatcher, and cgroup to handle
/// signals from the agent.
#[derive(Debug)]
pub struct Runner {
    config: Config,
    filecache: Option<FileCacheState>,
    cgroup: Option<CgroupState>,
    dispatcher: Dispatcher,

    /// We "mint" new message ids by incrementing this counter and taking the value.
    ///
    /// **Note**: This counter is always odd, so that we avoid collisions between the IDs generated
    /// by us vs the autoscaler-agent.
    counter: usize,

    last_upscale_request_at: Option<Instant>,

    /// A signal to kill the main thread produced by `self.run()`. This is triggered
    /// when the server receives a new connection. When the thread receives the
    /// signal off this channel, it will gracefully shutdown.
    kill: broadcast::Receiver<()>,
}

#[derive(Debug)]
struct CgroupState {
    watcher: watch::Receiver<(Instant, cgroup::MemoryHistory)>,
    /// If [`cgroup::MemoryHistory::avg_non_reclaimable`] exceeds `threshold`, we send upscale
    /// requests.
    threshold: u64,
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

    /// Minimum fraction of total system memory that the threshold for cgroup non-reclaimable
    /// memory usage must be below
    cgroup_min_overhead_fraction: f64,

    cgroup_downscale_threshold_buffer_bytes: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sys_buffer_bytes: 100 * MiB,
            cgroup_min_overhead_fraction: 0.15,
            cgroup_downscale_threshold_buffer_bytes: 100 * MiB,
        }
    }
}

impl Config {
    fn cgroup_threshold(&self, total_mem: u64, file_cache_disk_size: u64) -> u64 {
        // If the file cache is in tmpfs, then it will count towards shmem usage of the cgroup,
        // and thus be non-reclaimable, so we should allow for additional memory usage.
        //
        // If the file cache sits on disk, our desired stable system state is for it to be fully
        // page cached (its contents should only be paged to/from disk in situations where we can't
        // upscale fast enough). Page-cached memory is reclaimable, so we need to lower the
        // threshold for non-reclaimable memory so we scale up *before* the kernel starts paging
        // out the file cache.
        let memory_remaining_for_cgroup = total_mem.saturating_sub(file_cache_disk_size);

        // Guarantee at least some wiggle room, even if there is none from the file cache
        let max_remaining = (total_mem as f64 * (1.0 - self.cgroup_min_overhead_fraction)) as u64;

        memory_remaining_for_cgroup.min(max_remaining)
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

        let mut state = Runner {
            config,
            filecache: None,
            cgroup: None,
            dispatcher,
            counter: 1, // NB: must be odd, see the comment about the field for more.
            last_upscale_request_at: None,
            kill,
        };

        let mem = get_total_system_memory();

        let mut file_cache_disk_size = 0;

        // We need to process file cache initialization before cgroup initialization, so that the memory
        // allocated to the file cache is appropriately taken into account when we decide the cgroup's
        // memory limits.
        if let Some(connstr) = &args.pgconnstr {
            info!("initializing file cache");
            let config = match args.file_cache_on_disk {
                true => FileCacheConfig::default_on_disk(),
                false => FileCacheConfig::default_in_memory(),
            };

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

            if args.file_cache_on_disk {
                file_cache_disk_size = actual_size;
            }

            state.filecache = Some(file_cache);
        }

        if let Some(name) = &args.cgroup {
            // Best not to set up cgroup stuff more than once, so we'll initialize cgroup state
            // now, and then set limits later.
            info!("initializing cgroup");

            let cgroup =
                CgroupWatcher::new(name.clone()).context("failed to create cgroup manager")?;

            let init_value = cgroup::MemoryHistory {
                avg_non_reclaimable: 0,
                samples_count: 0,
                samples_span: Duration::ZERO,
            };
            let (hist_tx, hist_rx) = watch::channel((Instant::now(), init_value));

            spawn_with_cancel(token, |_| error!("cgroup watcher terminated"), async move {
                cgroup.watch(hist_tx).await
            });

            let threshold = state.config.cgroup_threshold(mem, file_cache_disk_size);
            info!(threshold, "set initial cgroup threshold",);

            state.cgroup = Some(CgroupState {
                watcher: hist_rx,
                threshold,
            });
        }

        Ok(state)
    }

    /// Attempt to downscale filecache + cgroup
    #[tracing::instrument(skip_all, fields(?target))]
    pub async fn try_downscale(&mut self, target: Resources) -> anyhow::Result<(bool, String)> {
        // Nothing to adjust
        if self.cgroup.is_none() && self.filecache.is_none() {
            info!("no action needed for downscale (no cgroup or file cache enabled)");
            return Ok((
                true,
                "monitor is not managing cgroup or file cache".to_string(),
            ));
        }

        let requested_mem = target.mem;
        let usable_system_memory = requested_mem.saturating_sub(self.config.sys_buffer_bytes);
        let (expected_file_cache_size, expected_file_cache_disk_size) = self
            .filecache
            .as_ref()
            .map(|file_cache| {
                let size = file_cache.config.calculate_cache_size(usable_system_memory);
                match file_cache.config.in_memory {
                    true => (size, 0),
                    false => (size, size),
                }
            })
            .unwrap_or((0, 0));
        if let Some(cgroup) = &self.cgroup {
            let (last_time, last_history) = *cgroup.watcher.borrow();

            // TODO: make the duration here configurable.
            if last_time.elapsed() > Duration::from_secs(5) {
                bail!("haven't gotten cgroup memory stats recently enough to determine downscaling information");
            } else if last_history.samples_count <= 1 {
                bail!("haven't received enough cgroup memory stats yet");
            }

            let new_threshold = self
                .config
                .cgroup_threshold(usable_system_memory, expected_file_cache_disk_size);

            let current = last_history.avg_non_reclaimable;

            if new_threshold < current + self.config.cgroup_downscale_threshold_buffer_bytes {
                let status = format!(
                    "{}: {} MiB (new threshold) < {} (current usage) + {} (downscale buffer)",
                    "calculated memory threshold too low",
                    bytes_to_mebibytes(new_threshold),
                    bytes_to_mebibytes(current),
                    bytes_to_mebibytes(self.config.cgroup_downscale_threshold_buffer_bytes)
                );

                info!(status, "discontinuing downscale");

                return Ok((false, status));
            }
        }

        // The downscaling has been approved. Downscale the file cache, then the cgroup.
        let mut status = vec![];
        let mut file_cache_disk_size = 0;
        if let Some(file_cache) = &mut self.filecache {
            let actual_usage = file_cache
                .set_file_cache_size(expected_file_cache_size)
                .await
                .context("failed to set file cache size")?;
            if !file_cache.config.in_memory {
                file_cache_disk_size = actual_usage;
            }
            let message = format!(
                "set file cache size to {} MiB (in memory = {})",
                bytes_to_mebibytes(actual_usage),
                file_cache.config.in_memory,
            );
            info!("downscale: {message}");
            status.push(message);
        }

        if let Some(cgroup) = &mut self.cgroup {
            let new_threshold = self
                .config
                .cgroup_threshold(usable_system_memory, file_cache_disk_size);

            let message = format!(
                "set cgroup memory threshold from {} MiB to {} MiB, of new total {} MiB",
                bytes_to_mebibytes(cgroup.threshold),
                bytes_to_mebibytes(new_threshold),
                bytes_to_mebibytes(usable_system_memory)
            );
            cgroup.threshold = new_threshold;
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
        if self.filecache.is_none() && self.cgroup.is_none() {
            info!("no action needed for upscale (no cgroup or file cache enabled)");
            return Ok(());
        }

        let new_mem = resources.mem;
        let usable_system_memory = new_mem.saturating_sub(self.config.sys_buffer_bytes);

        let mut file_cache_disk_size = 0;
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
            if !file_cache.config.in_memory {
                file_cache_disk_size = actual_usage;
            }

            if actual_usage != expected_usage {
                warn!(
                    "file cache was set to a different size that we wanted: target = {} Mib, actual= {} Mib",
                    bytes_to_mebibytes(expected_usage),
                    bytes_to_mebibytes(actual_usage)
                )
            }
        }

        if let Some(cgroup) = &mut self.cgroup {
            let new_threshold = self
                .config
                .cgroup_threshold(usable_system_memory, file_cache_disk_size);

            info!(
                "set cgroup memory threshold from {} MiB to {} MiB of new total {} MiB",
                bytes_to_mebibytes(cgroup.threshold),
                bytes_to_mebibytes(new_threshold),
                bytes_to_mebibytes(usable_system_memory)
            );
            cgroup.threshold = new_threshold;
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

    // TODO: don't propagate errors, probably just warn!?
    #[tracing::instrument(skip_all)]
    pub async fn run(&mut self) -> anyhow::Result<()> {
        info!("starting dispatcher");
        loop {
            tokio::select! {
                signal = self.kill.recv() => {
                    match signal {
                        Ok(()) => return Ok(()),
                        Err(e) => bail!("failed to receive kill signal: {e}")
                    }
                }

                // New memory stats from the cgroup, *may* need to request upscaling, if we've
                // exceeded the threshold
                result = self.cgroup.as_mut().unwrap().watcher.changed(), if self.cgroup.is_some() => {
                    result.context("failed to receive from cgroup memory stats watcher")?;

                    let cgroup = self.cgroup.as_ref().unwrap();

                    let (_time, cgroup_mem_stat) = *cgroup.watcher.borrow();

                    // If we haven't exceeded the threshold, then we're all ok
                    if cgroup_mem_stat.avg_non_reclaimable < cgroup.threshold {
                        continue;
                    }

                    // Otherwise, we generally want upscaling. But, if it's been less than 1 second
                    // since the last time we requested upscaling, ignore the event, to avoid
                    // spamming the agent.
                    if let Some(t) = self.last_upscale_request_at {
                        let elapsed = t.elapsed();
                        if elapsed < Duration::from_secs(1) {
                            info!(
                                elapsed_millis = elapsed.as_millis(),
                                avg_non_reclaimable = bytes_to_mebibytes(cgroup_mem_stat.avg_non_reclaimable),
                                threshold = bytes_to_mebibytes(cgroup.threshold),
                                "cgroup memory stats are high enough to upscale but too soon to forward the request, ignoring",
                            );
                            continue;
                        }
                    }

                    self.last_upscale_request_at = Some(Instant::now());

                    info!(
                        avg_non_reclaimable = bytes_to_mebibytes(cgroup_mem_stat.avg_non_reclaimable),
                        threshold = bytes_to_mebibytes(cgroup.threshold),
                        "cgroup memory stats are high enough to upscale, requesting upscale",
                    );

                    self.counter += 2; // Increment, preserving parity (i.e. keep the
                                       // counter odd). See the field comment for more.
                    self.dispatcher
                        .send(OutboundMsg::new(OutboundMsgKind::UpscaleRequest {}, self.counter))
                        .await
                        .context("failed to send message")?;
                },

                // there is a message from the agent
                msg = self.dispatcher.source.next() => {
                    if let Some(msg) = msg {
                        // Don't use 'message' as a key as the string also uses
                        // that for its key
                        info!(?msg, "received message");
                        match msg {
                            Ok(msg) => {
                                let message: InboundMsg = match msg {
                                    Message::Text(text) => {
                                        serde_json::from_str(&text).context("failed to deserialize text message")?
                                    }
                                    other => {
                                        warn!(
                                            // Don't use 'message' as a key as the
                                            // string also uses that for its key
                                            msg = ?other,
                                            "agent should only send text messages but received different type"
                                        );
                                        continue
                                    },
                                };

                                let out = match self.process_message(message.clone()).await {
                                    Ok(Some(out)) => out,
                                    Ok(None) => continue,
                                    Err(e) => {
                                        let error = e.to_string();
                                        warn!(?error, "error handling message");
                                        OutboundMsg::new(
                                            OutboundMsgKind::InternalError {
                                                error
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
                            Err(e) => warn!("{e}"),
                        }
                    } else {
                        anyhow::bail!("dispatcher connection closed")
                    }
                }
            }
        }
    }
}
