//! [`Timeline`] instances are rarely supposed to be used directly.
//!
//! Inside, such timelines hold the only instance [`tokio::sync::watch::Sender`],
//! that gets dropped/changed based on certain events.
//! There's a main `Arc<Timeline>` instance present in [`crate::tenant::Tenant`],
//! which gets dropped on timeline deletion.
//!
//! Such [`Arc`] is not supposed be shared elsewhere, since otherwise dropping main instance
//! won't do anything, hence the wrappers are used to access [`Timeline`] methods, but
//! ensure that no stray instance copies are stored.

use std::{
    marker::PhantomData,
    ops::Deref,
    sync::{Arc, Weak},
    time::Duration,
};

use anyhow::Context;
use pageserver_api::models::{DownloadRemoteLayersTaskInfo, TimelineState};
use tokio::sync::oneshot;
use utils::{id::TenantTimelineId, lsn::Lsn};

use crate::{pgdatadir_mapping::CalculateLogicalSizeError, tenant::storage_layer::RemoteLayer};

use super::Timeline;

/// A weak reference to the main [`Timeline`] instance, that's stored in [`crate::tenant::Tenant`].
/// It provides a way to upgrade to the timeline instance (with all its API), if the timeline is still in pageserver's memory.
///
/// This way, the weak reference could be held in any pageserver separate corner (e.g. [`crate::walreceiver`] loop), allowing
/// pageserver to remove and clean up actual timeline and its data and be sure, that all subsequent weak ref upgrades will fail.
///
/// Such semantics means, that operations that use [`TimelineRef`] may interrupt (if implemented to reupgrade periodically),
/// while operations that use `&Timeline` are "uninterruptable".
#[derive(Clone)]
pub struct TimelineRef {
    timeline: Weak<Timeline>,
    pub id: TenantTimelineId,
}

const ACTIVE_TIMELINE_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

// XXX: the current API is under the development: "just" timeline and "active" timeline separation is not the final one.
// It's more appropriate to allow the weak ref to consider timeline state and allow upgrading to "read" and "write" instances,
// depending on the state and the ability to perform GC/compaction ("write") or just return metadata ("read"), or something else?
//
// `TimelineGuard` would need more improvements later, in particular, we need to track such guards and wait for them to stop before
// graceful shutdown.
impl TimelineRef {
    // Let only Timeline module the ability to create weak references on itself.
    pub(super) fn new(timeline: &Timeline) -> Self {
        Self {
            timeline: Weak::clone(&timeline.myself),
            id: TenantTimelineId::new(timeline.tenant_id, timeline.timeline_id),
        }
    }

    /// Successfully upgrades to [`TimelineGuard`] if the timeline is in pageserver's memory.
    /// No state checks are made, only timeline liveness itself.
    pub fn timeline(&self) -> anyhow::Result<TimelineGuard<'_>> {
        Ok(TimelineGuard {
            timeline: self
                .timeline
                .upgrade()
                .with_context(|| format!("Timeline {} is dropped", self.id))?,
            _phantom: PhantomData::default(),
        })
    }

    /// Successfully upgrades to [`TimelineGuard`] if the timeline is in pageserver's memory and
    /// has [`TimelineState::Active`] state when upgraded.
    ///
    /// Further state checks are user's responsibility, the active state can change.
    pub fn active_timeline(&self) -> anyhow::Result<TimelineGuard<'_>> {
        let timeline = self.timeline()?;
        anyhow::ensure!(
            timeline.is_active(),
            "Timeline {} is not active, state: {:?}",
            self.id,
            timeline.current_state(),
        );
        Ok(timeline)
    }

    /// Attempts to upgrade the timeline ref and wait for the timeline to get [`TimelineState::Active`] state (if not already).
    /// Bails if the timeout expires or timeline fails to become active (e.g. gets into end state that never transitions to active).
    ///
    /// Further state checks are user's responsibility, the active state can change.
    pub async fn get_active_timeline_with_timeout(&self) -> anyhow::Result<TimelineGuard<'_>> {
        let timeline = self.timeline()?;
        match tokio::time::timeout(
            ACTIVE_TIMELINE_WAIT_TIMEOUT,
            timeline.wait_to_become_active(),
        )
        .await
        {
            Ok(Ok(())) => Ok(timeline),
            Ok(Err(e)) => Err(e),
            Err(_) => anyhow::bail!("Timeout waiting for timeline {} to become Active", self.id),
        }
    }
}

/// A wrapper around `Arc<Timeline>` that prevents it from being dropped fully and allows accessing timeline's methods.
/// Not intended to be cloned or stored somewhere: use it to access timeline's properties and drop it afterwards.
/// By reacquiring the guard periodically, the timeline-accessing processes can stop when the timeline is being shut down.
pub struct TimelineGuard<'a> {
    timeline: Arc<Timeline>,
    _phantom: PhantomData<&'a Timeline>,
}

// All `Timeline::` methods here exist because `Arc<Timeline>` is needed for them to run
impl<'a> TimelineGuard<'a> {
    pub fn get_current_logical_size(&self) -> anyhow::Result<(u64, bool)> {
        Timeline::get_current_logical_size(&self.timeline)
    }

    pub async fn spawn_download_all_remote_layers(
        &self,
    ) -> Result<DownloadRemoteLayersTaskInfo, DownloadRemoteLayersTaskInfo> {
        Timeline::spawn_download_all_remote_layers(&self.timeline).await
    }

    pub async fn download_remote_layer(
        &self,
        remote_layer: Arc<RemoteLayer>,
    ) -> anyhow::Result<()> {
        Timeline::download_remote_layer(Arc::clone(&self.timeline), remote_layer).await
    }

    pub fn spawn_ondemand_logical_size_calculation(
        &self,
        lsn: Lsn,
    ) -> oneshot::Receiver<Result<u64, CalculateLogicalSizeError>> {
        Timeline::spawn_ondemand_logical_size_calculation(&self.timeline, lsn)
    }

    async fn wait_to_become_active(&self) -> anyhow::Result<()> {
        let mut receiver = self.state.subscribe();
        loop {
            let current_state = *receiver.borrow_and_update();
            match current_state {
                TimelineState::Suspended => {
                    // in these states, there's a chance that we can reach ::Active
                    receiver.changed().await?;
                }
                TimelineState::Active { .. } => {
                    return Ok(());
                }
                TimelineState::Broken | TimelineState::Stopping => {
                    // There's no chance the tenant can transition back into ::Active
                    anyhow::bail!(
                        "Timeline {}/{} will not become active. Current state: {:?}",
                        self.timeline.tenant_id,
                        self.timeline.timeline_id,
                        current_state,
                    );
                }
            }
        }
    }
}

impl<'a> Deref for TimelineGuard<'a> {
    // Avoid exposing `Arc` here, so no `Arc::clone` leaks are possible.
    type Target = Timeline;

    fn deref(&self) -> &Self::Target {
        self.timeline.as_ref()
    }
}
