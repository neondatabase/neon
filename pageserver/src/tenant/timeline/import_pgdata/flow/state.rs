use core::panic;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{info, info_span, instrument};
use utils::{
    id::TimelineId,
    shard::TenantShardId,
    sync::gate::{Gate, GateGuard},
};

use crate::{
    context::RequestContext,
    span::debug_assert_current_span_has_tenant_and_timeline_id,
    task_mgr::TaskKind,
    tenant::{timeline, IndexPart},
};

use super::index_part_format;

pub(crate) struct Timeline {
    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
    inner: Arc<Mutex<Inner>>,
}

enum Inner {
    Done,
    WaitActivate(WaitActivate),
    Running(Running),
}

struct WaitActivate {
    cancel: CancellationToken,
    s3_uri: String,
}

struct Running {
    cancel: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
    receiver: tokio::sync::oneshot::Receiver<TaskResult>,
}

pub enum ActivateEffect {
    ActivateNow(TaskResult),
    DelayUntilImportDone(tokio::sync::oneshot::Receiver<TaskResult>),
}

pub struct TaskResult {
    gate_guard: GateGuard,
}

impl Timeline {
    pub(crate) fn from_index_part(
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        cancel: CancellationToken,
        index_part: &IndexPart,
    ) -> Self {
        Self {
            tenant_shard_id,
            timeline_id,
            inner: Arc::new(Mutex::new(match index_part.import_pgdata {
                None => Inner::Done,
                Some(index_part_format::Root::V1(index_part_format::V1::Done)) => Inner::Done,
                Some(index_part_format::Root::V1(index_part_format::V1::InProgress(
                    index_part_format::InProgress { s3_uri },
                ))) => Inner::WaitActivate(WaitActivate { s3_uri, cancel }),
            })),
        }
    }

    #[instrument(skip_all)]
    pub fn activate(&self, timeline_gate_guard: GateGuard) -> ActivateEffect {
        debug_assert_current_span_has_tenant_and_timeline_id();
        let mut inner = self.inner.lock().unwrap();
        match &mut *inner {
            Inner::Done => {
                info!("Timeline is already done");
                return ActivateEffect::ActivateNow(timeline_gate_guard);
            }
            Inner::WaitActivate(WaitActivate { cancel, s3_uri }) => {
                info!("spawning task");

                let (sender, receiver) = tokio::sync::oneshot::channel();

                let task = Task {
                    cancel: cancel.clone(),
                    s3_uri: s3_uri,
                    timeline_gate_guard,
                    sender,
                    inner: self.inner.clone(),
                };
                let join_handle = tokio::spawn(task.run().instrument(info_span!(
                    "timeline_import_pgdata",
                    tenant_id = %self.tenant_shard_id.tenant_id,
                    shard_id = %self.tenant_shard_id.shard_slug(),
                    timeline_id = %self.timeline_id
                )));
                *inner = Inner::Running(Running {
                    join_handle,
                    cancel,
                    receiver,
                });
                todo!()
            }
            Inner::Running => {
                warn!("task is already running, this should not happen");
            }
        }
    }

    #[instrument(skip_all)]
    pub async fn shutdown(&self) {
        debug_assert_current_span_has_tenant_and_timeline_id();
        let mut inner = self.inner.lock().unwrap();
        match &mut *inner {
            Inner::Done | Inner::WaitActivate(_) => {
                return;
            }
            Inner::Running(running) => {
                info!("cancelling");
                running.join_handle.cancel();
                info!("Waiting for task to finish");
                match running.join_handle.await {
                    Ok(()) => {
                        info!("");
                    }
                    Err(err) => {
                        info!("Task finished with error: {err:#}");
                    }
                }
            }
        }
    }
}

struct Task {
    cancel: CancellationToken,
    s3_uri: String,
    timeline_gate_guard: GateGuard,
    sender: tokio::sync::oneshot::Sender<TaskResult>,
    inner: Arc<Mutex<Inner>>,
}

impl Task {
    async fn run(self) {
        debug_assert_current_span_has_tenant_and_timeline_id();

        self.sender.send(TaskResult {
            gate_guard: self.timeline_gate_guard,
        });
    }
}
