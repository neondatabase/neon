use async_trait;
use std::{
    collections::HashMap,
    marker::PhantomData,
    time::{Duration, Instant},
};

use pageserver_api::shard::TenantShardId;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use utils::completion::Barrier;

use super::{CommandRequest, CommandResponse};

/// Scheduling interval is the time between calls to JobGenerator::schedule.
/// When we schedule jobs, the job generator may provide a hint of its preferred
/// interval, which we will respect within these intervals.
const MAX_SCHEDULING_INTERVAL: Duration = Duration::from_secs(10);
const MIN_SCHEDULING_INTERVAL: Duration = Duration::from_secs(1);

#[derive(thiserror::Error, Debug)]
pub(super) enum YieldingLoopError {
    #[error("Cancelled")]
    Cancelled,
}

/// Helper for long synchronous loops, e.g. over all tenants in the system.  Periodically
/// yields to avoid blocking the executor, and after resuming checks the provided
/// cancellation token to drop out promptly on shutdown.
pub(super) async fn yielding_loop<I, T, F>(
    interval: usize,
    cancel: &CancellationToken,
    iter: I,
    mut visitor: F,
) -> Result<(), YieldingLoopError>
where
    I: Iterator<Item = T>,
    F: FnMut(T),
{
    for (i, item) in iter.enumerate() {
        visitor(item);

        if i + 1 % interval == 0 {
            tokio::task::yield_now().await;
            if cancel.is_cancelled() {
                return Err(YieldingLoopError::Cancelled);
            }
        }
    }

    Ok(())
}

/// Scheduling helper for background work across many tenants.
///
/// PE: a 'PEnding' type for job descriptors that are ready to run
/// PR: a 'Running' type for jobs that have been spawned
/// C : a 'Completion' type that spawned jobs will send when they finish
pub(super) struct TenantBackgroundJobs<G, PE, PR, C, CMD>
where
    C: TenantScoped,
    PE: TenantScoped,
    PR: HasBarrier,
    G: JobGenerator<PE, PR, C, CMD>,
{
    generator: G,

    /// Ready to run.  Will progress to `running` once concurrent limit is satisfied, or
    /// be removed on next scheduling pass.
    pending: std::collections::VecDeque<PE>,

    /// Tasks currently running in Self::tasks for these tenants.  Check this map
    /// before pushing more work into pending for the same tenant.
    running: HashMap<TenantShardId, PR>,

    tasks: JoinSet<()>,

    /// Channel for our child tasks to send results to: we use a channel for results rather than
    /// just getting task results via JoinSet because we need the channel's recv() "sleep until something
    /// is available" semantic, rather than JoinSet::join_next()'s "sleep until next thing is available _or_ I'm empty"
    /// behavior.
    task_result_tx: tokio::sync::mpsc::UnboundedSender<C>,
    task_result_rx: tokio::sync::mpsc::UnboundedReceiver<C>,

    concurrency: usize,

    /// How often we would like schedule_interval to be called.
    pub(super) scheduling_interval: Duration,

    _phantom: PhantomData<(PE, PR, C, CMD)>,
}

/// For types that logically belong to a particular tenant shard, and can
/// provide its ID on demand.
pub(super) trait TenantScoped {
    fn get_tenant_shard_id(&self) -> &TenantShardId;
}

/// For types that contain a Barrier that may be waited on
pub(super) trait HasBarrier {
    fn get_barrier(&self) -> Barrier;
}

pub(super) struct SchedulingResult<PE> {
    pub(super) jobs: Vec<PE>,
    /// The job generator would like to be called again this soon
    pub(super) want_interval: Option<Duration>,
}

#[async_trait::async_trait]
pub(crate) trait JobGenerator<PE, PR, C, CMD>
where
    C: TenantScoped,
    PE: TenantScoped,
    PR: HasBarrier,
{
    /// Called at each scheduling interval.  Return a list of jobs to run, most urgent first.
    ///
    /// This function may be expensive (e.g. walk all tenants), but should not do any I/O.
    /// Implementations should take care to yield the executor periodically if running
    /// very long loops.
    ///
    /// Yielding a job here does _not_ guarantee that it will run: if the queue of pending
    /// jobs is not drained by the next scheduling interval, pending jobs will be cleared
    /// and re-generated.
    async fn schedule(&mut self) -> SchedulingResult<PE>;

    /// Called when a pending job is ready to be run.
    /// //
    /// The spawn operation _must_ spawn a task.  The task spawned _must_ send
    /// its result to the provided result channel (including in error cases).
    /// TODO: refactor so that implemeter can't violate these invariants.
    fn spawn(
        &mut self,
        join_set: &mut JoinSet<()>,
        result_tx: tokio::sync::mpsc::UnboundedSender<C>,
        pending_job: PE,
    ) -> PR;

    /// Called when a job previously spawned with spawn() transmits its completion
    fn on_completion(&mut self, completion: C);

    /// Called when a command is received.  A job will be spawned immediately if the return
    /// value is Some, ignoring concurrency limits and the pending queue.
    fn on_command(&mut self, cmd: CMD) -> anyhow::Result<PE>;
}

impl<G, PE, PR, C, CMD> TenantBackgroundJobs<G, PE, PR, C, CMD>
where
    C: TenantScoped,
    PE: TenantScoped,
    PR: HasBarrier,
    G: JobGenerator<PE, PR, C, CMD>,
{
    pub(super) fn new(generator: G, concurrency: usize) -> Self {
        let (task_result_tx, task_result_rx) = tokio::sync::mpsc::unbounded_channel();

        Self {
            generator,
            pending: std::collections::VecDeque::new(),
            running: HashMap::new(),
            tasks: JoinSet::new(),
            task_result_rx,
            task_result_tx,
            concurrency,
            scheduling_interval: MAX_SCHEDULING_INTERVAL,
            _phantom: PhantomData,
        }
    }

    pub(super) async fn run(
        &mut self,
        mut command_queue: tokio::sync::mpsc::Receiver<CommandRequest<CMD>>,
        background_jobs_can_start: Barrier,
        cancel: CancellationToken,
    ) {
        tracing::info!("Waiting for background_jobs_can start...");
        background_jobs_can_start.wait().await;
        tracing::info!("background_jobs_can is ready, proceeding.");

        while !cancel.is_cancelled() {
            // Look for new work: this is relatively expensive because we have to go acquire the lock on
            // the tenant manager to retrieve tenants, and then iterate over them to figure out which ones
            // require an upload.
            self.schedule_iteration(&cancel).await;

            if cancel.is_cancelled() {
                return;
            }

            // Schedule some work, if concurrency limit permits it
            self.spawn_pending();

            // Between scheduling iterations, we will:
            //  - Drain any complete tasks and spawn pending tasks
            //  - Handle incoming administrative commands
            //  - Check our cancellation token
            let next_scheduling_iteration = Instant::now()
                .checked_add(self.scheduling_interval)
                .unwrap_or_else(|| {
                    tracing::warn!(
                        "Scheduling interval invalid ({}s), running immediately!",
                        self.scheduling_interval.as_secs_f64()
                    );
                    Instant::now()
                });
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        tracing::info!("joining tasks");
                        self.shutdown().await;
                        tracing::info!("terminating on cancellation token.");

                        break;
                    },
                    _ = tokio::time::sleep(next_scheduling_iteration.duration_since(Instant::now())) => {
                        tracing::debug!("woke for scheduling interval");
                        break;},
                    cmd = command_queue.recv() => {
                        tracing::debug!("woke for command queue");
                        let cmd = match cmd {
                            Some(c) =>c,
                            None => {
                                // SecondaryController was destroyed, and this has raced with
                                // our CancellationToken
                                tracing::info!("terminating on command queue destruction");
                                cancel.cancel();
                                break;
                            }
                        };

                        let CommandRequest{
                            response_tx,
                            payload
                        } = cmd;
                        self.handle_command(payload, response_tx);
                    },
                    _ = async {
                        let completion = self.process_next_completion().await;
                        self.generator.on_completion(completion);
                        if !cancel.is_cancelled() {
                            self.spawn_pending();
                        }
                     } => {}
                }
            }
        }
    }

    /// For all pending tenants that are elegible for execution, spawn their task.
    ///
    /// Caller provides the spawn operation, we track the resulting execution.
    ///
    /// The spawn operation _must_ spawn a task.  The task spawned _must_ send
    /// its result to the provided result channel (including in error cases).
    /// TODO: refactor so that caller can't violate these invariants.
    fn spawn_pending(&mut self) {
        while !self.pending.is_empty() && self.running.len() < self.concurrency {
            // unwrap: loop condition includes !is_empty()
            let pending = self.pending.pop_front().unwrap();
            let tenant_shard_id = *pending.get_tenant_shard_id();
            let in_progress =
                self.generator
                    .spawn(&mut self.tasks, self.task_result_tx.clone(), pending);

            self.running.insert(tenant_shard_id, in_progress);
        }
    }

    /// For administrative commands: skip the pending queue, ignore concurrency limits
    fn spawn_now(&mut self, job: PE) -> &PR {
        let tenant_shard_id = *job.get_tenant_shard_id();
        let in_progress = self
            .generator
            .spawn(&mut self.tasks, self.task_result_tx.clone(), job);

        self.running.insert(tenant_shard_id, in_progress);
        self.running
            .get(&tenant_shard_id)
            .expect("We just inserted this")
    }

    /// Wait until the next task completes, and handle its completion
    ///
    /// Cancellation: this method is cancel-safe.
    async fn process_next_completion(&mut self) -> C {
        match self.task_result_rx.recv().await {
            Some(r) => {
                self.running.remove(r.get_tenant_shard_id());
                r
            }
            None => {
                unreachable!("Result sender is stored on Self");
            }
        }
    }

    /// Convert the command into a pending job, spawn it, and when the spawned
    /// job completes, send the result down `response_tx`.
    fn handle_command(
        &mut self,
        cmd: CMD,
        response_tx: tokio::sync::oneshot::Sender<CommandResponse>,
    ) {
        let job = match self.generator.on_command(cmd) {
            Ok(j) => j,
            Err(e) => {
                response_tx.send(CommandResponse { result: Err(e) }).ok();
                return;
            }
        };

        let tenant_shard_id = job.get_tenant_shard_id();
        let barrier = if let Some(barrier) = self.get_running(tenant_shard_id) {
            barrier
        } else {
            let running = self.spawn_now(job);
            running.get_barrier().clone()
        };

        // This task does no I/O: it only listens for a barrier's completion and then
        // sends to the command response channel.  It is therefore safe to spawn this without
        // any gates/task_mgr hooks.
        tokio::task::spawn(async move {
            barrier.wait().await;

            response_tx.send(CommandResponse { result: Ok(()) }).ok();
        });
    }

    fn get_running(&self, tenant_shard_id: &TenantShardId) -> Option<Barrier> {
        self.running.get(tenant_shard_id).map(|r| r.get_barrier())
    }

    /// Periodic execution phase: inspect all attached tenants and schedule any work they require.
    ///
    /// The type in `tenants` should be a tenant-like structure, e.g. [`crate::tenant::Tenant`] or [`crate::tenant::secondary::SecondaryTenant`]
    ///
    /// This function resets the pending list: it is assumed that the caller may change their mind about
    /// which tenants need work between calls to schedule_iteration.
    async fn schedule_iteration(&mut self, cancel: &CancellationToken) {
        let SchedulingResult {
            jobs,
            want_interval,
        } = self.generator.schedule().await;

        // Adjust interval based on feedback from the job generator
        if let Some(want_interval) = want_interval {
            // Calculation uses second granularity: this scheduler is not intended for high frequency tasks
            self.scheduling_interval = Duration::from_secs(std::cmp::min(
                std::cmp::max(MIN_SCHEDULING_INTERVAL.as_secs(), want_interval.as_secs()),
                MAX_SCHEDULING_INTERVAL.as_secs(),
            ));
        }

        // The priority order of previously scheduled work may be invalidated by current state: drop
        // all pending work (it will be re-scheduled if still needed)
        self.pending.clear();

        // While iterating over the potentially-long list of tenants, we will periodically yield
        // to avoid blocking executor.
        yielding_loop(1000, cancel, jobs.into_iter(), |job| {
            // Skip tenants that already have a write in flight
            if !self.running.contains_key(job.get_tenant_shard_id()) {
                self.pending.push_back(job);
            }
        })
        .await
        .ok();
    }

    /// It is the callers responsibility to make sure that the tasks they scheduled
    /// respect an appropriate cancellation token, to shut down promptly.
    async fn shutdown(&mut self) {
        // We do not simply drop the JoinSet, in order to have an orderly shutdown without cancellation.
        while let Some(_r) = self.tasks.join_next().await {}
    }
}
