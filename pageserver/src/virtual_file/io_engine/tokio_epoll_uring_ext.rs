//! Like [`::tokio_epoll_uring::thread_local_system()`], but with pageserver-specific
//! handling in case the instance can't launched.
//!
//! This is primarily necessary due to ENOMEM aka OutOfMemory errors during io_uring creation
//! on older kernels, such as some (but not all) older kernels in the Linux 5.10 series.
//! See <https://github.com/neondatabase/neon/issues/6373#issuecomment-1905814391> for more details.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::{error, info, info_span, warn, Instrument};
use utils::backoff::{DEFAULT_BASE_BACKOFF_SECONDS, DEFAULT_MAX_BACKOFF_SECONDS};

use tokio_epoll_uring::{System, SystemHandle};

use crate::virtual_file::on_fatal_io_error;

use crate::metrics::tokio_epoll_uring::{self as metrics, THREAD_LOCAL_METRICS_STORAGE};

#[derive(Clone)]
struct ThreadLocalState(Arc<ThreadLocalStateInner>);

struct ThreadLocalStateInner {
    cell: tokio::sync::OnceCell<SystemHandle<metrics::ThreadLocalMetrics>>,
    launch_attempts: AtomicU32,
    /// populated through fetch_add from [`THREAD_LOCAL_STATE_ID`]
    thread_local_state_id: u64,
}

impl Drop for ThreadLocalStateInner {
    fn drop(&mut self) {
        THREAD_LOCAL_METRICS_STORAGE.remove_system(self.thread_local_state_id);
    }
}

impl ThreadLocalState {
    pub fn new() -> Self {
        Self(Arc::new(ThreadLocalStateInner {
            cell: tokio::sync::OnceCell::default(),
            launch_attempts: AtomicU32::new(0),
            thread_local_state_id: THREAD_LOCAL_STATE_ID.fetch_add(1, Ordering::Relaxed),
        }))
    }

    pub fn make_id_string(&self) -> String {
        format!("{}", self.0.thread_local_state_id)
    }
}

static THREAD_LOCAL_STATE_ID: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static THREAD_LOCAL: ThreadLocalState = ThreadLocalState::new();
}

/// Panics if we cannot [`System::launch`].
pub async fn thread_local_system() -> Handle {
    let fake_cancel = CancellationToken::new();
    loop {
        let thread_local_state = THREAD_LOCAL.with(|arc| arc.clone());
        let inner = &thread_local_state.0;
        let get_or_init_res = inner
            .cell
            .get_or_try_init(|| async {
                let attempt_no = inner
                    .launch_attempts
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let span = info_span!("tokio_epoll_uring_ext::thread_local_system", thread_local=%thread_local_state.make_id_string(), %attempt_no);
                async {
                    // Rate-limit retries per thread-local.
                    // NB: doesn't yield to executor at attempt_no=0.
                    utils::backoff::exponential_backoff(
                        attempt_no,
                        DEFAULT_BASE_BACKOFF_SECONDS,
                        DEFAULT_MAX_BACKOFF_SECONDS,
                        &fake_cancel,
                    )
                    .await;
                    let per_system_metrics = metrics::THREAD_LOCAL_METRICS_STORAGE.register_system(inner.thread_local_state_id);
                    let res = System::launch_with_metrics(per_system_metrics)
                    // this might move us to another executor thread => loop outside the get_or_try_init, not inside it
                    .await;
                    match res {
                        Ok(system) => {
                            info!("successfully launched system");
                            metrics::THREAD_LOCAL_LAUNCH_SUCCESSES.inc();
                            Ok(system)
                        }
                        Err(tokio_epoll_uring::LaunchResult::IoUringBuild(e)) if e.kind() == std::io::ErrorKind::OutOfMemory => {
                            warn!("not enough locked memory to tokio-epoll-uring, will retry");
                            info_span!("stats").in_scope(|| {
                                emit_launch_failure_process_stats();
                            });
                            metrics::THREAD_LOCAL_LAUNCH_FAILURES.inc();
                            metrics::THREAD_LOCAL_METRICS_STORAGE.remove_system(inner.thread_local_state_id);
                            Err(())
                        }
                        // abort the process instead of panicking because pageserver usually becomes half-broken if we panic somewhere.
                        // This is equivalent to a fatal IO error.
                        Err(ref e @ tokio_epoll_uring::LaunchResult::IoUringBuild(ref inner)) => {
                            error!(error=%e, "failed to launch thread-local tokio-epoll-uring, this should not happen, aborting process");
                            info_span!("stats").in_scope(|| {
                                emit_launch_failure_process_stats();
                            });
                            on_fatal_io_error(inner, "launch thread-local tokio-epoll-uring");
                        },
                    }
                }
                .instrument(span)
                .await
            })
            .await;
        if get_or_init_res.is_ok() {
            return Handle(thread_local_state);
        }
    }
}

fn emit_launch_failure_process_stats() {
    // tokio-epoll-uring stats
    // vmlck + rlimit
    // number of threads
    // rss / system memory usage generally

    let tokio_epoll_uring::metrics::GlobalMetrics {
        systems_created,
        systems_destroyed,
    } = tokio_epoll_uring::metrics::global();
    info!(systems_created, systems_destroyed, "tokio-epoll-uring");

    match procfs::process::Process::myself() {
        Ok(myself) => {
            match myself.limits() {
                Ok(limits) => {
                    info!(?limits.max_locked_memory, "/proc/self/limits");
                }
                Err(error) => {
                    info!(%error, "no limit stats due to error");
                }
            }

            match myself.status() {
                Ok(status) => {
                    let procfs::process::Status {
                        vmsize,
                        vmlck,
                        vmpin,
                        vmrss,
                        rssanon,
                        rssfile,
                        rssshmem,
                        vmdata,
                        vmstk,
                        vmexe,
                        vmlib,
                        vmpte,
                        threads,
                        ..
                    } = status;
                    info!(
                        vmsize,
                        vmlck,
                        vmpin,
                        vmrss,
                        rssanon,
                        rssfile,
                        rssshmem,
                        vmdata,
                        vmstk,
                        vmexe,
                        vmlib,
                        vmpte,
                        threads,
                        "/proc/self/status"
                    );
                }
                Err(error) => {
                    info!(%error, "no status status due to error");
                }
            }
        }
        Err(error) => {
            info!(%error, "no process stats due to error");
        }
    };
}

#[derive(Clone)]
pub struct Handle(ThreadLocalState);

impl std::ops::Deref for Handle {
    type Target = SystemHandle<metrics::ThreadLocalMetrics>;

    fn deref(&self) -> &Self::Target {
        self.0
             .0
            .cell
            .get()
            .expect("must be already initialized when using this")
    }
}
