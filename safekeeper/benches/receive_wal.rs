//! WAL ingestion benchmarks.

use std::time::Instant;

use camino_tempfile::Utf8TempDir;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use futures::future::BoxFuture;
use postgres_ffi::v17::xlog_utils::WalGenerator;
use safekeeper::metrics::WalStorageMetrics;
use safekeeper::safekeeper::{
    AppendRequest, AppendRequestHeader, ProposerAcceptorMessage, ProposerElected, SafeKeeper,
    TermHistory,
};
use safekeeper::state::{TimelinePersistentState, TimelineState};
use safekeeper::{control_file, wal_storage, SafeKeeperConf};
use utils::id::{NodeId, TenantId, TenantTimelineId, TimelineId};
use utils::lsn::Lsn;

const NODE_ID: NodeId = NodeId(1);
const TENANT_ID: TenantId = TenantId::from_array([1; 16]);
const TIMELINE_ID: TimelineId = TimelineId::from_array([1; 16]);
const TTID: TenantTimelineId = TenantTimelineId::new(TENANT_ID, TIMELINE_ID);

// Register benchmarks with Criterion.
criterion_group!(benches, bench_process_msg, wal_acceptor);
criterion_main!(benches);

/// A benchmarking environment. Uses a tempdir for storage, removed on drop.
struct Env {
    /// TODO: we should add variants with in-memory storage, to measure non-IO costs. This would be
    /// easier if SafeKeeper used trait objects for storage rather than generics.
    safekeeper: SafeKeeper<control_file::FileStorage, wal_storage::PhysicalStorage>,
    /// Benchmark directory. Deleted when dropped.
    #[allow(unused)]
    tempdir: Utf8TempDir,
}

impl Env {
    /// Creates a new benchmarking environment in a temporary directory. fsync controls whether to
    /// enable fsyncing. Panics on errors.
    async fn setup(fsync: bool) -> Self {
        let tempdir = camino_tempfile::tempdir().unwrap();

        let mut conf = SafeKeeperConf::for_tests();
        conf.my_id = NODE_ID;
        conf.no_sync = !fsync;
        conf.workdir = tempdir.path().to_path_buf();

        let mut pstate = TimelinePersistentState::empty();
        pstate.tenant_id = TENANT_ID;
        pstate.timeline_id = TIMELINE_ID;

        let wal =
            wal_storage::PhysicalStorage::new(&TTID, tempdir.path().to_path_buf(), &conf, &pstate)
                .unwrap();
        let ctrl =
            control_file::FileStorage::create_new(tempdir.path().to_path_buf(), &conf, pstate)
                .await
                .unwrap();
        let state = TimelineState::new(ctrl);
        let mut safekeeper = SafeKeeper::new(state, wal, NODE_ID).unwrap();

        // Emulate an initial election.
        safekeeper
            .process_msg(&ProposerAcceptorMessage::Elected(ProposerElected {
                term: 1,
                start_streaming_at: Lsn(0),
                term_history: TermHistory(vec![(1, Lsn(0)).into()]),
                timeline_start_lsn: Lsn(0),
            }))
            .await
            .expect("election failed");

        Self {
            tempdir,
            safekeeper,
        }
    }
}

/// Benchmarks SafeKeeper::process_msg for AppendRequest.
fn bench_process_msg(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("process_msg", |b| {
        let mut env = runtime.block_on(Env::setup(false));
        let mut walgen = WalGenerator::new();
        let (mut begin_lsn, mut end_lsn) = (Lsn(0), Lsn(0));

        b.iter_batched_ref(
            // Pre-construct WAL records and requests, so it doesn't skew the benchmark.
            || {
                let wal_data = walgen.next().unwrap();
                begin_lsn = end_lsn;
                end_lsn += wal_data.len() as u64;
                ProposerAcceptorMessage::AppendRequest(AppendRequest {
                    h: AppendRequestHeader {
                        term: 1,
                        term_start_lsn: Lsn(0),
                        begin_lsn,
                        end_lsn,
                        commit_lsn: Lsn(0),
                        truncate_lsn: Lsn(0),
                        proposer_uuid: [0; 16],
                    },
                    wal_data,
                })
            },
            // Benchmark message processing (time per message).
            |msg| {
                runtime
                    .block_on(env.safekeeper.process_msg(msg))
                    .expect("message failed")
            },
            BatchSize::SmallInput,
        )
    });
}

/// Benchmarks WalAcceptor.
///
/// TODO: it is currently only possible to use a timeline with on-disk storage, because StateSK only
/// supports the Safekeeper variant <control_file::FileStorage, wal_storage::PhysicalStorage>. Make
/// Safekeeper use trait objects for storage and add a variant with in-memory storage.
///
/// TODO: WalAcceptor doesn't actually need a full timeline, only Safekeeper::process_msg().
/// Consider decoupling them to simplify setup of benchmarks and tests.
fn wal_acceptor(_c: &mut Criterion) {}

/// Noop WAL storage. Doesn't actually store or process anything, only tracks metadata.
#[allow(dead_code)]
struct NoopWAL {
    write_lsn: Lsn,
    flush_lsn: Lsn,
}

impl NoopWAL {
    #[allow(dead_code)]
    fn new() -> Self {
        Self {
            write_lsn: Lsn(0),
            flush_lsn: Lsn(0),
        }
    }
}

impl wal_storage::Storage for NoopWAL {
    fn write_lsn(&self) -> Lsn {
        self.write_lsn
    }

    fn flush_lsn(&self) -> Lsn {
        self.flush_lsn
    }

    async fn initialize_first_segment(&mut self, _init_lsn: Lsn) -> anyhow::Result<()> {
        Ok(())
    }

    async fn write_wal(&mut self, startpos: Lsn, buf: &[u8]) -> anyhow::Result<()> {
        assert_eq!(
            startpos, self.write_lsn,
            "startpos does not match write_lsn"
        );
        self.write_lsn += buf.len() as u64;
        Ok(())
    }

    async fn flush_wal(&mut self) -> anyhow::Result<()> {
        assert!(self.write_lsn >= self.flush_lsn);
        // Just fudge the flush LSN instead of detecting WAL record boundaries.
        self.flush_lsn = self.write_lsn;
        Ok(())
    }

    async fn truncate_wal(&mut self, end_pos: Lsn) -> anyhow::Result<()> {
        assert!(end_pos <= self.write_lsn);
        self.write_lsn = end_pos;
        self.flush_lsn = end_pos;
        Ok(())
    }

    fn remove_up_to(
        &self,
        _segno_up_to: postgres_ffi::XLogSegNo,
    ) -> BoxFuture<'static, anyhow::Result<()>> {
        unimplemented!()
    }

    fn get_metrics(&self) -> WalStorageMetrics {
        unimplemented!()
    }
}

/// In-memory control file for timeline state.
#[allow(dead_code)]
struct MemoryControl {
    state: TimelinePersistentState,
    last_persist_at: Instant,
}

impl MemoryControl {
    #[allow(dead_code)]
    fn new() -> Self {
        let mut state = TimelinePersistentState::empty();
        state.tenant_id = TenantId::from_array([1; 16]);
        state.timeline_id = TimelineId::from_array([1; 16]);
        Self {
            state,
            last_persist_at: Instant::now(),
        }
    }
}

impl control_file::Storage for MemoryControl {
    async fn persist(&mut self, state: &TimelinePersistentState) -> anyhow::Result<()> {
        self.state = state.clone();
        self.last_persist_at = Instant::now();
        Ok(())
    }

    fn last_persist_at(&self) -> Instant {
        self.last_persist_at
    }
}

impl std::ops::Deref for MemoryControl {
    type Target = TimelinePersistentState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}
