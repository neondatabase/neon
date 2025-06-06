//! WAL ingestion benchmarks.

use std::time::Instant;

use criterion::{Criterion, criterion_group, criterion_main};
use pprof::criterion::{Output, PProfProfiler};
use sk_ps_discovery::{
    AttachmentUpdate, RemoteConsistentLsnAdv, TenantShardAttachmentId, TimelineAttachmentId,
};
use utils::{
    generation::Generation,
    id::{NodeId, TenantId, TenantTimelineId, TimelineId},
    lsn::Lsn,
    shard::ShardIndex,
};

/// Use jemalloc and enable profiling, to mirror bin/safekeeper.rs.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[allow(non_upper_case_globals)]
#[unsafe(export_name = "malloc_conf")]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:21\0";

// Register benchmarks with Criterion.
criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_simple,
);
criterion_main!(benches);

fn bench_simple(c: &mut Criterion) {
    let mut g = c.benchmark_group("simple");

    // setup
    let mut world = sk_ps_discovery::World::default();

    // Simplified view: lots of unsharded tenants with one timeline each
    let n_pageservers = 20;
    let n_tenant_shards_per_pageserver = 2000;
    for ps_id in 1..=n_pageservers {
        for _ in ..n_tenant_shards_per_pageserver {
            let tenant_id = TenantId::generate();
            let timeline_id = TimelineId::generate();
            for generation in 10..=11 {
                let tenant_shard_attachment_id = TenantShardAttachmentId {
                    tenant_id,
                    shard_id: ShardIndex::unsharded(),
                    generation: Generation::Valid(generation),
                };
                let timeline_attachment = TimelineAttachmentId {
                    tenant_timeline_id: TenantTimelineId {
                        tenant_id,
                        timeline_id,
                    },
                    shard_id: ShardIndex::unsharded(),
                    generation: Generation::Valid(generation),
                };
                world.update_attachment(AttachmentUpdate {
                    tenant_shard_attachment_id,
                    action: sk_ps_discovery::AttachmentUpdateAction::Attach {
                        ps_id: NodeId(ps_id),
                    },
                });
                world.handle_remote_consistent_lsn_advertisement(RemoteConsistentLsnAdv {
                    remote_consistent_lsn: Lsn(23),
                    attachment: timeline_attachment,
                });
            }
            world.handle_commit_lsn_advancement(
                TenantTimelineId {
                    tenant_id,
                    timeline_id,
                },
                Lsn(42),
            );
        }
    }

    // setup done
    let world = world;
    g.bench_function("get_commit_lsn_advertisements", |bencher| {
        bencher.iter_custom(|iters| {
            let started = Instant::now();

            for _ in 0..iters {
                criterion::black_box(world.get_commit_lsn_advertisements());
            }

            let elapsed = started.elapsed();
            elapsed
        });
    });

    g.finish();
}
