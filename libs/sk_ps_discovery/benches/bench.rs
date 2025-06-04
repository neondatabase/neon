//! WAL ingestion benchmarks.

use std::time::Instant;

use criterion::Criterion;
use hex::FromHex;
use sk_ps_discovery::{
    AttachmentUpdate, RemoteConsistentLsnAdv, TenantShardAttachmentId, TimelineAttachmentId,
};
use utils::{
    generation::Generation,
    id::{TenantId, TenantTimelineId, TimelineId},
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

    // Simplified view: lots of tenants with one timeline each
    let n_tenants = 400_000;
    for t in 1..=n_tenants {
        let ps_id = NodeId(23);
        let tenant_id = TenantId::from_hex(format!("{t:x}")).unwrap();
        let timeline_id = TimelineId::generate();
        let tenant_shard_attachment_id = TenantShardAttachmentId {
            tenant_id,
            shard_id: ShardIndex::unsharded(),
            generation: Generation(0),
        };
        let timeline_attachment = TimelineAttachmentId {
            tenant_shard_attachment_id,
            timeline_id,
        };
        world.update_attachment(AttachmentUpdate {
            tenant_shard_attachment_id,
            action: sk_ps_discovery::AttachmentUpdateAction::Attach { ps_id },
        });
        world.handle_remote_consistent_lsn_advertisement(RemoteConsistentLsnAdv {
            remote_consistent_lsn: Lsn(23),
            attachment: timeline_attachment,
        });
        world.handle_commit_lsn_advancement(
            TenantTimelineId {
                tenant_id,
                timeline_id,
            },
            Lsn(42),
        );
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
