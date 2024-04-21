use std::env;

use bytes::Bytes;
use camino::Utf8PathBuf;
use criterion::{criterion_group, criterion_main, Criterion};
use pageserver::{
    config::PageServerConf,
    context::RequestContext,
    page_cache,
    repository::Value,
    tenant::storage_layer::InMemoryLayer,
    virtual_file::{self, api::IoEngineKind},
};
use pageserver_api::{key::Key, shard::TenantShardId};
use utils::{
    bin_ser::BeSer,
    id::{TenantId, TimelineId},
};

// A very cheap hash for generating non-sequential keys.
fn murmurhash32(mut h: u32) -> u32 {
    h ^= h >> 16;
    h = h.wrapping_mul(0x85ebca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2ae35);
    h ^= h >> 16;
    h
}

enum KeyLayout {
    /// Sequential unique keys
    Sequential,
    /// Random unique keys
    Random,
    /// Random keys, but only use the bits from the mask of them
    RandomReuse(u32),
}

enum WriteDelta {
    Yes,
    No,
}

async fn ingest(
    conf: &'static PageServerConf,
    put_size: usize,
    put_count: usize,
    key_layout: KeyLayout,
    write_delta: WriteDelta,
) -> anyhow::Result<()> {
    let mut lsn = utils::lsn::Lsn(1000);
    let mut key = Key::from_i128(0x0);

    let timeline_id = TimelineId::generate();
    let tenant_id = TenantId::generate();
    let tenant_shard_id = TenantShardId::unsharded(tenant_id);

    tokio::fs::create_dir_all(conf.timeline_path(&tenant_shard_id, &timeline_id)).await?;

    let layer = InMemoryLayer::create(conf, timeline_id, tenant_shard_id, lsn).await?;

    let data = Value::Image(Bytes::from(vec![0u8; put_size])).ser()?;
    let ctx = RequestContext::new(
        pageserver::task_mgr::TaskKind::WalReceiverConnectionHandler,
        pageserver::context::DownloadBehavior::Download,
    );

    for i in 0..put_count {
        lsn += put_size as u64;

        match key_layout {
            KeyLayout::Sequential => {
                // Use sequential order to illustrate the experience a user is likely to have
                // when ingesting bulk data.
                key.field3 = i as u32;
            }
            KeyLayout::Random => {
                // Use random-order keys to avoid giving a false advantage to data structures that are
                // faster when inserting on the end.
                key.field3 = murmurhash32(i as u32);
            }
            KeyLayout::RandomReuse(mask) => {
                // Use low bits only, to limit cardinality
                key.field3 = murmurhash32(i as u32) & mask;
            }
        }

        layer.put_value(key, lsn, &data, &ctx).await?;
    }
    layer.freeze(lsn + 1).await;

    if matches!(write_delta, WriteDelta::Yes) {
        let (_desc, path) = layer.write_to_disk(&ctx).await?;
        tokio::fs::remove_file(path).await?;
    }

    Ok(())
}

/// Wrapper to instantiate a tokio runtime
fn ingest_main(
    conf: &'static PageServerConf,
    put_size: usize,
    put_count: usize,
    key_layout: KeyLayout,
    write_delta: WriteDelta,
) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async move {
        let r = ingest(conf, put_size, put_count, key_layout, write_delta).await;
        if let Err(e) = r {
            panic!("{e:?}");
        }
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let repo_dir: Utf8PathBuf = env::current_dir().unwrap().try_into().unwrap();
    let repo_dir = repo_dir.join("bench_data");
    eprintln!("Data directory: {repo_dir}");

    let conf: &'static PageServerConf = Box::leak(Box::new(
        pageserver::config::PageServerConf::dummy_conf(repo_dir),
    ));
    virtual_file::init(16384, IoEngineKind::TokioEpollUring);
    page_cache::init(conf.page_cache_size);

    {
        let mut group = c.benchmark_group("ingest-small-values");
        let put_size = 100usize;
        let put_count = 128 * 1024 * 1024 / put_size;
        group.throughput(criterion::Throughput::Bytes((put_size * put_count) as u64));
        group.sample_size(10);
        group.bench_function("ingest 128MB/100b seq", |b| {
            b.iter(|| {
                ingest_main(
                    conf,
                    put_size,
                    put_count,
                    KeyLayout::Sequential,
                    WriteDelta::Yes,
                )
            })
        });
        group.bench_function("ingest 128MB/100b rand", |b| {
            b.iter(|| {
                ingest_main(
                    conf,
                    put_size,
                    put_count,
                    KeyLayout::Random,
                    WriteDelta::Yes,
                )
            })
        });
        group.bench_function("ingest 128MB/100b rand-1024keys", |b| {
            b.iter(|| {
                ingest_main(
                    conf,
                    put_size,
                    put_count,
                    KeyLayout::RandomReuse(0x3ff),
                    WriteDelta::Yes,
                )
            })
        });
        group.bench_function("ingest 128MB/100b seq, no delta", |b| {
            b.iter(|| {
                ingest_main(
                    conf,
                    put_size,
                    put_count,
                    KeyLayout::Sequential,
                    WriteDelta::No,
                )
            })
        });
    }

    {
        let mut group = c.benchmark_group("ingest-big-values");
        let put_size = 8192usize;
        let put_count = 128 * 1024 * 1024 / put_size;
        group.throughput(criterion::Throughput::Bytes((put_size * put_count) as u64));
        group.sample_size(10);
        group.bench_function("ingest 128MB/8k seq", |b| {
            b.iter(|| {
                ingest_main(
                    conf,
                    put_size,
                    put_count,
                    KeyLayout::Sequential,
                    WriteDelta::Yes,
                )
            })
        });
        group.bench_function("ingest 128MB/8k seq, no delta", |b| {
            b.iter(|| {
                ingest_main(
                    conf,
                    put_size,
                    put_count,
                    KeyLayout::Sequential,
                    WriteDelta::No,
                )
            })
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
