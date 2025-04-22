use std::env;
use std::num::NonZeroUsize;

use bytes::Bytes;
use camino::Utf8PathBuf;
use criterion::{Criterion, criterion_group, criterion_main};
use pageserver::config::PageServerConf;
use pageserver::context::{DownloadBehavior, RequestContext};
use pageserver::l0_flush::{L0FlushConfig, L0FlushGlobalState};
use pageserver::task_mgr::TaskKind;
use pageserver::tenant::storage_layer::InMemoryLayer;
use pageserver::{page_cache, virtual_file};
use pageserver_api::key::Key;
use pageserver_api::models::virtual_file::IoMode;
use pageserver_api::shard::TenantShardId;
use pageserver_api::value::Value;
use tokio_util::sync::CancellationToken;
use utils::bin_ser::BeSer;
use utils::id::{TenantId, TimelineId};
use wal_decoder::serialized_batch::SerializedValueBatch;

// A very cheap hash for generating non-sequential keys.
fn murmurhash32(mut h: u32) -> u32 {
    h ^= h >> 16;
    h = h.wrapping_mul(0x85ebca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2ae35);
    h ^= h >> 16;
    h
}

#[derive(serde::Serialize, Clone, Copy, Debug)]
enum KeyLayout {
    /// Sequential unique keys
    Sequential,
    /// Random unique keys
    Random,
    /// Random keys, but only use the bits from the mask of them
    RandomReuse(u32),
}

#[derive(serde::Serialize, Clone, Copy, Debug)]
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

    let ctx =
        RequestContext::new(TaskKind::DebugTool, DownloadBehavior::Error).with_scope_debug_tools();

    let gate = utils::sync::gate::Gate::default();
    let cancel = CancellationToken::new();

    let layer = InMemoryLayer::create(
        conf,
        timeline_id,
        tenant_shard_id,
        lsn,
        &gate,
        &cancel,
        &ctx,
    )
    .await?;

    let data = Value::Image(Bytes::from(vec![0u8; put_size]));
    let data_ser_size = data.serialized_size().unwrap() as usize;
    let ctx = RequestContext::new(
        pageserver::task_mgr::TaskKind::WalReceiverConnectionHandler,
        pageserver::context::DownloadBehavior::Download,
    );

    const BATCH_SIZE: usize = 16;
    let mut batch = Vec::new();

    for i in 0..put_count {
        lsn += put_size as u64;

        // Generate lots of keys within a single relation, which simulates the typical bulk ingest case: people
        // usually care the most about write performance when they're blasting a huge batch of data into a huge table.
        match key_layout {
            KeyLayout::Sequential => {
                // Use sequential order to illustrate the experience a user is likely to have
                // when ingesting bulk data.
                key.field6 = i as u32;
            }
            KeyLayout::Random => {
                // Use random-order keys to avoid giving a false advantage to data structures that are
                // faster when inserting on the end.
                key.field6 = murmurhash32(i as u32);
            }
            KeyLayout::RandomReuse(mask) => {
                // Use low bits only, to limit cardinality
                key.field6 = murmurhash32(i as u32) & mask;
            }
        }

        batch.push((key.to_compact(), lsn, data_ser_size, data.clone()));
        if batch.len() >= BATCH_SIZE {
            let this_batch = std::mem::take(&mut batch);
            let serialized = SerializedValueBatch::from_values(this_batch);
            layer.put_batch(serialized, &ctx).await?;
        }
    }
    if !batch.is_empty() {
        let this_batch = std::mem::take(&mut batch);
        let serialized = SerializedValueBatch::from_values(this_batch);
        layer.put_batch(serialized, &ctx).await?;
    }
    layer.freeze(lsn + 1).await;

    if matches!(write_delta, WriteDelta::Yes) {
        let l0_flush_state = L0FlushGlobalState::new(L0FlushConfig::Direct {
            max_concurrency: NonZeroUsize::new(1).unwrap(),
        });
        let (_desc, path) = layer
            .write_to_disk(&ctx, None, l0_flush_state.inner(), &gate, cancel.clone())
            .await?
            .unwrap();
        tokio::fs::remove_file(path).await?;
    }

    Ok(())
}

/// Wrapper to instantiate a tokio runtime
fn ingest_main(
    conf: &'static PageServerConf,
    io_mode: IoMode,
    put_size: usize,
    put_count: usize,
    key_layout: KeyLayout,
    write_delta: WriteDelta,
) {
    pageserver::virtual_file::set_io_mode(io_mode);

    let runtime = tokio::runtime::Builder::new_multi_thread()
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

/// Declare a series of benchmarks for the Pageserver's ingest write path.
///
/// This benchmark does not include WAL decode: it starts at InMemoryLayer::put_value, and ends either
/// at freezing the ephemeral layer, or writing the ephemeral layer out to an L0 (depending on whether WriteDelta is set).
///
/// Genuine disk I/O is used, so expect results to differ depending on storage.  However, when running on
/// a fast disk, CPU is the bottleneck at time of writing.
fn criterion_benchmark(c: &mut Criterion) {
    let temp_dir_parent: Utf8PathBuf = env::current_dir().unwrap().try_into().unwrap();
    let temp_dir = camino_tempfile::tempdir_in(temp_dir_parent).unwrap();
    eprintln!("Data directory: {}", temp_dir.path());

    let conf: &'static PageServerConf = Box::leak(Box::new(
        pageserver::config::PageServerConf::dummy_conf(temp_dir.path().to_path_buf()),
    ));
    virtual_file::init(
        16384,
        {
            #[cfg(not(target_os = "linux"))]
            const _: () = {
                panic!(
                    "This benchmark does I/O and can only give a representative result on Linux"
                );
            };
            #[cfg(target_os = "linux")]
            pageserver_api::models::virtual_file::IoEngineKind::TokioEpollUring
        },
        // immaterial, each `ingest_main` invocation below overrides this
        conf.virtual_file_io_mode,
        // without actually doing syncs, buffered writes have an unfair advantage over direct IO writes
        virtual_file::SyncMode::Sync,
    );
    page_cache::init(conf.page_cache_size);

    #[derive(serde::Serialize)]
    struct ExplodedParameters {
        io_mode: IoMode,
        volume_mib: usize,
        key_size: usize,
        key_layout: KeyLayout,
        write_delta: WriteDelta,
    }
    #[derive(Clone)]
    struct HandPickedParameters {
        volume_mib: usize,
        key_size: usize,
        key_layout: KeyLayout,
        write_delta: WriteDelta,
    }
    let expect = vec![
        // Small values (100b) tests
        HandPickedParameters {
            volume_mib: 128,
            key_size: 100,
            key_layout: KeyLayout::Sequential,
            write_delta: WriteDelta::Yes,
        },
        HandPickedParameters {
            volume_mib: 128,
            key_size: 100,
            key_layout: KeyLayout::Random,
            write_delta: WriteDelta::Yes,
        },
        HandPickedParameters {
            volume_mib: 128,
            key_size: 100,
            key_layout: KeyLayout::RandomReuse(0x3ff),
            write_delta: WriteDelta::Yes,
        },
        HandPickedParameters {
            volume_mib: 128,
            key_size: 100,
            key_layout: KeyLayout::Sequential,
            write_delta: WriteDelta::No,
        },
        // Large values (8k) tests
        HandPickedParameters {
            volume_mib: 128,
            key_size: 8192,
            key_layout: KeyLayout::Sequential,
            write_delta: WriteDelta::Yes,
        },
        HandPickedParameters {
            volume_mib: 128,
            key_size: 8192,
            key_layout: KeyLayout::Sequential,
            write_delta: WriteDelta::No,
        },
    ];
    let exploded_parameters = {
        let mut out = Vec::new();
        for io_mode in [IoMode::Buffered, IoMode::Direct] {
            for param in expect.clone() {
                let HandPickedParameters {
                    volume_mib,
                    key_size,
                    key_layout,
                    write_delta,
                } = param;
                out.push(ExplodedParameters {
                    io_mode,
                    volume_mib,
                    key_size,
                    key_layout,
                    write_delta,
                });
            }
        }
        out
    };
    impl ExplodedParameters {
        fn benchmark_id(&self) -> String {
            let ExplodedParameters {
                io_mode,
                volume_mib,
                key_size,
                key_layout,
                write_delta,
            } = self;
            format!(
                "io_mode={io_mode:?} volume_mib={volume_mib:?} key_size_bytes={key_size:?} key_layout={key_layout:?} write_delta={write_delta:?}"
            )
        }
    }
    let mut group = c.benchmark_group("ingest");
    for params in exploded_parameters {
        let id = params.benchmark_id();
        let ExplodedParameters {
            io_mode,
            volume_mib,
            key_size,
            key_layout,
            write_delta,
        } = params;
        let put_count = volume_mib * 1024 * 1024 / key_size;
        group.throughput(criterion::Throughput::Bytes((key_size * put_count) as u64));
        group.sample_size(10);
        group.bench_function(id, |b| {
            b.iter(|| ingest_main(conf, io_mode, key_size, put_count, key_layout, write_delta))
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

/*
cargo bench --bench bench_ingest

im4gn.2xlarge:

ingest/io_mode=Buffered volume_mib=128 key_size_bytes=100 key_layout=Sequential write_delta=Yes
                        time:   [1.8491 s 1.8540 s 1.8592 s]
                        thrpt:  [68.847 MiB/s 69.039 MiB/s 69.222 MiB/s]
ingest/io_mode=Buffered volume_mib=128 key_size_bytes=100 key_layout=Random write_delta=Yes
                        time:   [2.6976 s 2.7123 s 2.7286 s]
                        thrpt:  [46.911 MiB/s 47.193 MiB/s 47.450 MiB/s]
ingest/io_mode=Buffered volume_mib=128 key_size_bytes=100 key_layout=RandomReuse(1023) write_delta=Y...
                        time:   [1.7433 s 1.7510 s 1.7600 s]
                        thrpt:  [72.729 MiB/s 73.099 MiB/s 73.423 MiB/s]
ingest/io_mode=Buffered volume_mib=128 key_size_bytes=100 key_layout=Sequential write_delta=No
                        time:   [499.63 ms 500.07 ms 500.46 ms]
                        thrpt:  [255.77 MiB/s 255.96 MiB/s 256.19 MiB/s]
ingest/io_mode=Buffered volume_mib=128 key_size_bytes=8192 key_layout=Sequential write_delta=Yes
                        time:   [456.97 ms 459.61 ms 461.92 ms]
                        thrpt:  [277.11 MiB/s 278.50 MiB/s 280.11 MiB/s]
ingest/io_mode=Buffered volume_mib=128 key_size_bytes=8192 key_layout=Sequential write_delta=No
                        time:   [158.82 ms 159.16 ms 159.56 ms]
                        thrpt:  [802.22 MiB/s 804.24 MiB/s 805.93 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=100 key_layout=Sequential write_delta=Yes
                        time:   [1.8856 s 1.8997 s 1.9179 s]
                        thrpt:  [66.740 MiB/s 67.380 MiB/s 67.882 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=100 key_layout=Random write_delta=Yes
                        time:   [2.7468 s 2.7625 s 2.7785 s]
                        thrpt:  [46.068 MiB/s 46.335 MiB/s 46.600 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=100 key_layout=RandomReuse(1023) write_delta=Yes
                        time:   [1.7689 s 1.7726 s 1.7767 s]
                        thrpt:  [72.045 MiB/s 72.208 MiB/s 72.363 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=100 key_layout=Sequential write_delta=No
                        time:   [497.64 ms 498.60 ms 499.67 ms]
                        thrpt:  [256.17 MiB/s 256.72 MiB/s 257.21 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=8192 key_layout=Sequential write_delta=Yes
                        time:   [493.72 ms 505.07 ms 518.03 ms]
                        thrpt:  [247.09 MiB/s 253.43 MiB/s 259.26 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=8192 key_layout=Sequential write_delta=No
                        time:   [267.76 ms 267.85 ms 267.96 ms]
                        thrpt:  [477.69 MiB/s 477.88 MiB/s 478.03 MiB/s]

Hetzner AX102:

ingest/io_mode=Buffered volume_mib=128 key_size_bytes=100 key_layout=Sequential write_delta=Yes
                        time:   [1.0683 s 1.1006 s 1.1386 s]
                        thrpt:  [112.42 MiB/s 116.30 MiB/s 119.82 MiB/s]
ingest/io_mode=Buffered volume_mib=128 key_size_bytes=100 key_layout=Random write_delta=Yes
                        time:   [1.5719 s 1.6012 s 1.6228 s]
                        thrpt:  [78.877 MiB/s 79.938 MiB/s 81.430 MiB/s]
ingest/io_mode=Buffered volume_mib=128 key_size_bytes=100 key_layout=RandomReuse(1023) write_delta=Y...
                        time:   [1.1095 s 1.1331 s 1.1580 s]
                        thrpt:  [110.53 MiB/s 112.97 MiB/s 115.37 MiB/s]
ingest/io_mode=Buffered volume_mib=128 key_size_bytes=100 key_layout=Sequential write_delta=No
                        time:   [303.20 ms 307.83 ms 311.90 ms]
                        thrpt:  [410.39 MiB/s 415.81 MiB/s 422.16 MiB/s]
ingest/io_mode=Buffered volume_mib=128 key_size_bytes=8192 key_layout=Sequential write_delta=Yes
                        time:   [406.34 ms 429.37 ms 451.63 ms]
                        thrpt:  [283.42 MiB/s 298.11 MiB/s 315.00 MiB/s]
ingest/io_mode=Buffered volume_mib=128 key_size_bytes=8192 key_layout=Sequential write_delta=No
                        time:   [134.01 ms 135.78 ms 137.48 ms]
                        thrpt:  [931.03 MiB/s 942.68 MiB/s 955.12 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=100 key_layout=Sequential write_delta=Yes
                        time:   [1.0406 s 1.0580 s 1.0772 s]
                        thrpt:  [118.83 MiB/s 120.98 MiB/s 123.00 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=100 key_layout=Random write_delta=Yes
                        time:   [1.5059 s 1.5339 s 1.5625 s]
                        thrpt:  [81.920 MiB/s 83.448 MiB/s 84.999 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=100 key_layout=RandomReuse(1023) write_delta=Yes
                        time:   [1.0714 s 1.0934 s 1.1161 s]
                        thrpt:  [114.69 MiB/s 117.06 MiB/s 119.47 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=100 key_layout=Sequential write_delta=No
                        time:   [262.68 ms 265.14 ms 267.71 ms]
                        thrpt:  [478.13 MiB/s 482.76 MiB/s 487.29 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=8192 key_layout=Sequential write_delta=Yes
                        time:   [375.19 ms 393.80 ms 411.40 ms]
                        thrpt:  [311.14 MiB/s 325.04 MiB/s 341.16 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=8192 key_layout=Sequential write_delta=No
                        time:   [123.02 ms 123.85 ms 124.66 ms]
                        thrpt:  [1.0027 GiB/s 1.0093 GiB/s 1.0161 GiB/s]
*/
