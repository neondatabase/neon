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
use strum::IntoEnumIterator;
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
        virtual_file::io_engine_for_bench(),
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
        for io_mode in IoMode::iter() {
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
                        time:   [1.2901 s 1.2943 s 1.2991 s]
                        thrpt:  [98.533 MiB/s 98.892 MiB/s 99.220 MiB/s]
ingest/io_mode=Buffered volume_mib=128 key_size_bytes=100 key_layout=Random write_delta=Yes
                        time:   [2.1387 s 2.1623 s 2.1845 s]
                        thrpt:  [58.595 MiB/s 59.197 MiB/s 59.851 MiB/s]
ingest/io_mode=Buffered volume_mib=128 key_size_bytes=100 key_layout=RandomReuse(1023) write_delta=Y...
                        time:   [1.2036 s 1.2074 s 1.2122 s]
                        thrpt:  [105.60 MiB/s 106.01 MiB/s 106.35 MiB/s]
ingest/io_mode=Buffered volume_mib=128 key_size_bytes=100 key_layout=Sequential write_delta=No
                        time:   [520.55 ms 521.46 ms 522.57 ms]
                        thrpt:  [244.94 MiB/s 245.47 MiB/s 245.89 MiB/s]
ingest/io_mode=Buffered volume_mib=128 key_size_bytes=8192 key_layout=Sequential write_delta=Yes
                        time:   [440.33 ms 442.24 ms 444.10 ms]
                        thrpt:  [288.22 MiB/s 289.43 MiB/s 290.69 MiB/s]
ingest/io_mode=Buffered volume_mib=128 key_size_bytes=8192 key_layout=Sequential write_delta=No
                        time:   [168.78 ms 169.42 ms 170.18 ms]
                        thrpt:  [752.16 MiB/s 755.52 MiB/s 758.40 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=100 key_layout=Sequential write_delta=Yes
                        time:   [1.2978 s 1.3094 s 1.3227 s]
                        thrpt:  [96.775 MiB/s 97.758 MiB/s 98.632 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=100 key_layout=Random write_delta=Yes
                        time:   [2.1976 s 2.2067 s 2.2154 s]
                        thrpt:  [57.777 MiB/s 58.006 MiB/s 58.245 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=100 key_layout=RandomReuse(1023) write_delta=Yes
                        time:   [1.2103 s 1.2160 s 1.2233 s]
                        thrpt:  [104.64 MiB/s 105.26 MiB/s 105.76 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=100 key_layout=Sequential write_delta=No
                        time:   [525.05 ms 526.37 ms 527.79 ms]
                        thrpt:  [242.52 MiB/s 243.17 MiB/s 243.79 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=8192 key_layout=Sequential write_delta=Yes
                        time:   [443.06 ms 444.88 ms 447.15 ms]
                        thrpt:  [286.26 MiB/s 287.72 MiB/s 288.90 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=8192 key_layout=Sequential write_delta=No
                        time:   [169.40 ms 169.80 ms 170.17 ms]
                        thrpt:  [752.21 MiB/s 753.81 MiB/s 755.60 MiB/s]
ingest/io_mode=DirectRw volume_mib=128 key_size_bytes=100 key_layout=Sequential write_delta=Yes
                        time:   [1.2844 s 1.2915 s 1.2990 s]
                        thrpt:  [98.536 MiB/s 99.112 MiB/s 99.657 MiB/s]
ingest/io_mode=DirectRw volume_mib=128 key_size_bytes=100 key_layout=Random write_delta=Yes
                        time:   [2.1431 s 2.1663 s 2.1900 s]
                        thrpt:  [58.446 MiB/s 59.087 MiB/s 59.726 MiB/s]
ingest/io_mode=DirectRw volume_mib=128 key_size_bytes=100 key_layout=RandomReuse(1023) write_delta=Y...
                        time:   [1.1906 s 1.1926 s 1.1947 s]
                        thrpt:  [107.14 MiB/s 107.33 MiB/s 107.51 MiB/s]
ingest/io_mode=DirectRw volume_mib=128 key_size_bytes=100 key_layout=Sequential write_delta=No
                        time:   [516.86 ms 518.25 ms 519.47 ms]
                        thrpt:  [246.40 MiB/s 246.98 MiB/s 247.65 MiB/s]
ingest/io_mode=DirectRw volume_mib=128 key_size_bytes=8192 key_layout=Sequential write_delta=Yes
                        time:   [536.50 ms 536.53 ms 536.60 ms]
                        thrpt:  [238.54 MiB/s 238.57 MiB/s 238.59 MiB/s]
ingest/io_mode=DirectRw volume_mib=128 key_size_bytes=8192 key_layout=Sequential write_delta=No
                        time:   [267.77 ms 267.90 ms 268.04 ms]
                        thrpt:  [477.53 MiB/s 477.79 MiB/s 478.02 MiB/s]

Hetzner AX102:

ingest/io_mode=Buffered volume_mib=128 key_size_bytes=100 key_layout=Sequential write_delta=Yes
                        time:   [836.58 ms 861.93 ms 886.57 ms]
                        thrpt:  [144.38 MiB/s 148.50 MiB/s 153.00 MiB/s]
ingest/io_mode=Buffered volume_mib=128 key_size_bytes=100 key_layout=Random write_delta=Yes
                        time:   [1.2782 s 1.3191 s 1.3665 s]
                        thrpt:  [93.668 MiB/s 97.037 MiB/s 100.14 MiB/s]
ingest/io_mode=Buffered volume_mib=128 key_size_bytes=100 key_layout=RandomReuse(1023) write_delta=Y...
                        time:   [791.27 ms 807.08 ms 822.95 ms]
                        thrpt:  [155.54 MiB/s 158.60 MiB/s 161.77 MiB/s]
ingest/io_mode=Buffered volume_mib=128 key_size_bytes=100 key_layout=Sequential write_delta=No
                        time:   [310.78 ms 314.66 ms 318.47 ms]
                        thrpt:  [401.92 MiB/s 406.79 MiB/s 411.87 MiB/s]
ingest/io_mode=Buffered volume_mib=128 key_size_bytes=8192 key_layout=Sequential write_delta=Yes
                        time:   [377.11 ms 387.77 ms 399.21 ms]
                        thrpt:  [320.63 MiB/s 330.10 MiB/s 339.42 MiB/s]
ingest/io_mode=Buffered volume_mib=128 key_size_bytes=8192 key_layout=Sequential write_delta=No
                        time:   [128.37 ms 132.96 ms 138.55 ms]
                        thrpt:  [923.83 MiB/s 962.69 MiB/s 997.11 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=100 key_layout=Sequential write_delta=Yes
                        time:   [900.38 ms 914.88 ms 928.86 ms]
                        thrpt:  [137.80 MiB/s 139.91 MiB/s 142.16 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=100 key_layout=Random write_delta=Yes
                        time:   [1.2538 s 1.2936 s 1.3313 s]
                        thrpt:  [96.149 MiB/s 98.946 MiB/s 102.09 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=100 key_layout=RandomReuse(1023) write_delta=Yes
                        time:   [787.17 ms 803.89 ms 820.63 ms]
                        thrpt:  [155.98 MiB/s 159.23 MiB/s 162.61 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=100 key_layout=Sequential write_delta=No
                        time:   [318.78 ms 321.89 ms 324.74 ms]
                        thrpt:  [394.16 MiB/s 397.65 MiB/s 401.53 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=8192 key_layout=Sequential write_delta=Yes
                        time:   [374.01 ms 383.45 ms 393.20 ms]
                        thrpt:  [325.53 MiB/s 333.81 MiB/s 342.24 MiB/s]
ingest/io_mode=Direct volume_mib=128 key_size_bytes=8192 key_layout=Sequential write_delta=No
                        time:   [137.98 ms 141.31 ms 143.57 ms]
                        thrpt:  [891.58 MiB/s 905.79 MiB/s 927.66 MiB/s]
ingest/io_mode=DirectRw volume_mib=128 key_size_bytes=100 key_layout=Sequential write_delta=Yes
                        time:   [613.69 ms 622.48 ms 630.97 ms]
                        thrpt:  [202.86 MiB/s 205.63 MiB/s 208.57 MiB/s]
ingest/io_mode=DirectRw volume_mib=128 key_size_bytes=100 key_layout=Random write_delta=Yes
                        time:   [1.0299 s 1.0766 s 1.1273 s]
                        thrpt:  [113.55 MiB/s 118.90 MiB/s 124.29 MiB/s]
ingest/io_mode=DirectRw volume_mib=128 key_size_bytes=100 key_layout=RandomReuse(1023) write_delta=Y...
                        time:   [637.80 ms 647.78 ms 658.01 ms]
                        thrpt:  [194.53 MiB/s 197.60 MiB/s 200.69 MiB/s]
ingest/io_mode=DirectRw volume_mib=128 key_size_bytes=100 key_layout=Sequential write_delta=No
                        time:   [266.09 ms 267.20 ms 268.31 ms]
                        thrpt:  [477.06 MiB/s 479.04 MiB/s 481.04 MiB/s]
ingest/io_mode=DirectRw volume_mib=128 key_size_bytes=8192 key_layout=Sequential write_delta=Yes
                        time:   [269.34 ms 273.27 ms 277.69 ms]
                        thrpt:  [460.95 MiB/s 468.40 MiB/s 475.24 MiB/s]
ingest/io_mode=DirectRw volume_mib=128 key_size_bytes=8192 key_layout=Sequential write_delta=No
                        time:   [123.18 ms 124.24 ms 125.15 ms]
                        thrpt:  [1022.8 MiB/s 1.0061 GiB/s 1.0148 GiB/s]
*/
