use bytes::{Bytes, BytesMut};
use camino::{Utf8Path, Utf8PathBuf};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pageserver::{tenant::storage_layer::InMemoryLayer, config::PageServerConf, context::{RequestContext, DownloadBehavior}, task_mgr::TaskKind, repository::Key, virtual_file};
use pageserver::repository::Value;
use utils::{id::{TimelineId, TenantId}, lsn::Lsn};

fn bench_writes(c: &mut Criterion) {
    // Boilerplate
    // TODO this setup can be avoided if I reuse TenantHarness but it's difficult
    //      because it's only compiled for tests, and it's hacky because tbh we
    //      shouldn't need this many inputs for a function that just writes bytes
    //      from memory to disk. Performance-critical functions should be
    //      self-contained (almost like they're separate libraries) and all the
    //      monolithic pageserver machinery should live outside.
    virtual_file::init(10);
    let repo_dir = Utf8PathBuf::from(&"/home/bojan/tmp/repo_dir");
    let conf = PageServerConf::dummy_conf(repo_dir);
    let conf: &'static PageServerConf = Box::leak(Box::new(conf));
    let timeline_id = TimelineId::generate();
    let tenant_id = TenantId::generate();
    let start_lsn = Lsn(0);
    let ctx = RequestContext::new(TaskKind::LayerFlushTask, DownloadBehavior::Error);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    fn test_img(s: &str) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(s.as_bytes());
        buf.resize(64, 0);

        buf.freeze()
    }

    // Make the InMemoryLayer that will be flushed
    let layer = rt.block_on(async {
        let l = InMemoryLayer::create(&conf, timeline_id, tenant_id, start_lsn).await.unwrap();

        let mut lsn = Lsn(0x10);
        let mut key = Key::from_hex("012222222233333333444444445500000000").unwrap();
        let mut blknum = 0;
        for _ in 0..100 {
            key.field6 = blknum;
            let val = Value::Image(test_img(&format!("{} at {}", blknum, lsn)));
            l.put_value(key, lsn, &val, &ctx).await.unwrap();

            lsn = Lsn(lsn.0 + 0x10);
            blknum += 1;
        }
        l
    });

    rt.block_on(async {
        layer.write_to_disk_bench(&ctx).await.unwrap();
    });


    let mut group = c.benchmark_group("g1");
    group.bench_function("f1", |b| {
        b.iter(|| {
            // TODO
        });
    });
    group.bench_function("f2", |b| {
        b.iter(|| {
            // TODO
        });
    });
    group.finish();
}


criterion_group!(group_1, bench_writes);
criterion_main!(group_1);
