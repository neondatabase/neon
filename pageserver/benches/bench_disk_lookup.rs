use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pageserver::{tenant::{disk_btree::{DiskBtreeBuilder, DiskBtreeReader, VisitDirection}, block_io::{BlockBuf, FileBlockReader}, storage_layer::DeltaLayerWriter}, repository::Key, virtual_file::{VirtualFile, self}, page_cache};
use std::{time::Instant, collections::BTreeMap};
use rand::prelude::{SeedableRng, SliceRandom, StdRng};
use utils::{id::{TimelineId, TenantId}, lsn::Lsn};
use std::{io::{Read, Write}, path::PathBuf};
use pageserver::config::PageServerConf;

struct MockLayer {
    pub path: PathBuf,
    pub index_start_blk: u32,
    pub index_root_blk: u32,
}

impl MockLayer {
    fn read(&self, key: i128) -> Option<u64> {
        // Read from disk btree
        let file = FileBlockReader::new(VirtualFile::open(&self.path).unwrap());
        let tree_reader = DiskBtreeReader::<_, 24>::new(
            self.index_start_blk,
            self.index_root_blk,
            file,
        );

        let key: Key = Key::from_i128(key);
        let mut key_bytes: [u8; 24] = [8u8; 24];
        key.write_to_byte_slice(&mut key_bytes);

        let mut result = None;
        tree_reader.visit(&key_bytes, VisitDirection::Backwards, |key, value| {
            if key == key_bytes {
                result = Some(value);
            }
            return false
        }).unwrap();

        result
    }
}

fn make_simple(n_keys: i128, name: &str) -> MockLayer {

    let now = Instant::now();
    let block_buf = BlockBuf::new();
    let mut writer = DiskBtreeBuilder::<_, 24>::new(block_buf);
    for i in 0..n_keys {
        let key: Key = Key::from_i128(i);
        let value: u64 = i as u64;

        let mut key_bytes: [u8; 24] = [8u8; 24];
        key.write_to_byte_slice(&mut key_bytes);
        writer.append(&key_bytes, value).unwrap();
    }
    let (index_root_blk, block_buf) = writer.finish().unwrap();
    println!("wrote {} keys to BlockBuf in {:?}", n_keys, now.elapsed());
    // wrote 4_000_000 keys to BlockBuf in 129.980503ms
    // wrote 40_000_000 keys to BlockBuf in 1.336874876s
    // (/ 52.0 (+ 0.129 0.062))

    let index_start_blk = 0; // ???
    let path = std::env::current_dir().unwrap()
        .parent().unwrap()
        .join("test_output")
        .join("bench_disk_lookup")
        .join("disk_btree")
        .join(name);
    std::fs::create_dir_all(path.clone().parent().unwrap()).unwrap();
    let layer = MockLayer {
        path: path.clone(),
        index_start_blk,
        index_root_blk,
    };

    let now = Instant::now();
    let mut file = VirtualFile::create(&path).unwrap();
    let mut total_len = 0;
    for buf in block_buf.blocks {
        file.write_all(buf.as_ref()).unwrap();
        total_len += buf.len();
    }
    println!("flushed {} bytes to disk in {:?}", total_len, now.elapsed());
    // flushed 52_355_072 bytes to disk in 62.540002ms     => 800 MB/s
    // flushed 523_411_456 bytes to disk in 551.762844ms   => 800 MB/s
    // flushed 523_411_456 bytes to disk in 4.989601463s   => 100 MB/s !!!!

    let now = Instant::now();
    file.sync_all().unwrap();
    println!("fsynced in {:?}", now.elapsed());
    // flushed 523411456 bytes to disk in 574.897513ms | fsynced in 45.079831ms
    // flushed 523411456 bytes to disk in 557.103133ms | fsynced in 56.976345ms
    // flushed 523411456 bytes to disk in 559.939736ms | fsynced in 58.743932ms
    // flushed 523411456 bytes to disk in 2.128451459s | fsynced in 1.662821424s
    // flushed 523411456 bytes to disk in 2.937101445s | fsynced in 1.452016294s
    // flushed 523411456 bytes to disk in 560.161377ms | fsynced in 63.579154ms
    // flushed 523411456 bytes to disk in 562.492048ms | fsynced in 46.795958ms
    // flushed 523411456 bytes to disk in 554.746062ms | fsynced in 69.815532ms
    // flushed 523411456 bytes to disk in 566.547446ms | fsynced in 52.785175ms
    layer
}

fn make_many(n_keys: i128, n_layers: i128) -> Vec<MockLayer> {
    (0..n_layers)
        .map(|i| make_simple(n_keys, &format!("layer_{}.tmp", i)))
        .collect()
}


// cargo bench --bench bench_disk_lookup
fn bench_disk_lookup(c: &mut Criterion) {
    virtual_file::init(10);
    page_cache::init(10000);

    // Results in a 40MB index
    let n_keys = 4_000_000;

    // One layer for each query
    let n_layers = 100;
    let n_queries = n_layers;

    let n_keys = 40_000_000;
    let n_layers = 10;

    // Write to disk btree
    let layers = make_many(n_keys, n_layers);
    return;

    // Write to mem btrees
    let mem_btrees: Vec<BTreeMap<i128, u64>> = (0..n_layers)
        .map(|_| (0..n_keys)
             .map(|i| (i as i128, i as u64))
             .collect())
        .collect();

    // Pick queries
    let rng = &mut StdRng::seed_from_u64(1);
    let queries: Vec<_> = (0..n_keys).collect();
    let queries: Vec<_> = queries.choose_multiple(rng, n_queries as usize).copied().collect();

    // Define and name the benchmark function
    let mut group = c.benchmark_group("g1");
    group.bench_function("disk_btree", |b| {
        b.iter(|| {
            for (i, q) in queries.clone().into_iter().enumerate() {
                black_box({
                    assert_eq!(layers[i].read(q), Some(q as u64));
                })
            }
        });
    });
    group.bench_function("mem_btree", |b| {
        b.iter(|| {
            for (i, q) in queries.clone().into_iter().enumerate() {
                black_box({
                    assert_eq!(mem_btrees[i].get(&q), Some(&(q as u64)));
                })
            }
        });
    });
    group.finish();
}

criterion_group!(group_1, bench_disk_lookup);
criterion_main!(group_1);
