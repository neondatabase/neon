use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pageserver::{tenant::{disk_btree::{DiskBtreeBuilder, DiskBtreeReader, VisitDirection}, block_io::{BlockBuf, FileBlockReader}, storage_layer::DeltaLayerWriter}, repository::Key, virtual_file::{VirtualFile, self}, page_cache};
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
            return true
        }).unwrap();

        result
    }
}

// cargo bench --bench bench_disk_lookup
fn bench_disk_lookup(c: &mut Criterion) {
    // TODO why go through the trouble of writing to disk if we read the index
    // into memory always?

    // TODO stack 100 disk btrees
    let _timeline = 0;

    virtual_file::init(10);
    page_cache::init(10000);

    let n_keys = 250000;

    // Write to disk btree
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
    let index_start_blk = 0; // ???
    let path = std::env::current_dir().unwrap().join("btree.tmp");
    let layer = MockLayer {
        path: path.clone(),
        index_start_blk,
        index_root_blk,
    };

    let mut file = VirtualFile::create(&path).unwrap();
    for buf in block_buf.blocks {
        file.write_all(buf.as_ref()).unwrap();
    }

    // Pick queries
    let rng = &mut StdRng::seed_from_u64(1);
    let queries: Vec<_> = (0..n_keys).collect();
    let queries: Vec<_> = queries.choose_multiple(rng, 100).copied().collect();

    // Define and name the benchmark function
    let mut group = c.benchmark_group("g1");
    group.bench_function("f1", |b| {
        b.iter(|| {
            for q in queries.clone().into_iter() {
                black_box({
                    assert_eq!(layer.read(q), Some(q as u64));
                })
            }
        });
    });
    group.finish();
}

criterion_group!(group_1, bench_disk_lookup);
criterion_main!(group_1);
