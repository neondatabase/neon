use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use neon_shmem::hash::HashMapAccess;
use neon_shmem::hash::HashMapInit;
use neon_shmem::hash::entry::Entry;
use rand::distr::{Distribution, StandardUniform};
use rand::prelude::*;
use std::default::Default;
use std::hash::BuildHasher;

// Taken from bindings to C code

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
#[repr(C)]
pub struct FileCacheKey {
    pub _spc_id: u32,
    pub _db_id: u32,
    pub _rel_number: u32,
    pub _fork_num: u32,
    pub _block_num: u32,
}

impl Distribution<FileCacheKey> for StandardUniform {
    // questionable, but doesn't need to be good randomness
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> FileCacheKey {
        FileCacheKey {
            _spc_id: rng.random(),
            _db_id: rng.random(),
            _rel_number: rng.random(),
            _fork_num: rng.random(),
            _block_num: rng.random(),
        }
    }
}

#[derive(Clone, Debug)]
#[repr(C)]
pub struct FileCacheEntry {
    pub _offset: u32,
    pub _access_count: u32,
    pub _prev: *mut FileCacheEntry,
    pub _next: *mut FileCacheEntry,
    pub _state: [u32; 8],
}

impl FileCacheEntry {
    fn dummy() -> Self {
        Self {
            _offset: 0,
            _access_count: 0,
            _prev: std::ptr::null_mut(),
            _next: std::ptr::null_mut(),
            _state: [0; 8],
        }
    }
}

// Utilities for applying operations.

#[derive(Clone, Debug)]
struct TestOp<K, V>(K, Option<V>);

fn apply_op<K: Clone + std::hash::Hash + Eq, V, S: std::hash::BuildHasher>(
    op: TestOp<K, V>,
    map: &mut HashMapAccess<K, V, S>,
) {
    let entry = map.entry(op.0);

    match op.1 {
        Some(new) => match entry {
            Entry::Occupied(mut e) => Some(e.insert(new)),
            Entry::Vacant(e) => {
                _ = e.insert(new).unwrap();
                None
            }
        },
        None => match entry {
            Entry::Occupied(e) => Some(e.remove()),
            Entry::Vacant(_) => None,
        },
    };
}

// Hash utilities

struct SeaRandomState {
    k1: u64,
    k2: u64,
    k3: u64,
    k4: u64,
}

impl std::hash::BuildHasher for SeaRandomState {
    type Hasher = seahash::SeaHasher;

    fn build_hasher(&self) -> Self::Hasher {
        seahash::SeaHasher::with_seeds(self.k1, self.k2, self.k3, self.k4)
    }
}

impl SeaRandomState {
    fn new() -> Self {
        let mut rng = rand::rng();
        Self {
            k1: rng.random(),
            k2: rng.random(),
            k3: rng.random(),
            k4: rng.random(),
        }
    }
}

fn small_benchs(c: &mut Criterion) {
    let mut group = c.benchmark_group("Small maps");
    group.sample_size(10);

    group.bench_function("small_rehash", |b| {
        let ideal_filled = 4_000_000;
        let size = 5_000_000;
        let mut writer = HashMapInit::new_resizeable(size, size * 2).attach_writer();
        let mut rng = rand::rng();
        while writer.get_num_buckets_in_use() < ideal_filled as usize {
            let key: FileCacheKey = rng.random();
            let val = FileCacheEntry::dummy();
            apply_op(TestOp(key, Some(val)), &mut writer);
        }
        b.iter(|| writer.shuffle());
    });

    group.bench_function("small_rehash_xxhash", |b| {
        let ideal_filled = 4_000_000;
        let size = 5_000_000;
        let mut writer = HashMapInit::new_resizeable(size, size * 2)
            .with_hasher(twox_hash::xxhash64::RandomState::default())
            .attach_writer();
        let mut rng = rand::rng();
        while writer.get_num_buckets_in_use() < ideal_filled as usize {
            let key: FileCacheKey = rng.random();
            let val = FileCacheEntry::dummy();
            apply_op(TestOp(key, Some(val)), &mut writer);
        }
        b.iter(|| writer.shuffle());
    });

    group.bench_function("small_rehash_ahash", |b| {
        let ideal_filled = 4_000_000;
        let size = 5_000_000;
        let mut writer = HashMapInit::new_resizeable(size, size * 2)
            .with_hasher(ahash::RandomState::default())
            .attach_writer();
        let mut rng = rand::rng();
        while writer.get_num_buckets_in_use() < ideal_filled as usize {
            let key: FileCacheKey = rng.random();
            let val = FileCacheEntry::dummy();
            apply_op(TestOp(key, Some(val)), &mut writer);
        }
        b.iter(|| writer.shuffle());
    });

    group.bench_function("small_rehash_seahash", |b| {
        let ideal_filled = 4_000_000;
        let size = 5_000_000;
        let mut writer = HashMapInit::new_resizeable(size, size * 2)
            .with_hasher(SeaRandomState::new())
            .attach_writer();
        let mut rng = rand::rng();
        while writer.get_num_buckets_in_use() < ideal_filled as usize {
            let key: FileCacheKey = rng.random();
            let val = FileCacheEntry::dummy();
            apply_op(TestOp(key, Some(val)), &mut writer);
        }
        b.iter(|| writer.shuffle());
    });

    group.finish();
}

fn real_benchs(c: &mut Criterion) {
    let mut group = c.benchmark_group("Realistic workloads");
    group.sample_size(10);
    group.bench_function("real_bulk_insert", |b| {
        let size = 125_000_000;
        let ideal_filled = 100_000_000;
        let mut rng = rand::rng();
        b.iter_batched(
            || HashMapInit::new_resizeable(size, size * 2).attach_writer(),
            |writer| {
                for _ in 0..ideal_filled {
                    let key: FileCacheKey = rng.random();
                    let val = FileCacheEntry::dummy();
                    let entry = writer.entry(key);
                    match entry {
                        Entry::Occupied(mut e) => {
                            std::hint::black_box(e.insert(val));
                        }
                        Entry::Vacant(e) => {
                            let _ = std::hint::black_box(e.insert(val).unwrap());
                        }
                    }
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("real_rehash", |b| {
        let size = 125_000_000;
        let ideal_filled = 100_000_000;
        let mut writer = HashMapInit::new_resizeable(size, size).attach_writer();
        let mut rng = rand::rng();
        while writer.get_num_buckets_in_use() < ideal_filled {
            let key: FileCacheKey = rng.random();
            let val = FileCacheEntry::dummy();
            apply_op(TestOp(key, Some(val)), &mut writer);
        }
        b.iter(|| writer.shuffle());
    });

    group.bench_function("real_rehash_hashbrown", |b| {
        let size = 125_000_000;
        let ideal_filled = 100_000_000;
        let mut writer = hashbrown::raw::RawTable::new();
        let mut rng = rand::rng();
        let hasher = rustc_hash::FxBuildHasher;
        unsafe {
            writer
                .resize(
                    size,
                    |(k, _)| hasher.hash_one(k),
                    hashbrown::raw::Fallibility::Infallible,
                )
                .unwrap();
        }
        while writer.len() < ideal_filled as usize {
            let key: FileCacheKey = rng.random();
            let val = FileCacheEntry::dummy();
            writer.insert(hasher.hash_one(&key), (key, val), |(k, _)| {
                hasher.hash_one(k)
            });
        }
        b.iter(|| unsafe {
            writer.table.rehash_in_place(
                &|table, index| {
                    hasher.hash_one(
                        &table
                            .bucket::<(FileCacheKey, FileCacheEntry)>(index)
                            .as_ref()
                            .0,
                    )
                },
                std::mem::size_of::<(FileCacheKey, FileCacheEntry)>(),
                if std::mem::needs_drop::<(FileCacheKey, FileCacheEntry)>() {
                    Some(|ptr| std::ptr::drop_in_place(ptr as *mut (FileCacheKey, FileCacheEntry)))
                } else {
                    None
                },
            )
        });
    });

    for elems in [2, 4, 8, 16, 32, 64, 96, 112] {
        group.bench_with_input(
            BenchmarkId::new("real_rehash_varied", elems),
            &elems,
            |b, &size| {
                let ideal_filled = size * 1_000_000;
                let size = 125_000_000;
                let mut writer = HashMapInit::new_resizeable(size, size).attach_writer();
                let mut rng = rand::rng();
                while writer.get_num_buckets_in_use() < ideal_filled as usize {
                    let key: FileCacheKey = rng.random();
                    let val = FileCacheEntry::dummy();
                    apply_op(TestOp(key, Some(val)), &mut writer);
                }
                b.iter(|| writer.shuffle());
            },
        );
        group.bench_with_input(
            BenchmarkId::new("real_rehash_varied_hashbrown", elems),
            &elems,
            |b, &size| {
                let ideal_filled = size * 1_000_000;
                let size = 125_000_000;
                let mut writer = hashbrown::raw::RawTable::new();
                let mut rng = rand::rng();
                let hasher = rustc_hash::FxBuildHasher;
                unsafe {
                    writer
                        .resize(
                            size,
                            |(k, _)| hasher.hash_one(k),
                            hashbrown::raw::Fallibility::Infallible,
                        )
                        .unwrap();
                }
                while writer.len() < ideal_filled as usize {
                    let key: FileCacheKey = rng.random();
                    let val = FileCacheEntry::dummy();
                    writer.insert(hasher.hash_one(&key), (key, val), |(k, _)| {
                        hasher.hash_one(k)
                    });
                }
                b.iter(|| unsafe {
                    writer.table.rehash_in_place(
                        &|table, index| {
                            hasher.hash_one(
                                &table
                                    .bucket::<(FileCacheKey, FileCacheEntry)>(index)
                                    .as_ref()
                                    .0,
                            )
                        },
                        std::mem::size_of::<(FileCacheKey, FileCacheEntry)>(),
                        if std::mem::needs_drop::<(FileCacheKey, FileCacheEntry)>() {
                            Some(|ptr| {
                                std::ptr::drop_in_place(ptr as *mut (FileCacheKey, FileCacheEntry))
                            })
                        } else {
                            None
                        },
                    )
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, small_benchs, real_benchs);
criterion_main!(benches);
