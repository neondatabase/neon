use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::hash::HashMapAccess;
use crate::hash::HashMapInit;
use crate::hash::Entry;
use crate::shmem::ShmemHandle;

use rand::seq::SliceRandom;
use rand::{Rng, RngCore};
use rand_distr::Zipf;

const TEST_KEY_LEN: usize = 16;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct TestKey([u8; TEST_KEY_LEN]);

impl From<&TestKey> for u128 {
    fn from(val: &TestKey) -> u128 {
        u128::from_be_bytes(val.0)
    }
}

impl From<u128> for TestKey {
    fn from(val: u128) -> TestKey {
        TestKey(val.to_be_bytes())
    }
}

impl<'a> From<&'a [u8]> for TestKey {
    fn from(bytes: &'a [u8]) -> TestKey {
        TestKey(bytes.try_into().unwrap())
    }
}

fn test_inserts<K: Into<TestKey> + Copy>(keys: &[K]) {	
    const MAX_MEM_SIZE: usize = 10000000;
    let shmem = ShmemHandle::new("test_inserts", 0, MAX_MEM_SIZE).unwrap();

    let init_struct = HashMapInit::<TestKey, usize>::init_in_shmem(100000, shmem);
    let mut w = init_struct.attach_writer();

    for (idx, k) in keys.iter().enumerate() {
		let hash = w.get_hash_value(&(*k).into());
		let res = w.entry_with_hash((*k).into(), hash);
		match res {
			Entry::Occupied(mut e) => { e.insert(idx); }
			Entry::Vacant(e) => {
				let res = e.insert(idx);
				assert!(res.is_ok());
			},
		};
    }

    for (idx, k) in keys.iter().enumerate() {
		let hash = w.get_hash_value(&(*k).into());
        let x = w.get_with_hash(&(*k).into(), hash);
        let value = x.as_deref().copied();
        assert_eq!(value, Some(idx));
    }

    //eprintln!("stats: {:?}", tree_writer.get_statistics());
}

#[test]
fn dense() {
    // This exercises splitting a node with prefix
    let keys: &[u128] = &[0, 1, 2, 3, 256];
    test_inserts(keys);

    // Dense keys
    let mut keys: Vec<u128> = (0..10000).collect();
    test_inserts(&keys);

    // Do the same in random orders
    for _ in 1..10 {
        keys.shuffle(&mut rand::rng());
        test_inserts(&keys);
    }
}

#[test]
fn sparse() {
    // sparse keys
    let mut keys: Vec<TestKey> = Vec::new();
    let mut used_keys = HashSet::new();
    for _ in 0..10000 {
        loop {
            let key = rand::random::<u128>();
            if used_keys.get(&key).is_some() {
                continue;
            }
            used_keys.insert(key);
            keys.push(key.into());
            break;
        }
    }
    test_inserts(&keys);
}

struct TestValue(AtomicUsize);

impl TestValue {
    fn new(val: usize) -> TestValue {
        TestValue(AtomicUsize::new(val))
    }

    fn load(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

impl Clone for TestValue {
    fn clone(&self) -> TestValue {
        TestValue::new(self.load())
    }
}

impl Debug for TestValue {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(fmt, "{:?}", self.load())
    }
}

#[derive(Clone, Debug)]
struct TestOp(TestKey, Option<usize>);

fn apply_op(
    op: &TestOp,
    map: &mut HashMapAccess<TestKey, usize>,
    shadow: &mut BTreeMap<TestKey, usize>,
) {
    eprintln!("applying op: {op:?}");

    // apply the change to the shadow tree first
    let shadow_existing = if let Some(v) = op.1 {
        shadow.insert(op.0, v)
    } else {
        shadow.remove(&op.0)
    };

	let hash = map.get_hash_value(&op.0);
	let entry = map.entry_with_hash(op.0, hash);
    let hash_existing = match op.1 {
		Some(new) => {
			match entry {
				Entry::Occupied(mut e) => Some(e.insert(new)),
				Entry::Vacant(e) => { e.insert(new).unwrap(); None },
			}
		},
		None => {
			match entry {
				Entry::Occupied(e) => Some(e.remove()),
				Entry::Vacant(_) => None,
			}
		},
	};

	assert_eq!(shadow_existing, hash_existing);
}

#[test]
fn random_ops() {
    const MAX_MEM_SIZE: usize = 10000000;
    let shmem = ShmemHandle::new("test_inserts", 0, MAX_MEM_SIZE).unwrap();

    let init_struct = HashMapInit::<TestKey, usize>::init_in_shmem(100000, shmem);
    let mut writer = init_struct.attach_writer();

    let mut shadow: std::collections::BTreeMap<TestKey, usize> = BTreeMap::new();

    let distribution = Zipf::new(u128::MAX as f64, 1.1).unwrap();
    let mut rng = rand::rng();
    for i in 0..100000 {
        let key: TestKey = (rng.sample(distribution) as u128).into();

        let op = TestOp(key, if rng.random_bool(0.75) { Some(i) } else { None });

        apply_op(&op, &mut writer, &mut shadow);

        if i % 1000 == 0 {
            eprintln!("{i} ops processed");
            //eprintln!("stats: {:?}", tree_writer.get_statistics());
            //test_iter(&tree_writer, &shadow);
        }
    }
}

#[test]
fn test_grow() {
    const MEM_SIZE: usize = 10000000;
    let shmem = ShmemHandle::new("test_grow", 0, MEM_SIZE).unwrap();

    let init_struct = HashMapInit::<TestKey, usize>::init_in_shmem(1000, shmem);
    let mut writer = init_struct.attach_writer();

    let mut shadow: std::collections::BTreeMap<TestKey, usize> = BTreeMap::new();

    let mut rng = rand::rng();
    for i in 0..10000 {
        let key: TestKey = ((rng.next_u32() % 1000) as u128).into();

        let op = TestOp(key, if rng.random_bool(0.75) { Some(i) } else { None });

        apply_op(&op, &mut writer, &mut shadow);

        if i % 1000 == 0 {
            eprintln!("{i} ops processed");
            //eprintln!("stats: {:?}", tree_writer.get_statistics());
            //test_iter(&tree_writer, &shadow);
        }
    }

    writer.grow(1500).unwrap();

    for i in 0..10000 {
        let key: TestKey = ((rng.next_u32() % 1500) as u128).into();

        let op = TestOp(key, if rng.random_bool(0.75) { Some(i) } else { None });

        apply_op(&op, &mut writer, &mut shadow);

        if i % 1000 == 0 {
            eprintln!("{i} ops processed");
            //eprintln!("stats: {:?}", tree_writer.get_statistics());
            //test_iter(&tree_writer, &shadow);
        }
    }
}


#[test]
fn test_shrink() {
    const MEM_SIZE: usize = 10000000;
    let shmem = ShmemHandle::new("test_shrink", 0, MEM_SIZE).unwrap();

    let init_struct = HashMapInit::<TestKey, usize>::init_in_shmem(1500, shmem);
    let mut writer = init_struct.attach_writer();

    let mut shadow: std::collections::BTreeMap<TestKey, usize> = BTreeMap::new();

    let mut rng = rand::rng();
    for i in 0..100 {
        let key: TestKey = ((rng.next_u32() % 1500) as u128).into();

        let op = TestOp(key, if rng.random_bool(0.75) { Some(i) } else { None });

        apply_op(&op, &mut writer, &mut shadow);

        if i % 1000 == 0 {
            eprintln!("{i} ops processed");
        }
    }

    writer.begin_shrink(1000);
	for i in 1000..1500 {
		if let Some(entry) = writer.entry_at_bucket(i) {
			shadow.remove(&entry._key);
			entry.remove();
		}
	}
	writer.finish_shrink().unwrap();
	
    for i in 0..10000 {
        let key: TestKey = ((rng.next_u32() % 1000) as u128).into();

        let op = TestOp(key, if rng.random_bool(0.75) { Some(i) } else { None });

        apply_op(&op, &mut writer, &mut shadow);

        if i % 1000 == 0 {
            eprintln!("{i} ops processed");
        }
    }
}
