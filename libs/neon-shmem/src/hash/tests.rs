use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::hash::HashMapAccess;
use crate::hash::HashMapInit;
use crate::hash::UpdateAction;
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
    let w = init_struct.attach_writer();

    for (idx, k) in keys.iter().enumerate() {
        let res = w.insert(&(*k).into(), idx);
        assert!(res.is_ok());
    }

    for (idx, k) in keys.iter().enumerate() {
        let x = w.get(&(*k).into());
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
    sut: &HashMapAccess<TestKey, TestValue>,
    shadow: &mut BTreeMap<TestKey, usize>,
) {
    eprintln!("applying op: {op:?}");

    // apply the change to the shadow tree first
    let shadow_existing = if let Some(v) = op.1 {
        shadow.insert(op.0, v)
    } else {
        shadow.remove(&op.0)
    };

    // apply to Art tree
    sut.update_with_fn(&op.0, |existing| {
        assert_eq!(existing.map(TestValue::load), shadow_existing);

        match (existing, op.1) {
            (None, None) => UpdateAction::Nothing,
            (None, Some(new_val)) => UpdateAction::Insert(TestValue::new(new_val)),
            (Some(_old_val), None) => UpdateAction::Remove,
            (Some(old_val), Some(new_val)) => {
                old_val.0.store(new_val, Ordering::Relaxed);
                UpdateAction::Nothing
            }
        }
    })
    .expect("out of memory");
}

#[test]
fn random_ops() {
    const MAX_MEM_SIZE: usize = 10000000;
    let shmem = ShmemHandle::new("test_inserts", 0, MAX_MEM_SIZE).unwrap();

    let init_struct = HashMapInit::<TestKey, TestValue>::init_in_shmem(100000, shmem);
    let writer = init_struct.attach_writer();

    let mut shadow: std::collections::BTreeMap<TestKey, usize> = BTreeMap::new();

    let distribution = Zipf::new(u128::MAX as f64, 1.1).unwrap();
    let mut rng = rand::rng();
    for i in 0..100000 {
        let key: TestKey = (rng.sample(distribution) as u128).into();

        let op = TestOp(key, if rng.random_bool(0.75) { Some(i) } else { None });

        apply_op(&op, &writer, &mut shadow);

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

    let init_struct = HashMapInit::<TestKey, TestValue>::init_in_shmem(1000, shmem);
    let writer = init_struct.attach_writer();

    let mut shadow: std::collections::BTreeMap<TestKey, usize> = BTreeMap::new();

    let mut rng = rand::rng();
    for i in 0..10000 {
        let key: TestKey = ((rng.next_u32() % 1000) as u128).into();

        let op = TestOp(key, if rng.random_bool(0.75) { Some(i) } else { None });

        apply_op(&op, &writer, &mut shadow);

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

        apply_op(&op, &writer, &mut shadow);

        if i % 1000 == 0 {
            eprintln!("{i} ops processed");
            //eprintln!("stats: {:?}", tree_writer.get_statistics());
            //test_iter(&tree_writer, &shadow);
        }
    }
}
