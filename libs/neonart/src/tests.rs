use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::ArtAllocator;
use crate::ArtMultiSlabAllocator;
use crate::TreeInitStruct;
use crate::TreeIterator;
use crate::TreeWriteAccess;
use crate::UpdateAction;

use crate::{Key, Value};

use rand::Rng;
use rand::seq::SliceRandom;
use rand_distr::Zipf;

const TEST_KEY_LEN: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct TestKey([u8; TEST_KEY_LEN]);

impl TestKey {
    const MIN: TestKey = TestKey([0; TEST_KEY_LEN]);
    const MAX: TestKey = TestKey([u8::MAX; TEST_KEY_LEN]);
}

impl Key for TestKey {
    const KEY_LEN: usize = TEST_KEY_LEN;
    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

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

impl Value for usize {}

fn test_inserts<K: Into<TestKey> + Copy>(keys: &[K]) {
    const MEM_SIZE: usize = 10000000;
    let mut area = Box::new_uninit_slice(MEM_SIZE);

    let allocator = ArtMultiSlabAllocator::new(&mut area);

    let init_struct = TreeInitStruct::<TestKey, usize, _>::new(allocator);
    let tree_writer = init_struct.attach_writer();

    for (idx, k) in keys.iter().enumerate() {
        let w = tree_writer.start_write();
        let res = w.insert(&(*k).into(), idx);
        assert!(res.is_ok());
    }

    for (idx, k) in keys.iter().enumerate() {
        let r = tree_writer.start_read();
        let value = r.get(&(*k).into());
        assert_eq!(value, Some(idx).as_ref());
    }

    eprintln!("stats: {:?}", tree_writer.get_statistics());
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
            if used_keys.contains(&key) {
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

impl Value for TestValue {}

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

fn apply_op<A: ArtAllocator<TestValue>>(
    op: &TestOp,
    tree: &TreeWriteAccess<TestKey, TestValue, A>,
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
    let w = tree.start_write();
    w.update_with_fn(&op.0, |existing| {
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

fn test_iter<A: ArtAllocator<TestValue>>(
    tree: &TreeWriteAccess<TestKey, TestValue, A>,
    shadow: &BTreeMap<TestKey, usize>,
) {
    let mut shadow_iter = shadow.iter();
    let mut iter = TreeIterator::new(&(TestKey::MIN..TestKey::MAX));

    loop {
        let shadow_item = shadow_iter.next().map(|(k, v)| (*k, *v));
        let r = tree.start_read();
        let item = iter.next(&r);

        if shadow_item != item.map(|(k, v)| (k, v.load())) {
            eprintln!(
                "FAIL: iterator returned {:?}, expected {:?}",
                item, shadow_item
            );
            tree.start_read().dump(&mut std::io::stderr());

            eprintln!("SHADOW:");
            for si in shadow {
                eprintln!("key: {:?}, val: {}", si.0, si.1);
            }
            panic!(
                "FAIL: iterator returned {:?}, expected {:?}",
                item, shadow_item
            );
        }
        if item.is_none() {
            break;
        }
    }
}

#[test]
fn random_ops() {
    const MEM_SIZE: usize = 10000000;
    let mut area = Box::new_uninit_slice(MEM_SIZE);

    let allocator = ArtMultiSlabAllocator::new(&mut area);

    let init_struct = TreeInitStruct::<TestKey, TestValue, _>::new(allocator);
    let tree_writer = init_struct.attach_writer();

    let mut shadow: std::collections::BTreeMap<TestKey, usize> = BTreeMap::new();

    let distribution = Zipf::new(u128::MAX as f64, 1.1).unwrap();
    let mut rng = rand::rng();
    for i in 0..100000 {
        let mut key: TestKey = (rng.sample(distribution) as u128).into();

        if rng.random_bool(0.10) {
            key = TestKey::from(u128::from(&key) | 0xffffffff);
        }

        let op = TestOp(key, if rng.random_bool(0.75) { Some(i) } else { None });

        apply_op(&op, &tree_writer, &mut shadow);

        if i % 1000 == 0 {
            eprintln!("{i} ops processed");
            eprintln!("stats: {:?}", tree_writer.get_statistics());
            test_iter(&tree_writer, &shadow);
        }
    }
}
