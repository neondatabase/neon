use std::collections::HashSet;
use std::collections::BTreeMap;

use crate::ArtAllocator;
use crate::ArtMultiSlabAllocator;
use crate::TreeInitStruct;
use crate::TreeWriteAccess;
use crate::TreeIterator;

use crate::{Key, Value};

use rand::seq::SliceRandom;
use rand::Rng;
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
        w.insert(&(*k).into(), idx);
    }

    for (idx, k) in keys.iter().enumerate() {
        let r = tree_writer.start_read();
        let value = r.get(&(*k).into());
        assert_eq!(value, Some(idx).as_ref());
    }

    eprintln!("stats: {:?}", tree_writer.start_write().get_statistics());
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



#[derive(Clone, Copy, Debug)]
struct TestOp(TestKey, Option<usize>);

fn apply_op<A: ArtAllocator<usize>>(op: &TestOp, tree: &TreeWriteAccess<TestKey, usize, A>, shadow: &mut BTreeMap<TestKey, usize>) {
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
        assert_eq!(existing, shadow_existing.as_ref());
        return op.1;
    });
}

fn test_iter<A: ArtAllocator<usize>>(tree: &TreeWriteAccess<TestKey, usize, A>, shadow: &BTreeMap<TestKey, usize>) {
    let mut shadow_iter = shadow.iter();
    let mut iter = TreeIterator::new(&(TestKey::MIN..TestKey::MAX));

    loop {
        let shadow_item = shadow_iter.next().map(|(k, v)| (k.clone(), v));
        let r = tree.start_read();
        let item = iter.next(&r);

        if shadow_item != item {
            eprintln!("FAIL: iterator returned {:?}, expected {:?}", item, shadow_item);
            tree.start_read().dump();

            eprintln!("SHADOW:");
            let mut si = shadow.iter();
            while let Some(si) = si.next() {
                eprintln!("key: {:?}, val: {}", si.0, si.1);
            }
            panic!("FAIL: iterator returned {:?}, expected {:?}", item, shadow_item);
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

    let init_struct = TreeInitStruct::<TestKey, usize, _>::new(allocator);
    let tree_writer = init_struct.attach_writer();

    let mut shadow: std::collections::BTreeMap<TestKey, usize> = BTreeMap::new();

    let distribution = Zipf::new(u128::MAX as f64, 1.1).unwrap();
    let mut rng = rand::rng();
    for i in 0..100000 {
        let key: TestKey = (rng.sample(distribution) as u128).into();

        let op = TestOp(
            key, 
            if rng.random_bool(0.75) {
                Some(i)
            } else {
                None
            },
        );

        apply_op(&op, &tree_writer, &mut shadow);

        if i % 1000 == 0 {
            eprintln!("{i} ops processed");
            eprintln!("stats: {:?}", tree_writer.start_write().get_statistics());
            test_iter(&tree_writer, &shadow);
        }
    }
}
