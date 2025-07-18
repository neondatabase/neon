use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::mem::MaybeUninit;

use crate::hash::Entry;
use crate::hash::HashMapAccess;
use crate::hash::HashMapInit;
use crate::hash::core::FullError;

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
    let w = HashMapInit::<TestKey, usize>::new_resizeable_named(100000, 120000, "test_inserts")
        .attach_writer();

    for (idx, k) in keys.iter().enumerate() {
        let res = w.entry((*k).into());
        match res {
            Entry::Occupied(mut e) => {
                e.insert(idx);
            }
            Entry::Vacant(e) => {
                let res = e.insert(idx);
                assert!(res.is_ok());
            }
        };
    }

    for (idx, k) in keys.iter().enumerate() {
        let x = w.get(&(*k).into());
        let value = x.as_deref().copied();
        assert_eq!(value, Some(idx));
    }
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

#[derive(Clone, Debug)]
struct TestOp(TestKey, Option<usize>);

fn apply_op(
    op: &TestOp,
    map: &mut HashMapAccess<TestKey, usize>,
    shadow: &mut BTreeMap<TestKey, usize>,
) {
    // apply the change to the shadow tree first
    let shadow_existing = if let Some(v) = op.1 {
        shadow.insert(op.0, v)
    } else {
        shadow.remove(&op.0)
    };

    let entry = map.entry(op.0);
    let hash_existing = match op.1 {
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

    assert_eq!(shadow_existing, hash_existing);
}

fn do_random_ops(
    num_ops: usize,
    size: u32,
    del_prob: f64,
    writer: &mut HashMapAccess<TestKey, usize>,
    shadow: &mut BTreeMap<TestKey, usize>,
    rng: &mut rand::rngs::ThreadRng,
) {
    for i in 0..num_ops {
        let key: TestKey = ((rng.next_u32() % size) as u128).into();
        let op = TestOp(
            key,
            if rng.random_bool(del_prob) {
                Some(i)
            } else {
                None
            },
        );
        apply_op(&op, writer, shadow);
    }
}

fn do_deletes(
    num_ops: usize,
    writer: &mut HashMapAccess<TestKey, usize>,
    shadow: &mut BTreeMap<TestKey, usize>,
) {
    for _ in 0..num_ops {
        let (k, _) = shadow.pop_first().unwrap();
        writer.remove(&k);
    }
}

fn do_shrink(
    writer: &mut HashMapAccess<TestKey, usize>,
    shadow: &mut BTreeMap<TestKey, usize>,
    from: u32,
    to: u32,
) {
    assert!(writer.shrink_goal().is_none());
    writer.begin_shrink(to);
    assert_eq!(writer.shrink_goal(), Some(to as usize));
    for i in to..from {
        if let Some(entry) = writer.entry_at_bucket(i as usize) {
            shadow.remove(&entry._key);
            entry.remove();
        }
    }
    let old_usage = writer.get_num_buckets_in_use();
    writer.finish_shrink().unwrap();
    assert!(writer.shrink_goal().is_none());
    assert_eq!(writer.get_num_buckets_in_use(), old_usage);
}

#[test]
fn random_ops() {
    let mut writer =
        HashMapInit::<TestKey, usize>::new_resizeable_named(100000, 120000, "test_random")
            .attach_writer();
    let mut shadow: std::collections::BTreeMap<TestKey, usize> = BTreeMap::new();

    let distribution = Zipf::new(u128::MAX as f64, 1.1).unwrap();
    let mut rng = rand::rng();
    for i in 0..100000 {
        let key: TestKey = (rng.sample(distribution) as u128).into();

        let op = TestOp(key, if rng.random_bool(0.75) { Some(i) } else { None });

        apply_op(&op, &mut writer, &mut shadow);
    }
}

#[test]
fn test_shuffle() {
    let mut writer = HashMapInit::<TestKey, usize>::new_resizeable_named(1000, 1200, "test_shuf")
        .attach_writer();
    let mut shadow: std::collections::BTreeMap<TestKey, usize> = BTreeMap::new();
    let mut rng = rand::rng();

    do_random_ops(10000, 1000, 0.75, &mut writer, &mut shadow, &mut rng);
    writer.shuffle();
    do_random_ops(10000, 1000, 0.75, &mut writer, &mut shadow, &mut rng);
}

#[test]
fn test_grow() {
    let mut writer = HashMapInit::<TestKey, usize>::new_resizeable_named(1000, 2000, "test_grow")
        .attach_writer();
    let mut shadow: std::collections::BTreeMap<TestKey, usize> = BTreeMap::new();
    let mut rng = rand::rng();

    do_random_ops(10000, 1000, 0.75, &mut writer, &mut shadow, &mut rng);
    let old_usage = writer.get_num_buckets_in_use();
    writer.grow(1500).unwrap();
    assert_eq!(writer.get_num_buckets_in_use(), old_usage);
    assert_eq!(writer.get_num_buckets(), 1500);
    do_random_ops(10000, 1500, 0.75, &mut writer, &mut shadow, &mut rng);
}

#[test]
fn test_clear() {
    let mut writer = HashMapInit::<TestKey, usize>::new_resizeable_named(1500, 2000, "test_clear")
        .attach_writer();
    let mut shadow: std::collections::BTreeMap<TestKey, usize> = BTreeMap::new();
    let mut rng = rand::rng();
    do_random_ops(2000, 1500, 0.75, &mut writer, &mut shadow, &mut rng);
    writer.clear();
    assert_eq!(writer.get_num_buckets_in_use(), 0);
    assert_eq!(writer.get_num_buckets(), 1500);
    while let Some((key, _)) = shadow.pop_first() {
        assert!(writer.get(&key).is_none());
    }
    do_random_ops(2000, 1500, 0.75, &mut writer, &mut shadow, &mut rng);
    for i in 0..(1500 - writer.get_num_buckets_in_use()) {
        writer.insert((1500 + i as u128).into(), 0).unwrap();
    }
    assert_eq!(writer.insert(5000.into(), 0), Err(FullError {}));
    writer.clear();
    assert!(writer.insert(5000.into(), 0).is_ok());
}

#[test]
fn test_idx_remove() {
    let mut writer = HashMapInit::<TestKey, usize>::new_resizeable_named(1500, 2000, "test_clear")
        .attach_writer();
    let mut shadow: std::collections::BTreeMap<TestKey, usize> = BTreeMap::new();
    let mut rng = rand::rng();
    do_random_ops(2000, 1500, 0.25, &mut writer, &mut shadow, &mut rng);
    for _ in 0..100 {
        let idx = (rng.next_u32() % 1500) as usize;
        if let Some(e) = writer.entry_at_bucket(idx) {
            shadow.remove(&e._key);
            e.remove();
        }
    }
    while let Some((key, val)) = shadow.pop_first() {
        assert_eq!(*writer.get(&key).unwrap(), val);
    }
}

#[test]
fn test_idx_get() {
    let mut writer = HashMapInit::<TestKey, usize>::new_resizeable_named(1500, 2000, "test_clear")
        .attach_writer();
    let mut shadow: std::collections::BTreeMap<TestKey, usize> = BTreeMap::new();
    let mut rng = rand::rng();
    do_random_ops(2000, 1500, 0.25, &mut writer, &mut shadow, &mut rng);
    for _ in 0..100 {
        let idx = (rng.next_u32() % 1500) as usize;
        if let Some(pair) = writer.get_at_bucket(idx) {
            {
                let v: *const usize = &pair.1;
                assert_eq!(writer.get_bucket_for_value(v), idx);
            }
            {
                let v: *const usize = &pair.1;
                assert_eq!(writer.get_bucket_for_value(v), idx);
            }
        }
    }
}

#[test]
fn test_shrink() {
    let mut writer = HashMapInit::<TestKey, usize>::new_resizeable_named(1500, 2000, "test_shrink")
        .attach_writer();
    let mut shadow: std::collections::BTreeMap<TestKey, usize> = BTreeMap::new();
    let mut rng = rand::rng();

    do_random_ops(10000, 1500, 0.75, &mut writer, &mut shadow, &mut rng);
    do_shrink(&mut writer, &mut shadow, 1500, 1000);
    assert_eq!(writer.get_num_buckets(), 1000);
    do_deletes(500, &mut writer, &mut shadow);
    do_random_ops(10000, 500, 0.75, &mut writer, &mut shadow, &mut rng);
    assert!(writer.get_num_buckets_in_use() <= 1000);
}

#[test]
fn test_shrink_grow_seq() {
    let mut writer =
        HashMapInit::<TestKey, usize>::new_resizeable_named(1000, 20000, "test_grow_seq")
            .attach_writer();
    let mut shadow: std::collections::BTreeMap<TestKey, usize> = BTreeMap::new();
    let mut rng = rand::rng();

    do_random_ops(500, 1000, 0.1, &mut writer, &mut shadow, &mut rng);
    eprintln!("Shrinking to 750");
    do_shrink(&mut writer, &mut shadow, 1000, 750);
    do_random_ops(200, 1000, 0.5, &mut writer, &mut shadow, &mut rng);
    eprintln!("Growing to 1500");
    writer.grow(1500).unwrap();
    do_random_ops(600, 1500, 0.1, &mut writer, &mut shadow, &mut rng);
    eprintln!("Shrinking to 200");
    while shadow.len() > 100 {
        do_deletes(1, &mut writer, &mut shadow);
    }
    do_shrink(&mut writer, &mut shadow, 1500, 200);
    do_random_ops(50, 1500, 0.25, &mut writer, &mut shadow, &mut rng);
    eprintln!("Growing to 10k");
    writer.grow(10000).unwrap();
    do_random_ops(10000, 5000, 0.25, &mut writer, &mut shadow, &mut rng);
}

#[test]
fn test_bucket_ops() {
    let writer = HashMapInit::<TestKey, usize>::new_resizeable_named(1000, 1200, "test_bucket_ops")
        .attach_writer();
    match writer.entry(1.into()) {
        Entry::Occupied(mut e) => {
            e.insert(2);
        }
        Entry::Vacant(e) => {
            _ = e.insert(2).unwrap();
        }
    }
    assert_eq!(writer.get_num_buckets_in_use(), 1);
    assert_eq!(writer.get_num_buckets(), 1000);
    assert_eq!(*writer.get(&1.into()).unwrap(), 2);
    let pos = match writer.entry(1.into()) {
        Entry::Occupied(e) => {
            assert_eq!(e._key, 1.into());
            e.bucket_pos as usize
        }
        Entry::Vacant(_) => {
            panic!("Insert didn't affect entry");
        }
    };
    assert_eq!(writer.entry_at_bucket(pos).unwrap()._key, 1.into());
    assert_eq!(*writer.get_at_bucket(pos).unwrap(), (1.into(), 2));
    {
        let ptr: *const usize = &*writer.get(&1.into()).unwrap();
        assert_eq!(writer.get_bucket_for_value(ptr), pos);
    }
    writer.remove(&1.into());
    assert!(writer.get(&1.into()).is_none());
}

#[test]
fn test_shrink_zero() {
    let mut writer =
        HashMapInit::<TestKey, usize>::new_resizeable_named(1500, 2000, "test_shrink_zero")
            .attach_writer();
    writer.begin_shrink(0);
    for i in 0..1500 {
        writer.entry_at_bucket(i).map(|x| x.remove());
    }
    writer.finish_shrink().unwrap();
    assert_eq!(writer.get_num_buckets_in_use(), 0);
    let entry = writer.entry(1.into());
    if let Entry::Vacant(v) = entry {
        assert!(v.insert(2).is_err());
    } else {
        panic!("Somehow got non-vacant entry in empty map.")
    }
    writer.grow(50).unwrap();
    let entry = writer.entry(1.into());
    if let Entry::Vacant(v) = entry {
        assert!(v.insert(2).is_ok());
    } else {
        panic!("Somehow got non-vacant entry in empty map.")
    }
    assert_eq!(writer.get_num_buckets_in_use(), 1);
}

#[test]
#[should_panic]
fn test_grow_oom() {
    let writer = HashMapInit::<TestKey, usize>::new_resizeable_named(1500, 2000, "test_grow_oom")
        .attach_writer();
    writer.grow(20000).unwrap();
}

#[test]
#[should_panic]
fn test_shrink_bigger() {
    let mut writer =
        HashMapInit::<TestKey, usize>::new_resizeable_named(1500, 2500, "test_shrink_bigger")
            .attach_writer();
    writer.begin_shrink(2000);
}

#[test]
#[should_panic]
fn test_shrink_early_finish() {
    let writer =
        HashMapInit::<TestKey, usize>::new_resizeable_named(1500, 2500, "test_shrink_early_finish")
            .attach_writer();
    writer.finish_shrink().unwrap();
}

#[test]
#[should_panic]
fn test_shrink_fixed_size() {
    let mut area = [MaybeUninit::uninit(); 10000];
    let init_struct = HashMapInit::<TestKey, usize>::with_fixed(3, &mut area);
    let mut writer = init_struct.attach_writer();
    writer.begin_shrink(1);
}
