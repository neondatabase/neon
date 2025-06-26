use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::mem::uninitialized;
use std::mem::MaybeUninit;
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
    let mut w = HashMapInit::<TestKey, usize>::new_resizeable_named(
		100000, 120000, "test_inserts"
	).attach_writer();

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
        let op = TestOp(key, if rng.random_bool(del_prob) { Some(i) } else { None });
        apply_op(&op, writer, shadow);
    }
}

fn do_deletes(
	num_ops: usize,
	writer: &mut HashMapAccess<TestKey, usize>,
	shadow: &mut BTreeMap<TestKey, usize>,
) {
	for i in 0..num_ops {
		let (k, _) = shadow.pop_first().unwrap();
		let hash = writer.get_hash_value(&k);
		writer.remove_with_hash(&k, hash);
	}
}

fn do_shrink(
	writer: &mut HashMapAccess<TestKey, usize>,
	shadow: &mut BTreeMap<TestKey, usize>,
	from: u32,
	to: u32
) {
	writer.begin_shrink(to);
	while writer.get_num_buckets_in_use() > to as usize {
		let (k, _) = shadow.pop_first().unwrap();
		let hash = writer.get_hash_value(&k);
		let entry = writer.entry_with_hash(k, hash);
		if let Entry::Occupied(mut e) = entry {
			e.remove();
		}
	}
	writer.finish_shrink().unwrap();
}

#[test]
fn random_ops() {
	let mut writer = HashMapInit::<TestKey, usize>::new_resizeable_named(
		100000, 120000, "test_random"
	).attach_writer();
    let mut shadow: std::collections::BTreeMap<TestKey, usize> = BTreeMap::new();
	
    let distribution = Zipf::new(u128::MAX as f64, 1.1).unwrap();
    let mut rng = rand::rng();
    for i in 0..100000 {
        let key: TestKey = (rng.sample(distribution) as u128).into();

        let op = TestOp(key, if rng.random_bool(0.75) { Some(i) } else { None });

        apply_op(&op, &mut writer, &mut shadow);

        if i % 1000 == 0 {
            eprintln!("{i} ops processed");
        }
    }
}


#[test]
fn test_shuffle() {
    let mut writer = HashMapInit::<TestKey, usize>::new_resizeable_named(
		1000, 1200, "test_shuf"
	).attach_writer();
    let mut shadow: std::collections::BTreeMap<TestKey, usize> = BTreeMap::new();
    let mut rng = rand::rng();

    do_random_ops(10000, 1000, 0.75, &mut writer, &mut shadow, &mut rng);
    writer.shuffle();
	do_random_ops(10000, 1000, 0.75, &mut writer, &mut shadow, &mut rng);
}

#[test]
fn test_grow() {
    let mut writer = HashMapInit::<TestKey, usize>::new_resizeable_named(
		1000, 2000, "test_grow"
	).attach_writer();
    let mut shadow: std::collections::BTreeMap<TestKey, usize> = BTreeMap::new();
    let mut rng = rand::rng();

    do_random_ops(10000, 1000, 0.75, &mut writer, &mut shadow, &mut rng);
    writer.grow(1500).unwrap();
	do_random_ops(10000, 1500, 0.75, &mut writer, &mut shadow, &mut rng);
}

#[test]
fn test_shrink() {
    let mut writer = HashMapInit::<TestKey, usize>::new_resizeable_named(
		1500, 2000, "test_shrink"
	).attach_writer();
    let mut shadow: std::collections::BTreeMap<TestKey, usize> = BTreeMap::new();
    let mut rng = rand::rng();
	
    do_random_ops(10000, 1500, 0.75, &mut writer, &mut shadow, &mut rng);
    do_shrink(&mut writer, &mut shadow, 1500, 1000);
	do_deletes(500, &mut writer, &mut shadow);
	do_random_ops(10000, 500, 0.75, &mut writer, &mut shadow, &mut rng);
	assert!(writer.get_num_buckets_in_use() <= 1000);
}

#[test]
fn test_shrink_grow_seq() {
    let mut writer = HashMapInit::<TestKey, usize>::new_resizeable_named(
		1000, 20000, "test_grow_seq"
	).attach_writer();
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
	do_shrink(&mut writer, &mut shadow, 1500, 200);
	do_deletes(100, &mut writer, &mut shadow);
	do_random_ops(50, 1500, 0.25, &mut writer, &mut shadow, &mut rng);
	eprintln!("Growing to 10k");
	writer.grow(10000).unwrap();
	do_random_ops(10000, 5000, 0.25, &mut writer, &mut shadow, &mut rng);
}

#[test]
fn test_bucket_ops() {
	let mut writer = HashMapInit::<TestKey, usize>::new_resizeable_named(
		1000, 1200, "test_bucket_ops"
	).attach_writer();
	let hash = writer.get_hash_value(&1.into());
	match writer.entry_with_hash(1.into(), hash) {
		Entry::Occupied(mut e) => { e.insert(2); },
		Entry::Vacant(e) => { e.insert(2).unwrap(); },
	}
	assert_eq!(writer.get_num_buckets_in_use(), 1);
	assert_eq!(writer.get_num_buckets(), 1000);
	assert_eq!(writer.get_with_hash(&1.into(), hash), Some(&2));
	let pos = match writer.entry_with_hash(1.into(), hash) {
		Entry::Occupied(e) => {
			assert_eq!(e._key, 1.into());
			let pos = e.bucket_pos as usize;
			assert_eq!(writer.entry_at_bucket(pos).unwrap()._key, 1.into());
			assert_eq!(writer.get_at_bucket(pos), Some(&(1.into(), 2)));
			pos
		},
		Entry::Vacant(_) => { panic!("Insert didn't affect entry"); },
	};
	let ptr: *const usize = writer.get_with_hash(&1.into(), hash).unwrap();
	assert_eq!(writer.get_bucket_for_value(ptr), pos);
	writer.remove_with_hash(&1.into(), hash);
	assert_eq!(writer.get_with_hash(&1.into(), hash), None);
}

#[test]
fn test_shrink_zero() {
	let mut writer = HashMapInit::<TestKey, usize>::new_resizeable_named(
		1500, 2000, "test_shrink_zero"
	).attach_writer();
	writer.begin_shrink(0);
	for i in 0..1500 {
		writer.entry_at_bucket(i).map(|x| x.remove());
	}
	writer.finish_shrink().unwrap();
	assert_eq!(writer.get_num_buckets_in_use(), 0);
	let hash = writer.get_hash_value(&1.into());
	let entry = writer.entry_with_hash(1.into(), hash);
	if let Entry::Vacant(v) = entry {
		assert!(v.insert(2).is_err());
	} else {
		panic!("Somehow got non-vacant entry in empty map.")
	}
	writer.grow(50).unwrap();
	let entry = writer.entry_with_hash(1.into(), hash);
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
    let mut writer = HashMapInit::<TestKey, usize>::new_resizeable_named(
		1500, 2000, "test_grow_oom"
	).attach_writer();
	writer.grow(20000).unwrap();
}

#[test]
#[should_panic]
fn test_shrink_bigger() {
    let mut writer = HashMapInit::<TestKey, usize>::new_resizeable_named(
		1500, 2500, "test_shrink_bigger"
	).attach_writer();
	writer.begin_shrink(2000);
}

#[test]
#[should_panic]
fn test_shrink_early_finish() {
    let mut writer = HashMapInit::<TestKey, usize>::new_resizeable_named(
		1500, 2500, "test_shrink_early_finish"
	).attach_writer();
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
