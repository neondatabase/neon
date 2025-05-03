use std::collections::HashSet;

use crate::ArtMultiSlabAllocator;
use crate::TreeInitStruct;

use crate::{Key, Value};

use rand::seq::SliceRandom;
use rand::thread_rng;

const TEST_KEY_LEN: usize = 16;

#[derive(Clone, Copy, Debug)]
struct TestKey([u8; TEST_KEY_LEN]);

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

impl Value for usize {}

fn test_inserts<K: Into<TestKey> + Copy>(keys: &[K]) {
    const MEM_SIZE: usize = 10000000;
    let mut area = Box::new_uninit_slice(MEM_SIZE);

    let allocator = ArtMultiSlabAllocator::new(&mut area);

    let init_struct = TreeInitStruct::<TestKey, usize, _>::new(allocator);
    let tree_writer = init_struct.attach_writer();

    for (idx, k) in keys.iter().enumerate() {
        let mut w = tree_writer.start_write();
        w.insert(&(*k).into(), idx);
        eprintln!("INSERTED {:?}", Into::<TestKey>::into(*k));
    }

    //tree_writer.start_read().dump();

    for (idx, k) in keys.iter().enumerate() {
        let r = tree_writer.start_read();
        let value = r.get(&(*k).into());
        assert_eq!(value, Some(idx));
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
        keys.shuffle(&mut thread_rng());
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
