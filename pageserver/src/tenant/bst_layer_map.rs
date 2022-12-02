use std::collections::BTreeMap;

use rpds::RedBlackTreeMap;


pub struct BSTLM {
    head: RedBlackTreeMap<u32, String>,
    historic: BTreeMap<u32, RedBlackTreeMap<u32, String>>,
}

/// Layer map (good enough for benchmarks) implemented using persistent segment tree
impl BSTLM {
    pub fn new() -> Self {
        BSTLM {
            head: RedBlackTreeMap::default(),
            historic: BTreeMap::default(),
        }
    }

    pub fn insert(self: &mut Self, key_begin: u32, key_end: u32, lsn: u32, value: String) {
        self.head.insert_mut(key_begin, value.clone());
        self.head.insert_mut(key_end, value.clone()); // TODO wrong value
        // TODO also remove what's in between
        self.historic.insert(lsn, self.head.clone());  // O(1) clone
    }

    pub fn query(self: &Self, key: u32, lsn: u32) -> Option<&String> {
        let version = self.historic.range(0..=lsn).rev().next()?.1;
        Some(version.range(key..).next()?.1)
    }
}


fn test_bstlm() {
    let mut bstlm = BSTLM::new();
    bstlm.insert(0, 5, 100, "layer 1".to_string());
    bstlm.insert(3, 9, 110, "layer 2".to_string());

    dbg!(bstlm.query(1, 105));
    dbg!(bstlm.query(4, 105));
    dbg!(bstlm.query(4, 115));
}

#[test]
fn test_bstlm_() {
    test_bstlm()
}
