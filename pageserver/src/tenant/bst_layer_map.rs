use std::collections::BTreeMap;

use rpds::RedBlackTreeMap;


/// Layer map implemented using persistent binary search tree.
/// This implementation is only good enough to run benchmarks,
/// so it's missing unnecessary details. Values are String for now.
pub struct BSTLM {
    /// Mapping key to the latest layer (if any) until the next key
    head: RedBlackTreeMap<u32, Option<String>>,

    /// All previous states of `self.head`
    historic: BTreeMap<u32, RedBlackTreeMap<u32, Option<String>>>,
}

impl std::fmt::Debug for BSTLM {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let head_vec: Vec<_> = self.head.iter().collect();
        write!(f, "BSTLM: head: {:?}", head_vec)
    }
}

impl BSTLM {
    pub fn new() -> Self {
        BSTLM {
            head: RedBlackTreeMap::default(),
            historic: BTreeMap::default(),
        }
    }

    pub fn insert(self: &mut Self, key_begin: u32, key_end: u32, lsn: u32, value: String) {
        // TODO check for off-by-one errors

        // NOTE The order of the following lines is important!!

        // Preserve information after right endpoint
        let value_at_end = match self.head.range(0..key_end).last() {
            Some((_, Some(v))) => Some(v.clone()),
            Some((_, None)) => None,
            None => None,
        };
        self.head.insert_mut(key_end, value_at_end);

        // Insert the left endpoint
        self.head.insert_mut(key_begin, Some(value.clone()));

        // Cover the inside of the interval
        let to_remove: Vec<_> = self.head.range((key_begin + 1)..key_end)
            .map(|(k, _)| k.clone())
            .collect();
        for key in to_remove {
            self.head.remove_mut(&key);
        }

        // Remember history. Clone is O(1)
        self.historic.insert(lsn, self.head.clone());
    }

    pub fn query(self: &Self, key: u32, lsn: u32) -> Option<&String> {
        // TODO check for off-by-one errors

        let version = self.historic.range(0..=lsn).rev().next()?.1;
        version.range(0..=key).rev().next()?.1.as_ref()
    }

    // TODO Add API for delta layers with lsn range.
    //      The easy solution is to only store images, and then from every
    //      image point to deltas on top of it. There might be something
    //      nicer but we have this solution as backup.
}


#[test]
fn test_bstlm() {
    let mut bstlm = BSTLM::new();
    bstlm.insert(0, 5, 100, "Layer 1".to_string());
    dbg!(&bstlm);
    bstlm.insert(3, 9, 110, "Layer 2".to_string());
    dbg!(&bstlm);
    bstlm.insert(5, 6, 120, "Layer 3".to_string());
    dbg!(&bstlm);

    // After Layer 1 insertion
    assert_eq!(bstlm.query(1, 105), Some(&"Layer 1".to_string()));
    assert_eq!(bstlm.query(4, 105), Some(&"Layer 1".to_string()));

    // After Layer 2 insertion
    assert_eq!(bstlm.query(4, 115), Some(&"Layer 2".to_string()));
    assert_eq!(bstlm.query(8, 115), Some(&"Layer 2".to_string()));
    assert_eq!(bstlm.query(11, 115), None);

    // After Layer 3 insertion
    assert_eq!(bstlm.query(4, 125), Some(&"Layer 2".to_string()));
    assert_eq!(bstlm.query(5, 125), Some(&"Layer 3".to_string()));

    assert_eq!(bstlm.query(7, 125), Some(&"Layer 2".to_string()));
}
