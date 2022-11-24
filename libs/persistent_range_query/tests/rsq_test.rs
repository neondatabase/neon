use persistent_range_query::naive::*;
use persistent_range_query::ops::rsq::AddAssignModification::Add;
use persistent_range_query::ops::rsq::*;
use persistent_range_query::ops::SameElementsInitializer;
use persistent_range_query::segment_tree::{MidpointableKey, PersistentSegmentTree};
use persistent_range_query::{PersistentVecStorage, VecReadableVersion};
use rand::{Rng, SeedableRng};
use std::ops::Range;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct K(u16);

impl IndexableKey for K {
    fn index(all_keys: &Range<Self>, key: &Self) -> usize {
        (key.0 as usize) - (all_keys.start.0 as usize)
    }

    fn element_range(all_keys: &Range<Self>, index: usize) -> Range<Self> {
        K(all_keys.start.0 + index as u16)..K(all_keys.start.0 + index as u16 + 1)
    }
}

impl SumOfSameElements<K> for i32 {
    fn sum(initial_element_value: &Self, keys: &Range<K>) -> Self {
        initial_element_value * (keys.end.0 - keys.start.0) as Self
    }
}

impl MidpointableKey for K {
    fn midpoint(range: &Range<Self>) -> Self {
        K(range.start.0 + (range.end.0 - range.start.0) / 2)
    }
}

fn test_storage<
    S: PersistentVecStorage<AddAssignModification<i32>, SameElementsInitializer<i32>, K>,
>() {
    let mut s = S::new(K(0)..K(12), SameElementsInitializer::new(0i32));
    assert_eq!(*s.get(&(K(0)..K(12))).sum(), 0);

    s.modify(&(K(2)..K(5)), &AddAssignModification::Add(3));
    assert_eq!(*s.get(&(K(0)..K(12))).sum(), 3 + 3 + 3);
    let s_old = s.freeze();

    s.modify(&(K(3)..K(6)), &AddAssignModification::Assign(10));
    assert_eq!(*s.get(&(K(0)..K(12))).sum(), 3 + 10 + 10 + 10);

    s.modify(&(K(4)..K(7)), &AddAssignModification::Add(2));
    assert_eq!(*s.get(&(K(0)..K(12))).sum(), 3 + 10 + 12 + 12 + 2);

    assert_eq!(*s.get(&(K(4)..K(6))).sum(), 12 + 12);
    assert_eq!(*s_old.get(&(K(4)..K(6))).sum(), 3);
}

#[test]
fn test_naive() {
    test_storage::<NaiveVecStorage<_, _, _>>();
}

#[test]
fn test_segment_tree() {
    test_storage::<PersistentSegmentTree<_, _, _>>();
}

#[test]
fn test_stress() {
    const LEN: u16 = 17_238;
    const OPERATIONS: i32 = 20_000;

    let mut rng = rand::rngs::StdRng::seed_from_u64(0);
    let mut naive: NaiveVecStorage<AddAssignModification<i32>, _, _> =
        NaiveVecStorage::new(K(0)..K(LEN), SameElementsInitializer::new(2i32));
    let mut segm_tree: PersistentSegmentTree<AddAssignModification<i32>, _, _> =
        PersistentSegmentTree::new(K(0)..K(LEN), SameElementsInitializer::new(2i32));

    fn gen_range(rng: &mut impl Rng) -> Range<K> {
        let l: u16 = rng.gen_range(0..LEN);
        let r: u16 = rng.gen_range(0..LEN);
        if l <= r {
            K(l)..K(r)
        } else {
            K(r)..K(l)
        }
    }

    for _ in 0..2 {
        let checksum_range = gen_range(&mut rng);
        let checksum_before: i32 = *naive.get(&checksum_range).sum();
        assert_eq!(checksum_before, *segm_tree.get(&checksum_range).sum());

        let naive_before = naive.freeze();
        let segm_tree_before = segm_tree.freeze();
        assert_eq!(checksum_before, *naive_before.get(&checksum_range).sum());
        assert_eq!(checksum_before, *segm_tree.get(&checksum_range).sum());

        for _ in 0..OPERATIONS {
            {
                let range = gen_range(&mut rng);
                assert_eq!(naive.get(&range).sum(), segm_tree.get(&range).sum());
            }
            {
                let range = gen_range(&mut rng);
                let val = rng.gen_range(-10i32..=10i32);
                let op = Add(val);
                naive.modify(&range, &op);
                segm_tree.modify(&range, &op);
            }
        }

        assert_eq!(checksum_before, *naive_before.get(&checksum_range).sum());
        assert_eq!(
            checksum_before,
            *segm_tree_before.get(&checksum_range).sum()
        );
    }
}
