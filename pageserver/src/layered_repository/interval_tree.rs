///
/// IntervalTree is data structure for holding intervals. It is generic
/// to make unit testing possible, but the only real user of it is the layer map,
///
/// It's inspired by the "segment tree" or a "statistic tree" as described in
/// https://en.wikipedia.org/wiki/Segment_tree. However, we use a B-tree to hold
/// the points instead of a binary tree. This is called an "interval tree" instead
/// of "segment tree" because the term "segment" is already using Zenith to mean
/// something else. To add to the confusion, there is another data structure known
/// as "interval tree" out there (see https://en.wikipedia.org/wiki/Interval_tree),
/// for storing intervals, but this isn't that.
///
/// The basic idea is to have a B-tree of "interesting Points". At each Point,
/// there is a list of intervals that contain the point. The Points are formed
/// from the start bounds of each interval; there is a Point for each distinct
/// start bound.
///
/// Operations:
///
/// To find intervals that contain a given point, you search the b-tree to find
/// the nearest Point <= search key. Then you just return the list of intervals.
///
/// To insert an interval, find the Point with start key equal to the inserted item.
/// If the Point doesn't exist yet, create it, by copying all the items from the
/// previous Point that cover the new Point. Then walk right, inserting the new
/// interval to all the Points that are contained by the new interval (including the
/// newly created Point).
///
/// To remove an interval, you scan the tree for all the Points that are contained by
/// the removed interval, and remove it from the list in each Point.
///
/// Requirements and assumptions:
///
/// - Can store overlapping items
/// - But there are not many overlapping items
/// - The interval bounds don't change after it is added to the tree
/// - Intervals are uniquely identified by pointer equality. You must not be insert the
///   same interval object twice, and `remove` uses pointer equality to remove the right
///   interval. It is OK to have two intervals with the same bounds, however.
///
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

pub struct IntervalTree<I: ?Sized>
where
    I: IntervalItem,
{
    points: BTreeMap<I::Key, Point<I>>,
}

struct Point<I: ?Sized> {
    /// All intervals that contain this point, in no particular order.
    ///
    /// We assume that there aren't a lot of overlappingg intervals, so that this vector
    /// never grows very large. If that assumption doesn't hold, we could keep this ordered
    /// by the end bound, to speed up `search`. But as long as there are only a few elements,
    /// a linear search is OK.
    elements: Vec<Arc<I>>,
}

/// Abstraction for an interval that can be stored in the tree
///
/// The start bound is inclusive and the end bound is exclusive. End must be greater
/// than start.
pub trait IntervalItem {
    type Key: Ord + Copy + Debug + Sized;

    fn start_key(&self) -> Self::Key;
    fn end_key(&self) -> Self::Key;

    fn bounds(&self) -> Range<Self::Key> {
        self.start_key()..self.end_key()
    }
}

impl<I: ?Sized> IntervalTree<I>
where
    I: IntervalItem,
{
    /// Return an element that contains 'key', or precedes it.
    ///
    /// If there are multiple candidates, returns the one with the highest 'end' key.
    pub fn search(&self, key: I::Key) -> Option<Arc<I>> {
        // Find the greatest point that precedes or is equal to the search key. If there is
        // none, returns None.
        let (_, p) = self.points.range(..=key).next_back()?;

        // Find the element with the highest end key at this point
        let highest_item = p
            .elements
            .iter()
            .reduce(|a, b| {
                // starting with Rust 1.53, could use `std::cmp::min_by_key` here
                if a.end_key() > b.end_key() {
                    a
                } else {
                    b
                }
            })
            .unwrap();
        Some(Arc::clone(highest_item))
    }

    /// Iterate over all items with start bound >= 'key'
    pub fn iter_newer(&self, key: I::Key) -> IntervalIter<I> {
        IntervalIter {
            point_iter: self.points.range(key..),
            elem_iter: None,
        }
    }

    /// Iterate over all items
    pub fn iter(&self) -> IntervalIter<I> {
        IntervalIter {
            point_iter: self.points.range(..),
            elem_iter: None,
        }
    }

    pub fn insert(&mut self, item: Arc<I>) {
        let start_key = item.start_key();
        let end_key = item.end_key();
        assert!(start_key < end_key);
        let bounds = start_key..end_key;

        // Find the starting point and walk forward from there
        let mut found_start_point = false;
        let iter = self.points.range_mut(bounds);
        for (point_key, point) in iter {
            if *point_key == start_key {
                found_start_point = true;
                // It is an error to insert the same item to the tree twice.
                assert!(
                    !point.elements.iter().any(|x| Arc::ptr_eq(x, &item)),
                    "interval is already in the tree"
                );
            }
            point.elements.push(Arc::clone(&item));
        }
        if !found_start_point {
            // Create a new Point for the starting point

            // Look at the previous point, and copy over elements that overlap with this
            // new point
            let mut new_elements: Vec<Arc<I>> = Vec::new();
            if let Some((_, prev_point)) = self.points.range(..start_key).next_back() {
                let overlapping_prev_elements = prev_point
                    .elements
                    .iter()
                    .filter(|x| x.bounds().contains(&start_key))
                    .cloned();

                new_elements.extend(overlapping_prev_elements);
            }
            new_elements.push(item);

            let new_point = Point {
                elements: new_elements,
            };
            self.points.insert(start_key, new_point);
        }
    }

    pub fn remove(&mut self, item: &Arc<I>) {
        // range search points
        let start_key = item.start_key();
        let end_key = item.end_key();
        let bounds = start_key..end_key;

        let mut points_to_remove: Vec<I::Key> = Vec::new();
        let mut found_start_point = false;
        for (point_key, point) in self.points.range_mut(bounds) {
            if *point_key == start_key {
                found_start_point = true;
            }
            let len_before = point.elements.len();
            point.elements.retain(|other| !Arc::ptr_eq(other, item));
            let len_after = point.elements.len();
            assert_eq!(len_after + 1, len_before);
            if len_after == 0 {
                points_to_remove.push(*point_key);
            }
        }
        assert!(found_start_point);

        for k in points_to_remove {
            self.points.remove(&k).unwrap();
        }
    }
}

pub struct IntervalIter<'a, I: ?Sized>
where
    I: IntervalItem,
{
    point_iter: std::collections::btree_map::Range<'a, I::Key, Point<I>>,
    elem_iter: Option<(I::Key, std::slice::Iter<'a, Arc<I>>)>,
}

impl<'a, I> Iterator for IntervalIter<'a, I>
where
    I: IntervalItem + ?Sized,
{
    type Item = Arc<I>;

    fn next(&mut self) -> Option<Self::Item> {
        // Iterate over all elements in all the points in 'point_iter'. To avoid
        // returning the same element twice, we only return each element at its
        // starting point.
        loop {
            // Return next remaining element from the current point
            if let Some((point_key, elem_iter)) = &mut self.elem_iter {
                for elem in elem_iter {
                    if elem.start_key() == *point_key {
                        return Some(Arc::clone(elem));
                    }
                }
            }
            // No more elements at this point. Move to next point.
            if let Some((point_key, point)) = self.point_iter.next() {
                self.elem_iter = Some((*point_key, point.elements.iter()));
                continue;
            } else {
                // No more points, all done
                return None;
            }
        }
    }
}

impl<I: ?Sized> Default for IntervalTree<I>
where
    I: IntervalItem,
{
    fn default() -> Self {
        IntervalTree {
            points: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt;

    #[derive(Debug)]
    struct MockItem {
        start_key: u32,
        end_key: u32,
        val: String,
    }
    impl IntervalItem for MockItem {
        type Key = u32;

        fn start_key(&self) -> u32 {
            self.start_key
        }
        fn end_key(&self) -> u32 {
            self.end_key
        }
    }
    impl MockItem {
        fn new(start_key: u32, end_key: u32) -> Self {
            MockItem {
                start_key,
                end_key,
                val: format!("{}-{}", start_key, end_key),
            }
        }
        fn new_str(start_key: u32, end_key: u32, val: &str) -> Self {
            MockItem {
                start_key,
                end_key,
                val: format!("{}-{}: {}", start_key, end_key, val),
            }
        }
    }
    impl fmt::Display for MockItem {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.val)
        }
    }
    #[rustfmt::skip]
    fn assert_search(
        tree: &IntervalTree<MockItem>,
        key: u32,
        expected: &[&str],
    ) -> Option<Arc<MockItem>> {
        if let Some(v) = tree.search(key) {
            let vstr = v.to_string();

            assert!(!expected.is_empty(), "search with {} returned {}, expected None", key, v);
            assert!(
                expected.contains(&vstr.as_str()),
                "search with {} returned {}, expected one of: {:?}",
                key, v, expected,
            );

            Some(v)
        } else {
            assert!(
                expected.is_empty(),
                "search with {} returned None, expected one of {:?}",
                key, expected
            );
            None
        }
    }

    fn assert_contents(tree: &IntervalTree<MockItem>, expected: &[&str]) {
        let mut contents: Vec<String> = tree.iter().map(|e| e.to_string()).collect();
        contents.sort();
        assert_eq!(contents, expected);
    }

    fn dump_tree(tree: &IntervalTree<MockItem>) {
        for (point_key, point) in tree.points.iter() {
            print!("{}:", point_key);
            for e in point.elements.iter() {
                print!(" {}", e);
            }
            println!();
        }
    }

    #[test]
    fn test_interval_tree_simple() {
        let mut tree: IntervalTree<MockItem> = IntervalTree::default();

        // Simple, non-overlapping ranges.
        tree.insert(Arc::new(MockItem::new(10, 11)));
        tree.insert(Arc::new(MockItem::new(11, 12)));
        tree.insert(Arc::new(MockItem::new(12, 13)));
        tree.insert(Arc::new(MockItem::new(18, 19)));
        tree.insert(Arc::new(MockItem::new(17, 18)));
        tree.insert(Arc::new(MockItem::new(15, 16)));

        assert_search(&tree, 9, &[]);
        assert_search(&tree, 10, &["10-11"]);
        assert_search(&tree, 11, &["11-12"]);
        assert_search(&tree, 12, &["12-13"]);
        assert_search(&tree, 13, &["12-13"]);
        assert_search(&tree, 14, &["12-13"]);
        assert_search(&tree, 15, &["15-16"]);
        assert_search(&tree, 16, &["15-16"]);
        assert_search(&tree, 17, &["17-18"]);
        assert_search(&tree, 18, &["18-19"]);
        assert_search(&tree, 19, &["18-19"]);
        assert_search(&tree, 20, &["18-19"]);

        // remove a few entries and search around them again
        tree.remove(&assert_search(&tree, 10, &["10-11"]).unwrap()); // first entry
        tree.remove(&assert_search(&tree, 12, &["12-13"]).unwrap()); // entry in the middle
        tree.remove(&assert_search(&tree, 18, &["18-19"]).unwrap()); // last entry
        assert_search(&tree, 9, &[]);
        assert_search(&tree, 10, &[]);
        assert_search(&tree, 11, &["11-12"]);
        assert_search(&tree, 12, &["11-12"]);
        assert_search(&tree, 14, &["11-12"]);
        assert_search(&tree, 15, &["15-16"]);
        assert_search(&tree, 17, &["17-18"]);
        assert_search(&tree, 18, &["17-18"]);
    }

    #[test]
    fn test_interval_tree_overlap() {
        let mut tree: IntervalTree<MockItem> = IntervalTree::default();

        // Overlapping items
        tree.insert(Arc::new(MockItem::new(22, 24)));
        tree.insert(Arc::new(MockItem::new(23, 25)));
        let x24_26 = Arc::new(MockItem::new(24, 26));
        tree.insert(Arc::clone(&x24_26));
        let x26_28 = Arc::new(MockItem::new(26, 28));
        tree.insert(Arc::clone(&x26_28));
        tree.insert(Arc::new(MockItem::new(25, 27)));

        assert_search(&tree, 22, &["22-24"]);
        assert_search(&tree, 23, &["22-24", "23-25"]);
        assert_search(&tree, 24, &["23-25", "24-26"]);
        assert_search(&tree, 25, &["24-26", "25-27"]);
        assert_search(&tree, 26, &["25-27", "26-28"]);
        assert_search(&tree, 27, &["26-28"]);
        assert_search(&tree, 28, &["26-28"]);
        assert_search(&tree, 29, &["26-28"]);

        tree.remove(&x24_26);
        tree.remove(&x26_28);
        assert_search(&tree, 23, &["22-24", "23-25"]);
        assert_search(&tree, 24, &["23-25"]);
        assert_search(&tree, 25, &["25-27"]);
        assert_search(&tree, 26, &["25-27"]);
        assert_search(&tree, 27, &["25-27"]);
        assert_search(&tree, 28, &["25-27"]);
        assert_search(&tree, 29, &["25-27"]);
    }

    #[test]
    fn test_interval_tree_nested() {
        let mut tree: IntervalTree<MockItem> = IntervalTree::default();

        // Items containing other items
        tree.insert(Arc::new(MockItem::new(31, 39)));
        tree.insert(Arc::new(MockItem::new(32, 34)));
        tree.insert(Arc::new(MockItem::new(33, 35)));
        tree.insert(Arc::new(MockItem::new(30, 40)));

        assert_search(&tree, 30, &["30-40"]);
        assert_search(&tree, 31, &["30-40", "31-39"]);
        assert_search(&tree, 32, &["30-40", "32-34", "31-39"]);
        assert_search(&tree, 33, &["30-40", "32-34", "33-35", "31-39"]);
        assert_search(&tree, 34, &["30-40", "33-35", "31-39"]);
        assert_search(&tree, 35, &["30-40", "31-39"]);
        assert_search(&tree, 36, &["30-40", "31-39"]);
        assert_search(&tree, 37, &["30-40", "31-39"]);
        assert_search(&tree, 38, &["30-40", "31-39"]);
        assert_search(&tree, 39, &["30-40"]);
        assert_search(&tree, 40, &["30-40"]);
        assert_search(&tree, 41, &["30-40"]);
    }

    #[test]
    fn test_interval_tree_duplicates() {
        let mut tree: IntervalTree<MockItem> = IntervalTree::default();

        // Duplicate keys
        let item_a = Arc::new(MockItem::new_str(55, 56, "a"));
        tree.insert(Arc::clone(&item_a));
        let item_b = Arc::new(MockItem::new_str(55, 56, "b"));
        tree.insert(Arc::clone(&item_b));
        let item_c = Arc::new(MockItem::new_str(55, 56, "c"));
        tree.insert(Arc::clone(&item_c));
        let item_d = Arc::new(MockItem::new_str(54, 56, "d"));
        tree.insert(Arc::clone(&item_d));
        let item_e = Arc::new(MockItem::new_str(55, 57, "e"));
        tree.insert(Arc::clone(&item_e));

        dump_tree(&tree);

        assert_search(
            &tree,
            55,
            &["55-56: a", "55-56: b", "55-56: c", "54-56: d", "55-57: e"],
        );
        tree.remove(&item_b);
        dump_tree(&tree);

        assert_contents(&tree, &["54-56: d", "55-56: a", "55-56: c", "55-57: e"]);

        tree.remove(&item_d);
        dump_tree(&tree);
        assert_contents(&tree, &["55-56: a", "55-56: c", "55-57: e"]);
    }

    #[test]
    #[should_panic]
    fn test_interval_tree_insert_twice() {
        let mut tree: IntervalTree<MockItem> = IntervalTree::default();

        // Inserting the same item twice is not cool
        let item = Arc::new(MockItem::new(1, 2));
        tree.insert(Arc::clone(&item));
        tree.insert(Arc::clone(&item)); // fails assertion
    }
}
