//! # Segment Tree
//! It is a competitive programming folklore data structure. Do not confuse with the interval tree.

use crate::{LazyRangeInitializer, PersistentVecStorage, RangeQueryResult, VecReadableVersion};
use std::ops::Range;
use std::rc::Rc;

pub trait MidpointableKey: Clone + Ord + Sized {
    fn midpoint(range: &Range<Self>) -> Self;
}

pub trait RangeModification<Key>: Clone + crate::RangeModification<Key> {}

// TODO: use trait alias when stabilized
impl<T: Clone + crate::RangeModification<Key>, Key> RangeModification<Key> for T {}

#[derive(Debug)]
struct Node<Modification: RangeModification<Key>, Key> {
    result: Modification::Result,
    modify_children: Modification,
    left: Option<Rc<Self>>,
    right: Option<Rc<Self>>,
}

// Manual implementation because we don't need `Key: Clone` for this, unlike with `derive`.
impl<Modification: RangeModification<Key>, Key> Clone for Node<Modification, Key> {
    fn clone(&self) -> Self {
        Node {
            result: self.result.clone(),
            modify_children: self.modify_children.clone(),
            left: self.left.clone(),
            right: self.right.clone(),
        }
    }
}

impl<Modification: RangeModification<Key>, Key> Node<Modification, Key> {
    fn new<Initializer: LazyRangeInitializer<Modification::Result, Key>>(
        range: &Range<Key>,
        initializer: &Initializer,
    ) -> Self {
        Node {
            result: initializer.get(range),
            modify_children: Modification::no_op(),
            left: None,
            right: None,
        }
    }

    pub fn apply(&mut self, modification: &Modification, range: &Range<Key>) {
        modification.apply(&mut self.result, range);
        Modification::compose(modification, &mut self.modify_children);
        if self.modify_children.is_reinitialization() {
            self.left = None;
            self.right = None;
        }
    }

    pub fn force_children<Initializer: LazyRangeInitializer<Modification::Result, Key>>(
        &mut self,
        initializer: &Initializer,
        range_left: &Range<Key>,
        range_right: &Range<Key>,
    ) {
        let left = Rc::make_mut(
            self.left
                .get_or_insert_with(|| Rc::new(Node::new(&range_left, initializer))),
        );
        let right = Rc::make_mut(
            self.right
                .get_or_insert_with(|| Rc::new(Node::new(&range_right, initializer))),
        );
        left.apply(&self.modify_children, &range_left);
        right.apply(&self.modify_children, &range_right);
        self.modify_children = Modification::no_op();
    }

    pub fn recalculate_from_children(&mut self, range_left: &Range<Key>, range_right: &Range<Key>) {
        assert!(self.modify_children.is_no_op());
        assert!(self.left.is_some());
        assert!(self.right.is_some());
        self.result = Modification::Result::combine(
            &self.left.as_ref().unwrap().result,
            &range_left,
            &self.right.as_ref().unwrap().result,
            &range_right,
        );
    }
}

fn split_range<Key: MidpointableKey>(range: &Range<Key>) -> (Range<Key>, Range<Key>) {
    let range_left = range.start.clone()..MidpointableKey::midpoint(range);
    let range_right = range_left.end.clone()..range.end.clone();
    (range_left, range_right)
}

pub struct PersistentSegmentTreeVersion<
    Modification: RangeModification<Key>,
    Initializer: LazyRangeInitializer<Modification::Result, Key>,
    Key: Clone,
> {
    root: Rc<Node<Modification, Key>>,
    all_keys: Range<Key>,
    initializer: Rc<Initializer>,
}

// Manual implementation because we don't need `Key: Clone` for this, unlike with `derive`.
impl<
        Modification: RangeModification<Key>,
        Initializer: LazyRangeInitializer<Modification::Result, Key>,
        Key: Clone,
    > Clone for PersistentSegmentTreeVersion<Modification, Initializer, Key>
{
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            all_keys: self.all_keys.clone(),
            initializer: self.initializer.clone(),
        }
    }
}

fn get<
    Modification: RangeModification<Key>,
    Initializer: LazyRangeInitializer<Modification::Result, Key>,
    Key: MidpointableKey,
>(
    node: &mut Rc<Node<Modification, Key>>,
    node_keys: &Range<Key>,
    initializer: &Initializer,
    keys: &Range<Key>,
) -> Modification::Result {
    if node_keys.end <= keys.start || keys.end <= node_keys.start {
        return Modification::Result::new_for_empty_range();
    }
    if keys.start <= node_keys.start && node_keys.end <= keys.end {
        return node.result.clone();
    }
    let node = Rc::make_mut(node);
    let (left_keys, right_keys) = split_range(node_keys);
    node.force_children(initializer, &left_keys, &right_keys);
    let mut result = get(node.left.as_mut().unwrap(), &left_keys, initializer, keys);
    Modification::Result::add(
        &mut result,
        &left_keys,
        &get(node.right.as_mut().unwrap(), &right_keys, initializer, keys),
        &right_keys,
    );
    result
}

fn modify<
    Modification: RangeModification<Key>,
    Initializer: LazyRangeInitializer<Modification::Result, Key>,
    Key: MidpointableKey,
>(
    node: &mut Rc<Node<Modification, Key>>,
    node_keys: &Range<Key>,
    initializer: &Initializer,
    keys: &Range<Key>,
    modification: &Modification,
) {
    if modification.is_no_op() || node_keys.end <= keys.start || keys.end <= node_keys.start {
        return;
    }
    let node = Rc::make_mut(node);
    if keys.start <= node_keys.start && node_keys.end <= keys.end {
        node.apply(modification, node_keys);
        return;
    }
    let (left_keys, right_keys) = split_range(node_keys);
    node.force_children(initializer, &left_keys, &right_keys);
    modify(
        node.left.as_mut().unwrap(),
        &left_keys,
        initializer,
        keys,
        &modification,
    );
    modify(
        node.right.as_mut().unwrap(),
        &right_keys,
        initializer,
        keys,
        &modification,
    );
    node.recalculate_from_children(&left_keys, &right_keys);
}

impl<
        Modification: RangeModification<Key>,
        Initializer: LazyRangeInitializer<Modification::Result, Key>,
        Key: MidpointableKey,
    > VecReadableVersion<Modification, Key>
    for PersistentSegmentTreeVersion<Modification, Initializer, Key>
{
    fn get(&self, keys: Range<Key>) -> Modification::Result {
        get(
            &mut self.root.clone(), // TODO: do not always force a branch
            &self.all_keys,
            self.initializer.as_ref(),
            &keys,
        )
    }
}

pub struct PersistentSegmentTree<
    Modification: RangeModification<Key>,
    Initializer: LazyRangeInitializer<Modification::Result, Key>,
    Key: MidpointableKey,
>(PersistentSegmentTreeVersion<Modification, Initializer, Key>);

impl<
        Modification: RangeModification<Key>,
        Initializer: LazyRangeInitializer<Modification::Result, Key>,
        Key: MidpointableKey,
    > VecReadableVersion<Modification, Key>
    for PersistentSegmentTree<Modification, Initializer, Key>
{
    fn get(&self, keys: Range<Key>) -> Modification::Result {
        self.0.get(keys)
    }
}

impl<
        Modification: RangeModification<Key>,
        Initializer: LazyRangeInitializer<Modification::Result, Key>,
        Key: MidpointableKey,
    > PersistentVecStorage<Modification, Initializer, Key>
    for PersistentSegmentTree<Modification, Initializer, Key>
{
    fn new(all_keys: Range<Key>, initializer: Initializer) -> Self {
        PersistentSegmentTree(PersistentSegmentTreeVersion {
            root: Rc::new(Node::new(&all_keys, &initializer)),
            all_keys: all_keys,
            initializer: Rc::new(initializer),
        })
    }

    type FrozenVersion = PersistentSegmentTreeVersion<Modification, Initializer, Key>;

    fn modify(&mut self, keys: Range<Key>, modification: Modification) {
        modify(
            &mut self.0.root, // TODO: do not always force a branch
            &self.0.all_keys,
            self.0.initializer.as_ref(),
            &keys,
            &modification,
        )
    }

    fn freeze(&mut self) -> Self::FrozenVersion {
        self.0.clone()
    }
}
