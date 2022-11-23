use std::ops::Range;

pub mod naive;
pub mod ops;

/// Should be a monoid:
/// * Identity element: for all a: combine(new_for_empty_range(), a) = combine(a, new_for_empty_range()) = a
/// * Associativity: for all a, b, c: combine(combine(a, b), c) == combine(a, combine(b, c))
pub trait RangeQueryResult<Key>: Sized {
    fn new_for_empty_range() -> Self;

    // Contract: left_range.end == right_range.start
    // left_range.start == left_range.end == right_range.start == right_range.end is still possible
    fn combine(left: &Self, left_range: &Range<Key>, right: &Self, right_range: &Range<Key>) -> Self
    where
        Self: Clone,
    {
        let mut left = left.clone();
        Self::add(&mut left, left_range, right, right_range);
        left
    }

    // TODO: does it work with non-Clone?
    fn add(left: &mut Self, left_range: &Range<Key>, right: &Self, right_range: &Range<Key>)
    where
        Self: Clone,
    {
        *left = Self::combine(left, left_range, right, right_range);
    }
}

pub trait LazyRangeInitializer<Result: RangeQueryResult<Key>, Key> {
    fn get(&self, range: &Range<Key>) -> Result;
}

/// Should be a monoid:
/// * Identity element: for all op: compose(no_op(), op) == compose(op, no_op()) == op
/// * Associativity: for all op_1, op_2, op_3: compose(compose(op_1, op_2), op_3) == compose(op_1, compose(op_2, op_3))
///
/// Should left act on Result:
/// * Identity operation: for all r: no_op().apply(r) == r
/// * Compatibility: for all op_1, op_2, r: op_1.apply(op_2.apply(r)) == compose(op_1, op_2).apply(r)
pub trait RangeModification<Key> {
    type Result: RangeQueryResult<Key>;

    fn no_op() -> Self;
    fn apply(&self, result: &mut Self::Result, range: &Range<Key>);
    fn compose(later: &Self, earlier: &mut Self);
}

pub trait VecVersion<Modification: RangeModification<Key>, Key>: Clone {
    fn get(&self, keys: Range<Key>) -> Modification::Result;
    fn modify(&mut self, keys: Range<Key>, modification: Modification);
}

pub trait PersistentVecStorage<
    Modification: RangeModification<Key>,
    Initializer: LazyRangeInitializer<Modification::Result, Key>,
    Key,
>
{
    type Version: VecVersion<Modification, Key>;
    fn new(all_keys: Range<Key>, initializer: Initializer) -> Self::Version;
}
