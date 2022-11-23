//! # Range Sum Query

use crate::ops::SameElementsInitializer;
use crate::{LazyRangeInitializer, RangeModification, RangeQueryResult};
use std::borrow::Borrow;
use std::ops::{AddAssign, Mul, Range, Sub};

// TODO: commutative Add

#[derive(Clone, Copy, Debug)]
pub struct SumResult<T> {
    sum: T,
}

impl<T> SumResult<T> {
    pub fn sum(&self) -> &T {
        &self.sum
    }
}

impl<T: for<'a> AddAssign<&'a T> + From<u8>, Key> RangeQueryResult<Key> for SumResult<T> {
    fn new_for_empty_range() -> Self {
        SumResult { sum: 0.into() }
    }

    fn add(left: &mut Self, _left_range: &Range<Key>, right: &Self, _right_range: &Range<Key>) {
        left.sum += &right.sum
    }
}

impl<Key, T, TR: Borrow<T>, KeyDiff> LazyRangeInitializer<Key, SumResult<T>>
    for SameElementsInitializer<TR>
where
    for<'a> &'a T: Mul<KeyDiff, Output = T>,
    for<'b> &'b Key: Sub<Output = KeyDiff>,
{
    fn get(&self, range: &Range<Key>) -> SumResult<T> {
        SumResult {
            sum: self.initial_element_value.borrow() * (&range.end - &range.start),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum AddAssignModification<T> {
    None,
    Add(T),
    Assign(T),
}

impl<T: Clone + for<'a> AddAssign<&'a T>, Key> RangeModification<SumResult<T>, Key>
    for AddAssignModification<T>
where
    for<'a> SameElementsInitializer<&'a T>: LazyRangeInitializer<Key, SumResult<T>>,
{
    fn no_op() -> Self {
        AddAssignModification::None
    }

    fn apply<'a>(&self, result: &'a mut SumResult<T>, range: &Range<Key>) {
        use AddAssignModification::*;
        match self {
            None => {}
            Add(x) | Assign(x) => {
                let to_add = SameElementsInitializer::new(x).get(range).sum;
                if let Assign(_) = self {
                    result.sum = to_add;
                } else {
                    result.sum += &to_add;
                }
            }
        }
    }

    fn compose(later: &Self, earlier: &mut Self) {
        use AddAssignModification::*;
        match (later, earlier) {
            (_, e @ None) => *e = later.clone(),
            (None, _) => {}
            (Assign(_), e) => *e = later.clone(),
            (Add(x), Add(y)) => *y += x,
            (Add(x), Assign(value)) => *value += x,
        }
    }
}
