//! # Range Sum Query

use crate::ops::SameElementsInitializer;
use crate::{LazyRangeInitializer, RangeModification, RangeQueryResult};
use std::borrow::Borrow;
use std::ops::{Add, AddAssign, Range};

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

impl<T: Clone + for<'a> AddAssign<&'a T> + From<u8>, Key> RangeQueryResult<Key> for SumResult<T>
where
    for<'a> &'a T: Add<&'a T, Output = T>,
{
    fn new_for_empty_range() -> Self {
        SumResult { sum: 0.into() }
    }

    fn combine(
        left: &Self,
        _left_range: &Range<Key>,
        right: &Self,
        _right_range: &Range<Key>,
    ) -> Self {
        SumResult {
            sum: &left.sum + &right.sum,
        }
    }

    fn add(left: &mut Self, _left_range: &Range<Key>, right: &Self, _right_range: &Range<Key>) {
        left.sum += &right.sum
    }
}

pub trait SumOfSameElements<Key> {
    fn sum(initial_element_value: &Self, keys: &Range<Key>) -> Self;
}

impl<T: SumOfSameElements<Key>, TB: Borrow<T>, Key> LazyRangeInitializer<SumResult<T>, Key>
    for SameElementsInitializer<TB>
where
    SumResult<T>: RangeQueryResult<Key>,
{
    fn get(&self, range: &Range<Key>) -> SumResult<T> {
        SumResult {
            sum: SumOfSameElements::sum(self.initial_element_value.borrow(), range),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum AddAssignModification<T> {
    None,
    Add(T),
    Assign(T),
}

impl<T: Clone + for<'a> AddAssign<&'a T>, Key> RangeModification<Key> for AddAssignModification<T>
where
    SumResult<T>: RangeQueryResult<Key>,
    for<'a> SameElementsInitializer<&'a T>: LazyRangeInitializer<SumResult<T>, Key>,
{
    type Result = SumResult<T>;

    fn no_op() -> Self {
        AddAssignModification::None
    }

    fn is_no_op(&self) -> bool {
        match self {
            AddAssignModification::None => true,
            _ => false,
        }
    }

    fn is_reinitialization(&self) -> bool {
        match self {
            AddAssignModification::Assign(_) => true,
            _ => false,
        }
    }

    fn apply(&self, result: &mut SumResult<T>, range: &Range<Key>) {
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
