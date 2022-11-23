use crate::{
    LazyRangeInitializer, PersistentVecStorage, RangeModification, RangeQueryResult, VecVersion,
};
use std::marker::PhantomData;
use std::ops::Range;

pub struct NaiveVecVersion<
    Key: Clone,
    Result: RangeQueryResult<Key> + Clone,
    Modification: RangeModification<Result, Key>,
> {
    all_keys: Range<Key>,
    values: Vec<Result>,
    _modification: PhantomData<Modification>,
}

impl<
        Key: Clone,
        Result: RangeQueryResult<Key> + Clone,
        Modification: RangeModification<Result, Key>,
    > Clone for NaiveVecVersion<Key, Result, Modification>
{
    fn clone(&self) -> Self {
        Self {
            all_keys: self.all_keys.clone(),
            values: self.values.clone(),
            _modification: PhantomData,
        }
    }
}

pub trait IndexableKey: Clone {
    fn index(all_keys: &Range<Self>, key: &Self) -> usize;
    fn element_range(all_keys: &Range<Self>, index: usize) -> Range<Self>;
}

impl<
        Key: IndexableKey,
        Result: Clone + RangeQueryResult<Key>,
        Modification: RangeModification<Result, Key>,
    > VecVersion<Key, Result, Modification> for NaiveVecVersion<Key, Result, Modification>
{
    fn get(&self, keys: Range<Key>) -> Result {
        let mut result = Result::new_for_empty_range();
        let mut result_range = keys.start.clone()..keys.start.clone();
        for index in IndexableKey::index(&self.all_keys, &keys.start)
            ..IndexableKey::index(&self.all_keys, &keys.end)
        {
            let element_range = IndexableKey::element_range(&self.all_keys, index);
            Result::add(
                &mut result,
                &result_range,
                &self.values[index],
                &element_range,
            );
            result_range.end = element_range.end;
        }
        result
    }

    fn modify(&mut self, keys: Range<Key>, modification: Modification) {
        for index in IndexableKey::index(&self.all_keys, &keys.start)
            ..IndexableKey::index(&self.all_keys, &keys.end)
        {
            let element_range = IndexableKey::element_range(&self.all_keys, index);
            modification.apply(&mut self.values[index], &element_range);
        }
    }
}

pub struct NaiveVecStorage;

impl<
        Key: IndexableKey,
        Result: Clone + RangeQueryResult<Key>,
        Initializer: LazyRangeInitializer<Key, Result>,
        Modification: RangeModification<Result, Key>,
    > PersistentVecStorage<Key, Result, Initializer, Modification> for NaiveVecStorage
{
    type Version = NaiveVecVersion<Key, Result, Modification>;

    fn new(all_keys: Range<Key>, initializer: Initializer) -> Self::Version {
        let mut values = Vec::with_capacity(IndexableKey::index(&all_keys, &all_keys.end));
        for index in 0..values.capacity() {
            values.push(initializer.get(&IndexableKey::element_range(&all_keys, index)));
        }
        NaiveVecVersion {
            all_keys,
            values,
            _modification: PhantomData,
        }
    }
}
