use crate::{
    LazyRangeInitializer, PersistentVecStorage, RangeModification, RangeQueryResult, VecVersion,
};
use std::ops::Range;

pub struct NaiveVecVersion<Modification: RangeModification<Key>, Key> {
    all_keys: Range<Key>,
    values: Vec<Modification::Result>,
}

impl<Modification: RangeModification<Key>, Key: Clone> Clone for NaiveVecVersion<Modification, Key>
where
    Modification::Result: Clone,
{
    fn clone(&self) -> Self {
        Self {
            all_keys: self.all_keys.clone(),
            values: self.values.clone(),
        }
    }
}

pub trait IndexableKey: Clone {
    fn index(all_keys: &Range<Self>, key: &Self) -> usize;
    fn element_range(all_keys: &Range<Self>, index: usize) -> Range<Self>;
}

impl<Modification: RangeModification<Key>, Key: IndexableKey> VecVersion<Modification, Key>
    for NaiveVecVersion<Modification, Key>
where
    Modification::Result: Clone,
{
    fn get(&self, keys: Range<Key>) -> Modification::Result {
        let mut result = Modification::Result::new_for_empty_range();
        let mut result_range = keys.start.clone()..keys.start.clone();
        for index in IndexableKey::index(&self.all_keys, &keys.start)
            ..IndexableKey::index(&self.all_keys, &keys.end)
        {
            let element_range = IndexableKey::element_range(&self.all_keys, index);
            Modification::Result::add(
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
        Modification: RangeModification<Key>,
        Initializer: LazyRangeInitializer<Modification::Result, Key>,
        Key: IndexableKey,
    > PersistentVecStorage<Modification, Initializer, Key> for NaiveVecStorage
where
    Modification::Result: Clone,
{
    type Version = NaiveVecVersion<Modification, Key>;

    fn new(all_keys: Range<Key>, initializer: Initializer) -> Self::Version {
        let mut values = Vec::with_capacity(IndexableKey::index(&all_keys, &all_keys.end));
        for index in 0..values.capacity() {
            values.push(initializer.get(&IndexableKey::element_range(&all_keys, index)));
        }
        NaiveVecVersion { all_keys, values }
    }
}
