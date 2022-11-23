use crate::{
    LazyRangeInitializer, PersistentVecStorage, RangeModification, RangeQueryResult,
    VecFrozenVersion, VecReadableVersion,
};
use std::marker::PhantomData;
use std::ops::Range;
use std::rc::Rc;

pub struct NaiveFrozenVersion<Modification: RangeModification<Key>, Key> {
    all_keys: Range<Key>,
    values: Rc<Box<Vec<Modification::Result>>>,
}

pub trait IndexableKey: Clone {
    fn index(all_keys: &Range<Self>, key: &Self) -> usize;
    fn element_range(all_keys: &Range<Self>, index: usize) -> Range<Self>;
}

fn get<Modification: RangeModification<Key>, Key: IndexableKey>(
    all_keys: &Range<Key>,
    values: &Vec<Modification::Result>,
    keys: Range<Key>,
) -> Modification::Result {
    let mut result = Modification::Result::new_for_empty_range();
    let mut result_range = keys.start.clone()..keys.start.clone();
    for index in
        IndexableKey::index(&all_keys, &keys.start)..IndexableKey::index(&all_keys, &keys.end)
    {
        let element_range = IndexableKey::element_range(&all_keys, index);
        Modification::Result::add(&mut result, &result_range, &values[index], &element_range);
        result_range.end = element_range.end;
    }
    result
}

impl<Modification: RangeModification<Key>, Key: IndexableKey> VecReadableVersion<Modification, Key>
    for NaiveFrozenVersion<Modification, Key>
{
    fn get(&self, keys: Range<Key>) -> Modification::Result {
        get::<Modification, Key>(&self.all_keys, &self.values, keys)
    }
}

// Manual implementation of `Clone` becase `derive` requires `Modification: Clone`
impl<'a, Modification: RangeModification<Key>, Key: Clone> Clone
    for NaiveFrozenVersion<Modification, Key>
{
    fn clone(&self) -> Self {
        Self {
            all_keys: self.all_keys.clone(),
            values: self.values.clone(),
        }
    }
}

impl<'a, Modification: RangeModification<Key>, Key: IndexableKey>
    VecFrozenVersion<Modification, Key> for NaiveFrozenVersion<Modification, Key>
{
}

// TODO: is it at all possible to store previous versions in this struct,
// without any Rc<>?
pub struct NaiveVecStorage<
    Modification: RangeModification<Key>,
    Initializer: LazyRangeInitializer<Modification::Result, Key>,
    Key: IndexableKey,
> {
    all_keys: Range<Key>,
    last_version: Vec<Modification::Result>,
    _initializer: PhantomData<Initializer>,
}

impl<
        Modification: RangeModification<Key>,
        Initializer: LazyRangeInitializer<Modification::Result, Key>,
        Key: IndexableKey,
    > VecReadableVersion<Modification, Key> for NaiveVecStorage<Modification, Initializer, Key>
where
    Modification::Result: Clone,
{
    fn get(&self, keys: Range<Key>) -> Modification::Result {
        get::<Modification, Key>(&self.all_keys, &self.last_version, keys)
    }
}

impl<
        Modification: RangeModification<Key>,
        Initializer: LazyRangeInitializer<Modification::Result, Key>,
        Key: IndexableKey,
    > PersistentVecStorage<Modification, Initializer, Key>
    for NaiveVecStorage<Modification, Initializer, Key>
where
    Modification::Result: Clone,
{
    fn new(all_keys: Range<Key>, initializer: Initializer) -> Self {
        let mut values = Vec::with_capacity(IndexableKey::index(&all_keys, &all_keys.end));
        for index in 0..values.capacity() {
            values.push(initializer.get(&IndexableKey::element_range(&all_keys, index)));
        }
        NaiveVecStorage {
            all_keys,
            last_version: values,
            _initializer: PhantomData,
        }
    }

    type FrozenVersion = NaiveFrozenVersion<Modification, Key>;

    fn modify(&mut self, keys: Range<Key>, modification: Modification) {
        for index in IndexableKey::index(&self.all_keys, &keys.start)
            ..IndexableKey::index(&self.all_keys, &keys.end)
        {
            let element_range = IndexableKey::element_range(&self.all_keys, index);
            modification.apply(&mut self.last_version[index], &element_range);
        }
    }

    fn freeze(&mut self) -> Self::FrozenVersion {
        NaiveFrozenVersion::<Modification, Key> {
            all_keys: self.all_keys.clone(),
            values: Rc::new(Box::new(self.last_version.clone())),
        }
    }
}
