use std::{hash::BuildHasherDefault, marker::PhantomData, ops::Index};

use lasso::{Spur, ThreadedRodeo};
use once_cell::sync::Lazy;
use rustc_hash::FxHasher;

pub trait InternId {}

pub struct StringInterner<Id> {
    inner: ThreadedRodeo<Spur, BuildHasherDefault<FxHasher>>,
    _id: PhantomData<Id>,
}

#[derive(PartialEq, Debug, Clone, Copy, Eq, Hash)]
pub struct InternedString<Id> {
    inner: Spur,
    _id: PhantomData<Id>,
}

impl<Id: InternId> StringInterner<Id> {
    pub fn new() -> Self {
        StringInterner {
            inner: ThreadedRodeo::with_hasher(BuildHasherDefault::<FxHasher>::default()),
            _id: PhantomData,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn current_memory_usage(&self) -> usize {
        self.inner.current_memory_usage()
    }

    pub fn get_or_intern(&self, s: &str) -> InternedString<Id> {
        InternedString {
            inner: self.inner.get_or_intern(s),
            _id: PhantomData,
        }
    }

    pub fn get(&self, s: &str) -> Option<InternedString<Id>> {
        Some(InternedString {
            inner: self.inner.get(s)?,
            _id: PhantomData,
        })
    }
}

impl<Id: InternId> Index<InternedString<Id>> for StringInterner<Id> {
    type Output = str;

    fn index(&self, index: InternedString<Id>) -> &Self::Output {
        self.inner.resolve(&index.inner)
    }
}

impl<Id: InternId> Default for StringInterner<Id> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct RoleNameTag;
impl InternId for RoleNameTag {}
pub type RoleNameInt = InternedString<RoleNameTag>;
pub type RoleNameInterner = StringInterner<RoleNameTag>;
pub static ROLE_NAMES: Lazy<RoleNameInterner> = Lazy::new(RoleNameInterner::default);

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct EndpointIdTag;
impl InternId for EndpointIdTag {}
pub type EndpointIdInt = InternedString<EndpointIdTag>;
pub type EndpointIdInterner = StringInterner<EndpointIdTag>;
pub static ENDPOINT_IDS: Lazy<EndpointIdInterner> = Lazy::new(EndpointIdInterner::default);

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct BranchIdTag;
impl InternId for BranchIdTag {}
pub type BranchIdInt = InternedString<BranchIdTag>;
pub type BranchIdInterner = StringInterner<BranchIdTag>;
pub static BRANCH_IDS: Lazy<BranchIdInterner> = Lazy::new(BranchIdInterner::default);

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProjectIdTag;
impl InternId for ProjectIdTag {}
pub type ProjectIdInt = InternedString<ProjectIdTag>;
pub type ProjectIdInterner = StringInterner<ProjectIdTag>;
pub static PROJECT_IDS: Lazy<ProjectIdInterner> = Lazy::new(ProjectIdInterner::default);

#[cfg(test)]
mod tests {
    use crate::intern::StringInterner;

    use super::InternId;

    struct MyId;
    impl InternId for MyId {}

    #[test]
    fn push_many_strings() {
        use rand::{rngs::StdRng, Rng, SeedableRng};
        use rand_distr::Zipf;

        let endpoint_dist = Zipf::new(500000, 0.8).unwrap();
        let endpoints = StdRng::seed_from_u64(272488357).sample_iter(endpoint_dist);

        let interner = StringInterner::<MyId>::new();

        const N: usize = 100_000;
        let mut verify = Vec::with_capacity(N);
        for endpoint in endpoints.take(N) {
            let endpoint = format!("ep-string-interning-{endpoint}");
            let key = interner.get_or_intern(&endpoint);
            verify.push((endpoint, key));
        }

        for (s, key) in verify {
            assert_eq!(interner[key], s);
        }

        // 2093056/59861 = 35 bytes per string
        assert_eq!(interner.len(), 59_861);
        // will have other overhead for the internal hashmaps that are not accounted for.
        assert_eq!(interner.current_memory_usage(), 2_093_056);
    }
}
