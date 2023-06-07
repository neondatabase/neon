use std::collections::{btree_map::Entry, BTreeMap};
use std::sync::Arc;
use utils::seqwait;

/// The ZST for which we impl the `super::Types` type collection trait.
struct TestTypes;

impl super::Types for TestTypes {
    type Key = usize;

    type Lsn = usize;

    type LsnCounter = UsizeCounter;

    type DeltaRecord = &'static str;

    type HistoricLayer = Arc<TestHistoricLayer>;

    type InMemoryLayer = TestInMemoryLayer;

    type HistoricStuff = TestHistoricStuff;
}

/// For testing, our in-memory layer is a simple hashmap.
#[derive(Clone, Default, Debug)]
struct TestInMemoryLayer {
    by_key: BTreeMap<usize, BTreeMap<usize, &'static str>>,
}

/// For testing, our historic layers are just in-memory layer objects with `frozen==true`.
struct TestHistoricLayer(TestInMemoryLayer);

/// This is the data structure that impls the `HistoricStuff` trait.
#[derive(Default, Clone)]
struct TestHistoricStuff {
    by_key: BTreeMap<usize, BTreeMap<usize, Arc<TestHistoricLayer>>>,
}

/// `seqwait::MonotonicCounter` impl
#[derive(Copy, Clone)]
pub struct UsizeCounter(usize);

// Our testing impl of HistoricStuff references the frozen InMemoryLayer objects
// from all the (key,lsn) entries that it covers.
// This mimics the (much more efficient) search tree in the real impl.
impl super::HistoricStuff for TestHistoricStuff {
    type Types = TestTypes;
    fn get_reconstruct_path(
        &self,
        key: usize,
        lsn: usize,
    ) -> Result<Vec<Arc<TestHistoricLayer>>, super::GetReconstructPathError> {
        let Some(bk) = self.by_key.get(&key) else {
                return Ok(vec![]);
            };
        Ok(bk.range(..=lsn).rev().map(|(_, l)| Arc::clone(l)).collect())
    }

    fn make_historic(&self, inmem: TestInMemoryLayer) -> Self {
        // For the purposes of testing, just turn the inmemory layer historic through the type system
        let historic = Arc::new(TestHistoricLayer(inmem));
        // Deep-copy
        let mut copy = self.by_key.clone();
        // Add the references to `inmem` to the deep-copied struct
        for (k, v) in historic.0.by_key.iter() {
            for (lsn, _deltas) in v.into_iter() {
                let by_key = copy.entry(*k).or_default();
                let overwritten = by_key.insert(*lsn, historic.clone());
                assert!(matches!(overwritten, None), "layers must not overlap");
            }
        }
        Self { by_key: copy }
    }
}

impl super::InMemoryLayer for TestInMemoryLayer {
    type Types = TestTypes;

    fn put(
        &mut self,
        key: usize,
        lsn: usize,
        delta: &'static str,
    ) -> Result<Self, super::InMemoryLayerPutError<&'static str>> {
        let mut clone = self.clone();
        drop(self);
        let by_key = clone.by_key.entry(key).or_default();
        match by_key.entry(lsn) {
            Entry::Occupied(_record) => {
                return Err(super::InMemoryLayerPutError {
                    delta,
                    kind: super::InMemoryLayerPutErrorKind::AlreadyHaveRecordForKeyAndLsn,
                });
            }
            Entry::Vacant(vacant) => vacant.insert(delta),
        };
        Ok(clone)
    }

    fn get(&self, key: usize, lsn: usize) -> Vec<&'static str> {
        let by_key = match self.by_key.get(&key) {
            Some(by_key) => by_key,
            None => return vec![],
        };
        by_key
            .range(..=lsn)
            .map(|(_, v)| v)
            .rev()
            .cloned()
            .collect()
    }
}

impl UsizeCounter {
    pub fn new(inital: usize) -> Self {
        UsizeCounter(inital)
    }
}

impl seqwait::MonotonicCounter<usize> for UsizeCounter {
    fn cnt_advance(&mut self, new_val: usize) {
        assert!(self.0 < new_val);
        self.0 = new_val;
    }

    fn cnt_value(&self) -> usize {
        self.0
    }
}

#[test]
fn basic() {
    let lm = TestHistoricStuff::default();

    let (r, mut rw) = super::new::<TestTypes>(UsizeCounter::new(0), lm);

    let r = Arc::new(r);
    let r2 = Arc::clone(&r);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let read_jh = rt.spawn(async move { r.get(0, 10).await });

    let mut rw = rt.block_on(async move {
        rw.put(0, 1, "foo").await.unwrap();
        rw.put(1, 1, "bar").await.unwrap();
        rw.put(0, 10, "baz").await.unwrap();
        rw
    });

    let read_res = rt.block_on(read_jh).unwrap().unwrap();
    assert!(
        read_res.historic_path.is_empty(),
        "we have pushed less than needed for flush"
    );
    assert_eq!(read_res.inmem_records, vec!["baz", "foo"]);

    let rw = rt.block_on(async move {
        rw.put(0, 11, "blup").await.unwrap();
        rw
    });
    let read_res = rt.block_on(async move { r2.get(0, 11).await.unwrap() });
    assert_eq!(read_res.historic_path.len(), 0);
    assert_eq!(read_res.inmem_records, vec!["blup", "baz", "foo"]);

    drop(rw);
}
