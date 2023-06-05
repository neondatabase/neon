//! Design with **immutable in-memory** and **immutable historic** stuff.

use std::{marker::PhantomData, time::Duration};

use utils::seqwait::{self, Advance, SeqWait, Wait};

pub trait Types {
    type Key: Copy;
    type Lsn: Ord + Copy;
    type LsnCounter: seqwait::MonotonicCounter<Self::Lsn> + Copy;
    type DeltaRecord;
    type HistoricLayer;
    type InMemoryLayer: InMemoryLayer<Types = Self> + Clone;
    type HistoricStuff: HistoricStuff<Types = Self> + Clone;
}

#[derive(thiserror::Error)]
pub struct InMemoryLayerPutError<DeltaRecord> {
    delta: DeltaRecord,
    kind: InMemoryLayerPutErrorKind,
}

#[derive(Debug)]
pub enum InMemoryLayerPutErrorKind {
    LayerFull,
    AlreadyHaveRecordForKeyAndLsn,
}

impl<DeltaRecord> std::fmt::Debug for InMemoryLayerPutError<DeltaRecord> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryLayerPutError")
            // would require DeltaRecord to impl Debug
            //         .field("delta", &self.delta)
            .field("kind", &self.kind)
            .finish()
    }
}

pub trait InMemoryLayer: std::fmt::Debug + Default + Clone {
    type Types: Types;
    fn put(
        &mut self,
        key: <Self::Types as Types>::Key,
        lsn: <Self::Types as Types>::Lsn,
        delta: <Self::Types as Types>::DeltaRecord,
    ) -> Result<Self, InMemoryLayerPutError<<Self::Types as Types>::DeltaRecord>>;
    fn get(
        &self,
        key: <Self::Types as Types>::Key,
        lsn: <Self::Types as Types>::Lsn,
    ) -> Vec<<Self::Types as Types>::DeltaRecord>;
}

#[derive(Debug, thiserror::Error)]
pub enum GetReconstructPathError {}

pub trait HistoricStuff {
    type Types: Types;
    fn get_reconstruct_path(
        &self,
        key: <Self::Types as Types>::Key,
        lsn: <Self::Types as Types>::Lsn,
    ) -> Result<Vec<<Self::Types as Types>::HistoricLayer>, GetReconstructPathError>;
    /// Produce a new version of `self` that includes the given inmem layer.
    fn make_historic(&self, inmem: <Self::Types as Types>::InMemoryLayer) -> Self;
}

struct Snapshot<T: Types> {
    _types: PhantomData<T>,
    inmem: Option<T::InMemoryLayer>,
    historic: T::HistoricStuff,
}

impl<T: Types> Clone for Snapshot<T> {
    fn clone(&self) -> Self {
        Self {
            _types: self._types.clone(),
            inmem: self.inmem.clone(),
            historic: self.historic.clone(),
        }
    }
}

pub struct Reader<T: Types> {
    wait: Wait<T::LsnCounter, T::Lsn, Snapshot<T>>,
}

pub struct ReadWriter<T: Types> {
    advance: Advance<T::LsnCounter, T::Lsn, Snapshot<T>>,
}

pub fn empty<T: Types>(
    lsn: T::LsnCounter,
    historic: T::HistoricStuff,
) -> (Reader<T>, ReadWriter<T>) {
    let state = Snapshot {
        _types: PhantomData::<T>::default(),
        inmem: None,
        historic: historic,
    };
    let (wait, advance) = SeqWait::new(lsn, state).split_spmc();
    let reader = Reader { wait };
    let read_writer = ReadWriter { advance };
    (reader, read_writer)
}

#[derive(Debug, thiserror::Error)]
pub enum GetError {
    #[error(transparent)]
    SeqWait(#[from] seqwait::SeqWaitError),
    #[error(transparent)]
    GetReconstructPath(#[from] GetReconstructPathError),
}

pub struct ReconstructWork<T: Types> {
    pub key: T::Key,
    pub lsn: T::Lsn,
    pub inmem_records: Vec<T::DeltaRecord>,
    pub historic_path: Vec<T::HistoricLayer>,
}

impl<T: Types> Reader<T> {
    pub async fn get(&self, key: T::Key, lsn: T::Lsn) -> Result<ReconstructWork<T>, GetError> {
        // XXX dedup with ReadWriter::get_nowait
        let state = self.wait.wait_for(lsn).await?;
        let inmem_records = state
            .inmem
            .as_ref()
            .map(|iml| iml.get(key, lsn))
            .unwrap_or_default();
        let historic_path = state.historic.get_reconstruct_path(key, lsn)?;
        Ok(ReconstructWork {
            key,
            lsn,
            inmem_records,
            historic_path,
        })
    }
}

#[derive(thiserror::Error)]
pub struct PutError<T: Types> {
    pub delta: T::DeltaRecord,
    pub kind: PutErrorKind,
}
#[derive(Debug)]
pub enum PutErrorKind {
    AlreadyHaveInMemoryRecordForKeyAndLsn,
}

impl<T: Types> std::fmt::Debug for PutError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PutError")
            // would need to require Debug for DeltaRecord
            // .field("delta", &self.delta)
            .field("kind", &self.kind)
            .finish()
    }
}

impl<T: Types> ReadWriter<T> {
    pub async fn put(
        &mut self,
        key: T::Key,
        lsn: T::Lsn,
        delta: T::DeltaRecord,
    ) -> Result<(), PutError<T>> {
        let (snapshot_lsn, snapshot) = self.advance.get_current_data();
        // TODO ensure snapshot_lsn <= lsn?
        let mut inmem = snapshot
            .inmem
            .unwrap_or_else(|| T::InMemoryLayer::default());
        // XXX: use the Advance as witness and only allow witness to access inmem in write mode
        match inmem.put(key, lsn, delta) {
            Ok(new_inmem) => {
                let new_snapshot = Snapshot {
                    _types: PhantomData,
                    inmem: Some(new_inmem),
                    historic: snapshot.historic,
                };
                self.advance.advance(lsn, Some(new_snapshot));
            }
            Err(InMemoryLayerPutError {
                delta,
                kind: InMemoryLayerPutErrorKind::AlreadyHaveRecordForKeyAndLsn,
            }) => {
                return Err(PutError {
                    delta,
                    kind: PutErrorKind::AlreadyHaveInMemoryRecordForKeyAndLsn,
                });
            }
            Err(InMemoryLayerPutError {
                delta,
                kind: InMemoryLayerPutErrorKind::LayerFull,
            }) => {
                let new_historic = snapshot.historic.make_historic(inmem);
                let mut new_inmem = T::InMemoryLayer::default();
                let new_inmem = new_inmem
                    .put(key, lsn, delta)
                    .expect("put into default inmem layer must not fail");
                let new_state = Snapshot {
                    _types: PhantomData::<T>::default(),
                    inmem: Some(new_inmem),
                    historic: new_historic,
                };
                self.advance.advance(lsn, Some(new_state));
            }
        }
        Ok(())
    }

    pub async fn force_flush(&mut self) -> tokio::io::Result<()> {
        let (snapshot_lsn, snapshot) = self.advance.get_current_data();
        let Snapshot {
            _types,
            inmem,
            historic,
        } = snapshot;
        // XXX: use the Advance as witness and only allow witness to access inmem in "write" mode
        let Some(inmem) = inmem else {
            // nothing to do
            return Ok(());
        };
        let new_historic = historic.make_historic(inmem);
        let new_snapshot = Snapshot {
            _types: PhantomData::<T>::default(),
            inmem: None,
            historic: new_historic,
        };
        self.advance.advance(snapshot_lsn, Some(new_snapshot)); // TODO: should fail if we're past snapshot_lsn
        Ok(())
    }

    pub async fn get_nowait(
        &self,
        key: T::Key,
        lsn: T::Lsn,
    ) -> Result<ReconstructWork<T>, GetError> {
        // XXX dedup with Reader::get
        let state = self
            .advance
            .wait_for_timeout(lsn, Duration::from_secs(0))
            // The await is never going to block because we pass from_secs(0).
            .await?;
        let inmem_records = state
            .inmem
            .as_ref()
            .map(|iml| iml.get(key, lsn))
            .unwrap_or_default();
        let historic_path = state.historic.get_reconstruct_path(key, lsn)?;
        Ok(ReconstructWork {
            key,
            lsn,
            inmem_records,
            historic_path,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{btree_map::Entry, BTreeMap};
    use std::sync::Arc;

    use crate::tests_common::UsizeCounter;

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

    #[test]
    fn basic() {
        let lm = TestHistoricStuff::default();

        let (r, mut rw) = super::empty::<TestTypes>(UsizeCounter::new(0), lm);

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
}
