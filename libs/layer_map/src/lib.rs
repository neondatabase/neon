use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::Duration,
};

use utils::seqwait::{self, Advance, SeqWait, Wait};

pub trait Types {
    type Key: Copy;
    type Lsn: Ord + Copy;
    type LsnCounter: seqwait::MonotonicCounter<Self::Lsn> + Copy;
    type DeltaRecord;
    type HistoricLayer;
    type InMemoryLayer: InMemoryLayer<Types = Self>;
    type HistoricStuff: HistoricStuff<Types = Self>;
}

pub enum InMemoryLayerPutError {
    Frozen,
    LayerFull,
    AlreadyHaveRecordForKeyAndLsn,
}

pub trait InMemoryLayer: std::fmt::Debug + Default + Clone {
    type Types: Types;
    fn put(
        &mut self,
        key: <Self::Types as Types>::Key,
        lsn: <Self::Types as Types>::Lsn,
        delta: <Self::Types as Types>::DeltaRecord,
    ) -> Result<(), (<Self::Types as Types>::DeltaRecord, InMemoryLayerPutError)>;
    fn get(
        &self,
        key: <Self::Types as Types>::Key,
        lsn: <Self::Types as Types>::Lsn,
    ) -> Vec<<Self::Types as Types>::DeltaRecord>;
    fn freeze(&mut self);
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

struct State<T: Types> {
    _types: PhantomData<T>,
    inmem: Mutex<Option<T::InMemoryLayer>>,
    historic: T::HistoricStuff,
}

pub struct Reader<T: Types> {
    shared: Wait<T::LsnCounter, T::Lsn, Arc<State<T>>>,
}

pub struct ReadWriter<T: Types> {
    shared: Advance<T::LsnCounter, T::Lsn, Arc<State<T>>>,
}

pub fn empty<T: Types>(
    lsn: T::LsnCounter,
    historic: T::HistoricStuff,
) -> (Reader<T>, ReadWriter<T>) {
    let state = Arc::new(State {
        _types: PhantomData::<T>::default(),
        inmem: Mutex::new(None),
        historic: historic,
    });
    let (wait_only, advance) = SeqWait::new(lsn, state).split_spmc();
    let reader = Reader { shared: wait_only };
    let read_writer = ReadWriter { shared: advance };
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
        let state = self.shared.wait_for(lsn).await?;
        let inmem_records = state
            .inmem
            .lock()
            .unwrap()
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
    delta: T::DeltaRecord,
    kind: PutErrorKind,
}
#[derive(Debug)]
pub enum PutErrorKind {
    AlreadyHaveInMemoryRecordForKeyAndLsn,
}

impl<T: Types> PutError<T> {
    pub fn delta(&self) -> &T::DeltaRecord {
        &self.delta
    }
    pub fn kind(&self) -> &PutErrorKind {
        &self.kind
    }
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
        let shared = self.shared.get_current_data();
        let mut inmem_guard = shared
            .inmem
            .try_lock()
            // XXX: use the Advance as witness and only allow witness to access inmem in write mode
            .expect("we are the only ones with the Advance at hand");
        let inmem = inmem_guard.get_or_insert_with(|| T::InMemoryLayer::default());
        match inmem.put(key, lsn, delta) {
            Ok(()) => {
                self.shared.advance(lsn, None);
            }
            Err((_delta, InMemoryLayerPutError::Frozen)) => {
                unreachable!("this method is &mut self, so, Rust guarantees that we are the only ones who can put() into the inmem layer, and if we freeze it as part of put, we make sure we don't try to put() again")
            }
            Err((delta, InMemoryLayerPutError::AlreadyHaveRecordForKeyAndLsn)) => {
                return Err(PutError {
                    delta,
                    kind: PutErrorKind::AlreadyHaveInMemoryRecordForKeyAndLsn,
                });
            }
            Err((_delta, InMemoryLayerPutError::LayerFull)) => {
                inmem.freeze();
                let inmem_clone = inmem.clone();
                drop(inmem);
                drop(inmem_guard);
                let new_historic = shared.historic.make_historic(inmem_clone);
                let new_state = Arc::new(State {
                    _types: PhantomData::<T>::default(),
                    inmem: Mutex::new(None),
                    historic: new_historic,
                });
                self.shared.advance(lsn, Some(new_state));
            }
        }
        Ok(())
    }

    pub async fn force_flush(&mut self) -> tokio::io::Result<()> {
        let shared = self.shared.get_current_data();
        let mut inmem_guard = shared
            .inmem
            .try_lock()
            // XXX: use the Advance as witness and only allow witness to access inmem in write mode
            .expect("we are the only ones with the Advance at hand");
        let Some(inmem) = &mut *inmem_guard else {
            // nothing to do
            return Ok(());
        };
        inmem.freeze();
        let inmem_clone = inmem.clone();
        // XXX don't hold the lock while writing the layer to disk ==> needs State::frozen
        let new_historic = shared.historic.make_historic(inmem_clone);
        let new_state = Arc::new(State {
            _types: PhantomData::<T>::default(),
            inmem: Mutex::new(None),
            historic: new_historic,
        });
        todo!("do something with new_state");
        drop(inmem_guard);
        Ok(())
    }

    pub async fn get_nowait(
        &self,
        key: T::Key,
        lsn: T::Lsn,
    ) -> Result<ReconstructWork<T>, GetError> {
        // XXX dedup with Reader::get
        let state = self
            .shared
            .wait_for_timeout(lsn, Duration::from_secs(0))
            // The await is never going to block because we pass from_secs(0).
            .await?;
        let inmem_records = state
            .inmem
            .lock()
            .unwrap()
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

    use crate::seqwait;

    struct TestTypes;

    impl super::Types for TestTypes {
        type Key = usize;

        type Lsn = usize;

        type LsnCounter = UsizeCounter;

        type DeltaRecord = &'static str;

        type HistoricLayer = Arc<HistoricLayer>;

        type InMemoryLayer = InMemoryLayer;

        type HistoricStuff = LayerMap;
    }

    struct HistoricLayer(InMemoryLayer);

    #[derive(Default)]
    struct LayerMap {
        by_key: BTreeMap<usize, BTreeMap<usize, Arc<HistoricLayer>>>,
    }

    #[derive(Copy, Clone)]
    struct UsizeCounter(usize);

    impl seqwait::MonotonicCounter<usize> for UsizeCounter {
        fn cnt_advance(&mut self, new_val: usize) {
            self.0 = new_val;
        }

        fn cnt_value(&self) -> usize {
            self.0
        }
    }

    // Our testing impl of HistoricStuff references the frozen InMemoryLayer objects
    // from all the (key,lsn) entries that it covers.
    // This mimics the (much more efficient) search tree in the real impl.
    impl super::HistoricStuff for LayerMap {
        type Types = TestTypes;
        fn get_reconstruct_path(
            &self,
            key: usize,
            lsn: usize,
        ) -> Result<Vec<Arc<HistoricLayer>>, super::GetReconstructPathError> {
            let Some(bk) = self.by_key.get(&key) else {
                return Ok(vec![]);
            };
            Ok(bk.range(..=lsn).rev().map(|(_, l)| Arc::clone(l)).collect())
        }

        fn make_historic(&self, inmem: InMemoryLayer) -> Self {
            // For the purposes of testing, just turn the inmemory layer historic through the type system
            let historic = Arc::new(HistoricLayer(inmem));
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

    #[derive(Clone, Default, Debug)]
    struct InMemoryLayer {
        frozen: bool,
        by_key: BTreeMap<usize, BTreeMap<usize, &'static str>>,
    }

    impl super::InMemoryLayer for InMemoryLayer {
        type Types = TestTypes;

        fn put(
            &mut self,
            key: usize,
            lsn: usize,
            delta: &'static str,
        ) -> Result<(), (&'static str, super::InMemoryLayerPutError)> {
            if self.frozen {
                return Err((delta, super::InMemoryLayerPutError::Frozen));
            }
            let by_key = self.by_key.entry(key).or_default();
            match by_key.entry(lsn) {
                Entry::Occupied(_record) => {
                    return Err((
                        delta,
                        super::InMemoryLayerPutError::AlreadyHaveRecordForKeyAndLsn,
                    ));
                }
                Entry::Vacant(vacant) => vacant.insert(delta),
            };
            Ok(())
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

        fn freeze(&mut self) {
            todo!()
        }
    }

    #[test]
    fn basic() {
        let lm = LayerMap::default();

        let (r, mut rw) = super::empty::<TestTypes>(UsizeCounter(0), lm);

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
