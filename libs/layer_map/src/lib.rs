use std::{
    cell::{RefCell, RefMut},
    future::Future,
    io::Read,
    ops::Deref,
    pin::Pin,
    sync::{Arc, Mutex, RwLock},
};

use utils::seqwait::{self, Advance, SeqWait, Wait};

pub enum InMemoryLayerPutError {
    Frozen,
    LayerFull,
    AlreadyHaveRecordForKeyAndLsn,
}

pub trait InMemoryLayer: std::fmt::Debug + Default + Clone {
    type Key;
    type Lsn;
    type DeltaRecord;
    fn put(
        &mut self,
        key: Self::Key,
        lsn: Self::Lsn,
        delta: Self::DeltaRecord,
    ) -> Result<(), (Self::DeltaRecord, InMemoryLayerPutError)>;
    fn get(&self, key: Self::Key, lsn: Self::Lsn) -> Vec<Self::DeltaRecord>;
    fn freeze(&mut self);
}

#[derive(Debug, thiserror::Error)]
pub enum GetReconstructPathError {}

pub trait HistoricStuff {
    type Key;
    type Lsn;
    type HistoricLayer;
    type InMemoryLayer;
    fn get_reconstruct_path(
        &self,
        key: Self::Key,
        lsn: Self::Lsn,
    ) -> Result<Vec<Self::HistoricLayer>, GetReconstructPathError>;
    /// Produce a new version of `self` that includes the given inmem layer.
    fn make_historic(&self, inmem: Self::InMemoryLayer) -> Self;
}

struct State<K, L, RD, Layer, H, IML>
where
    K: Copy,
    H: HistoricStuff<Key = K, Lsn = L, HistoricLayer = Layer, InMemoryLayer = IML>,
    IML: InMemoryLayer<Key = K, Lsn = L, DeltaRecord = RD>,
{
    inmem: Mutex<Option<IML>>,
    historic: H,
}

pub struct Reader<K, C, L, RD, Layer, H, IML>
where
    K: Copy,
    C: seqwait::MonotonicCounter<L> + Copy,
    L: Ord + Copy,
    H: HistoricStuff<Key = K, Lsn = L, HistoricLayer = Layer, InMemoryLayer = IML>,
    IML: InMemoryLayer<Key = K, Lsn = L, DeltaRecord = RD>,
{
    shared: Wait<C, L, Arc<State<K, L, RD, Layer, H, IML>>>,
}

pub struct ReadWriter<K, C, L, RD, Layer, H, IML>
where
    K: Copy,
    C: seqwait::MonotonicCounter<L> + Copy,
    L: Ord + Copy,
    H: HistoricStuff<Key = K, Lsn = L, HistoricLayer = Layer, InMemoryLayer = IML>,
    IML: InMemoryLayer<Key = K, Lsn = L, DeltaRecord = RD>,
{
    shared: Advance<C, L, Arc<State<K, L, RD, Layer, H, IML>>>,
}

pub enum Record<D, I> {
    Delta(D),
    Image(I),
}

pub struct Lsn;

pub struct PageImage;

pub fn empty<K, C, L, RD, Layer, H, IML>(
    lsn: C,
    historic: H,
) -> (
    Reader<K, C, L, RD, Layer, H, IML>,
    ReadWriter<K, C, L, RD, Layer, H, IML>,
)
where
    K: Copy,
    C: seqwait::MonotonicCounter<L> + Copy,
    L: Ord + Copy,
    H: HistoricStuff<Key = K, Lsn = L, HistoricLayer = Layer, InMemoryLayer = IML>,
    IML: InMemoryLayer<Key = K, Lsn = L, DeltaRecord = RD>,
{
    let state = Arc::new(State {
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

pub struct ReconstructWork<K, L, RD, Layer> {
    key: K,
    lsn: L,
    inmem_records: Vec<RD>,
    historic_path: Vec<Layer>,
}

impl<K, C, L, RD, Layer, H, IML> Reader<K, C, L, RD, Layer, H, IML>
where
    K: Copy,
    C: seqwait::MonotonicCounter<L> + Copy,
    L: Ord + Copy,
    H: HistoricStuff<Key = K, Lsn = L, HistoricLayer = Layer, InMemoryLayer = IML>,
    IML: InMemoryLayer<Key = K, Lsn = L, DeltaRecord = RD>,
{
    pub async fn get(&self, key: K, lsn: L) -> Result<ReconstructWork<K, L, RD, Layer>, GetError> {
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

impl<K, C, L, RD, Layer, H, IML> ReadWriter<K, C, L, RD, Layer, H, IML>
where
    K: Copy,
    C: seqwait::MonotonicCounter<L> + Copy,
    L: Ord + Copy,
    H: HistoricStuff<Key = K, Lsn = L, HistoricLayer = Layer, InMemoryLayer = IML>,
    IML: InMemoryLayer<Key = K, Lsn = L, DeltaRecord = RD>,
{
    pub async fn put(&mut self, key: K, lsn: L, delta: RD) -> tokio::io::Result<()> {
        let shared = self.shared.get_current_data();
        let mut inmem_guard = shared
            .inmem
            .try_lock()
            // XXX: use the Advance as witness and only allow witness to access inmem in write mode
            .expect("we are the only ones with the Advance at hand");
        let inmem = inmem_guard.get_or_insert_with(|| IML::default());
        match inmem.put(key, lsn, delta) {
            Ok(()) => {
                self.shared.advance(lsn, None);
            }
            Err((delta, InMemoryLayerPutError::Frozen)) => {
                unreachable!("this method is &mut self, so, Rust guarantees that we are the only ones who can put() into the inmem layer, and if we freeze it as part of put, we make sure we don't try to put() again")
            }
            Err((delta, InMemoryLayerPutError::AlreadyHaveRecordForKeyAndLsn)) => {
                todo!("propagate error to caller")
            }
            Err((delta, InMemoryLayerPutError::LayerFull)) => {
                inmem.freeze();
                let inmem_clone = inmem.clone();
                drop(inmem);
                drop(inmem_guard);
                todo!("write out to disk; does the layer map need to distinguish between writing out and finished writing out?");
                let new_historic = shared.historic.make_historic(inmem_clone);
                let new_state = Arc::new(State {
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
        let Some(inmem) = inmem_guard else {
            // nothing to do
            return Ok(());
        };
        inmem.freeze();
        let inmem_clone = inmem.clone();
        let new_historic = shared.historic.make_historic(inmem_clone);
        let new_state = Arc::new(State {
            inmem: Mutex::new(None),
            historic: new_historic,
        });
        Ok(())
    }

    pub async fn get_nowait(
        &self,
        key: K,
        lsn: L,
    ) -> Result<ReconstructWork<K, L, RD, Layer>, GetError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{
            btree_map::{Entry, Range},
            BTreeMap, HashMap,
        },
        sync::Arc,
    };

    use crate::seqwait;

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

    impl super::HistoricStuff for LayerMap {
        type Key = usize;
        type Lsn = usize;
        type HistoricLayer = Arc<HistoricLayer>;
        type InMemoryLayer = InMemoryLayer;

        fn get_reconstruct_path(
            &self,
            key: Self::Key,
            lsn: Self::Lsn,
        ) -> Result<Vec<Self::HistoricLayer>, super::GetReconstructPathError> {
            let Some(bk) = self.by_key.get(&key) else {
                return Ok(vec![]);
            };
            Ok(bk.range(..=lsn).rev().map(|(_, l)| Arc::clone(l)).collect())
        }

        fn make_historic(&self, inmem: Self::InMemoryLayer) -> Self {
            let historic = Arc::new(HistoricLayer(inmem));
            // The returned copy of self references `historic` from all the (key,lsn) entries that it covers.
            // In the real codebase, this is a search tree that is less accurate.
            let mut copy = self.by_key.clone();
            for (k, v) in historic.0.by_key.iter() {
                for (lsn, deltas) in v.into_iter() {
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
        type Key = usize;
        type Lsn = usize;
        type DeltaRecord = &'static str;

        fn put(
            &mut self,
            key: Self::Key,
            lsn: Self::Lsn,
            delta: Self::DeltaRecord,
        ) -> Result<(), (Self::DeltaRecord, super::InMemoryLayerPutError)> {
            if self.frozen {
                return Err((delta, super::InMemoryLayerPutError::Frozen));
            }
            let by_key = self.by_key.entry(key).or_default();
            let by_key_and_lsn = match by_key.entry(lsn) {
                Entry::Occupied(record) => {
                    return Err((
                        delta,
                        super::InMemoryLayerPutError::AlreadyHaveRecordForKeyAndLsn,
                    ));
                }
                Entry::Vacant(vacant) => vacant.insert(delta),
            };
            Ok(())
        }

        fn get(&self, key: Self::Key, lsn: Self::Lsn) -> Vec<Self::DeltaRecord> {
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

        let (r, mut rw) = super::empty::<
            usize,
            UsizeCounter,
            usize,
            &'static str,
            Arc<HistoricLayer>,
            LayerMap,
            InMemoryLayer,
        >(UsizeCounter(0), lm);

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

        rw.put(key, lsn, delta)
    }
}
