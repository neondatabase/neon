//! Generic implementation of WAL record data flows.
//!
//! # Overview
//!
//! This crate implements the core data flows inside pageserver:
//!
//! 1. WAL records from `walreceiver`, via in-memory layers, into on-disk L0 layers.
//! 2. Data re-shuffeling through compaciton.
//! 3. Page image creation & garbage collection through GC.
//! 4. `GetPage@LSN`: retrieval of WAL records and page images for feeding into WAL redo.
//!
//! The implementation assumes the following concepts,
//! but is fully generic over their implementation in order to facilitate unit testing:
//!
//! - **Delta Records**: data is written into the system in the form of self-descriptive deltas.
//!   For the Pageserver use case, these deltas are derived from Postgres WAL records.
//! - **Page Numbers**: Delta Records always affect a single key.
//!   That key is called page number, because, in the Pageserver use case, the Postgres table page numbers are the keys.
//! - **LSN**: When writing Delta Records into the system, they are associated with a monotonically increasing LSN.
//!   Subsequently written Delta Records must have increasing LSNs.
//! - **Page Images**: Delta Records for a given page can be used to reconstruct the page. Think of it like squashing diffs.
//!   - When sorting the Delta Records for a given key by their LSN, any prefix of that sorting can be squashed into a page image.
//!   - Delta Records following such a squash can be squashed into that page image.
//!   - In Pageserver, WAL redo implements the (pure) function of squashing.
//! - **In-Memory Layer**: an object that represents an "unfinished" L0 layer file, holding Delta Records in insertion order.
//!   "Unfinished" means that we're still writing Delta Records to that file.
//! - **Historic Layer**: an object that represents a "finished" layer file, at any compaction level.
//!   Such objects reside on disk and/or in remote storage.
//!   They may contain Delta Records, Page Images, or a mixture thereof. It doesn't matter.
//! - **HistoricStuff**: an efficient lookup data structure to find the list of Historic Layer objects
//!   that hold the Delta Records / PageImages required to reconstruct a Page Image at a given LSN.
//!
//! # API
//!
//! The common idea for both flavors is that there is a Read-end and a ReadWrite-end.
//!
//! The ReadWrite-end is used to push new `DeltaRecord @ LSN`s into the system.
//! In Pageserver, this is used by the `WalReceiver`.
//!
//! The Read-end provides the `GetPage@LSN` API.
//! In the current iteration, we actually return something called `ReconstructWork`,
//! i.e., we leave the work of reading the values from the layers, and the WAL redo invocation
//! to the caller. This is to keep the scope limited, and to prepare for clustered pageservers (aka "sharding").
//!
//! ## Immutability
//!
//! The traits defined by this crate assume immutable data structures that are multi-versioned.
//! As an example for what "immutable" means, take the case where we add a new Historic Layer to HistoricStuff.
//! Traditionally, one would use shared mutable state, i.e. `Arc<RwLock<...>>`.
//! To insert the new Historic Layer, we would acquire the RwLock in write mode, and modifying a lookup data structure to accomodate the new layer.
//! The Read-ends would use RwLock in read mode to read from the data structure.
//!
//! Conversely, with  *immutable data structures*, writers create new version (aka *snapshot*) of the lookup data structure.
//! New reads on the Read-ends will use the new snapshot, but old ongoing reads would use the old version(s).
//! A common implementation would likely share the Historic Layer objects, e.g., using `Arc`.
//! And maybe there's internally mutable state inside the layer objects, e.g., to track residence (i.e., *on-demand downloaded* vs *evicted*).
//! But the important point is that there's no synchronization / lock-holding except when grabbing a reference to the snapshot (Read-end), or when publishing a new snapshot (ReadWrite-end).
//!
//! ## Scope
//! The following concerns are considered implementation details from the perspective of this crate:
//!
//! - **Layer File Persistence**: `HistoricStuff::make_historic` is responsible for this.
//! - **Reading Layer Files**: the `ReconstructWork` that the Read-end returns from `GetPage@LSN` requests contains the list of layers to consult.
//!   The crate consumer is responsible for reading the layers & doing WAL redo.
//!   Likely the implementation of `HistoricStuff` plays a role here, because it is responsible for persisting the layer files.
//! - **Layer Eviction & On-Demand Download**: this is just an aspect of the above.
//!   The crate consumer can choose to implement eviction & on-demand download however they wish.
//!   The only requirement is that the Historic Layer object give the same answers at all time.
//!   - For example, a `layer cache` module or service can take care of layer uploads, eviction, and on-demand downloads.
//!     Initially, the `layer cache` can be local-only, but, over time, it can be multi-machine / clustered pagesevers / aka "sharding".

use std::{marker::PhantomData, time::Duration};

use utils::seqwait::{self, Advance, SeqWait, Wait};

#[cfg(test)]
mod tests;

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
