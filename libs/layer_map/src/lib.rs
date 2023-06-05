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
//! In addition to above concepts, this crate provides two different flavors of the generic implementation.
//!
//! * Concurrent data structures with shared mutable state; may use multi-versioning internally for improved concurrency, but, can also just use a simple mutex.
//! * Immutable data structures; multi-versioned;
//!
//! As an example, take the case where we add a new Historic Layer to HistoricStuff.
//! In the *concurrent data structures* flavor, that could mean acquiring RwLock in write mode, and modifying a lookup data structure to accomodate the new layer.
//! Conversely, in the *immutable data structures* flavor, we would create a new version (aka snapshot) of the lookup data structure and make new reads use it.
//! Old ongoing reads would continue to use the old version(s).
//! Internally, the new snapshot would reference the same Historic Layer objects as the old snapshots, plus the newly added version.
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


pub mod inmem_imm_historic_imm;
pub mod inmem_shared_historic_imm;

#[cfg(test)]
mod tests_common;
