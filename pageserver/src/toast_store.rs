use anyhow::Result;
use lz4_flex;
use serde::{Deserialize, Serialize};
use std::ops::RangeBounds;
use std::path::Path;
use tracing::*;
use yakv::storage::{Key, Storage, StorageIterator, Value};
use zenith_utils::bin_ser::BeSer;

const TOAST_SEGMENT_SIZE: usize = 2 * 1024;
const CHECKPOINT_INTERVAL: u64 = 1u64 * 1024 * 1024 * 1024;
const MAIN_CACHE_SIZE: usize = 8 * 1024; // 64Mb
const TOAST_CACHE_SIZE: usize = 1024; // 8Mb
const MAIN_COMMIT_THRESHOLD: usize = MAIN_CACHE_SIZE / 2;
const TOAST_COMMIT_THRESHOLD: usize = TOAST_CACHE_SIZE / 2;

const TOAST_VALUE_TAG: u8 = 0;
const PLAIN_VALUE_TAG: u8 = 1;

type ToastId = u32;

///
/// Toast storage consistof two KV databases: one for storing main index
/// and second for storing sliced BLOB (values larger than 2kb).
/// BLOBs and main data are stored in different databases to improve
/// data locality and reduce key size for TOAST segments.
///
pub struct ToastStore {
    main: Storage,       // primary storage
    toast: Storage,      // storage for TOAST segments
    next_id: ToastId,    // counter used to identify new TOAST segments
    pub committed: bool, // last transaction was committed (not delayed)
}

///
/// TOAST reference
///
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct ToastRef {
    toast_id: ToastId,    // assigned TOAST indetifier
    orig_size: u32,       //  Original (uncompressed) value size
    compressed_size: u32, // Compressed object size
}

///
/// Identifier of TOAST segment.
///
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct ToastSegId {
    toast_id: ToastId,
    segno: u32, // segment number used to extract segments in proper order
}

pub struct ToastIterator<'a> {
    store: &'a ToastStore,
    iter: StorageIterator<'a>,
}

impl<'a> Iterator for ToastIterator<'a> {
    type Item = Result<(Key, Value)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().and_then(|res| {
            Some(res.and_then(|(key, value)| Ok((key, self.store.detoast(value)?))))
        })
    }
}

impl<'a> DoubleEndedIterator for ToastIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back().and_then(|res| {
            Some(res.and_then(|(key, value)| Ok((key, self.store.detoast(value)?))))
        })
    }
}

//
// FIXME-KK: not using WAL now. Implement asynchronous or delayed commit.
//
impl ToastStore {
    pub fn new(path: &Path) -> Result<ToastStore> {
        Ok(ToastStore {
            main: Storage::open(
                &path.join("main.db"),
                Some(&path.join("main.log")),
                MAIN_CACHE_SIZE,
                CHECKPOINT_INTERVAL,
            )?,
            toast: Storage::open(
                &path.join("toast.db"),
                Some(&path.join("toast.log")),
                TOAST_CACHE_SIZE,
                CHECKPOINT_INTERVAL,
            )?,
            next_id: 0,
            committed: false,
        })
    }

    pub fn put(&mut self, key: &Key, value: &Value) -> Result<()> {
        let mut main_tx = self.main.start_transaction();
        let value_len = value.len();
        let main_pinned;
        if value_len >= TOAST_SEGMENT_SIZE {
            let mut toast_tx = self.toast.start_transaction();
            if self.next_id == 0 {
                self.next_id = toast_tx
                    .iter()
                    .next_back()
                    .transpose()?
                    .map_or(0u32, |(key, _value)| {
                        ToastSegId::des(&key).unwrap().toast_id
                    });
            }
            self.next_id += 1;
            let toast_id = self.next_id;
            let compressed_data = lz4_flex::compress(value);
            let compressed_data_len = compressed_data.len();
            let mut offs: usize = 0;
            let mut segno = 0u32;
            while offs + TOAST_SEGMENT_SIZE <= compressed_data_len {
                toast_tx.put(
                    &ToastSegId { toast_id, segno }.ser()?,
                    &compressed_data[offs..offs + TOAST_SEGMENT_SIZE].to_vec(),
                )?;
                offs += TOAST_SEGMENT_SIZE;
                segno += 1;
            }
            if offs < compressed_data_len {
                toast_tx.put(
                    &ToastSegId { toast_id, segno }.ser()?,
                    &compressed_data[offs..].to_vec(),
                )?;
            }
            let mut value = ToastRef {
                toast_id,
                orig_size: value_len as u32,
                compressed_size: compressed_data_len as u32,
            }
            .ser()?;
            value.insert(0, TOAST_VALUE_TAG);
            main_tx.put(key, &value)?;
            main_pinned = main_tx.get_cache_info().pinned;
            // If we are going to commit main storage, then we have to commit toast storage first to avoid dangling references
            if main_pinned > MAIN_COMMIT_THRESHOLD
                || toast_tx.get_cache_info().pinned > TOAST_COMMIT_THRESHOLD
            {
                toast_tx.commit()?;
            } else {
                toast_tx.delay()?;
            }
        } else {
            let mut vec = Vec::with_capacity(value.len() + 1);
            vec.push(PLAIN_VALUE_TAG);
            vec.extend_from_slice(&value);
            main_tx.put(key, &vec)?;
            main_pinned = main_tx.get_cache_info().pinned;
        }
        if main_pinned > MAIN_COMMIT_THRESHOLD {
            main_tx.commit()?;
            self.committed = true;
        } else {
            main_tx.delay()?;
            self.committed = false;
        }
        Ok(())
    }

    pub fn checkpoint(&self) -> Result<()> {
        let mut main_tx = self.main.start_transaction();
        let mut toast_tx = self.toast.start_transaction();
        toast_tx.commit()?;
        main_tx.commit()?;
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        self.main
            .get(&key.to_vec())
            .transpose()
            .and_then(|res| Some(res.and_then(|value| Ok(self.detoast(value)?))))
            .transpose()
    }

    pub fn iter(&self) -> ToastIterator<'_> {
        self.range(..)
    }

    pub fn range<R: RangeBounds<Key>>(&self, range: R) -> ToastIterator<'_> {
        ToastIterator {
            store: self,
            iter: self.main.range(range),
        }
    }

    pub fn remove(&mut self, key: &Key) -> Result<()> {
        let mut main_tx = self.main.start_transaction();
        if let Some(value) = main_tx.get(key)? {
            main_tx.remove(key)?;
            let main_pinned = main_tx.get_cache_info().pinned;
            if value[0] == TOAST_VALUE_TAG {
                let mut toast_tx = self.toast.start_transaction();
                let toast_ref = ToastRef::des(&value[1..])?;
                let n_segments = ((toast_ref.compressed_size as usize + TOAST_SEGMENT_SIZE - 1)
                    / TOAST_SEGMENT_SIZE) as u32;
                for segno in 0..n_segments {
                    toast_tx.remove(
                        &ToastSegId {
                            toast_id: toast_ref.toast_id,
                            segno,
                        }
                        .ser()?,
                    )?;
                }
                // If we are going to commit main storage, then we have to commit toast storage first to avoid dangling references
                if main_pinned > MAIN_COMMIT_THRESHOLD
                    || toast_tx.get_cache_info().pinned > TOAST_COMMIT_THRESHOLD
                {
                    toast_tx.commit()?;
                } else {
                    toast_tx.delay()?;
                }
            }
            if main_pinned > MAIN_COMMIT_THRESHOLD {
                main_tx.commit()?;
                self.committed = true;
            } else {
                main_tx.delay()?;
                self.committed = false;
            }
        } else {
            self.committed = false;
        }
        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        self.toast.close()?; // commit and close TOAST store first to avoid dangling references
        self.main.close()?;
        Ok(())
    }

    fn detoast(&self, mut value: Value) -> Result<Value> {
        if value[0] == TOAST_VALUE_TAG {
            // TOAST chain
            let toast_ref = ToastRef::des(&value[1..])?;
            let mut toast: Value = Vec::with_capacity(toast_ref.orig_size as usize);
            let n_segments = ((toast_ref.compressed_size as usize + TOAST_SEGMENT_SIZE - 1)
                / TOAST_SEGMENT_SIZE) as u32;
            let from = ToastSegId {
                toast_id: toast_ref.toast_id,
                segno: 0,
            }
            .ser()?;
            let till = ToastSegId {
                toast_id: toast_ref.toast_id,
                segno: n_segments,
            }
            .ser()?;
            for seg in self.toast.range(from..till) {
                toast.extend_from_slice(&seg?.1);
            }
            Ok(lz4_flex::decompress(&toast, toast_ref.orig_size as usize)?)
        } else {
            value.remove(0); // remove toast tag
            Ok(value)
        }
    }
}

impl Drop for ToastStore {
    fn drop(&mut self) {
        info!("Storage closed");
        // FIXME-KK: better call close() explicitly
        self.close().unwrap();
    }
}
