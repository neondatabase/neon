use anyhow::Result;
use lz4_flex;
use serde::{Deserialize, Serialize};
use std::ops::RangeBounds;
use std::path::Path;
use yakv::storage::{Key, Storage, StorageIterator, Value};
use zenith_utils::bin_ser::BeSer;

const TOAST_SEGMENT_SIZE: usize = 2 * 1024;
const CHECKPOINT_INTERVAL: u64 = 1u64 * 1024 * 1024 * 1024;
const CACHE_SIZE: usize = 1024; // 8Mb

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
	pub update_count: u64,
    pub index: Storage, // primary storage
    blobs: Storage,     // storage for TOAST segments
    next_id: ToastId,   // counter used to identify new TOAST segments
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
			update_count: 0,
            index: Storage::open(
                &path.join("index.db"),
                None,
                CACHE_SIZE,
                CHECKPOINT_INTERVAL,
            )?,
            blobs: Storage::open(
                &path.join("blobs.db"),
                None,
                CACHE_SIZE,
                CHECKPOINT_INTERVAL,
            )?,
            next_id: 0,
        })
    }

    pub fn put(&mut self, key: &Key, value: &Value) -> Result<()> {
        let mut index_tx = self.index.start_transaction();
        let value_len = value.len();
		self.update_count += 1;
        if value_len >= TOAST_SEGMENT_SIZE {
            let mut blobs_tx = self.blobs.start_transaction();
            if self.next_id == 0 {
                self.next_id = blobs_tx
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
                blobs_tx.put(
                    &ToastSegId { toast_id, segno }.ser()?,
                    &compressed_data[offs..offs + TOAST_SEGMENT_SIZE].to_vec(),
                )?;
                offs += TOAST_SEGMENT_SIZE;
                segno += 1;
            }
            if offs < compressed_data_len {
                blobs_tx.put(
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
            index_tx.put(key, &value)?;
            blobs_tx.commit()?;
        } else {
            let mut vec = Vec::with_capacity(value.len() + 1);
            vec.push(PLAIN_VALUE_TAG);
            vec.extend_from_slice(&value);
            index_tx.put(key, &vec)?;
        }
        index_tx.commit()?;
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        self.index
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
            iter: self.index.range(range),
        }
    }

    pub fn remove(&self, key: &Key) -> Result<()> {
        let mut index_tx = self.index.start_transaction();
        if let Some(value) = index_tx.get(key)? {
            if value[0] == TOAST_VALUE_TAG {
                let mut blobs_tx = self.blobs.start_transaction();
                let toast_ref = ToastRef::des(&value[1..])?;
                let n_segments = ((toast_ref.compressed_size as usize + TOAST_SEGMENT_SIZE - 1)
                    / TOAST_SEGMENT_SIZE) as u32;
                for segno in 0..n_segments {
                    blobs_tx.remove(
                        &ToastSegId {
                            toast_id: toast_ref.toast_id,
                            segno,
                        }
                        .ser()?,
                    )?;
                }
                blobs_tx.commit()?;
            }
            index_tx.remove(key)?;
        }
        index_tx.commit()?;
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
            for seg in self.blobs.range(from..till) {
                toast.extend_from_slice(&seg?.1);
            }
            Ok(lz4_flex::decompress(&toast, toast_ref.orig_size as usize)?)
        } else {
            value.remove(0); // remove toast tag
            Ok(value)
        }
    }
}
