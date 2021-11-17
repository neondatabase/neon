use anyhow::{anyhow, Result};
use lz4_flex;
use std::convert::TryInto;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use tracing::*;
use yakv::storage::{Key, Storage, StorageConfig, StorageIterator, Value};

const TOAST_SEGMENT_SIZE: usize = 2 * 1024;
const CHECKPOINT_INTERVAL: u64 = 1u64 * 1024 * 1024 * 1024;
const CACHE_SIZE: usize = 32 * 1024; // 256Mb
const COMMIT_THRESHOLD: usize = CACHE_SIZE / 2;
const WAL_FLUSH_THRESHOLD: u32 = 128; // 1Mb

///
/// Toast storage consistof two KV databases: one for storing main index
/// and second for storing sliced BLOB (values larger than 2kb).
/// BLOBs and main data are stored in different databases to improve
/// data locality and reduce key size for TOAST segments.
///
pub struct ToastStore {
    db: Storage,         // key-value database
    pub committed: bool, // last transaction was committed (not delayed)
}

pub struct ToastIterator<'a> {
    iter: StorageIterator<'a>,
}

impl<'a> Iterator for ToastIterator<'a> {
    type Item = Result<(Key, Value)>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut toast: Option<Vec<u8>> = None;
        let mut next_segno = 0u16;
        while let Some(elem) = self.iter.next() {
            if let Ok((key, value)) = elem {
                let key_len = key.len();
                let n_segments =
                    u16::from_be_bytes(key[key_len - 4..key_len - 2].try_into().unwrap());
                let segno = u16::from_be_bytes(key[key_len - 2..].try_into().unwrap());
                let key = key[..key_len - 4].to_vec();
                if n_segments != 0 {
                    // TOAST
                    assert_eq!(segno, next_segno);
                    if next_segno == 0 {
                        toast = Some(Vec::with_capacity(n_segments as usize * TOAST_SEGMENT_SIZE))
                    }
                    toast.as_mut().unwrap().extend_from_slice(&value);
                    next_segno = segno + 1;
                    if next_segno == n_segments {
                        let res = lz4_flex::decompress_size_prepended(&toast.unwrap());
                        return Some(if let Ok(decompressed_data) = res {
                            Ok((key, decompressed_data))
                        } else {
                            Err(anyhow!(res.unwrap_err()))
                        });
                    }
                } else {
                    return Some(Ok((key, value)));
                }
            } else {
                return Some(elem);
            }
        }
        assert_eq!(next_segno, 0);
        None
    }
}

impl<'a> DoubleEndedIterator for ToastIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let mut toast: Option<Vec<u8>> = None;
        let mut next_segno = 0u16;
        while let Some(elem) = self.iter.next_back() {
            if let Ok((key, value)) = elem {
                assert!(value.len() != 0);
                let key_len = key.len();
                let n_segments =
                    u16::from_be_bytes(key[key_len - 4..key_len - 2].try_into().unwrap());
                let segno = u16::from_be_bytes(key[key_len - 2..].try_into().unwrap());
                let key = key[..key_len - 4].to_vec();
                if n_segments != 0 {
                    // TOAST
                    assert!(segno + 1 == next_segno || next_segno == 0);
                    if next_segno == 0 {
                        let len = (n_segments - 1) as usize * TOAST_SEGMENT_SIZE + value.len();
                        let mut vec = vec![0u8; len];
                        vec[len - value.len()..].copy_from_slice(&value);
                        toast = Some(vec);
                    } else {
                        toast.as_mut().unwrap()[segno as usize * TOAST_SEGMENT_SIZE
                            ..(segno + 1) as usize * TOAST_SEGMENT_SIZE]
                            .copy_from_slice(&value);
                    }
                    next_segno = segno;
                    if next_segno == 0 {
                        let toast = toast.unwrap();
                        if toast.len() == 0 {
                            warn!("n_segments={}", n_segments);
                        }
                        assert!(toast.len() != 0);
                        let res = lz4_flex::decompress_size_prepended(&toast);
                        return Some(if let Ok(decompressed_data) = res {
                            Ok((key, decompressed_data))
                        } else {
                            Err(anyhow!(res.unwrap_err()))
                        });
                    }
                } else {
                    return Some(Ok((key, value)));
                }
            } else {
                return Some(elem);
            }
        }
        assert_eq!(next_segno, 0);
        None
    }
}

//
// FIXME-KK: not using WAL now. Implement asynchronous or delayed commit.
//
impl ToastStore {
    pub fn new(path: &Path) -> Result<ToastStore> {
        Ok(ToastStore {
            db: Storage::open(
                &path.join("pageserver.db"),
                Some(&path.join("pageserver.log")),
                StorageConfig {
                    cache_size: CACHE_SIZE,
                    checkpoint_interval: CHECKPOINT_INTERVAL,
                    wal_flush_threshold: WAL_FLUSH_THRESHOLD,
                },
            )?,
            committed: false,
        })
    }

    pub fn put(&mut self, key: &Key, value: &Value) -> Result<()> {
        let mut tx = self.db.start_transaction();
        let value_len = value.len();
        let mut key = key.clone();
        self.committed = false;
        if value_len >= TOAST_SEGMENT_SIZE {
            let compressed_data = lz4_flex::compress_prepend_size(value);
            let compressed_data_len = compressed_data.len();
            let mut offs: usize = 0;
            let mut segno = 0u16;
            let n_segments =
                ((compressed_data_len + TOAST_SEGMENT_SIZE - 1) / TOAST_SEGMENT_SIZE) as u16;
            assert!(n_segments != 0);
            key.extend_from_slice(&n_segments.to_be_bytes());
            key.extend_from_slice(&[0u8; 2]);
            let key_len = key.len();
            while offs + TOAST_SEGMENT_SIZE <= compressed_data_len {
                key[key_len - 2..].copy_from_slice(&segno.to_be_bytes());
                tx.put(
                    &key,
                    &compressed_data[offs..offs + TOAST_SEGMENT_SIZE].to_vec(),
                )?;
                offs += TOAST_SEGMENT_SIZE;
                segno += 1;
            }
            if offs < compressed_data_len {
                key[key_len - 2..].copy_from_slice(&segno.to_be_bytes());
                tx.put(&key, &compressed_data[offs..].to_vec())?;
            }
        } else {
            key.extend_from_slice(&[0u8; 4]);
            tx.put(&key, value)?;
        }
        if tx.get_cache_info().pinned > COMMIT_THRESHOLD {
            tx.commit()?;
            self.committed = true;
        } else {
            tx.delay()?;
        }
        Ok(())
    }

    pub fn checkpoint(&self) -> Result<()> {
        let mut tx = self.db.start_transaction();
        tx.commit()?;
        Ok(())
    }

    pub fn iter(&self) -> ToastIterator<'_> {
        self.range(..)
    }

    pub fn range<R: RangeBounds<Key>>(&self, range: R) -> ToastIterator<'_> {
        let from = match range.start_bound() {
            Bound::Included(key) => {
                let mut key = key.clone();
                key.extend_from_slice(&[0u8; 4]);
                Bound::Included(key)
            }
            Bound::Excluded(key) => {
                let mut key = key.clone();
                key.extend_from_slice(&[0u8; 4]);
                Bound::Excluded(key)
            }
            _ => Bound::Unbounded,
        };
        let till = match range.end_bound() {
            Bound::Included(key) => {
                let mut key = key.clone();
                key.extend_from_slice(&[0xFFu8; 4]);
                Bound::Included(key)
            }
            Bound::Excluded(key) => {
                let mut key = key.clone();
                key.extend_from_slice(&[0xFFu8; 4]);
                Bound::Excluded(key)
            }
            _ => Bound::Unbounded,
        };
        ToastIterator {
            iter: self.db.range((from, till)),
        }
    }

    pub fn remove(&mut self, key: &Key) -> Result<()> {
        let mut tx = self.db.start_transaction();
        let mut min_key = key.clone();
        let mut max_key = key.clone();
        min_key.extend_from_slice(&[0u8; 4]);
        max_key.extend_from_slice(&[0xFFu8; 4]);
        let mut iter = tx.range(&min_key..&max_key);
        self.committed = false;
        if let Some(entry) = iter.next() {
            let mut key = entry?.0.clone();
            let key_len = key.len();
            let n_segments = u16::from_be_bytes(key[key_len - 4..key_len - 2].try_into().unwrap());
            if n_segments != 0 {
                // TOAST
                for i in 0..n_segments {
                    key[key_len - 2..].copy_from_slice(&i.to_be_bytes());
                    tx.remove(&key)?;
                }
            } else {
                tx.remove(&key)?;
            }
        }
        if tx.get_cache_info().pinned > COMMIT_THRESHOLD {
            tx.commit()?;
            self.committed = true;
        } else {
            tx.delay()?;
        }
        Ok(())
    }
}
