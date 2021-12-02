use anyhow::{anyhow, Result};
use lz4_flex;
use std::convert::TryInto;
use std::ops::{Bound, RangeBounds};
use std::path::Path;

use yakv::storage::{
    Key, ReadOnlyTransaction, Select, Storage, StorageConfig, StorageIterator, Transaction, Value,
};

const TOAST_SEGMENT_SIZE: usize = 2 * 1024;
const CACHE_SIZE: usize = 32 * 1024; // 256Mb
                                     //const CACHE_SIZE: usize = 128 * 1024; // 1Gb

///
/// Toast storage consistof two KV databases: one for storing main index
/// and second for storing sliced BLOB (values larger than 2kb).
/// BLOBs and main data are stored in different databases to improve
/// data locality and reduce key size for TOAST segments.
///
pub struct ToastStore {
    db: Storage, // key-value database
}

pub struct ToastIterator<'a> {
    iter: StorageIterator<'a>,
}

pub struct ToastSnapshot<'a> {
    tx: ReadOnlyTransaction<'a>,
}

impl<'a> ToastSnapshot<'a> {
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
            iter: self.tx.range((from, till)),
        }
    }

    pub fn iter(&self) -> ToastIterator<'_> {
        self.range(..)
    }
}

impl<'a> Iterator for ToastIterator<'a> {
    type Item = Result<(Key, Value)>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut toast: Option<Vec<u8>> = None;
        let mut next_segno = 0u16;
        for elem in &mut self.iter {
            let res = if let Ok((key, value)) = elem {
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
                    if next_segno != n_segments {
                        continue;
                    }
                    let res = lz4_flex::decompress_size_prepended(&toast.unwrap());
                    if let Ok(decompressed_data) = res {
                        Ok((key, decompressed_data))
                    } else {
                        Err(anyhow!(res.unwrap_err()))
                    }
                } else {
                    Ok((key, value))
                }
            } else {
                elem
            };
            return Some(res);
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
                assert!(!value.is_empty());
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
                        assert!(!toast.is_empty());
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
                StorageConfig {
                    cache_size: CACHE_SIZE,
                    nosync: false,
                },
            )?,
        })
    }

    pub fn put(&self, key: Key, value: Value) -> Result<()> {
        let mut tx = self.db.start_transaction();
        self.tx_remove(&mut tx, &key)?;
        let value_len = value.len();
        let mut key = key;
        if value_len >= TOAST_SEGMENT_SIZE {
            let compressed_data = lz4_flex::compress_prepend_size(&value);
            let compressed_data_len = compressed_data.len();
            let mut offs: usize = 0;
            let mut segno = 0u16;
            let n_segments =
                ((compressed_data_len + TOAST_SEGMENT_SIZE - 1) / TOAST_SEGMENT_SIZE) as u16;
            assert!(n_segments != 0);
            key.extend_from_slice(&n_segments.to_be_bytes());
            key.extend_from_slice(&[0u8; 2]);
            let key_len = key.len();
            while offs + TOAST_SEGMENT_SIZE < compressed_data_len {
                key[key_len - 2..].copy_from_slice(&segno.to_be_bytes());
                tx.put(
                    &key,
                    &compressed_data[offs..offs + TOAST_SEGMENT_SIZE].to_vec(),
                )?;
                offs += TOAST_SEGMENT_SIZE;
                segno += 1;
            }
            key[key_len - 2..].copy_from_slice(&segno.to_be_bytes());
            tx.put(&key, &compressed_data[offs..].to_vec())?;
        } else {
            key.extend_from_slice(&[0u8; 4]);
            tx.put(&key, &value)?;
        }
        tx.delay();
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        let tx = self.db.start_transaction();
        tx.commit()?;
        Ok(())
    }

    pub fn take_snapshot(&self) -> ToastSnapshot<'_> {
        ToastSnapshot {
            tx: self.db.read_only_transaction(),
        }
    }

    pub fn remove(&self, key: Key) -> Result<()> {
        let mut tx = self.db.start_transaction();
        self.tx_remove(&mut tx, &key)?;
        tx.delay();
        Ok(())
    }

    pub fn tx_remove(&self, tx: &mut Transaction, key: &[u8]) -> Result<()> {
        let mut min_key = key.to_vec();
        let mut max_key = key.to_vec();
        min_key.extend_from_slice(&[0u8; 4]);
        max_key.extend_from_slice(&[0xFFu8; 4]);
        let mut iter = tx.range(&min_key..&max_key);
        if let Some(entry) = iter.next() {
            let mut key = entry?.0;
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
        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.db.get_database_info().db_used
    }
}
