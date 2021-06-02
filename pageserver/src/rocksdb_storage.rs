//!
//! An implementation of the ObjectStore interface, backed by RocksDB
//!
use crate::object_store::{ObjectKey, ObjectStore};
use crate::repository::{BufferTag, ObjectTag, RelTag};
use crate::PageServerConf;
use crate::ZTimelineId;
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StorageKey {
    obj_key: ObjectKey,
    lsn: Lsn,
}

struct GarbageCollector {
    garbage: Mutex<HashSet<Vec<u8>>>,
}

impl GarbageCollector {
    fn new() -> GarbageCollector {
        GarbageCollector {
            garbage: Mutex::new(HashSet::new()),
        }
    }

    fn mark_for_deletion(&self, key: &[u8]) {
        let mut garbage = self.garbage.lock().unwrap();
        garbage.insert(key.to_vec());
    }

    fn was_deleted(&self, key: &[u8]) -> bool {
        let key = key.to_vec();
        let mut garbage = self.garbage.lock().unwrap();
        return garbage.remove(&key);
    }
}

pub struct RocksObjectStore {
    _conf: &'static PageServerConf,

    // RocksDB handle
    db: rocksdb::DB,
    gc: Arc<GarbageCollector>,
}

impl ObjectStore for RocksObjectStore {
    fn get(&self, key: &ObjectKey, lsn: Lsn) -> Result<Vec<u8>> {
        let val = self.db.get(StorageKey::ser(&StorageKey {
            obj_key: key.clone(),
            lsn,
        })?)?;
        if let Some(val) = val {
            Ok(val)
        } else {
            bail!("could not find page {:?}", key);
        }
    }

    fn put(&self, key: &ObjectKey, lsn: Lsn, value: &[u8]) -> Result<()> {
        self.db.put(
            StorageKey::ser(&StorageKey {
                obj_key: key.clone(),
                lsn,
            })?,
            value,
        )?;
        Ok(())
    }

    fn unlink(&self, key: &ObjectKey, lsn: Lsn) -> Result<()> {
        self.gc.mark_for_deletion(&StorageKey::ser(&StorageKey {
            obj_key: key.clone(),
            lsn,
        })?);
        Ok(())
    }

    /// Iterate through page versions of given page, starting from the given LSN.
    /// The versions are walked in descending LSN order.
    fn object_versions<'a>(
        &'a self,
        key: &ObjectKey,
        lsn: Lsn,
    ) -> Result<Box<dyn Iterator<Item = (Lsn, Vec<u8>)> + 'a>> {
        let iter = RocksObjectVersionIter::new(&self.db, key, lsn)?;
        Ok(Box::new(iter))
    }

    /// Iterate through all timeline objects
    fn list_objects<'a>(
        &'a self,
        timeline: ZTimelineId,
        nonrel_only: bool,
        lsn: Lsn,
    ) -> Result<Box<dyn Iterator<Item = ObjectTag> + 'a>> {
        let iter = RocksObjectIter::new(&self.db, timeline, nonrel_only, lsn)?;
        Ok(Box::new(iter))
    }

    /// Get a list of all distinct relations in given tablespace and database.
    ///
    /// TODO: This implementation is very inefficient, it scans
    /// through all entries in the given database. In practice, this
    /// is used for CREATE DATABASE, and usually the template database is small.
    /// But if it's not, this will be slow.
    fn list_rels(
        &self,
        timelineid: ZTimelineId,
        spcnode: u32,
        dbnode: u32,
        lsn: Lsn,
    ) -> Result<HashSet<RelTag>> {
        // FIXME: This scans everything. Very slow

        let mut rels: HashSet<RelTag> = HashSet::new();

        let searchkey = StorageKey {
            obj_key: ObjectKey {
                timeline: timelineid,
                tag: ObjectTag::RelationBuffer(BufferTag {
                    rel: RelTag {
                        spcnode,
                        dbnode,
                        relnode: 0,
                        forknum: 0u8,
                    },
                    blknum: 0,
                }),
            },
            lsn: Lsn(0),
        };
        let mut iter = self.db.raw_iterator();
        iter.seek(searchkey.ser()?);
        while iter.valid() {
            let key = StorageKey::des(iter.key().unwrap())?;
            if let ObjectTag::RelationBuffer(buf_tag) = key.obj_key.tag {
                if buf_tag.rel.spcnode != spcnode || buf_tag.rel.dbnode != dbnode {
                    break;
                }
                if key.lsn < lsn {
                    rels.insert(buf_tag.rel);
                }
            } else {
                break;
            }
            iter.next();
        }

        Ok(rels)
    }
}

impl RocksObjectStore {
    /// Open a RocksDB database.
    pub fn open(conf: &'static PageServerConf) -> Result<RocksObjectStore> {
        let opts = Self::get_rocksdb_opts();
        let obj_store = Self::new(conf, opts)?;
        Ok(obj_store)
    }

    /// Create a new, empty RocksDB database.
    pub fn create(conf: &'static PageServerConf) -> Result<RocksObjectStore> {
        let path = conf.workdir.join("rocksdb-storage");
        std::fs::create_dir(&path)?;

        let mut opts = Self::get_rocksdb_opts();
        opts.create_if_missing(true);
        opts.set_error_if_exists(true);
        let obj_store = Self::new(conf, opts)?;
        Ok(obj_store)
    }

    fn new(conf: &'static PageServerConf, mut opts: rocksdb::Options) -> Result<RocksObjectStore> {
        let path = conf.workdir.join("rocksdb-storage");
        let gc = Arc::new(GarbageCollector::new());
        let gc_ref = gc.clone();
        opts.set_compaction_filter("ttl", move |_level: u32, _key: &[u8], _val: &[u8]| {
            if gc_ref.was_deleted(_key) {
                rocksdb::compaction_filter::Decision::Remove
            } else {
                rocksdb::compaction_filter::Decision::Keep
            }
        });
        let db = rocksdb::DB::open(&opts, &path)?;
        let obj_store = RocksObjectStore {
            _conf: conf,
            db,
            gc,
        };
        Ok(obj_store)
    }

    /// common options used by `open` and `create`
    fn get_rocksdb_opts() -> rocksdb::Options {
        let mut opts = rocksdb::Options::default();
        opts.set_use_fsync(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts
    }
}

///
/// Iterator for `object_versions`. Returns all page versions of a given block, in
/// reverse LSN order.
///
struct RocksObjectVersionIter<'a> {
    obj_key: ObjectKey,
    dbiter: rocksdb::DBRawIterator<'a>,
    first_call: bool,
}
impl<'a> RocksObjectVersionIter<'a> {
    fn new(
        db: &'a rocksdb::DB,
        obj_key: &ObjectKey,
        lsn: Lsn,
    ) -> Result<RocksObjectVersionIter<'a>> {
        let key = StorageKey {
            obj_key: obj_key.clone(),
            lsn,
        };
        let mut dbiter = db.raw_iterator();
        dbiter.seek_for_prev(StorageKey::ser(&key)?); // locate last entry
        Ok(RocksObjectVersionIter {
            first_call: true,
            obj_key: obj_key.clone(),
            dbiter,
        })
    }
}
impl<'a> Iterator for RocksObjectVersionIter<'a> {
    type Item = (Lsn, Vec<u8>);

    fn next(&mut self) -> std::option::Option<Self::Item> {
        if self.first_call {
            self.first_call = false;
        } else {
            self.dbiter.prev(); // walk backwards
        }

        if !self.dbiter.valid() {
            return None;
        }
        let key = StorageKey::des(self.dbiter.key().unwrap()).unwrap();
        if key.obj_key.tag != self.obj_key.tag {
            return None;
        }
        let val = self.dbiter.value().unwrap();
        let result = val.to_vec();

        Some((key.lsn, result))
    }
}

///
/// Iterator for `list_objects`. Returns all objects preceeding specified LSN
///
struct RocksObjectIter<'a> {
    timeline: ZTimelineId,
    key: StorageKey,
    nonrel_only: bool,
    lsn: Lsn,
    dbiter: rocksdb::DBRawIterator<'a>,
}
impl<'a> RocksObjectIter<'a> {
    fn new(
        db: &'a rocksdb::DB,
        timeline: ZTimelineId,
        nonrel_only: bool,
        lsn: Lsn,
    ) -> Result<RocksObjectIter<'a>> {
        let key = StorageKey {
            obj_key: ObjectKey {
                timeline,
                tag: ObjectTag::FirstKey,
            },
            lsn: Lsn(0),
        };
        let dbiter = db.raw_iterator();
        Ok(RocksObjectIter {
            key,
            timeline,
            nonrel_only,
            lsn,
            dbiter,
        })
    }
}
impl<'a> Iterator for RocksObjectIter<'a> {
    type Item = ObjectTag;

    fn next(&mut self) -> std::option::Option<Self::Item> {
        loop {
            self.dbiter.seek(StorageKey::ser(&self.key).unwrap());
            if !self.dbiter.valid() {
                return None;
            }
            let key = StorageKey::des(self.dbiter.key().unwrap()).unwrap();
            if key.obj_key.timeline != self.timeline {
                // End of this timeline
                return None;
            }
            self.key = key.clone();
            self.key.lsn = Lsn(u64::MAX); // next seek should skip all versions
            if key.lsn <= self.lsn {
                // visible in this snapshot
                if self.nonrel_only {
                    match key.obj_key.tag {
                        ObjectTag::RelationMetadata(_) => return None,
                        ObjectTag::RelationBuffer(_) => return None,
                        _ => return Some(key.obj_key.tag),
                    }
                } else {
                    return Some(key.obj_key.tag);
                }
            }
        }
    }
}
