//!
//! An implementation of the ObjectStore interface, backed by BTreeMap
//!
use crate::object_key::*;
use crate::object_store::ObjectStore;
use crate::repository::RelTag;
use crate::PageServerConf;
use crate::ZTimelineId;
use anyhow::{bail, Result};
use std::collections::{BTreeMap,HashSet};
use std::sync::RwLock;
use zenith_utils::lsn::Lsn;
use std::ops::Bound::*;
use serde::{Deserialize, Serialize};
use zenith_utils::bin_ser::BeSer;
use std::io::prelude::*;
use std::fs::File;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct StorageKey {
    obj_key: ObjectKey,
    lsn: Lsn,
}

impl StorageKey {
    /// The first key for a given timeline
    fn timeline_start(timeline: ZTimelineId) -> Self {
        Self {
            obj_key: ObjectKey {
                timeline,
                tag: ObjectTag::FirstTag,
            },
            lsn: Lsn(0),
        }
    }
}

pub struct InmemObjectStore {
    conf: &'static PageServerConf,
    db: RwLock<BTreeMap<StorageKey, Vec<u8>>>,
}

impl ObjectStore for InmemObjectStore {
    fn get(&self, key: &ObjectKey, lsn: Lsn) -> Result<Vec<u8>> {
        let db = self.db.read().unwrap();
        let val = db.get(&StorageKey {
            obj_key: key.clone(),
            lsn,
        });
        if let Some(val) = val {
            Ok(val.clone())
        } else {
            bail!("could not find page {:?}", key);
        }
    }

    fn get_next_key(&self, key: &ObjectKey) -> Result<Option<ObjectKey>> {
        let search_key = StorageKey {
            obj_key: key.clone(),
            lsn: Lsn(0),
        };
        let db = self.db.read().unwrap();
        for pair in db.range(&search_key..) {
            let key = pair.0;
            return Ok(Some(key.obj_key.clone()));
        }
		Ok(None)
    }

    fn put(&self, key: &ObjectKey, lsn: Lsn, value: &[u8]) -> Result<()> {
        let mut db = self.db.write().unwrap();
        db.insert(
            StorageKey {
                obj_key: key.clone(),
                lsn,
            },
            value.to_vec(),
        );
        Ok(())
    }

    fn unlink(&self, key: &ObjectKey, lsn: Lsn) -> Result<()> {
        let mut db = self.db.write().unwrap();
        db.remove(&StorageKey {
            obj_key: key.clone(),
            lsn,
        });
        Ok(())
    }

    /// Iterate through page versions of given page, starting from the given LSN.
    /// The versions are walked in descending LSN order.
    fn object_versions<'a>(
        &'a self,
        key: &ObjectKey,
        lsn: Lsn,
    ) -> Result<Box<dyn Iterator<Item = (Lsn, Vec<u8>)> + 'a>> {
        let from = StorageKey {
            obj_key: key.clone(),
            lsn: Lsn(0),
        };
        let till = StorageKey {
            obj_key: key.clone(),
            lsn,
        };
        let db = self.db.read().unwrap();
		let versions: Vec<(Lsn, Vec<u8>)> = db.range(from..=till).map(|pair|(pair.0.lsn, pair.1.clone())).collect();
        Ok(Box::new(InmemObjectVersionIter::new(versions)))
	}

    /// Iterate through all timeline objects
    fn list_objects<'a>(
        &'a self,
        timeline: ZTimelineId,
        nonrel_only: bool,
        lsn: Lsn,
    ) -> Result<Box<dyn Iterator<Item = ObjectTag> + 'a>> {
        let curr_key = StorageKey::timeline_start(timeline);

        Ok(Box::new(InmemObjectIter {
			store: &self,
            curr_key,
            timeline,
			nonrel_only,
            lsn,
        }))
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

        let mut search_rel_tag = RelTag {
            spcnode,
            dbnode,
            relnode: 0,
            forknum: 0u8,
        };
		let db = self.db.read().unwrap();
		'outer: loop {
			let search_key = StorageKey {
				obj_key: ObjectKey {
					timeline: timelineid,
					tag: ObjectTag::RelationMetadata(search_rel_tag),
				},
				lsn: Lsn(0),
			};
			for pair in db.range(&search_key..) {
				let key = pair.0;

				if let ObjectTag::RelationMetadata(rel_tag) = key.obj_key.tag {
					if spcnode != 0 && rel_tag.spcnode != spcnode
						|| dbnode != 0 && rel_tag.dbnode != dbnode
					{
						break 'outer;
					}
					if key.lsn <= lsn {
						// visible in this snapshot
						rels.insert(rel_tag);
					}
					search_rel_tag = rel_tag;
					// skip to next relation
					// FIXME: What if relnode is u32::MAX ?
					search_rel_tag.relnode += 1;
					continue  'outer;
				} else {
					// no more relation metadata entries
					break 'outer;
				}
			}
        }

        Ok(rels)
    }

    /// Iterate through versions of all objects in a timeline.
    ///
    /// Returns objects in increasing key-version order.
    /// Returns all versions up to and including the specified LSN.
    fn objects<'a>(
        &'a self,
        timeline: ZTimelineId,
        lsn: Lsn,
    ) -> Result<Box<dyn Iterator<Item = Result<(ObjectTag, Lsn, Vec<u8>)>> + 'a>> {
        let curr_key = StorageKey::timeline_start(timeline);

        Ok(Box::new(InmemObjects {
			store: &self,
            curr_key,
            timeline,
            lsn,
        }))
    }

    fn compact(&self) {
    }
}

impl Drop for InmemObjectStore {
	fn drop(&mut self) {
		let path = self.conf.workdir.join("objstore.dmp");
		let mut f = File::create(path).unwrap();
		f.write(&self.db.ser().unwrap()).unwrap();
	}
}

impl InmemObjectStore {
    pub fn open(conf: &'static PageServerConf) -> Result<InmemObjectStore> {
		let path = conf.workdir.join("objstore.dmp");
		let mut f = File::open(path)?;
		let mut buffer = Vec::new();
		// read the whole file
		f.read_to_end(&mut buffer)?;
		let db = RwLock::new(BTreeMap::des(&buffer)?);
		Ok(InmemObjectStore {
            conf: conf,
            db
        })
    }

    pub fn create(conf: &'static PageServerConf) -> Result<InmemObjectStore> {
        Ok(InmemObjectStore {
            conf: conf,
            db: RwLock::new(BTreeMap::new()),
        })
    }
}

///
/// Iterator for `object_versions`. Returns all page versions of a given block, in
/// reverse LSN order.
///
struct InmemObjectVersionIter {
	versions: Vec<(Lsn, Vec<u8>)>,
	curr: usize,
}
impl InmemObjectVersionIter {
    fn new(versions: Vec<(Lsn, Vec<u8>)>) -> InmemObjectVersionIter {
		let curr = versions.len();
        InmemObjectVersionIter {
            versions,
			curr
		}
    }
}
impl Iterator for InmemObjectVersionIter {
    type Item = (Lsn, Vec<u8>);

    fn next(&mut self) -> std::option::Option<Self::Item> {
		if self.curr == 0 {
			None
		} else {
			self.curr -= 1;
			Some(self.versions[self.curr].clone())
		}
    }
}

struct InmemObjects<'r> {
    store: &'r InmemObjectStore,
	curr_key: StorageKey,
    timeline: ZTimelineId,
    lsn: Lsn,
}

impl<'r> Iterator for InmemObjects<'r> {
    // TODO consider returning Box<[u8]>
    type Item = Result<(ObjectTag, Lsn, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
		self.next_result().transpose()
    }
}

impl<'r> InmemObjects<'r> {
    fn next_result(&mut self) -> Result<Option<(ObjectTag, Lsn, Vec<u8>)>> {
		let db = self.store.db.read().unwrap();
        for pair in db.range((Excluded(&self.curr_key),Unbounded)) {
			let key = pair.0;
			if key.obj_key.timeline != self.timeline {
                return Ok(None);
            }
			if key.lsn > self.lsn {
                // TODO can speed up by seeking iterator
                continue;
            }
			self.curr_key = key.clone();
			let value = pair.1.clone();
            return Ok(Some((key.obj_key.tag, key.lsn, value)));
		}
        Ok(None)
    }
}

///
/// Iterator for `list_objects`. Returns all objects preceeding specified LSN
///
struct InmemObjectIter<'a> {
    store: &'a InmemObjectStore,
	curr_key: StorageKey,
    timeline: ZTimelineId,
	nonrel_only: bool,
    lsn: Lsn,
}

impl<'a> Iterator for InmemObjectIter<'a> {
    type Item = ObjectTag;

    fn next(&mut self) -> std::option::Option<Self::Item> {
		let db = self.store.db.read().unwrap();
		'outer: loop {
			for pair in db.range((Excluded(&self.curr_key),Unbounded)) {
				let key = pair.0;
				if key.obj_key.timeline != self.timeline {
					return None;
				}
				self.curr_key = key.clone();
				self.curr_key.lsn = Lsn(u64::MAX); // next seek should skip all versions
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
				continue 'outer;
			}
			return None;
		}
    }
}
