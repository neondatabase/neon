use crate::repository::Value;
use crate::walrecord::NeonWalRecord;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use utils::lsn::Lsn;

/// VirtualValue is stored in some Layers instead of the normal repository::Value.
///
/// It describes one or more Values that are associated to one
/// repostiory::Key, containing zero or one base image with all WAL records
/// that need to apply to this preceding page image. In balanced tree-based
/// indexes this reduces the number of full Keys we need to store, thus
/// reducing the size of the layer's index and increasing cache efficiency.
///
/// Additionally, the abstraction paves the way to implement compression in the
/// layer file themselves, as we'd just need to add a new variant to the
/// VirtualValue type for compressed types. Examples of such optimizations
/// are bitpacked and delta-encoded LSNs in the Lineage variants of this enum.
///
/// NOTE: Once committed into a hosted branch, these variants _must_ remain
/// in this order, and cannot be removed - they are part of the specification
/// of the physical layout of the DeltaLayer file. Any reordering is going to
/// change the meaning of bytes in existing files and break the compatibility
/// with old layers; so make sure you don't reorder these, nor should you
/// update the layout of existing variants. You can update new variants as long
/// as no user data is written using these variants.
///
/// NOTE: The first two variants are cloned over from repository::Value, which
/// was the definition of the stored data in DeltaLayer before VirtualValue.
/// These variants have the same layout and index, so they should (de)serialize
/// into the same binary format, guaranteeing backwards compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VirtualValue {
    /// NaturalImage: A natural WAL image, picked from PostgreSQL WAL.
    NaturalImage(Bytes),
    /// NaturalWalRecord: A natural WAL record, picked from PostgreSQL WAL.
    NaturalWalRecord(NeonWalRecord),
    /// ClosedLineage: A page image, followed by a set of WAL records that are
    /// applied to that page image.
    ClosedLineage {
        image: Bytes,
        lsns: Vec<Lsn>,
        records: Vec<NeonWalRecord>,
    },
    /// ClosedRecLineage: A will-init WAL record, followed by a set of WAL
    /// records that are applied to the page image of the WAL record.
    ClosedRecLineage {
        image_rec: NeonWalRecord,
        lsns: Vec<Lsn>,
        records: Vec<NeonWalRecord>,
    },
    /// OpenLineage: A set of WAL records that are applied to the same page,
    /// but that do not have a known page image in this Layer.
    OpenLineage {
        lsns: Vec<Lsn>,
        records: Vec<NeonWalRecord>,
    },
}

impl VirtualValue {
    pub(crate) fn into_value_vec(self, lsn: Lsn) -> Vec<(Lsn, Value)> {
        match self {
            VirtualValue::NaturalImage(img) => vec![(lsn, Value::Image(img))],
            VirtualValue::NaturalWalRecord(rec) => vec![(lsn, Value::WalRecord(rec))],
            VirtualValue::ClosedLineage {
                image,
                lsns,
                records,
            } => {
                let mut res = Vec::with_capacity(lsns.len() + 1);

                res.push((lsn, Value::Image(image)));

                for (lsn, rec) in Iterator::zip(lsns.into_iter(), records.into_iter()) {
                    res.push((lsn, Value::WalRecord(rec)));
                }

                res
            }
            VirtualValue::ClosedRecLineage {
                image_rec,
                lsns,
                records,
            } => {
                let mut res = Vec::with_capacity(lsns.len() + 1);

                res.push((lsn, Value::WalRecord(image_rec)));

                for (lsn, rec) in Iterator::zip(lsns.into_iter(), records.into_iter()) {
                    res.push((lsn, Value::WalRecord(rec)));
                }

                res
            }
            VirtualValue::OpenLineage { lsns, mut records } => {
                let mut res = Vec::with_capacity(lsns.len() + 1);
                let first_record = records.remove(0);

                res.push((lsn, Value::WalRecord(first_record)));

                for (lsn, rec) in Iterator::zip(lsns.into_iter(), records.into_iter()) {
                    res.push((lsn, Value::WalRecord(rec)));
                }

                res
            }
        }
    }

    pub(crate) fn will_init(&self) -> bool {
        match self {
            VirtualValue::NaturalImage(_) => true,
            VirtualValue::NaturalWalRecord(rec) => rec.will_init(),
            VirtualValue::ClosedLineage { .. } => true,
            VirtualValue::ClosedRecLineage { .. } => true,
            VirtualValue::OpenLineage { .. } => false,
        }
    }
}

impl From<Value> for VirtualValue {
    fn from(value: Value) -> Self {
        match value {
            Value::Image(img) => VirtualValue::NaturalImage(img),
            Value::WalRecord(rec) => VirtualValue::NaturalWalRecord(rec),
        }
    }
}

#[must_use = "deconstruct the value using ::finish to make sure you don't lose intermediate values"]
pub struct VirtualValueBuilder {
    state: Option<(Lsn, VirtualValue)>,
}

impl VirtualValueBuilder {
    pub fn new() -> Self {
        Self { state: None }
    }

    #[must_use = "intermediate emitted values should be stored"]
    pub fn push(&mut self, new_lsn: Lsn, value: Value) -> Option<(Lsn, VirtualValue)> {
        if let Some((lsn, _)) = &self.state {
            assert!(new_lsn > *lsn);
        }

        match value {
            Value::Image(img) => {
                let res = self.state.take();
                self.state = Some((new_lsn, VirtualValue::NaturalImage(img)));
                res
            }
            Value::WalRecord(new_rec) => {
                if new_rec.will_init() {
                    let res = self.state.take();
                    self.state = Some((new_lsn, VirtualValue::NaturalWalRecord(new_rec)));
                    return res;
                }

                match self.state.take() {
                    None => {
                        self.state = Some((new_lsn, VirtualValue::NaturalWalRecord(new_rec)));
                        None
                    }
                    Some((start_lsn, virtual_value)) => {
                        let new_vv = match virtual_value {
                            VirtualValue::NaturalImage(img) => VirtualValue::ClosedLineage {
                                image: img,
                                lsns: vec![new_lsn],
                                records: vec![new_rec],
                            },
                            VirtualValue::NaturalWalRecord(vv_start) => {
                                if vv_start.will_init() {
                                    VirtualValue::ClosedRecLineage {
                                        image_rec: vv_start,
                                        lsns: vec![new_lsn],
                                        records: vec![new_rec],
                                    }
                                } else {
                                    VirtualValue::OpenLineage {
                                        lsns: vec![new_lsn],
                                        records: vec![vv_start, new_rec],
                                    }
                                }
                            }
                            VirtualValue::ClosedLineage {
                                image,
                                mut lsns,
                                mut records,
                            } => {
                                lsns.push(new_lsn);
                                records.push(new_rec);

                                VirtualValue::ClosedLineage {
                                    image,
                                    lsns,
                                    records,
                                }
                            }
                            VirtualValue::ClosedRecLineage {
                                image_rec,
                                mut lsns,
                                mut records,
                            } => {
                                lsns.push(new_lsn);
                                records.push(new_rec);

                                VirtualValue::ClosedRecLineage {
                                    image_rec,
                                    lsns,
                                    records,
                                }
                            }
                            VirtualValue::OpenLineage {
                                mut lsns,
                                mut records,
                            } => {
                                lsns.push(new_lsn);
                                records.push(new_rec);

                                VirtualValue::OpenLineage { lsns, records }
                            }
                        };

                        self.state = Some((start_lsn, new_vv));

                        None
                    }
                }
            }
        }
    }

    #[must_use]
    pub fn finish(mut self) -> Option<(Lsn, VirtualValue)> {
        self.state.take()
    }
}
