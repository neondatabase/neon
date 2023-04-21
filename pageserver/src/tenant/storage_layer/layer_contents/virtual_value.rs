use crate::repository::Value;
use crate::walrecord::NeonWalRecord;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use utils::lsn::Lsn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VirtualValue {
    NaturalImage(Bytes),
    NaturalWalRecord(NeonWalRecord),
    ClosedLineage {
        image: Bytes,
        lsns: Vec<Lsn>,
        records: Vec<NeonWalRecord>,
    },
    ClosedRecLineage {
        image_rec: NeonWalRecord,
        lsns: Vec<Lsn>,
        records: Vec<NeonWalRecord>,
    },
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
            VirtualValue::ClosedLineage { .. } => true,
            VirtualValue::ClosedRecLineage { .. } => true,
            VirtualValue::NaturalWalRecord(rec) => rec.will_init(),
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

pub struct VirtualValueBuilder {
    state: Option<(Lsn, VirtualValue)>,
}

impl VirtualValueBuilder {
    pub fn new() -> Self {
        Self { state: None }
    }

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

    pub fn finish(mut self) -> Option<(Lsn, VirtualValue)> {
        self.state.take()
    }
}

impl Drop for VirtualValueBuilder {
    fn drop(&mut self) {
        assert!(self.state.is_none());
    }
}
