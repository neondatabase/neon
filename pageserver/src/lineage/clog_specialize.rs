use crate::repository::Value;
use crate::walrecord::{
    ClogSetAborted, ClogSetCommitted, MultiXactMember, MultixactMembersCreate,
    MultixactOffsetCreate, ZenithWalRecord,
};
use anyhow::{anyhow, Error};
use bytes::{Bytes, BytesMut};
use postgres_ffi::pg_constants::{TRANSACTION_STATUS_ABORTED, TRANSACTION_STATUS_COMMITTED};
use postgres_ffi::xlog_utils::TimestampTz;
use postgres_ffi::{MultiXactId, MultiXactOffset, MultiXactStatus, TransactionId};
use utils::bitpacker::u64packing::{U64Packer, U64Unpacker};
use utils::bitpacker::{Packer, U64PackedData, Unpacker};

#[derive(Clone, Debug)]
pub struct CLogCommittedReader {
    counts: U64Unpacker,
    timestamps: U64Unpacker,
    xids: U64Unpacker,
}

#[derive(Clone, Debug)]
pub struct CLogCommittedWriter {
    counts: U64Packer,
    timestamps: U64Packer,
    xids: U64Packer,
}

#[derive(Clone, Debug)]
pub struct CLogAbortedReader {
    counts: U64Unpacker,
    xids: U64Unpacker,
}

#[derive(Clone, Debug)]
pub struct CLogAbortedWriter {
    counts: U64Packer,
    xids: U64Packer,
}

#[derive(Clone, Debug)]
pub struct CLogBothReader {
    status: U64Unpacker,
    timestamps: U64Unpacker,
    counts: U64Unpacker,
    xids: U64Unpacker,
}

#[derive(Clone, Debug)]
pub struct CLogBothWriter {
    status: U64Packer,
    timestamps: U64Packer,
    counts: U64Packer,
    xids: U64Packer,
}

#[derive(Clone, Debug)]
pub struct MultixactOffsetsReader {
    mxids: U64Unpacker,
    moffs: U64Unpacker,
}

#[derive(Clone, Debug)]
pub struct MultixactOffsetsWriter {
    mxids: U64Packer,
    moffs: U64Packer,
}

#[derive(Clone, Debug)]
pub struct MultixactMembersReader {
    moffs: U64Unpacker,
    nmembers: U64Unpacker,
    member_xids: U64Unpacker,
    member_stati: U64Unpacker,
}

#[derive(Clone, Debug)]
pub struct MultixactMembersWriter {
    moffs: U64Packer,
    nmembers: U64Packer,
    member_xids: U64Packer,
    member_stati: U64Packer,
}

impl From<Bytes> for CLogCommittedReader {
    fn from(bytes: Bytes) -> Self {
        let (counts, rest) = U64PackedData::from_bytes(bytes);
        let (timestamps, rest) = U64PackedData::from_bytes(rest);
        let (xids, rest) = U64PackedData::from_bytes(rest);

        assert_eq!(rest.len(), 0);

        Self {
            counts: <U64Unpacker as Unpacker>::from(&counts),
            timestamps: <U64Unpacker as Unpacker>::from(&timestamps),
            xids: <U64Unpacker as Unpacker>::from(&xids),
        }
    }
}

impl Iterator for CLogCommittedReader {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        let count = self.counts.next()?;
        let mut xids: Vec<TransactionId> = Vec::with_capacity(count as usize);

        for _ in 0..count {
            xids.push(self.xids.next()? as TransactionId);
        }

        Some(Value::WalRecord(ZenithWalRecord::ClogSetCommitted(
            ClogSetCommitted {
                xids,
                timestamp: self.timestamps.next()? as TimestampTz,
            },
        )))
    }
}

impl TryFrom<Vec<Value>> for CLogCommittedWriter {
    type Error = Error;

    fn try_from(vec: Vec<Value>) -> Result<Self, Self::Error> {
        let mut me = Self {
            counts: U64Packer::new(),
            timestamps: U64Packer::new(),
            xids: U64Packer::new(),
        };

        for it in vec.into_iter() {
            let data = match it {
                Value::WalRecord(ZenithWalRecord::ClogSetCommitted(data)) => data,
                _ => return Err(anyhow!("Unexpected record in data: {:?}", &it)),
            };

            me.counts.add(data.xids.len() as u64);
            me.timestamps.add(data.timestamp as u64);

            for xid in data.xids.iter() {
                me.xids.add(*xid as u64);
            }
        }

        Ok(me)
    }
}

impl From<CLogCommittedWriter> for Bytes {
    fn from(value: CLogCommittedWriter) -> Bytes {
        let res_counts = value.counts.finish().as_bytes();
        let res_timestamps = value.timestamps.finish().as_bytes();
        let res_xids = value.xids.finish().as_bytes();

        let mut result =
            BytesMut::with_capacity(res_counts.len() + res_timestamps.len() + res_xids.len());

        result.extend_from_slice(&res_counts);
        result.extend_from_slice(&res_timestamps);
        result.extend_from_slice(&res_xids);

        result.freeze()
    }
}

impl From<Bytes> for CLogAbortedReader {
    fn from(bytes: Bytes) -> Self {
        let (counts, bytes) = U64PackedData::from_bytes(bytes);
        let (xids, rest) = U64PackedData::from_bytes(bytes);

        assert_eq!(rest.len(), 0);

        Self {
            counts: <U64Unpacker as Unpacker>::from(&counts),
            xids: <U64Unpacker as Unpacker>::from(&xids),
        }
    }
}

impl Iterator for CLogAbortedReader {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        let count = self.counts.next()?;
        let mut xids: Vec<TransactionId> = Vec::with_capacity(count as usize);

        for _ in 0..count {
            xids.push(self.xids.next()? as TransactionId);
        }

        Some(Value::WalRecord(ZenithWalRecord::ClogSetAborted(
            ClogSetAborted { xids },
        )))
    }
}

impl TryFrom<Vec<Value>> for CLogAbortedWriter {
    type Error = anyhow::Error;

    fn try_from(vec: Vec<Value>) -> Result<Self, Self::Error> {
        let mut me = Self {
            counts: U64Packer::new(),
            xids: U64Packer::new(),
        };

        for it in vec.into_iter() {
            let data = match it {
                Value::WalRecord(ZenithWalRecord::ClogSetAborted(data)) => data,
                _ => return Err(anyhow!("Unexpected record in data: {:?}", &it)),
            };

            me.counts.add(data.xids.len() as u64);

            for xid in data.xids.iter() {
                me.xids.add(*xid as u64);
            }
        }

        Ok(me)
    }
}

impl From<CLogAbortedWriter> for Bytes {
    fn from(value: CLogAbortedWriter) -> Bytes {
        let res_counts = value.counts.finish().as_bytes();
        let res_xids = value.xids.finish().as_bytes();

        let mut result = BytesMut::with_capacity(res_counts.len() + res_xids.len());

        result.extend_from_slice(&res_counts);
        result.extend_from_slice(&res_xids);

        result.freeze()
    }
}

impl From<Bytes> for CLogBothReader {
    fn from(bytes: Bytes) -> Self {
        let (status, bytes) = U64PackedData::from_bytes(bytes);
        let (timestamps, bytes) = U64PackedData::from_bytes(bytes);
        let (counts, bytes) = U64PackedData::from_bytes(bytes);
        let (xids, rest) = U64PackedData::from_bytes(bytes);

        assert_eq!(rest.len(), 0);

        Self {
            status: <U64Unpacker as Unpacker>::from(&status),
            timestamps: <U64Unpacker as Unpacker>::from(&timestamps),
            counts: <U64Unpacker as Unpacker>::from(&counts),
            xids: <U64Unpacker as Unpacker>::from(&xids),
        }
    }
}

impl Iterator for CLogBothReader {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        let status = self.status.next()?;
        let count = self.counts.next()?;
        let mut xids: Vec<TransactionId> = Vec::with_capacity(count as usize);

        for _ in 0..count {
            xids.push(self.xids.next()? as TransactionId);
        }

        match status as u8 {
            TRANSACTION_STATUS_COMMITTED => Some(Value::WalRecord(
                ZenithWalRecord::ClogSetCommitted(ClogSetCommitted {
                    xids,
                    timestamp: self.timestamps.next()? as TimestampTz,
                }),
            )),
            TRANSACTION_STATUS_ABORTED => Some(Value::WalRecord(ZenithWalRecord::ClogSetAborted(
                ClogSetAborted { xids },
            ))),
            _ => None,
        }
    }
}

impl TryFrom<Vec<Value>> for CLogBothWriter {
    type Error = Error;

    fn try_from(vec: Vec<Value>) -> Result<Self, Self::Error> {
        let mut me = Self {
            status: U64Packer::new(),
            timestamps: U64Packer::new(),
            counts: U64Packer::new(),
            xids: U64Packer::new(),
        };

        for it in vec.into_iter() {
            let xids = match it {
                Value::WalRecord(ZenithWalRecord::ClogSetCommitted(data)) => {
                    me.status.add(TRANSACTION_STATUS_COMMITTED as u64);
                    me.timestamps.add(data.timestamp as u64);
                    data.xids
                }
                Value::WalRecord(ZenithWalRecord::ClogSetAborted(data)) => {
                    me.status.add(TRANSACTION_STATUS_ABORTED as u64);
                    data.xids
                }
                _ => return Err(anyhow!("Unexpected record in data: {:?}", &it)),
            };
            me.counts.add(xids.len() as u64);
            for xid in xids.iter() {
                me.xids.add(*xid as u64);
            }
        }

        Ok(me)
    }
}

impl From<CLogBothWriter> for Bytes {
    fn from(value: CLogBothWriter) -> Bytes {
        let res_status = value.status.finish().as_bytes();
        let res_timestamps = value.timestamps.finish().as_bytes();
        let res_counts = value.counts.finish().as_bytes();
        let res_xids = value.xids.finish().as_bytes();

        let mut result = BytesMut::with_capacity(
            res_status.len() + res_timestamps.len() + res_counts.len() + res_xids.len(),
        );

        result.extend_from_slice(&res_status);
        result.extend_from_slice(&res_timestamps);
        result.extend_from_slice(&res_counts);
        result.extend_from_slice(&res_xids);

        result.freeze()
    }
}

impl From<Bytes> for MultixactOffsetsReader {
    fn from(bytes: Bytes) -> Self {
        let (mxids, bytes) = U64PackedData::from_bytes(bytes);
        let (moffs, rest) = U64PackedData::from_bytes(bytes);

        assert_eq!(rest.len(), 0);

        Self {
            mxids: <U64Unpacker as Unpacker>::from(&mxids),
            moffs: <U64Unpacker as Unpacker>::from(&moffs),
        }
    }
}

impl Iterator for MultixactOffsetsReader {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        let mid = self.mxids.next()? as MultiXactId;
        let moff = self.moffs.next()? as MultiXactOffset;

        Some(Value::WalRecord(ZenithWalRecord::MultixactOffsetCreate(
            MultixactOffsetCreate { mid, moff },
        )))
    }
}

impl TryFrom<Vec<Value>> for MultixactOffsetsWriter {
    type Error = Error;

    fn try_from(vec: Vec<Value>) -> Result<Self, Self::Error> {
        let mut me = Self {
            mxids: U64Packer::new(),
            moffs: U64Packer::new(),
        };

        for it in vec.into_iter() {
            let data = match it {
                Value::WalRecord(ZenithWalRecord::MultixactOffsetCreate(data)) => data,
                _ => return Err(anyhow!("Unexpected record in data: {:?}", &it)),
            };

            me.mxids.add(data.mid as u64);
            me.moffs.add(data.moff as u64);
        }

        Ok(me)
    }
}

impl From<MultixactOffsetsWriter> for Bytes {
    fn from(val: MultixactOffsetsWriter) -> Bytes {
        let mxids = val.mxids.finish().as_bytes();
        let moffs = val.moffs.finish().as_bytes();

        let mut result = BytesMut::with_capacity(mxids.len() + moffs.len());

        result.extend_from_slice(&mxids);
        result.extend_from_slice(&moffs);

        result.freeze()
    }
}

impl From<Bytes> for MultixactMembersReader {
    fn from(bytes: Bytes) -> Self {
        let (moffs, rest) = U64PackedData::from_bytes(bytes);
        let (nmembers, rest) = U64PackedData::from_bytes(rest);
        let (member_xids, rest) = U64PackedData::from_bytes(rest);
        let (member_stati, rest) = U64PackedData::from_bytes(rest);

        assert_eq!(rest.len(), 0);

        Self {
            moffs: <U64Unpacker as Unpacker>::from(&moffs),
            nmembers: <U64Unpacker as Unpacker>::from(&nmembers),
            member_xids: <U64Unpacker as Unpacker>::from(&member_xids),
            member_stati: <U64Unpacker as Unpacker>::from(&member_stati),
        }
    }
}

impl Iterator for MultixactMembersReader {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        let moff = self.moffs.next()? as MultiXactOffset;
        let nmembers = self.nmembers.next()? as usize;

        let mut members = Vec::with_capacity(nmembers);

        for _ in 0..nmembers {
            members.push(MultiXactMember {
                xid: self.member_xids.next()? as TransactionId,
                status: self.member_stati.next()? as MultiXactStatus,
            })
        }

        Some(Value::WalRecord(ZenithWalRecord::MultixactMembersCreate(
            MultixactMembersCreate { moff, members },
        )))
    }
}

impl TryFrom<Vec<Value>> for MultixactMembersWriter {
    type Error = Error;

    fn try_from(vec: Vec<Value>) -> Result<Self, Self::Error> {
        let mut me = Self {
            moffs: U64Packer::new(),
            nmembers: U64Packer::new(),
            member_xids: U64Packer::new(),
            member_stati: U64Packer::new(),
        };

        for it in vec.into_iter() {
            let data = match it {
                Value::WalRecord(ZenithWalRecord::MultixactMembersCreate(data)) => data,
                _ => return Err(anyhow!("Unexpected record in data: {:?}", &it)),
            };

            me.moffs.add(data.moff as u64);
            me.nmembers.add(data.members.len() as u64);
            for it in data.members.iter() {
                me.member_xids.add(it.xid as u64);
                me.member_stati.add(it.status as u64);
            }
        }

        Ok(me)
    }
}

impl From<MultixactMembersWriter> for Bytes {
    fn from(other: MultixactMembersWriter) -> Bytes {
        let moffs = other.moffs.finish().as_bytes();
        let nmembers = other.nmembers.finish().as_bytes();
        let member_xids = other.member_xids.finish().as_bytes();
        let member_stati = other.member_stati.finish().as_bytes();

        let mut result = BytesMut::with_capacity(
            moffs.len() + nmembers.len() + member_xids.len() + member_stati.len(),
        );

        result.extend_from_slice(&moffs);
        result.extend_from_slice(&nmembers);
        result.extend_from_slice(&member_xids);
        result.extend_from_slice(&member_stati);

        result.freeze()
    }
}

#[cfg(test)]
mod tests {}
