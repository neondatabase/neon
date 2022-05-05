use crate::lineage::spec::RecordIOps;
use crate::repository::Value;
use bytes::Bytes;
use utils::bitpacker::lsn_packing::LsnUnpacker;
use utils::bitpacker::u64packing::U64Unpacker;
use utils::bitpacker::{U64DeltaPackedData, U64PackedData, Unpacker};
use utils::lsn::Lsn;

pub struct LineageReader<T> {
    lsns: LsnUnpacker,
    lsn_limit: Lsn,
    inner: T,
}

impl<T> LineageReader<T> {
    pub fn new(bytes: Bytes, limit: Lsn) -> Self
    where
        T: Iterator<Item = Value> + From<Bytes>,
    {
        let (dpd, rest) = U64DeltaPackedData::from_bytes(bytes);
        let lsns = <LsnUnpacker as Unpacker>::from(&dpd);
        Self {
            lsns,
            lsn_limit: limit,
            inner: <T as From<Bytes>>::from(rest),
        }
    }

    pub fn num_records(bytes: &Bytes) -> usize {
        let (dpd, _) = U64DeltaPackedData::from_bytes(bytes.clone());

        dpd.num_entries()
    }
}

impl<T> Iterator for LineageReader<T>
where
    T: Iterator<Item = Value> + From<Bytes>,
{
    type Item = (Lsn, Value);

    fn next(&mut self) -> Option<Self::Item> {
        let next_lsn = self.lsns.next()?;
        if next_lsn > self.lsn_limit {
            return None;
        }
        let value = <T as Iterator>::next(&mut self.inner)?;
        Some((next_lsn, value))
    }
}

pub struct ReadRecords<T: RecordIOps> {
    iter: <T as RecordIOps>::ReaderIter,
}

impl<T> From<Bytes> for ReadRecords<T>
where
    T: RecordIOps,
{
    fn from(bytes: Bytes) -> Self {
        Self {
            iter: T::read_records_from(Box::new(BytesUnpacker::from(bytes))),
        }
    }
}

impl<T> Iterator for ReadRecords<T>
where
    T: RecordIOps,
{
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

struct BytesUnpacker {
    lengths: U64Unpacker,
    remainder: Bytes,
}

impl From<Bytes> for BytesUnpacker {
    fn from(bytes: Bytes) -> Self {
        let (packed_data, remainder) = U64PackedData::from_bytes(bytes);

        Self {
            lengths: <U64Unpacker as Unpacker>::from(&packed_data),
            remainder,
        }
    }
}

impl Iterator for BytesUnpacker {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        let length = self.lengths.next()?;
        let result = self.remainder.slice(..length as usize);
        self.remainder = self.remainder.slice(length as usize..);

        Some(result)
    }
}
