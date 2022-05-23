use crate::lineage::spec::RecordIOps;
use crate::repository::Value;
use bytes::Bytes;
use utils::bitpacker::lsn_packing::LsnUnpacker;
use utils::bitpacker::u64packing::U64Unpacker;
use utils::bitpacker::{U64DeltaPackedData, U64PackedData, Unpacker};
use utils::lsn::Lsn;

/// LineageReader reads a serialized version of a lineage. This struct is
/// responsible for the deserialization of the LSNs, and delegates the
/// deserialization of records to it's inner type.
///
/// Values are only returned through iteration, so as to limit deserialization
/// efforts to a minimum.
///
/// [T] is unbound on the type to provide num_records without a concrete reader
/// implementation type.
pub struct LineageReader<T> {
    /// The unpacker for the contained LSNs. Returned in increasing order.
    lsns: LsnUnpacker,
    /// Up to which LSN must we return the values?
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

    /// How many records are there in this lineage, assuming that the Bytes is
    /// the serialized representation of that lineage?
    pub fn num_records(bytes: &Bytes) -> usize {
        let (dpd, _) = U64DeltaPackedData::from_bytes(bytes.clone());

        dpd.num_entries()
    }
}

/// Iterator implementation for LineageReader.
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
        let value = <T as Iterator>::next(&mut self.inner).unwrap();
        Some((next_lsn, value))
    }
}

/// Helper for reading lineages have been written on a record-by-record basis
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

/// Deserializer helper for serialized Iter<Bytes>.
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
        let length = self.lengths.next().unwrap();
        let result = self.remainder.slice(..length as usize);
        self.remainder = self.remainder.slice(length as usize..);

        Some(result)
    }
}
