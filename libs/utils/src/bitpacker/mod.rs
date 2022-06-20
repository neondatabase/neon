mod bitpack_overrides;
pub mod lsn_packing;
pub mod u64delta_packing;
pub mod u64packing;

use crate::bitpacker::u64packing::U64Packer;
use byteorder::{ReadBytesExt, WriteBytesExt, BE};
use bytes::Bytes;
use core::mem::size_of;
use std::io::{Cursor, Write};

/// A bitpacker for LSNs.
/// Stores LSNs in a compact, incremental manner.
///
/// Access times:
///     Random: O(n)
///     First: O(1) (amortized)
///     Next: O(1) (amortized)
///
/// Overhead: 1 byte per block of 32 LSNs for storing the number of significant bits stored,
///  plus 1 varuint per 32 LSNs for storing the minimum increment in those 32 LSNs.
/// The 1 byte per 32 LSNs overhead can be reused for up to 3 more sequential blocks of 32 LSNs.
///
/// Implementation taken and adapted from tantivy_bitpacker::BlockedBitpacker; under MIT licence.
///
/// Alterations from the original:
///  - Block size from 128 to 32
///     LSNs are highly variable; smaller blocks should be more efficient.
///  - We do differential encoding instead of absolute encoding.
///      O(1) random access is not important; it is more important to keep numbers small.
///  - We manually / unpack the Metadata entries into the 'offset_and_bits' field (here: 'bits').
///  - We've applied an optimization utilizing two unused bits in the num_bits_block metadata variable.
///  - We truncate the 2 unused LSN bits (WAL records are always aligned to 4 bytes; we don't need
///     those last 2 bits of precision).

const COMPRESS_BLOCK_SIZE: usize = 32;
/// Alignment is to MAXALIGN, which is 8 bytes,
/// which means we can round up Lsns to the next multiple of 8.
pub const IGNORE_LSN_BITS: usize = 3;

pub trait Packer {
    type Item;
    type Serialized;
    /// Add a value to the packer
    fn add(&mut self, value: Self::Item);
    /// Flush the packer. Some packers might not be able to start up again after this.
    fn flush(&mut self);
    /// Finalize the packer
    fn finish(self) -> Self::Serialized;
    /// Check the current amount of packed values.
    fn len(&self) -> usize;
    /// Check if it is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait Unpacker: Iterator {
    type Serialized;

    fn remaining(&self) -> usize;
    fn len(&self) -> usize;
    fn from(serialized: &Self::Serialized) -> Self;
    /// Check if it is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct U64PackedData {
    pub(crate) n_items: usize,
    pub(crate) metadata: Bytes,
    pub(crate) data: Bytes,
}

impl U64PackedData {
    pub fn as_bytes(&self) -> Bytes {
        let mut cursor = Cursor::new(Vec::<u8>::with_capacity(
            size_of::<u32>() + self.metadata.len() + self.data.len(),
        ));

        WriteBytesExt::write_u32::<BE>(&mut cursor, self.n_items as u32).unwrap();
        cursor.write_all(&self.metadata[..]).unwrap();
        cursor.write_all(&self.data[..]).unwrap();

        Bytes::from(cursor.into_inner())
    }

    pub fn from_bytes(bytes: Bytes) -> (Self, Bytes) {
        let mut cursor = Cursor::new(&bytes[..]);
        let n_items = ReadBytesExt::read_u32::<BE>(&mut cursor).unwrap() as usize;
        let meta_start = cursor.position() as usize;

        let varlen_bytes = bytes.slice(meta_start..);

        let (meta_length, data_length) = U64Packer::lengths(n_items, &varlen_bytes);

        let metadata = varlen_bytes.slice(0..meta_length);
        let data = varlen_bytes.slice(meta_length..(meta_length + data_length));
        let remainder = varlen_bytes.slice((meta_length + data_length)..);

        let result = Self {
            n_items,
            metadata,
            data,
        };

        (result, remainder)
    }

    pub fn num_entries(&self) -> usize {
        self.n_items
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct U64DeltaPackedData {
    pub(crate) base_value: u64,
    pub(crate) deltas: U64PackedData,
}

impl U64DeltaPackedData {
    pub fn as_bytes(&self) -> Bytes {
        let deltas_vec = self.deltas.as_bytes();
        let mut cursor = Cursor::new(Vec::<u8>::with_capacity(
            deltas_vec.len() + size_of::<u64>(),
        ));

        WriteBytesExt::write_u64::<BE>(&mut cursor, self.base_value).unwrap();

        cursor.write_all(&deltas_vec[..]).unwrap();

        Bytes::from(cursor.into_inner())
    }

    pub fn from_bytes(slice: Bytes) -> (Self, Bytes) {
        let mut cursor = Cursor::new(&slice[..]);
        let base_value = ReadBytesExt::read_u64::<BE>(&mut cursor).unwrap();
        let inner_start = cursor.position() as usize;
        let (deltas, remaining) = U64PackedData::from_bytes(slice.slice(inner_start..));

        let result = Self { base_value, deltas };

        (result, remaining)
    }

    pub fn num_entries(&self) -> usize {
        self.deltas.n_items
    }
}

#[cfg(test)]
mod test {
    use crate::bitpacker::lsn_packing::{LsnPacker, LsnUnpacker};
    use crate::bitpacker::{Packer, U64DeltaPackedData, Unpacker};
    use crate::lsn::Lsn;
    use rand::Rng;
    use std::ops::Shl;

    fn gen_lsn_vec(base_lsn: Lsn, config_pairs: Vec<(usize, usize)>) -> Vec<Lsn> {
        let mut lsns = vec![];
        let mut base_lsn = base_lsn;

        let mut rng = rand::thread_rng();

        for (n_lsns, n_bits) in config_pairs {
            for _ in 0..n_lsns {
                let my_length: u32 = rng.gen();
                let rec_interval = match n_bits {
                    1..=29 => ((my_length >> (32 - n_bits)) & 0xFFFF_FFF8) as u64,
                    30..=61 => (my_length as u64).shl(n_bits + 3 - 32),
                    _ => panic!("Invalid value for n_bits: {}", n_bits),
                };

                base_lsn += rec_interval;
                lsns.push(base_lsn);
            }
        }

        lsns
    }

    fn test_packer_util(data: Vec<Lsn>) {
        let first = *data.first().unwrap();

        let mut packer = LsnPacker::new(first);
        for it in data.iter() {
            packer.add(*it);
        }
        packer.flush();
        let packed = packer.finish();

        let ser = packed.as_bytes();
        let (res, remaining) = U64DeltaPackedData::from_bytes(ser);
        assert_eq!(remaining.len(), 0);
        assert_eq!(res, packed);

        let unpacker = <LsnUnpacker as Unpacker>::from(&packed);
        let recovered = unpacker.collect::<Vec<Lsn>>();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_valid_lsns() {
        let mut data = gen_lsn_vec(Lsn(0), vec![(128, 32)]);

        data.sort();

        test_packer_util(data);
        test_packer_util(vec![Lsn(0), Lsn(128), Lsn(0xffff_fff0)]);
    }

    #[test]
    fn test_multi_range_lsns() {
        let mut data = gen_lsn_vec(Lsn(0), vec![(32, 15), (15, 24), (23, 12)]);

        data.sort();

        test_packer_util(data);
    }
}
