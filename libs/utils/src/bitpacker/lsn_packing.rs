use crate::bitpacker::u64delta_packing::{U64DeltaPacker, U64DeltaUnpacker};
use crate::bitpacker::{Packer, U64DeltaPackedData, Unpacker, IGNORE_LSN_BITS};
use crate::lsn::Lsn;

#[inline]
fn to_u64(lsn: Lsn) -> u64 {
    debug_assert!((lsn.0) & ((1 << IGNORE_LSN_BITS) - 1) == 0);

    lsn.0 >> IGNORE_LSN_BITS
}

#[inline]
fn to_lsn(val: u64) -> Lsn {
    debug_assert!(val < (((1 << IGNORE_LSN_BITS) - 1) << (64 - IGNORE_LSN_BITS)));

    Lsn(val << IGNORE_LSN_BITS)
}

#[derive(Clone, Debug)]
pub struct LsnPacker {
    inner: U64DeltaPacker,
}

impl LsnPacker {
    pub fn new(base_value: Lsn) -> Self {
        Self {
            inner: U64DeltaPacker::new(to_u64(base_value)),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LsnUnpacker {
    inner: U64DeltaUnpacker,
}

impl Packer for LsnPacker {
    type Item = Lsn;
    type Serialized = U64DeltaPackedData;

    #[inline]
    fn add(&mut self, value: Lsn) {
        self.inner.add(to_u64(value))
    }

    fn flush(&mut self) {
        self.inner.flush();
    }

    fn finish(self) -> U64DeltaPackedData {
        self.inner.finish()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl Iterator for LsnUnpacker {
    type Item = Lsn;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.inner.next()?;

        Some(to_lsn(next))
    }
}

impl Unpacker for LsnUnpacker {
    type Serialized = U64DeltaPackedData;

    fn remaining(&self) -> usize {
        self.inner.remaining()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn from(serialized: &U64DeltaPackedData) -> Self {
        Self {
            inner: <U64DeltaUnpacker as Unpacker>::from(serialized),
        }
    }
}
