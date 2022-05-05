use crate::bitpacker::u64packing::{U64Packer, U64Unpacker};
use crate::bitpacker::{Packer, U64DeltaPackedData, Unpacker};

#[derive(Clone, Debug)]
pub struct U64DeltaPacker {
    /// base value of the packer.
    base_value: u64,
    /// last value that was packed.
    latest_value: u64,
    /// packer for the deltas.
    u64_packer: U64Packer,
}

#[derive(Clone, Debug)]
pub struct U64DeltaUnpacker {
    /// Last value that was packed.
    latest_value: u64,
    /// packer for the deltas.
    u64_unpacker: U64Unpacker,
}

impl U64DeltaPacker {
    pub fn new(base_value: u64) -> Self {
        Self {
            base_value,
            latest_value: base_value,
            u64_packer: U64Packer::new(),
        }
    }
}

impl Packer for U64DeltaPacker {
    type Item = u64;
    type Serialized = U64DeltaPackedData;

    #[inline]
    fn add(&mut self, value: u64) {
        debug_assert!(value >= self.latest_value);

        let delta = value - self.latest_value;
        self.u64_packer.add(delta);
        self.latest_value = value;
    }

    fn flush(&mut self) {
        self.u64_packer.flush();
    }

    fn finish(self) -> U64DeltaPackedData {
        U64DeltaPackedData {
            base_value: self.base_value,
            deltas: self.u64_packer.finish(),
        }
    }

    fn len(&self) -> usize {
        self.u64_packer.len()
    }
}

impl Iterator for U64DeltaUnpacker {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        let delta = self.u64_unpacker.next()?;

        self.latest_value += delta;

        Some(self.latest_value)
    }
}

impl Unpacker for U64DeltaUnpacker {
    type Serialized = U64DeltaPackedData;

    fn remaining(&self) -> usize {
        self.u64_unpacker.remaining()
    }

    fn len(&self) -> usize {
        self.u64_unpacker.len()
    }

    fn from(serialized: &U64DeltaPackedData) -> Self {
        Self {
            latest_value: serialized.base_value,
            u64_unpacker: <U64Unpacker as Unpacker>::from(&serialized.deltas),
        }
    }
}
