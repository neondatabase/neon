#[derive(Clone, Debug, Default)]
pub struct BitUnpacker {
    num_bits: u64,
    mask: u64,
}

impl BitUnpacker {
    pub fn new(num_bits: u8) -> BitUnpacker {
        let mask: u64 = if num_bits == 64 {
            !0u64
        } else {
            (1u64 << num_bits) - 1u64
        };
        BitUnpacker {
            num_bits: u64::from(num_bits),
            mask,
        }
    }

    #[inline]
    pub fn get(&self, idx: u64, data: &[u8]) -> u64 {
        if self.num_bits == 0 {
            return 0u64;
        }
        let num_bits = self.num_bits;
        let mask = self.mask;
        let addr_in_bits = idx * num_bits;
        let addr = addr_in_bits >> 3;
        let bit_shift = addr_in_bits & 7;

        if addr + 8 <= data.len() as u64 {
            let bytes: [u8; 8] = (&data[(addr as usize)..(addr as usize) + 8])
                .try_into()
                .unwrap();
            let val_unshifted_unmasked: u64 = u64::from_le_bytes(bytes);
            let val_shifted = (val_unshifted_unmasked >> bit_shift) as u64;
            val_shifted & mask
        } else {
            let mut bytes = [0u8; 8];

            for i in 0..(data.len() - (addr as usize)) {
                bytes[i] = data[(addr as usize) + i];
            }

            let val_unshifted_unmasked: u64 = u64::from_le_bytes(bytes);
            let val_shifted = (val_unshifted_unmasked >> bit_shift) as u64;
            val_shifted & mask
        }
    }
}
