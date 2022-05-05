use crate::bitpacker::bitpack_overrides::BitUnpacker;
use crate::bitpacker::{Packer, U64PackedData, Unpacker, COMPRESS_BLOCK_SIZE};
use bytes::{Buf, Bytes};
use std::io::{Cursor, Read, Write};
use tantivy_bitpacker::BitPacker;
use varuint::{ReadVarint, WriteVarint};

/// See tantivy_bitpacker::compute_num_bits -- we don't support encoding of bitlengths between 56
/// and 64 bits, as a limitation of the current implementation.
fn encode_bits(num: u8) -> u8 {
    match num {
        0..=56 => num,
        64 => 57,
        _ => panic!("encode_n_bits: invalid num {}", num),
    }
}

fn decode_bits(num: u8) -> u8 {
    match num & 0b0011_1111 {
        0..=56 => num,
        57 => 64,
        _ => panic!("decode_bits: invalid num {}", num),
    }
}

#[derive(Clone, Debug)]
pub struct U64Packer {
    n_items: usize,
    buffer: Vec<u64>,
    compressed_data: Vec<u8>,
    metadata_bits: Cursor<Vec<u8>>,
    bitsindex: usize,
}

impl Default for U64Packer {
    fn default() -> Self {
        Self::new()
    }
}

impl U64Packer {
    pub fn new() -> Self {
        Self {
            n_items: 0,
            buffer: Vec::with_capacity(COMPRESS_BLOCK_SIZE),
            compressed_data: vec![0; 8],
            metadata_bits: Cursor::new(vec![]),
            bitsindex: 0,
        }
    }

    pub fn lengths(num_items: usize, meta: &[u8]) -> (usize, usize) {
        let mut cursor = Cursor::new(meta);
        let mut total_length_in_bits = 0usize;
        let mut items_remaining = num_items;
        while items_remaining > 0 {
            let blocks_and_size = cursor.get_u8();
            let mut num_blocks = (blocks_and_size >> 6) as usize + 1;
            let num_bits_per_item = decode_bits(blocks_and_size & 0b0011_1111) as usize;
            while num_blocks > 0 {
                let _ = ReadVarint::<u64>::read_varint(&mut cursor).unwrap();
                let num_items_in_block = items_remaining.min(COMPRESS_BLOCK_SIZE);
                items_remaining -= num_items_in_block;
                num_blocks -= 1;
                total_length_in_bits += num_items_in_block * num_bits_per_item;
            }
        }

        let data_length = (total_length_in_bits + 7) / 8;

        (cursor.position() as usize, data_length)
    }
}

impl Packer for U64Packer {
    type Item = u64;
    type Serialized = U64PackedData;

    #[inline]
    fn add(&mut self, value: u64) {
        self.buffer.push(value);
        self.n_items += 1;

        if self.buffer.len() >= COMPRESS_BLOCK_SIZE {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if let Some((min_value, max_value)) = tantivy_bitpacker::minmax(self.buffer.iter()) {
            let mut bit_packer = BitPacker::new();
            let num_bits_block = tantivy_bitpacker::compute_num_bits(*max_value - min_value);
            // todo performance: the padding handling could be done better, e.g. use a slice and
            // return num_bytes written from bitpacker
            self.compressed_data
                .resize(self.compressed_data.len() - 8, 0); // remove padding for bitpacker
                                                            // todo performance: for some bit_width we
                                                            // can encode multiple vals into the
                                                            // mini_buffer before checking to flush
                                                            // (to be done in BitPacker)
            for val in self.buffer.iter() {
                bit_packer
                    .write(*val - min_value, num_bits_block, &mut self.compressed_data)
                    .expect("cannot write bitpacking to output"); // write to in memory can't fail
            }
            bit_packer.flush(&mut self.compressed_data).unwrap();

            // If we do not yet have any data in the bits field, we can't apply the following
            // optimization.
            let skip_push = if !self.metadata_bits.get_ref().is_empty() {
                // Optimization: As the num_bits_block value is always between 0 and 62 (= 64 -
                // IGNORE_LSN_BITS), we can commandeer the top 2 bits of our
                // 'bits of precision in next BLOCKSZ ints' u8 value to store how many sequential
                // blocks use this number of bits. This saves ~ 1 byte per sequential block of this
                // prefix, so in a worst case this encoding is equal to that of a native
                // delta-encoded blocked_bitpacker, but does much better when the deltas vary
                // significantly between blocks.
                // It does have some extra overhead though, in that we need to store 4x as many
                // min_values as opposed to the tantivy_bitpacker::BlockedBitpacker implementation.
                let val = self.metadata_bits.get_ref()[self.bitsindex];

                (val & 0b11_1111 == encode_bits(num_bits_block)) && (val >> 6 != 0b11)
            } else {
                false
            };

            // If we use the same number of bits as the previous block and the previous block info
            // still has bits left, we've updated that block. In all other cases, we just add a
            // new block here.
            if skip_push {
                self.metadata_bits.get_mut()[self.bitsindex] += 1 << 6;
            } else {
                assert!(encode_bits(num_bits_block) <= 0b0011_1111);
                self.bitsindex = self.metadata_bits.position() as usize;
                let written_len = self
                    .metadata_bits
                    .write(&[encode_bits(num_bits_block); 1])
                    .expect("cannot write to memory?");
                assert_eq!(written_len, 1);
            }

            self.metadata_bits
                .write_varint(*min_value)
                .expect("Cannot write varint");

            self.buffer.clear();
            self.compressed_data
                .resize(self.compressed_data.len() + 8, 0); // add padding for bitpacker
        }
    }

    fn finish(mut self) -> U64PackedData {
        self.flush();

        assert!(self.buffer.is_empty());

        let mut compressed_data = self.compressed_data;
        compressed_data.resize(compressed_data.len() - 8, 0);

        U64PackedData {
            n_items: self.n_items,
            metadata: Bytes::from(self.metadata_bits.into_inner()),
            data: Bytes::from(compressed_data),
        }
    }

    fn len(&self) -> usize {
        self.n_items
    }
}

#[derive(Clone, Debug)]
/// Unpacker for U64Packer's data.
pub struct U64Unpacker {
    /// Total number of items in the stream.
    n_items: usize,
    /// remaining number of items in the stream.
    n_remaining: usize,
    /// buffer of decompressed values
    buffer: Vec<u64>,
    /// offset of the next block in the buffer.
    next_buf_block_offset: usize,
    /// compressed blocks
    data: Bytes,
    /// bit-lengths of compressed blocks.
    metadata: Cursor<Bytes>,
}

impl U64Unpacker {
    fn unpack_next_block(&mut self) -> Option<()> {
        if !self.metadata.has_remaining() {
            return None;
        }

        let blocks_and_bits = {
            let mut it = [0u8; 1];
            let len_read = self.metadata.read(&mut it[..]).ok()?;
            assert_eq!(len_read, 1);
            it[0]
        };

        let num_blocks = 1 + (blocks_and_bits >> 6) as usize;
        let num_bits = decode_bits(blocks_and_bits & 0b0011_1111);

        self.buffer.resize(
            self.n_remaining
                .min(num_blocks as usize * COMPRESS_BLOCK_SIZE),
            0,
        );
        let bit_unpacker = BitUnpacker::new(num_bits);
        let mut base_offsets = Vec::<u64>::with_capacity(num_blocks as usize);

        for _ in 0..num_blocks {
            base_offsets.push(self.metadata.read_varint().ok()?);
        }

        for idx in 0..self.buffer.len() {
            self.buffer[idx] = base_offsets[idx / COMPRESS_BLOCK_SIZE]
                + bit_unpacker.get(
                    idx as u64,
                    &self.data[self.next_buf_block_offset as usize..],
                );
        }

        self.n_remaining -= self.buffer.len();

        // Add to the offset. All but the last block are guaranteed to be a multiple of
        // BLOCK_SIZE in size, thus always divisible by 8.
        self.next_buf_block_offset += (self.buffer.len() * num_bits as usize) / 8;

        Some(())
    }
}

impl Iterator for U64Unpacker {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() {
            self.unpack_next_block();
        }
        if self.n_remaining == 0 && self.buffer.is_empty() {
            return None;
        }
        Some(self.buffer.remove(0))
    }
}

impl Unpacker for U64Unpacker {
    type Serialized = U64PackedData;

    fn remaining(&self) -> usize {
        self.n_remaining
    }

    fn len(&self) -> usize {
        self.n_items
    }

    fn from(serialized: &U64PackedData) -> Self {
        Self {
            n_items: serialized.n_items,
            n_remaining: serialized.n_items,
            buffer: Vec::with_capacity(COMPRESS_BLOCK_SIZE * 4),
            next_buf_block_offset: 0,
            data: serialized.data.clone(),
            metadata: Cursor::new(serialized.metadata.clone()),
        }
    }
}
