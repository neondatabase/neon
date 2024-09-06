use std::collections::HashMap;

const BLOCK_SIZE: usize = 8192;

/// A simple in-memory implementation of a block storage. Can be used to implement external
/// storage in tests.
pub struct BlockStorage {
    blocks: HashMap<u64, [u8; BLOCK_SIZE]>,
}

impl Default for BlockStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockStorage {
    pub fn new() -> Self {
        BlockStorage {
            blocks: HashMap::new(),
        }
    }

    pub fn read(&self, pos: u64, buf: &mut [u8]) {
        let mut buf_offset = 0;
        let mut storage_pos = pos;
        while buf_offset < buf.len() {
            let block_id = storage_pos / BLOCK_SIZE as u64;
            let block = self.blocks.get(&block_id).unwrap_or(&[0; BLOCK_SIZE]);
            let block_offset = storage_pos % BLOCK_SIZE as u64;
            let block_len = BLOCK_SIZE as u64 - block_offset;
            let buf_len = buf.len() - buf_offset;
            let copy_len = std::cmp::min(block_len as usize, buf_len);
            buf[buf_offset..buf_offset + copy_len]
                .copy_from_slice(&block[block_offset as usize..block_offset as usize + copy_len]);
            buf_offset += copy_len;
            storage_pos += copy_len as u64;
        }
    }

    pub fn write(&mut self, pos: u64, buf: &[u8]) {
        let mut buf_offset = 0;
        let mut storage_pos = pos;
        while buf_offset < buf.len() {
            let block_id = storage_pos / BLOCK_SIZE as u64;
            let block = self.blocks.entry(block_id).or_insert([0; BLOCK_SIZE]);
            let block_offset = storage_pos % BLOCK_SIZE as u64;
            let block_len = BLOCK_SIZE as u64 - block_offset;
            let buf_len = buf.len() - buf_offset;
            let copy_len = std::cmp::min(block_len as usize, buf_len);
            block[block_offset as usize..block_offset as usize + copy_len]
                .copy_from_slice(&buf[buf_offset..buf_offset + copy_len]);
            buf_offset += copy_len;
            storage_pos += copy_len as u64
        }
    }
}
