use std::convert::TryInto;

const CHUNK_SIZE: usize = 1024;

///
/// ChunkedBuffer is an expandable byte buffer. You can append to the end of it, and you
/// can read at any byte position. The advantage over a plain Vec<u8> is that the
/// buffer consists of smaller chunks, so that a big buffer doesn't require one huge
/// allocation, and expanding doesn't require copying all the data.
///
#[derive(Debug, Default)]
pub struct ChunkedBuffer {
    // Clippy considers it unnecessary to have Box here, since the vector is stored
    // on the heap anyway. But we have our reasons for that: we want each chunk to
    // be allocated separately, so that we don't require one huge allocation, and so
    // that expanding the array doesn't require copying all of the data. (Clippy
    // knows about that and doesn't complain if the array is large enough, 4096
    // bytes is the default threshold. Our chunk size is smaller than that so that
    // heuristic doesn't save us.)
    #[allow(clippy::vec_box)]
    chunks: Vec<Box<[u8; CHUNK_SIZE]>>,

    length: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ChunkToken {
    start: usize,
    length: usize,
}

impl ChunkedBuffer {
    pub fn read(&self, token: &ChunkToken) -> Vec<u8> {
        let mut buf = Vec::with_capacity(token.length);

        let chunk_idx = token.start / CHUNK_SIZE;
        let mut chunk_iter = self.chunks[chunk_idx..].iter();
        let mut bytes_remaining = token.length;
        if bytes_remaining > 0 {
            let start_offset = token.start % CHUNK_SIZE;
            let chunk_bytes = &chunk_iter.next().unwrap()[start_offset..];
            let bytes = &chunk_bytes[..chunk_bytes.len().min(bytes_remaining)];
            buf.extend_from_slice(bytes);
            bytes_remaining -= bytes.len();
        }

        while bytes_remaining > 0 {
            let chunk_bytes = chunk_iter.next().unwrap();
            let bytes = &chunk_bytes[..chunk_bytes.len().min(bytes_remaining)];
            buf.extend_from_slice(bytes);
            bytes_remaining -= bytes.len();
        }

        debug_assert_eq!(buf.len(), token.length);
        buf
    }

    pub fn write(&mut self, mut buf: &[u8]) -> ChunkToken {
        let token = ChunkToken {
            start: self.length,
            length: buf.len(),
        };

        while !buf.is_empty() {
            let chunk_idx = self.length / CHUNK_SIZE;
            let chunk = match self.chunks.get_mut(chunk_idx) {
                Some(chunk) => chunk,
                None => {
                    debug_assert_eq!(self.length % CHUNK_SIZE, 0);
                    debug_assert_eq!(chunk_idx, self.chunks.len());
                    self.chunks.push(heap_alloc_chunk());
                    self.chunks.last_mut().unwrap()
                }
            };

            let offset = self.length % CHUNK_SIZE;
            let length = buf.len().min(CHUNK_SIZE - offset);

            chunk[offset..][..length].copy_from_slice(&buf[..length]);
            buf = &buf[length..];
            self.length += length;
        }

        token
    }
}

fn heap_alloc_chunk() -> Box<[u8; CHUNK_SIZE]> {
    vec![0u8; CHUNK_SIZE].into_boxed_slice().try_into().unwrap()
}

#[cfg(test)]
mod tests {
    use super::{ChunkToken, ChunkedBuffer, CHUNK_SIZE};

    fn gen_bytes(len: usize) -> Vec<u8> {
        let mut buf = vec![0u8; len];
        for (idx, val) in buf.iter_mut().enumerate() {
            *val = idx as u8;
        }

        buf
    }

    #[test]
    fn one_item() {
        fn test(data: &[u8]) {
            let mut chunked_buffer = ChunkedBuffer::default();
            let token = chunked_buffer.write(data);
            assert_eq!(
                ChunkToken {
                    start: 0,
                    length: data.len(),
                },
                token
            );

            let buf = chunked_buffer.read(&token);
            assert_eq!(data, buf.as_slice());
        }

        test(b"");
        test(b"a");
        test(b"abc");

        test(&gen_bytes(CHUNK_SIZE - 1));
        test(&gen_bytes(CHUNK_SIZE));
        test(&gen_bytes(CHUNK_SIZE + 1));

        test(&gen_bytes(2 * CHUNK_SIZE - 1));
        test(&gen_bytes(2 * CHUNK_SIZE));
        test(&gen_bytes(2 * CHUNK_SIZE + 1));
    }

    #[test]
    fn many_items() {
        let mut chunked_buffer = ChunkedBuffer::default();
        let mut start = 0;
        let mut expected = Vec::new();

        let mut test = |data: &[u8]| {
            let token = chunked_buffer.write(data);
            assert_eq!(
                ChunkToken {
                    start,
                    length: data.len(),
                },
                token
            );

            expected.push((token, data.to_vec()));

            for (token, expected_data) in &expected {
                let buf = chunked_buffer.read(token);
                assert_eq!(expected_data, buf.as_slice());
            }

            start += data.len();
        };

        test(b"abc");
        test(b"");
        test(b"a");

        test(&gen_bytes(CHUNK_SIZE - 1));
        test(&gen_bytes(CHUNK_SIZE));
        test(&gen_bytes(CHUNK_SIZE + 1));

        test(&gen_bytes(2 * CHUNK_SIZE - 1));
        test(&gen_bytes(2 * CHUNK_SIZE));
        test(&gen_bytes(2 * CHUNK_SIZE + 1));
    }
}
