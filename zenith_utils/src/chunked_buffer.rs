use std::cmp::min;
use std::io::{Read, Write};

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
    len: usize,
}

impl ChunkedBuffer {
    /// Return current length of the buffer, in bytes
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Create a "cursor" for reading from the buffer.
    pub fn reader(&self, pos: u64) -> ChunkedBufferReader {
        ChunkedBufferReader {
            parent: self,
            pos: pos as usize,
        }
    }

    /// Write 'buf' to the given byte position in the buffer.
    ///
    /// The buffer is expanded if needed. If 'pos' is past the end of the buffer,
    /// the "gap" is filled with zeros.
    ///
    /// This can stop short, depending on where the chunk boundaries are. But
    /// always writes at least one byte.
    pub fn write_at(&mut self, buf: &[u8], pos: usize) -> Result<usize, std::io::Error> {
        let chunk_idx = pos / CHUNK_SIZE;
        let chunk_off = pos % CHUNK_SIZE;

        while chunk_idx >= self.chunks.len() {
            self.chunks.push(Box::new([0; CHUNK_SIZE]));
        }

        let n = min(CHUNK_SIZE - chunk_off, buf.len());

        let chunk = &mut self.chunks[chunk_idx];

        chunk[chunk_off..(chunk_off + n)].copy_from_slice(&buf[0..n]);

        if pos + n > self.len {
            self.len = pos + n;
        }
        Ok(n)
    }

    /// Like 'write_at', but doesn't stop until the whole buffer has been written.
    pub fn write_all_at(&mut self, buf: &[u8], pos: usize) -> Result<(), std::io::Error> {
        let mut nwritten = 0;

        while nwritten < buf.len() {
            nwritten += self.write_at(&buf[nwritten..], pos + nwritten)?;
        }
        Ok(())
    }
}

impl Write for ChunkedBuffer {
    /// Append to the end of the buffer.
    ///
    /// Note how this interacts with 'write_at'. If you use write_at to expand the
    /// buffer, the "write position" for this function changes too.
    fn write(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        self.write_at(buf, self.len)
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

pub struct ChunkedBufferReader<'a> {
    parent: &'a ChunkedBuffer,
    pos: usize,
}

impl<'a> Read for ChunkedBufferReader<'a> {
    fn read(&mut self, dst: &mut [u8]) -> Result<usize, std::io::Error> {
        if self.pos >= self.parent.len {
            return Ok(0);
        }

        let chunk_idx = self.pos / CHUNK_SIZE;
        let chunk_off = self.pos % CHUNK_SIZE;

        let len = min(
            min(self.parent.len - self.pos, CHUNK_SIZE - chunk_off),
            dst.len(),
        );

        let chunk = &self.parent.chunks[chunk_idx];

        dst[0..len].copy_from_slice(&chunk[chunk_off..(chunk_off + len)]);

        self.pos += len;

        Ok(len)
    }
}

#[test]
fn test_chunked_buffer() -> Result<(), std::io::Error> {
    let mut chunked_buffer = ChunkedBuffer::default();

    let mut testdata = [0u8; 10000];
    for (i, b) in testdata.iter_mut().enumerate() {
        *b = (i % 256) as u8;
    }

    chunked_buffer.write_all(&testdata)?;

    // Test reader
    let mut resultdata = [0u8; 10000];
    let mut reader = chunked_buffer.reader(0);
    reader.read_exact(&mut resultdata)?;
    assert_eq!(resultdata, testdata);

    // test zero-length read
    let mut resultdata = [0u8; 10000];
    let mut reader = chunked_buffer.reader(0);
    reader.read_exact(&mut resultdata[0..0])?;

    // test reads around chunk boundaries
    let mut resultdata = [0u8; 10000];
    let mut reader = chunked_buffer.reader(0);
    reader.read_exact(&mut resultdata[0..1])?;
    reader.read_exact(&mut resultdata[1..CHUNK_SIZE])?;
    assert_eq!(resultdata[0..CHUNK_SIZE], testdata[0..CHUNK_SIZE]);

    let mut resultdata = [0u8; 10000];
    let mut reader = chunked_buffer.reader(10);
    reader.read_exact(&mut resultdata[10..CHUNK_SIZE + 10])?;
    assert_eq!(
        resultdata[10..CHUNK_SIZE + 10],
        testdata[10..CHUNK_SIZE + 10]
    );

    let mut resultdata = [0u8; 10000];
    let mut reader = chunked_buffer.reader((CHUNK_SIZE - 1) as u64);
    reader.read_exact(&mut resultdata[(CHUNK_SIZE - 1)..CHUNK_SIZE])?;
    assert_eq!(resultdata[CHUNK_SIZE - 1], testdata[CHUNK_SIZE - 1]);
    reader.read_exact(&mut resultdata[(CHUNK_SIZE)..(CHUNK_SIZE + 1)])?;
    assert_eq!(resultdata[CHUNK_SIZE], testdata[CHUNK_SIZE]);

    // Read at the end
    let mut resultdata = [0u8; 10000];
    let mut reader = chunked_buffer.reader(9900);
    reader.read_exact(&mut resultdata[9900..10000])?;
    assert_eq!(resultdata[9900..10000], testdata[9900..10000]);

    // Read past the end
    let mut resultdata = [0u8; 10001];
    let mut reader = chunked_buffer.reader(9900);
    assert!(reader.read_exact(&mut resultdata[9900..10001]).is_err());

    let mut resultdata = [0u8; 10000];
    let mut reader = chunked_buffer.reader(20000);
    assert_eq!(reader.read(&mut resultdata)?, 0);

    Ok(())
}
