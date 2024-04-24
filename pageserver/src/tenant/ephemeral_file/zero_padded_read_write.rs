//! The heart of how [`super::EphemeralFile`] does its reads and writes.
//!
//! # Writes
//!
//! [`super::EphemeralFile`] writes small, borrowed buffers using [`RW::write_all_borrowed`].
//! The [`RW`] batches these into [`RW::TAIL_SZ`] bigger writes, using [`owned_buffers_io::write::BufferedWriter`].
//!
//! # Reads
//!
//! [`super::EphemeralFile`] always reads full [`PAGE_SZ`]ed blocks using [`RW::read_blk`].
//!
//! The [`RW`] serves these reads either from the buffered writer's in-memory buffer
//! or redirects the caller to read from the underlying [`VirtualFile`]` if they have already
//! been flushed.
//!
//! The current caller is [`super::page_caching::RW`]. In case it gets redirected to read from
//! [`VirtualFile`], it consults the [`crate::page_cache`] first.

mod zero_padded_buffer;

use crate::{
    page_cache::PAGE_SZ,
    virtual_file::{
        owned_buffers_io::{
            self,
            write::{Buffer, OwnedAsyncWriter},
        },
    },
};

const TAIL_SZ: usize = PAGE_SZ;

/// See module-level comment.
pub struct RW<W: OwnedAsyncWriter> {
    buffered_writer: owned_buffers_io::write::BufferedWriter<
        zero_padded_buffer::Buf<TAIL_SZ>,
        owned_buffers_io::util::size_tracking_writer::Writer<W>,
    >,
}

pub enum ReadResult<'a, W> {
    NeedsReadFromWriter { writer: &'a W },
    ServedFromZeroPaddedMutableTail { buffer: &'a [u8; PAGE_SZ] },
}

impl<W> RW<W>
where
    W: OwnedAsyncWriter,
{
    pub fn new(writer: W) -> Self {
        let bytes_flushed_tracker =
            owned_buffers_io::util::size_tracking_writer::Writer::new(writer);
        let buffered_writer = owned_buffers_io::write::BufferedWriter::new(
            bytes_flushed_tracker,
            zero_padded_buffer::Buf::default(),
        );
        Self { buffered_writer }
    }

    pub(crate) fn as_writer(&self) -> &W {
        self.buffered_writer.as_inner().as_inner()
    }

    pub async fn write_all_borrowed(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffered_writer.write_buffered_borrowed(buf).await
    }

    pub fn bytes_written(&self) -> u64 {
        let flushed_offset = self.buffered_writer.as_inner().bytes_written();
        let buffer: &zero_padded_buffer::Buf<{ TAIL_SZ }> = self.buffered_writer.inspect_buffer();
        flushed_offset + u64::try_from(buffer.pending()).unwrap()
    }

    pub(crate) async fn read_blk(&self, blknum: u32) -> Result<ReadResult<'_, W>, std::io::Error> {
        let flushed_offset = self.buffered_writer.as_inner().bytes_written();
        let buffer: &zero_padded_buffer::Buf<{ TAIL_SZ }> = self.buffered_writer.inspect_buffer();
        let buffered_offset = flushed_offset + u64::try_from(buffer.pending()).unwrap();
        let read_offset = (blknum as u64) * (PAGE_SZ as u64);

        // The trailing page ("block") might only be partially filled,
        // yet the blob_io code relies on us to return a full PAGE_SZed slice anyway.
        // Moreover, it has to be zero-padded, because when we still had
        // a write-back page cache, it provided pre-zeroed pages, and blob_io came to rely on it.
        // DeltaLayer probably has the same issue, not sure why it needs no special treatment.
        // => check here that the read doesn't go beyond this potentially trailing
        // => the zero-padding is done in the `else` branch below
        let blocks_written = if buffered_offset % (PAGE_SZ as u64) == 0 {
            buffered_offset / (PAGE_SZ as u64)
        } else {
            (buffered_offset / (PAGE_SZ as u64)) + 1
        };
        if (blknum as u64) >= blocks_written {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, anyhow::anyhow!("read past end of ephemeral_file: read=0x{read_offset:x} buffered=0x{buffered_offset:x} flushed=0x{flushed_offset}")));
        }

        // assertions for the `if-else` below
        assert_eq!(
            flushed_offset % (TAIL_SZ as u64), 0,
            "we only use write_buffered_borrowed to write to the buffered writer, so it's guaranteed that flushes happen buffer.cap()-sized chunks"
        );
        assert_eq!(
            flushed_offset % (PAGE_SZ as u64),
            0,
            "the logic below can't handle if the page is spread across the flushed part and the buffer"
        );

        if read_offset < flushed_offset {
            assert!(read_offset + (PAGE_SZ as u64) <= flushed_offset);
            Ok(ReadResult::NeedsReadFromWriter {
                writer: self.as_writer(),
            })
        } else {
            let read_offset_in_buffer = read_offset
                .checked_sub(flushed_offset)
                .expect("would have taken `if` branch instead of this one");
            let read_offset_in_buffer = usize::try_from(read_offset_in_buffer).unwrap();
            let zero_padded_slice = buffer.as_zero_padded_slice();
            let page = &zero_padded_slice[read_offset_in_buffer..(read_offset_in_buffer + PAGE_SZ)];
            Ok(ReadResult::ServedFromZeroPaddedMutableTail {
                buffer: page
                    .try_into()
                    .expect("the slice above got it as page-size slice"),
            })
        }
    }
}
