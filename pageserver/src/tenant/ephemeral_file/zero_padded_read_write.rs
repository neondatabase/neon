//! The heart of how [`super::EphemeralFile`] does its reads and writes.
//!
//! # Writes
//!
//! [`super::EphemeralFile`] writes small, borrowed buffers using [`RW::write_all_borrowed`].
//! The [`RW`] batches these into [`TAIL_SZ`] bigger writes, using [`owned_buffers_io::write::BufferedWriter`].
//!
//! # Reads
//!
//! [`super::EphemeralFile`] always reads full [`PAGE_SZ`]ed blocks using [`RW::read_blk`].
//!
//! The [`RW`] serves these reads either from the buffered writer's in-memory buffer
//! or redirects the caller to read from the underlying [`OwnedAsyncWriter`]
//! if the read is for the prefix that has already been flushed.
//!
//! # Current Usage
//!
//! The current user of this module is [`super::page_caching::RW`].

mod zero_padded;

use anyhow::Context;

use crate::{
    context::RequestContext,
    page_cache::PAGE_SZ,
    virtual_file::owned_buffers_io::{
        self,
        write::{Buffer, OwnedAsyncWriter},
    },
};

const TAIL_SZ: usize = 64 * 1024;

/// See module-level comment.
pub struct RW<W: OwnedAsyncWriter> {
    buffered_writer: owned_buffers_io::write::BufferedWriter<
        zero_padded::Buffer<TAIL_SZ>,
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
            zero_padded::Buffer::default(),
        );
        Self { buffered_writer }
    }

    pub(crate) fn as_writer(&self) -> &W {
        self.buffered_writer.as_inner().as_inner()
    }

    pub async fn write_all_borrowed(
        &mut self,
        buf: &[u8],
        ctx: &RequestContext,
    ) -> std::io::Result<usize> {
        self.buffered_writer.write_buffered_borrowed(buf, ctx).await
    }

    pub fn bytes_written(&self) -> u32 {
        let flushed_offset = self.buffered_writer.as_inner().bytes_written();
        let flushed_offset = u32::try_from(flushed_offset).with_context(|| format!("buffered_writer.write_buffered_borrowed() disallows sizes larger than u32::MAX: {flushed_offset}")).unwrap();
        let buffer: &zero_padded::Buffer<TAIL_SZ> = self.buffered_writer.inspect_buffer();
        let buffer_pending = u32::try_from(buffer.pending()).expect("TAIL_SZ is < u32::MAX");
        flushed_offset.checked_add(buffer_pending).with_context(|| format!("buffered_writer.write_buffered_borrowed() disallows sizes larger than u32::MAX: {flushed_offset} + {buffer_pending}")).unwrap()
    }

    /// Get a slice of all blocks that [`Self::read_blk`] would return as [`ReadResult::ServedFromZeroPaddedMutableTail`].
    pub fn get_tail_zero_padded(&self) -> &[u8] {
        let buffer: &zero_padded::Buffer<TAIL_SZ> = self.buffered_writer.inspect_buffer();
        let buffer_written_up_to = buffer.pending();
        // pad to next page boundary
        let read_up_to = if buffer_written_up_to % PAGE_SZ == 0 {
            buffer_written_up_to
        } else {
            buffer_written_up_to
                .checked_add(PAGE_SZ - (buffer_written_up_to % PAGE_SZ))
                .unwrap()
        };
        &buffer.as_zero_padded_slice()[0..read_up_to]
    }

    pub(crate) async fn read_blk(&self, blknum: u32) -> Result<ReadResult<'_, W>, std::io::Error> {
        let flushed_offset =
            u32::try_from(self.buffered_writer.as_inner().bytes_written()).expect("");
        let buffer: &zero_padded::Buffer<TAIL_SZ> = self.buffered_writer.inspect_buffer();
        let buffered_offset = flushed_offset + u32::try_from(buffer.pending()).unwrap();
        let page_sz = u32::try_from(PAGE_SZ).unwrap();
        let read_offset = blknum.checked_mul(page_sz).unwrap();

        // The trailing page ("block") might only be partially filled,
        // yet the blob_io code relies on us to return a full PAGE_SZed slice anyway.
        // Moreover, it has to be zero-padded, because when we still had
        // a write-back page cache, it provided pre-zeroed pages, and blob_io came to rely on it.
        // DeltaLayer probably has the same issue, not sure why it needs no special treatment.
        // => check here that the read doesn't go beyond this potentially trailing
        // => the zero-padding is done in the `else` branch below
        let blocks_written = if buffered_offset % page_sz == 0 {
            buffered_offset / page_sz
        } else {
            (buffered_offset / page_sz) + 1
        };
        if blknum >= blocks_written {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, anyhow::anyhow!("read past end of ephemeral_file: read=0x{read_offset:x} buffered=0x{buffered_offset:x} flushed=0x{flushed_offset}")));
        }

        // assertions for the `if-else` below
        assert_eq!(
            flushed_offset % (u32::try_from(TAIL_SZ).unwrap()), 0,
            "we only use write_buffered_borrowed to write to the buffered writer, so it's guaranteed that flushes happen buffer.cap()-sized chunks"
        );
        assert_eq!(
            flushed_offset % page_sz,
            0,
            "the logic below can't handle if the page is spread across the flushed part and the buffer"
        );

        if read_offset < flushed_offset {
            assert!(read_offset + page_sz <= flushed_offset);
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
