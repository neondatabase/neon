use crate::virtual_file::{owned_buffers_io, VirtualFile};

use super::zero_padded_buffer;

pub struct Writer<const TAIL_SZ: usize> {
    buffered_writer: owned_buffers_io::write::BufferedWriter<
        zero_padded_buffer::Buf<TAIL_SZ>,
        owned_buffers_io::util::size_tracking_writer::Writer<VirtualFile>,
    >,
    bytes_amount: u64,
}

impl<const TAIL_SZ: usize> Writer<TAIL_SZ> {
    pub fn new(dst: VirtualFile) -> Self {
        let bytes_flushed_tracker = owned_buffers_io::util::size_tracking_writer::Writer::new(dst);
        let buffered = owned_buffers_io::write::BufferedWriter::new(
            bytes_flushed_tracker,
            zero_padded_buffer::Buf::default(),
        );
        Self {
            buffered_writer: { buffered },
            bytes_amount: 0,
        }
    }

    pub fn buffered_offset(&self) -> u64 {
        self.bytes_amount
    }

    pub fn flushed_offset(&self) -> u64 {
        self.buffered_writer.as_inner().bytes_written()
    }

    pub fn inspect_buffer(&self) -> &zero_padded_buffer::Buf<TAIL_SZ> {
        self.buffered_writer.inspect_buffer()
    }

    pub(crate) fn as_inner_virtual_file(&self) -> &VirtualFile {
        self.buffered_writer.as_inner().as_inner()
    }

    #[inline(always)]
    pub async fn write_all_borrowed(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let nwritten = self.buffered_writer.write_buffered_borrowed(buf).await?;
        self.bytes_amount += u64::try_from(nwritten).unwrap();
        Ok(nwritten)
    }
}
