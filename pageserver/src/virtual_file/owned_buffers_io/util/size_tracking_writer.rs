use crate::virtual_file::owned_buffers_io::write::OwnedAsyncWriter;
use tokio_epoll_uring::{BoundedBuf, IoBuf};

pub struct Writer<W> {
    dst: W,
    bytes_amount: u64,
}

impl<W> Writer<W> {
    pub fn new(dst: W) -> Self {
        Self {
            dst,
            bytes_amount: 0,
        }
    }
    pub fn bytes_written(&self) -> u64 {
        self.bytes_amount
    }
    pub fn as_inner(&self) -> &W {
        &self.dst
    }
    /// Returns the wrapped `VirtualFile` object as well as the number
    /// of bytes that were written to it through this object.
    pub fn into_inner(self) -> (u64, W) {
        (self.bytes_amount, self.dst)
    }
}

impl<W> OwnedAsyncWriter for Writer<W>
where
    W: OwnedAsyncWriter,
{
    #[inline(always)]
    async fn write_all<B: BoundedBuf<Buf = Buf>, Buf: IoBuf + Send>(
        &mut self,
        buf: B,
    ) -> std::io::Result<(usize, B::Buf)> {
        let (nwritten, buf) = self.dst.write_all(buf).await?;
        self.bytes_amount += u64::try_from(nwritten).unwrap();
        Ok((nwritten, buf))
    }

    #[inline(always)]
    async fn write_all_borrowed(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let nwritten = self.dst.write_all_borrowed(buf).await?;
        self.bytes_amount += u64::try_from(nwritten).unwrap();
        Ok(nwritten)
    }
}
