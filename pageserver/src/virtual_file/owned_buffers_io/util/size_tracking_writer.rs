use crate::virtual_file::{owned_buffers_io::write::OwnedAsyncWriter, VirtualFile};
use tokio_epoll_uring::{BoundedBuf, IoBuf};

pub struct Writer {
    dst: VirtualFile,
    bytes_amount: u64,
}

impl Writer {
    pub fn new(dst: VirtualFile) -> Self {
        Self {
            dst,
            bytes_amount: 0,
        }
    }
    /// Returns the wrapped `VirtualFile` object as well as the number
    /// of bytes that were written to it through this object.
    pub fn into_inner(self) -> (u64, VirtualFile) {
        (self.bytes_amount, self.dst)
    }
}

impl OwnedAsyncWriter for Writer {
    #[inline(always)]
    async fn write_all<B: BoundedBuf<Buf = Buf>, Buf: IoBuf + Send>(
        &mut self,
        buf: B,
    ) -> std::io::Result<(usize, B::Buf)> {
        let (buf, res) = self.dst.write_all(buf).await;
        let nwritten = res?;
        self.bytes_amount += u64::try_from(nwritten).unwrap();
        Ok((nwritten, buf))
    }
}
