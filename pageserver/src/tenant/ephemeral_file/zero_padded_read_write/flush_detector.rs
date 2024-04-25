use crate::virtual_file::owned_buffers_io::write::Buffer;

struct FlushDetector<B> {
    
    inner: B,
}

impl<B> Buffer for FlushDetector where B: Buffer {
    type IoBuf = B::IoBuf;

    fn cap(&self) -> usize {
        self.
    }

    fn extend_from_slice(&mut self, other: &[u8]) {
        todo!()
    }

    fn pending(&self) -> usize {
        todo!()
    }

    fn flush(self) -> tokio_epoll_uring::Slice<Self::IoBuf> {
        todo!()
    }

    fn reuse_after_flush(iobuf: Self::IoBuf) -> Self {
        todo!()
    }
}
