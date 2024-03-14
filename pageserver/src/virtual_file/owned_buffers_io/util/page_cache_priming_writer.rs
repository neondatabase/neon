use std::ops::{Deref, Range};

use tokio_epoll_uring::BoundedBuf;
use tracing::error;

use crate::{
    page_cache::{self, PAGE_SZ},
    virtual_file::{owned_buffers_io::write::OwnedAsyncWriter, VirtualFile},
};

pub struct Writer<const N: usize, W: OwnedAsyncWriter, C: Cache<N, W>> {
    under: W,
    written: u64,
    cache: C,
}

pub trait Cache<const PAGE_SZ: usize, W> {
    async fn fill_cache(&self, under: &W, page_no: u64, contents: &[u8; PAGE_SZ]);
}

impl<const N: usize, W, C> Writer<N, W, C>
where
    C: Cache<N, W>,
    W: OwnedAsyncWriter,
{
    pub fn new(under: W, cache: C) -> Self {
        Self {
            under,
            written: 0,
            cache,
        }
    }
    pub fn as_inner(&self) -> &W {
        &self.under
    }
    fn invariants(&self) {
        assert_eq!(
            self.written % (N as u64),
            0,
            "writes must happen in multiples of N"
        );
    }
}

impl<const N: usize, W, C> OwnedAsyncWriter for Writer<N, W, C>
where
    W: OwnedAsyncWriter,
    C: Cache<N, W>,
{
    async fn write_all<
        B: tokio_epoll_uring::BoundedBuf<Buf = Buf, Bounds = Bounds>,
        Buf: tokio_epoll_uring::IoBuf + Send,
        Bounds: std::ops::RangeBounds<usize>,
    >(
        &mut self,
        buf: B,
    ) -> std::io::Result<(usize, B::Buf)> {
        let buf = buf.slice_full();
        assert_eq!(buf.bytes_init() % N, 0);
        self.invariants();
        let pre = self.written;
        let saved_bounds = buf.bounds();
        let debug_assert_contents_eq = if cfg!(debug_assertions) {
            Some(buf[..].to_vec())
        } else {
            None
        };
        let res = self.under.write_all(buf).await;
        let res = if let Ok((nwritten, buf)) = res {
            assert_eq!(nwritten % N, 0);
            let buf = tokio_epoll_uring::Slice::from_buf_bounds(buf, saved_bounds);
            if let Some(before) = debug_assert_contents_eq {
                debug_assert_eq!(&before[..], &buf[..]);
            }
            assert_eq!(nwritten, buf.bytes_init());
            let new_written = self.written + (nwritten as u64);
            for page_no_in_buf in 0..(nwritten / N) {
                let page: &[u8; N] = todo!();
                self.cache
                    .fill_cache(&self.under, self.written / (N as u64), page)
                    .await;
                self.written += (N as u64);
            }
            self.written += nwritten as u64;
            Ok((nwritten, tokio_epoll_uring::Slice::into_inner(buf)))
        } else {
            res
        };
        self.invariants();
        res
    }

    async fn write_all_borrowed(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // TODO: use type system to ensure this doesn't happen at runtime
        panic!("don't put the types together this way")
    }
}

pub struct VirtualFileAdaptor {
    file: VirtualFile,
    file_id: page_cache::FileId,
}

impl VirtualFileAdaptor {
    pub fn new(file: VirtualFile, file_id: page_cache::FileId) -> Self {
        Self { file, file_id }
    }
    pub fn as_inner(&self) -> &VirtualFile {
        &self.file
    }
}

impl<'c> Cache<{ PAGE_SZ }, VirtualFileAdaptor> for &'c crate::page_cache::PageCache {
    async fn fill_cache(&self, under: &VirtualFileAdaptor, page_no: u64, contents: &[u8; PAGE_SZ]) {
        match self
            .read_immutable_buf(
                under.file_id,
                u32::try_from(page_no).expect("files larger than u32::MAX * 8192 aren't supported"),
                todo!("funnel through context"),
            )
            .await
        {
            Ok(crate::page_cache::ReadBufResult::Found(guard)) => {
                debug_assert_eq!(guard.deref(), contents);
            }
            Ok(crate::page_cache::ReadBufResult::NotFound(guard)) => {
                guard.copy_from_slice(contents);
                guard.mark_valid();
            }
            Err(e) => {
                error!("failed to get immutable buf to pre-warm page cache: {e:?}");
            }
        }
    }
}

impl OwnedAsyncWriter for VirtualFileAdaptor {
    async fn write_all<
        B: tokio_epoll_uring::BoundedBuf<Buf = Buf, Bounds = Bounds>,
        Buf: tokio_epoll_uring::IoBuf + Send,
        Bounds: std::ops::RangeBounds<usize>,
    >(
        &mut self,
        buf: B,
    ) -> std::io::Result<(usize, B::Buf)> {
        OwnedAsyncWriter::write_all(&mut self.file, buf).await
    }

    async fn write_all_borrowed(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // TODO: use type system to ensure this doesn't happen at runtime
        panic!("don't put the types together this way")
    }
}
