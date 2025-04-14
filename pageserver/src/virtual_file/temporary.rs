use tracing::error;

use crate::context::RequestContext;

use super::{
    MaybeFatalIo, VirtualFile,
    owned_buffers_io::{
        io_buf_aligned::IoBufAligned,
        io_buf_ext::FullSlice,
        write::{BufferedWriterSink, OwnedAsyncWriter},
    },
};

/// A wrapper around [`super::VirtualFile`] that deletes the file on drop.
/// For use as a [`BufferedWriterSink`] in [`super::owned_buffers_io::write::BufferedWriter`].
#[derive(Debug)]
pub struct TempVirtualFile(Option<VirtualFile>);

impl OwnedAsyncWriter for TempVirtualFile {
    fn write_all_at<Buf: IoBufAligned + Send>(
        &self,
        buf: FullSlice<Buf>,
        offset: u64,
        ctx: &RequestContext,
    ) -> impl std::future::Future<Output = (FullSlice<Buf>, std::io::Result<()>)> + Send {
        VirtualFile::write_all_at(self, buf, offset, ctx)
    }
}

impl BufferedWriterSink for TempVirtualFile {
    fn cleanup(self) {
        drop(self);
    }

    async fn set_len(&self, len: u64, ctx: &RequestContext) -> std::io::Result<()> {
        let file = self.0.as_ref().expect("only None after into_inner or drop");
        file.set_len(len, ctx).await
    }
}

impl Drop for TempVirtualFile {
    fn drop(&mut self) {
        let Some(file) = self.0.take() else {
            return;
        };
        let path = file.path();
        if let Err(e) =
            std::fs::remove_file(path).maybe_fatal_err("failed to remove the virtual file")
        {
            error!(err=%e, path=%path, "failed to remove");
        }
    }
}

impl std::ops::Deref for TempVirtualFile {
    type Target = VirtualFile;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("only None after into_inner or drop")
    }
}

impl std::ops::DerefMut for TempVirtualFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().expect("only None after into_inner or drop")
    }
}

impl TempVirtualFile {
    pub fn new(virtual_file: VirtualFile) -> Self {
        Self(Some(virtual_file))
    }
    pub fn disarm_into_inner(mut self) -> VirtualFile {
        self.0
            .take()
            .expect("only None after into_inner or drop, and we are into_inner, and we consume")
    }
}
