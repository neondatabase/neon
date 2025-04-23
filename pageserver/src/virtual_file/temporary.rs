use tracing::error;
use utils::sync::gate::GateGuard;

use crate::context::RequestContext;

use super::{
    MaybeFatalIo, VirtualFile,
    owned_buffers_io::{
        io_buf_aligned::IoBufAligned, io_buf_ext::FullSlice, write::OwnedAsyncWriter,
    },
};

/// A wrapper around [`super::VirtualFile`] that deletes the file on drop.
/// For use as a [`OwnedAsyncWriter`] in [`super::owned_buffers_io::write::BufferedWriter`].
#[derive(Debug)]
pub struct TempVirtualFile {
    inner: Option<Inner>,
}

#[derive(Debug)]
struct Inner {
    file: VirtualFile,
    /// Gate guard is held on as long as we need to do operations in the path (delete on drop)
    _gate_guard: GateGuard,
}

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

impl Drop for TempVirtualFile {
    fn drop(&mut self) {
        let Some(Inner { file, _gate_guard }) = self.inner.take() else {
            return;
        };
        let path = file.path();
        if let Err(e) =
            std::fs::remove_file(path).maybe_fatal_err("failed to remove the virtual file")
        {
            error!(err=%e, path=%path, "failed to remove");
        }
        drop(_gate_guard);
    }
}

impl std::ops::Deref for TempVirtualFile {
    type Target = VirtualFile;

    fn deref(&self) -> &Self::Target {
        &self
            .inner
            .as_ref()
            .expect("only None after into_inner or drop")
            .file
    }
}

impl std::ops::DerefMut for TempVirtualFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self
            .inner
            .as_mut()
            .expect("only None after into_inner or drop")
            .file
    }
}

impl TempVirtualFile {
    pub fn new(virtual_file: VirtualFile, gate_guard: GateGuard) -> Self {
        Self {
            inner: Some(Inner {
                file: virtual_file,
                _gate_guard: gate_guard,
            }),
        }
    }
    /// XXX better name for this API, nb we're also dropping the gate guard as part of .take().expect().file
    pub fn disarm_into_inner(mut self) -> VirtualFile {
        self.inner
            .take()
            .expect("only None after into_inner or drop, and we are into_inner, and we consume")
            .file
    }
}
