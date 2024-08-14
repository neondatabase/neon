pub(crate) mod buffer;
use std::alloc::{Layout, LayoutError};

// use anyhow::bail;
// use nix::libc;
// use nix::libc::statx as Statx;
use bytes::{Bytes, BytesMut};

/// Allocates an aligned buffer.
pub fn alloc_aligned(len: usize, align: usize) -> Result<Box<[u8]>, LayoutError> {
    if len == 0 {
        return Ok(<Box<[u8]>>::default());
    }
    let layout = Layout::array::<u8>(len)?.align_to(align)?;
    assert_eq!(layout.align(), align);
    let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
    let slice_ptr = core::ptr::slice_from_raw_parts_mut(ptr, len);
    Ok(unsafe { Box::from_raw(slice_ptr) })
}

pub(crate) trait TryAllocAligned {
    fn try_alloc_aligned(capacity: usize, align: usize) -> Result<Self, LayoutError>
    where
        Self: Sized;
}

impl TryAllocAligned for bytes::BytesMut {
    fn try_alloc_aligned(capacity: usize, align: usize) -> Result<Self, LayoutError> {
        let buf = alloc_aligned(capacity, align)?;
        let x = Bytes::from(buf);

        // Ok(BytesMut::from(Bytes::from(buf)))
        todo!()
    }
}

#[cfg(target_os = "linux")]
mod dio_mem_alignment {
    use std::mem::MaybeUninit;
    use std::os::unix::ffi::OsStrExt;

    use anyhow::bail;
    use nix::libc;
    use nix::libc::statx as Statx;

    /// Direct IO alignment info.
    pub(crate) struct StatxDioAlignInfo {
        stx_dio_mem_align: usize,
        stx_dio_offset_align: usize,
    }

    /// Gets Direct IO alignment info through `statx(2)` system call.
    /// TODO: Does our machine support this?
    ///
    /// Adapted from https://gist.github.com/problame/1c35cac41b7cd617779f8aae50f97155/revisions.
    fn statx_align_info<P: AsRef<std::path::Path>>(path: P) -> anyhow::Result<StatxDioAlignInfo> {
        const REQUESTS: [(&str, u32); 2] = [
            ("STATX_BASIC_STATS", libc::STATX_BASIC_STATS),
            ("STATX_DIOALIGN", libc::STATX_DIOALIGN),
        ];

        let mask = REQUESTS.iter().map(|(_, v)| v).fold(0, |l, r| l | r);
        let mut statx_buf = unsafe { MaybeUninit::<Statx>::zeroed().assume_init() };
        let status = unsafe {
            let c_path = path.as_ref().as_os_str().as_bytes();
            let c_path = c_path as *const _ as *mut libc::c_char;
            libc::statx(
                libc::AT_FDCWD,
                c_path,
                libc::AT_SYMLINK_NOFOLLOW,
                mask,
                &mut statx_buf as *mut Statx,
            )
        };
        if status != 0 {
            bail!("Error checking alignment: {}", status);
        }

        let mut request_not_fulfilled = Vec::new();
        for (name, r) in REQUESTS {
            if statx_buf.stx_attributes_mask & r as u64 == 0 {
                request_not_fulfilled.push(name);
            }
        }
        if !request_not_fulfilled.is_empty() {
            bail!(
                "One or more requested statx attributes not supported: {:?}",
                request_not_fulfilled,
            )
        }

        if statx_buf.stx_mode as u32 & libc::S_IFREG != libc::S_IFREG {
            bail!("not a regular file: statx.mode={}", statx_buf.stx_mode);
        }

        Ok(StatxDioAlignInfo {
            stx_dio_mem_align: statx_buf.stx_dio_mem_align as usize,
            stx_dio_offset_align: statx_buf.stx_dio_offset_align as usize,
        })
    }
}
