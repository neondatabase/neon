use std::alloc::{Layout, LayoutError};

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

        Ok(BytesMut::from(Bytes::from(buf)))
    }
}
