pub const XLOG_DBASE_CREATE: u8 = 0x00;
pub const XLOG_DBASE_DROP: u8 = 0x10;

pub const BKPIMAGE_IS_COMPRESSED: u8 = 0x02; /* page image is compressed */
pub const BKPIMAGE_APPLY: u8 = 0x04; /* page image should be restored during replay */
pub const SIZEOF_RELMAPFILE: usize = 512; /* sizeof(RelMapFile) in relmapper.c */

pub fn bkpimg_is_compressed(bimg_info: u8) -> bool {
    (bimg_info & BKPIMAGE_IS_COMPRESSED) != 0
}
