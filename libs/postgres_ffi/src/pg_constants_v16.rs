pub const XACT_XINFO_HAS_DROPPED_STATS: u32 = 1u32 << 8;

pub const XLOG_DBASE_CREATE_FILE_COPY: u8 = 0x00;
pub const XLOG_DBASE_CREATE_WAL_LOG: u8 = 0x10;
pub const XLOG_DBASE_DROP: u8 = 0x20;

pub const BKPIMAGE_APPLY: u8 = 0x02; /* page image should be restored during replay */
pub const BKPIMAGE_COMPRESS_PGLZ: u8 = 0x04; /* page image is compressed */
pub const BKPIMAGE_COMPRESS_LZ4: u8 = 0x08; /* page image is compressed */
pub const BKPIMAGE_COMPRESS_ZSTD: u8 = 0x10; /* page image is compressed */

pub const SIZEOF_RELMAPFILE: usize = 524; /* sizeof(RelMapFile) in relmapper.c */

pub use super::super::v14::bindings::PGDATA_SUBDIRS;

pub fn bkpimg_is_compressed(bimg_info: u8) -> bool {
    const ANY_COMPRESS_FLAG: u8 = BKPIMAGE_COMPRESS_PGLZ | BKPIMAGE_COMPRESS_LZ4 | BKPIMAGE_COMPRESS_ZSTD;

    (bimg_info & ANY_COMPRESS_FLAG) != 0
}
