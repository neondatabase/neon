// From pg_tablespace_d.h
//
pub const DEFAULTTABLESPACE_OID: u32 = 1663;
pub const GLOBALTABLESPACE_OID: u32 = 1664;
//Special values for non-rel files' tags
//TODO maybe use enum?
pub const PG_CONTROLFILE_FORKNUM: u32 = 42;
pub const PG_FILENODEMAP_FORKNUM: u32 = 43;
pub const PG_XACT_FORKNUM: u32 = 44;
pub const PG_MXACT_OFFSETS_FORKNUM: u32 = 45;
pub const PG_MXACT_MEMBERS_FORKNUM: u32 = 46;

//
// constants from clog.h
//
pub const CLOG_XACTS_PER_BYTE: u32 = 4;
pub const CLOG_XACTS_PER_PAGE: u32 = 8192 * CLOG_XACTS_PER_BYTE;
pub const CLOG_BITS_PER_XACT: u8 = 2;
pub const CLOG_XACT_BITMASK: u8 = (1 << CLOG_BITS_PER_XACT) - 1;

pub const TRANSACTION_STATUS_COMMITTED: u8 = 0x01;
pub const TRANSACTION_STATUS_ABORTED: u8 = 0x02;
pub const TRANSACTION_STATUS_SUB_COMMITTED: u8 = 0x03;

pub const CLOG_ZEROPAGE: u8 = 0x00;
pub const CLOG_TRUNCATE: u8 = 0x10;

// From xact.h
pub const XLOG_XACT_COMMIT: u8 = 0x00;
pub const XLOG_XACT_ABORT: u8 = 0x20;

/* mask for filtering opcodes out of xl_info */
pub const XLOG_XACT_OPMASK: u8 = 0x70;
/* does this record have a 'xinfo' field or not */
pub const XLOG_XACT_HAS_INFO: u8 = 0x80;

/*
 * The following flags, stored in xinfo, determine which information is
 * contained in commit/abort records.
 */
pub const XACT_XINFO_HAS_DBINFO: u32 = 1u32 << 0;
pub const XACT_XINFO_HAS_SUBXACTS: u32 = 1u32 << 1;
pub const XACT_XINFO_HAS_RELFILENODES: u32 = 1u32 << 2;
pub const XACT_XINFO_HAS_INVALS: u32 = 1u32 << 3;
pub const XACT_XINFO_HAS_TWOPHASE: u32 = 1u32 << 4;
// pub const XACT_XINFO_HAS_ORIGIN: u32 = 1u32 << 5;
// pub const XACT_XINFO_HAS_AE_LOCKS: u32 = 1u32 << 6;
// pub const XACT_XINFO_HAS_GID: u32 = 1u32 << 7;

// From pg_control.h and rmgrlist.h
pub const XLOG_SWITCH: u8 = 0x40;
pub const XLOG_SMGR_TRUNCATE: u8 = 0x20;
pub const RM_XLOG_ID: u8 = 0;
pub const RM_XACT_ID: u8 = 1;
pub const RM_SMGR_ID: u8 = 2;
pub const RM_CLOG_ID: u8 = 3;
pub const RM_DBASE_ID: u8 = 4;
pub const RM_TBLSPC_ID: u8 = 5;
// pub const RM_MULTIXACT_ID:u8 = 6;

// from xlogreader.h
pub const XLR_INFO_MASK: u8 = 0x0F;
pub const XLR_RMGR_INFO_MASK: u8 = 0xF0;

// from dbcommands_xlog.h
pub const XLOG_DBASE_CREATE: u8 = 0x00;
pub const XLOG_DBASE_DROP: u8 = 0x10;

pub const XLOG_TBLSPC_CREATE: u8 = 0x00;
pub const XLOG_TBLSPC_DROP: u8 = 0x10;

pub const SIZEOF_XLOGRECORD: u32 = 24;

// FIXME:
pub const BLCKSZ: u16 = 8192;

//
// from xlogrecord.h
//
pub const XLR_MAX_BLOCK_ID: u8 = 32;

pub const XLR_BLOCK_ID_DATA_SHORT: u8 = 255;
pub const XLR_BLOCK_ID_DATA_LONG: u8 = 254;
pub const XLR_BLOCK_ID_ORIGIN: u8 = 253;
pub const XLR_BLOCK_ID_TOPLEVEL_XID: u8 = 252;

pub const BKPBLOCK_FORK_MASK: u8 = 0x0F;
pub const _BKPBLOCK_FLAG_MASK: u8 = 0xF0;
pub const BKPBLOCK_HAS_IMAGE: u8 = 0x10; /* block data is an XLogRecordBlockImage */
pub const BKPBLOCK_HAS_DATA: u8 = 0x20;
pub const BKPBLOCK_WILL_INIT: u8 = 0x40; /* redo will re-init the page */
pub const BKPBLOCK_SAME_REL: u8 = 0x80; /* RelFileNode omitted, same as previous */

/* Information stored in bimg_info */
pub const BKPIMAGE_HAS_HOLE: u8 = 0x01; /* page image has "hole" */
pub const BKPIMAGE_IS_COMPRESSED: u8 = 0x02; /* page image is compressed */
pub const BKPIMAGE_APPLY: u8 = 0x04; /* page image should be restored during replay */
