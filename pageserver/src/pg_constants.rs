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
pub const XACT_XINFO_HAS_DBINFO: u32 = 1;
pub const XACT_XINFO_HAS_SUBXACTS: u32 = 2;
pub const XACT_XINFO_HAS_RELFILENODES: u32 = 4;

// From pg_control.h and rmgrlist.h
pub const XLOG_SWITCH: u8 = 0x40;
pub const XLOG_SMGR_TRUNCATE: u8 = 0x20;
pub const RM_XLOG_ID: u8 = 0;
pub const RM_XACT_ID: u8 = 1;
pub const RM_SMGR_ID: u8 = 2;
pub const RM_CLOG_ID: u8 = 3;
// pub const RM_MULTIXACT_ID:u8 = 6;

// from xlogreader.h
pub const XLR_INFO_MASK: u8 = 0x0F;
pub const XLR_RMGR_INFO_MASK: u8 = 0xF0;
