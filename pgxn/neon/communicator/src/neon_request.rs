
// Definitions of some core PostgreSQL datatypes.

/// XLogRecPtr is defined in "access/xlogdefs.h" as:
///
/// ```
/// typedef uint64 XLogRecPtr;
/// ```
/// cbindgen:no-export
pub type XLogRecPtr = u64;

pub type CLsn = XLogRecPtr;
pub type COid = u32;

// This conveniently matches PG_IOV_MAX
pub const MAX_GETPAGEV_PAGES: usize = 32;

use std::ffi::CStr;

use pageserver_page_api::{self as page_api, SlruKind};

/// Request from a Postgres backend to the communicator process
#[allow(clippy::large_enum_variant)]
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub enum NeonIORequest {
    Empty,

    // Read requests. These are C-friendly variants of the corresponding structs in
    // pageserver_page_api.
    RelExists(CRelExistsRequest),
    RelSize(CRelSizeRequest),
    GetPageV(CGetPageVRequest),
    ReadSlruSegment(CReadSlruSegmentRequest),
    PrefetchV(CPrefetchVRequest),
    DbSize(CDbSizeRequest),

    // Write requests. These are needed to keep the relation size cache and LFC up-to-date.
    // They are not sent to the pageserver.
    WritePage(CWritePageRequest),
    RelExtend(CRelExtendRequest),
    RelZeroExtend(CRelZeroExtendRequest),
    RelCreate(CRelCreateRequest),
    RelTruncate(CRelTruncateRequest),
    RelUnlink(CRelUnlinkRequest),

    // Other requests
    UpdateCachedRelSize(CUpdateCachedRelSizeRequest),
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub enum NeonIOResult {
    Empty,
    RelExists(bool),
    RelSize(u32),

    /// the result pages are written to the shared memory addresses given in the request
    GetPageV,
    /// The result is written to the file, path to which is provided
    /// in the request. The [`u64`] value here is the number of blocks.
    ReadSlruSegment(u64),

    /// A prefetch request returns as soon as the request has been received by the communicator.
    /// It is processed in the background.
    PrefetchVLaunched,

    DbSize(u64),

    // FIXME design compact error codes. Can't easily pass a string or other dynamic data.
    // currently, this is 'errno'
    Error(i32),

    Aborted,

    /// used for all write requests
    WriteOK,
}

impl NeonIORequest {
    /// All requests include a unique request ID, which can be used to trace the execution
    /// of a request all the way to the pageservers. The request ID needs to be unique
    /// within the lifetime of the Postgres instance (but not across servers or across
    /// restarts of the same server).
    pub fn request_id(&self) -> u64 {
        use NeonIORequest::*;
        match self {
            Empty => 0,
            RelExists(req) => req.request_id,
            RelSize(req) => req.request_id,
            GetPageV(req) => req.request_id,
            ReadSlruSegment(req) => req.request_id,
            PrefetchV(req) => req.request_id,
            DbSize(req) => req.request_id,
            WritePage(req) => req.request_id,
            RelExtend(req) => req.request_id,
            RelZeroExtend(req) => req.request_id,
            RelCreate(req) => req.request_id,
            RelTruncate(req) => req.request_id,
            RelUnlink(req) => req.request_id,
            UpdateCachedRelSize(req) => req.request_id,
        }
    }
}

/// Special quick result to a CGetPageVRequest request, indicating that the
/// the requested pages are present in the local file cache. The backend can
/// read the blocks directly from the given LFC blocks.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CCachedGetPageVResult {
    pub cache_block_numbers: [u64; MAX_GETPAGEV_PAGES],
}

/// ShmemBuf represents a buffer in shared memory.
///
/// SAFETY: The pointer must point to an area in shared memory. The functions allow you to liberally
/// get a mutable pointer to the contents; it is the caller's responsibility to ensure that you
/// don't access a buffer that's you're not allowed to. Inappropriate access to the buffer doesn't
/// violate Rust's safety semantics, but it will mess up and crash Postgres.
///
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct ShmemBuf {
    // Pointer to where the result is written or where to read from. Must point into a buffer in shared memory!
    pub ptr: *mut u8,
}

unsafe impl Send for ShmemBuf {}
unsafe impl Sync for ShmemBuf {}

unsafe impl uring_common::buf::IoBuf for ShmemBuf {
    fn stable_ptr(&self) -> *const u8 {
        self.ptr
    }

    fn bytes_init(&self) -> usize {
        crate::BLCKSZ
    }

    fn bytes_total(&self) -> usize {
        crate::BLCKSZ
    }
}

unsafe impl uring_common::buf::IoBufMut for ShmemBuf {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }

    unsafe fn set_init(&mut self, pos: usize) {
        if pos > crate::BLCKSZ {
            panic!(
                "set_init called past end of buffer, pos {}, buffer size {}",
                pos,
                crate::BLCKSZ
            );
        }
    }
}

impl ShmemBuf {
    pub fn as_mut_ptr(&self) -> *mut u8 {
        self.ptr
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CRelExistsRequest {
    pub request_id: u64,
    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CRelSizeRequest {
    pub request_id: u64,
    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CGetPageVRequest {
    pub request_id: u64,
    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub block_number: u32,
    pub nblocks: u8,

    // These fields define where the result is written. Must point into a buffer in shared memory!
    pub dest: [ShmemBuf; MAX_GETPAGEV_PAGES],
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CReadSlruSegmentRequest {
    pub request_id: u64,
    pub slru_kind: SlruKind,
    pub segment_number: u32,
    pub request_lsn: CLsn,
    /// Must be a null-terminated C string containing the file path
    /// where the communicator will write the SLRU segment.
    pub destination_file_path: ShmemBuf,
}

impl CReadSlruSegmentRequest {
    /// Returns the file path where the communicator will write the
    /// SLRU segment.
    pub(crate) fn destination_file_path(&self) -> String {
        unsafe { CStr::from_ptr(self.destination_file_path.as_mut_ptr() as *const _) }
            .to_string_lossy()
            .into_owned()
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CPrefetchVRequest {
    pub request_id: u64,
    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub block_number: u32,
    pub nblocks: u8,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CDbSizeRequest {
    pub request_id: u64,
    pub db_oid: COid,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CWritePageRequest {
    pub request_id: u64,
    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub block_number: u32,
    pub lsn: CLsn,

    // `src` defines the new page contents. Must point into a buffer in shared memory!
    pub src: ShmemBuf,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CRelExtendRequest {
    pub request_id: u64,
    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub block_number: u32,
    pub lsn: CLsn,

    // `src` defines the new page contents. Must point into a buffer in shared memory!
    pub src: ShmemBuf,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CRelZeroExtendRequest {
    pub request_id: u64,
    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub block_number: u32,
    pub nblocks: u32,
    pub lsn: CLsn,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CRelCreateRequest {
    pub request_id: u64,
    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub lsn: CLsn,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CRelTruncateRequest {
    pub request_id: u64,
    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub nblocks: u32,
    pub lsn: CLsn,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CRelUnlinkRequest {
    pub request_id: u64,
    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub lsn: CLsn,
}

impl CRelExistsRequest {
    pub fn reltag(&self) -> page_api::RelTag {
        page_api::RelTag {
            spcnode: self.spc_oid,
            dbnode: self.db_oid,
            relnode: self.rel_number,
            forknum: self.fork_number,
        }
    }
}

impl CRelSizeRequest {
    pub fn reltag(&self) -> page_api::RelTag {
        page_api::RelTag {
            spcnode: self.spc_oid,
            dbnode: self.db_oid,
            relnode: self.rel_number,
            forknum: self.fork_number,
        }
    }
}

impl CGetPageVRequest {
    pub fn reltag(&self) -> page_api::RelTag {
        page_api::RelTag {
            spcnode: self.spc_oid,
            dbnode: self.db_oid,
            relnode: self.rel_number,
            forknum: self.fork_number,
        }
    }
}

impl CPrefetchVRequest {
    pub fn reltag(&self) -> page_api::RelTag {
        page_api::RelTag {
            spcnode: self.spc_oid,
            dbnode: self.db_oid,
            relnode: self.rel_number,
            forknum: self.fork_number,
        }
    }
}

impl CWritePageRequest {
    pub fn reltag(&self) -> page_api::RelTag {
        page_api::RelTag {
            spcnode: self.spc_oid,
            dbnode: self.db_oid,
            relnode: self.rel_number,
            forknum: self.fork_number,
        }
    }
}

impl CRelExtendRequest {
    pub fn reltag(&self) -> page_api::RelTag {
        page_api::RelTag {
            spcnode: self.spc_oid,
            dbnode: self.db_oid,
            relnode: self.rel_number,
            forknum: self.fork_number,
        }
    }
}

impl CRelZeroExtendRequest {
    pub fn reltag(&self) -> page_api::RelTag {
        page_api::RelTag {
            spcnode: self.spc_oid,
            dbnode: self.db_oid,
            relnode: self.rel_number,
            forknum: self.fork_number,
        }
    }
}

impl CRelCreateRequest {
    pub fn reltag(&self) -> page_api::RelTag {
        page_api::RelTag {
            spcnode: self.spc_oid,
            dbnode: self.db_oid,
            relnode: self.rel_number,
            forknum: self.fork_number,
        }
    }
}

impl CRelTruncateRequest {
    pub fn reltag(&self) -> page_api::RelTag {
        page_api::RelTag {
            spcnode: self.spc_oid,
            dbnode: self.db_oid,
            relnode: self.rel_number,
            forknum: self.fork_number,
        }
    }
}

impl CRelUnlinkRequest {
    pub fn reltag(&self) -> page_api::RelTag {
        page_api::RelTag {
            spcnode: self.spc_oid,
            dbnode: self.db_oid,
            relnode: self.rel_number,
            forknum: self.fork_number,
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CUpdateCachedRelSizeRequest {
    pub request_id: u64,
    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub nblocks: u32,
    pub lsn: CLsn,
}

impl CUpdateCachedRelSizeRequest {
    pub fn reltag(&self) -> page_api::RelTag {
        page_api::RelTag {
            spcnode: self.spc_oid,
            dbnode: self.db_oid,
            relnode: self.rel_number,
            forknum: self.fork_number,
        }
    }
}
