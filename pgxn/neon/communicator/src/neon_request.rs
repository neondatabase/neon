pub type CLsn = u64;
pub type COid = u32;

// This conveniently matches PG_IOV_MAX
pub const MAX_GETPAGEV_PAGES: usize = 32;

use pageserver_page_api as page_api;

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
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub enum NeonIOResult {
    Empty,
    RelExists(bool),
    RelSize(u32),

    /// the result pages are written to the shared memory addresses given in the request
    GetPageV,

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
    // These fields define where the result is written. Must point into a buffer in shared memory!
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
    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CRelSizeRequest {
    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CGetPageVRequest {
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
pub struct CPrefetchVRequest {
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
    pub db_oid: COid,
    pub request_lsn: CLsn,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CWritePageRequest {
    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub block_number: u32,
    pub lsn: CLsn,

    // These fields define where the result is written. Must point into a buffer in shared memory!
    pub src: ShmemBuf,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CRelExtendRequest {
    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub block_number: u32,
    pub lsn: CLsn,

    // These fields define page contents. Must point into a buffer in shared memory!
    pub src: ShmemBuf,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CRelZeroExtendRequest {
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
    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CRelTruncateRequest {
    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub nblocks: u32,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CRelUnlinkRequest {
    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub block_number: u32,
    pub nblocks: u32,
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
