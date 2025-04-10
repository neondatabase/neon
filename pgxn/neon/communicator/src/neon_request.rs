type Lsn = u64;
type Oid = u32;

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub enum NeonIORequest {
    Empty,
    RelExists(RelExistsRequest),
    RelSize(RelSizeRequest),
    GetPage(GetPageRequest),
    DbSize(DbSizeRequest),
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub enum NeonIOResult {
    Empty,
    RelExists(bool),
    RelSize(u32),

    // the result is written to the shared memory address given in the request
    GetPage,

    DbSize(u64),

    // FIXME design compact error codes. Can't easily pass a string or other dynamic data.
    Error(i32),
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct RelExistsRequest {
    pub spc_oid: Oid,
    pub db_oid: Oid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub request_lsn: Lsn,
    pub not_modified_since: Lsn,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct RelSizeRequest {
    pub spc_oid: Oid,
    pub db_oid: Oid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub request_lsn: Lsn,
    pub not_modified_since: Lsn,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct GetPageRequest {
    pub spc_oid: Oid,
    pub db_oid: Oid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub block_number: u32,
    pub request_lsn: Lsn,
    pub not_modified_since: Lsn,
    pub dest_ptr: usize,
    pub dest_size: u32,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct DbSizeRequest {
    pub db_oid: Oid,
    pub request_lsn: Lsn,
    pub not_modified_since: Lsn,
}
