type CLsn = u64;
type COid = u32;

use pageserver_data_api::model;
use utils::lsn::Lsn;

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub enum NeonIORequest {
    Empty,
    RelExists(CRelExistsRequest),
    RelSize(CRelSizeRequest),
    GetPage(CGetPageRequest),
    DbSize(CDbSizeRequest),
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
pub struct CRelExistsRequest {
    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub request_lsn: CLsn,
    pub not_modified_since_lsn: CLsn,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CRelSizeRequest {
    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub request_lsn: CLsn,
    pub not_modified_since: CLsn,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CGetPageRequest {
    pub spc_oid: COid,
    pub db_oid: COid,
    pub rel_number: u32,
    pub fork_number: u8,
    pub block_number: u32,
    pub request_lsn: CLsn,
    pub not_modified_since: CLsn,

    // These fields define where the result is written. Must point into a buffer in shared memory!
    pub dest_ptr: usize,
    pub dest_size: u32,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CDbSizeRequest {
    pub db_oid: COid,
    pub request_lsn: CLsn,
    pub not_modified_since: CLsn,
}

impl From<&CRelExistsRequest> for model::RelExistsRequest {
    fn from(value: &CRelExistsRequest) -> model::RelExistsRequest {
        model::RelExistsRequest {
            common: model::RequestCommon {
                request_lsn: Lsn(value.request_lsn),
                not_modified_since_lsn: Lsn(value.not_modified_since_lsn),
            },
            rel: model::RelTag {
                spc_oid: value.spc_oid,
                db_oid: value.db_oid,
                rel_number: value.rel_number,
                fork_number: value.fork_number,
            },
        }
    }
}

impl From<&CRelSizeRequest> for model::RelSizeRequest {
    fn from(value: &CRelSizeRequest) -> model::RelSizeRequest {
        model::RelSizeRequest {
            common: model::RequestCommon {
                request_lsn: Lsn(value.request_lsn),
                not_modified_since_lsn: Lsn(value.not_modified_since),
            },
            rel: model::RelTag {
                spc_oid: value.spc_oid,
                db_oid: value.db_oid,
                rel_number: value.rel_number,
                fork_number: value.fork_number,
            },
        }
    }
}

impl From<&CGetPageRequest> for model::GetPageRequest {
    fn from(value: &CGetPageRequest) -> model::GetPageRequest {
        model::GetPageRequest {
            common: model::RequestCommon {
                request_lsn: Lsn(value.request_lsn),
                not_modified_since_lsn: Lsn(value.not_modified_since),
            },
            rel: model::RelTag {
                spc_oid: value.spc_oid,
                db_oid: value.db_oid,
                rel_number: value.rel_number,
                fork_number: value.fork_number,
            },
            block_number: value.block_number,
        }
    }
}

impl From<&CDbSizeRequest> for model::DbSizeRequest {
    fn from(value: &CDbSizeRequest) -> model::DbSizeRequest {
        model::DbSizeRequest {
            common: model::RequestCommon {
                request_lsn: Lsn(value.request_lsn),
                not_modified_since_lsn: Lsn(value.not_modified_since),
            },
            db_oid: value.db_oid,
        }
    }
}
