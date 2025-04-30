//! Structs representing the canonical page service API.
//!
//! These mirror the pageserver APIs and the structs automatically generated
//! from the protobuf specification. The differences are:
//!
//! - Types that are in fact required by the API are not Options. The protobuf "required"
//!   attribute is deprecated and 'prost' marks a lot of members as optional because of that.
//!   (See https://github.com/tokio-rs/prost/issues/800 for a gripe on this)
//!
//! - Use more precise datatypes, e.g. Lsn and uints shorter than 32 bits.
//!
//! TODO: these types should be used in the Pageserver for actual processing,
//! instead of being cast into internal mirror types.

use utils::lsn::Lsn;

use crate::proto;

#[derive(Clone, Debug)]
pub struct RequestCommon {
    pub request_lsn: Lsn,
    pub not_modified_since_lsn: Lsn,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct RelTag {
    pub spc_oid: u32,
    pub db_oid: u32,
    pub rel_number: u32,
    pub fork_number: u8,
}

#[derive(Clone, Debug)]
pub struct RelExistsRequest {
    pub common: RequestCommon,
    pub rel: RelTag,
}

#[derive(Clone, Debug)]
pub struct RelSizeRequest {
    pub common: RequestCommon,
    pub rel: RelTag,
}

#[derive(Clone, Debug)]
pub struct RelSizeResponse {
    pub num_blocks: u32,
}

#[derive(Clone, Debug)]
pub struct GetPageRequest {
    pub id: u64,
    pub common: RequestCommon,
    pub rel: RelTag,
    pub block_number: u32,
}

#[derive(Clone, Debug)]
pub struct GetPageResponse {
    pub page_image: std::vec::Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct DbSizeRequest {
    pub common: RequestCommon,
    pub db_oid: u32,
}

#[derive(Clone, Debug)]
pub struct DbSizeResponse {
    pub num_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct GetBaseBackupRequest {
    pub common: RequestCommon,
    pub replica: bool,
}

#[derive(Clone, Debug)]
pub struct GetSlruSegmentRequest {
    pub common: RequestCommon,
    pub kind: u8, // TODO: SlruKind
    pub segno: u32,
}

//--- Conversions to/from the generated proto types

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("the value for field `{0}` is invalid")]
    InvalidValue(&'static str),
    #[error("the required field `{0}` is missing ")]
    Missing(&'static str),
}

impl From<ProtocolError> for tonic::Status {
    fn from(e: ProtocolError) -> Self {
        match e {
            ProtocolError::InvalidValue(_field) => tonic::Status::invalid_argument(e.to_string()),
            ProtocolError::Missing(_field) => tonic::Status::invalid_argument(e.to_string()),
        }
    }
}

impl From<&RelTag> for proto::RelTag {
    fn from(value: &RelTag) -> proto::RelTag {
        proto::RelTag {
            spc_oid: value.spc_oid,
            db_oid: value.db_oid,
            rel_number: value.rel_number,
            fork_number: value.fork_number as u32,
        }
    }
}
impl TryFrom<&proto::RelTag> for RelTag {
    type Error = ProtocolError;

    fn try_from(value: &proto::RelTag) -> Result<RelTag, ProtocolError> {
        Ok(RelTag {
            spc_oid: value.spc_oid,
            db_oid: value.db_oid,
            rel_number: value.rel_number,
            fork_number: value
                .fork_number
                .try_into()
                .or(Err(ProtocolError::InvalidValue("fork_number")))?,
        })
    }
}

impl From<&RequestCommon> for proto::RequestCommon {
    fn from(value: &RequestCommon) -> proto::RequestCommon {
        proto::RequestCommon {
            request_lsn: value.request_lsn.into(),
            not_modified_since_lsn: value.not_modified_since_lsn.into(),
        }
    }
}
impl From<&proto::RequestCommon> for RequestCommon {
    fn from(value: &proto::RequestCommon) -> RequestCommon {
        RequestCommon {
            request_lsn: value.request_lsn.into(),
            not_modified_since_lsn: value.not_modified_since_lsn.into(),
        }
    }
}

impl From<&RelExistsRequest> for proto::RelExistsRequest {
    fn from(value: &RelExistsRequest) -> proto::RelExistsRequest {
        proto::RelExistsRequest {
            common: Some((&value.common).into()),
            rel: Some((&value.rel).into()),
        }
    }
}
impl TryFrom<&proto::RelExistsRequest> for RelExistsRequest {
    type Error = ProtocolError;

    fn try_from(value: &proto::RelExistsRequest) -> Result<RelExistsRequest, ProtocolError> {
        Ok(RelExistsRequest {
            common: (&value.common.ok_or(ProtocolError::Missing("common"))?).into(),
            rel: (&value.rel.ok_or(ProtocolError::Missing("rel"))?).try_into()?,
        })
    }
}

impl From<&RelSizeRequest> for proto::RelSizeRequest {
    fn from(value: &RelSizeRequest) -> proto::RelSizeRequest {
        proto::RelSizeRequest {
            common: Some((&value.common).into()),
            rel: Some((&value.rel).into()),
        }
    }
}
impl TryFrom<&proto::RelSizeRequest> for RelSizeRequest {
    type Error = ProtocolError;

    fn try_from(value: &proto::RelSizeRequest) -> Result<RelSizeRequest, ProtocolError> {
        Ok(RelSizeRequest {
            common: (&value.common.ok_or(ProtocolError::Missing("common"))?).into(),
            rel: (&value.rel.ok_or(ProtocolError::Missing("rel"))?).try_into()?,
        })
    }
}

impl From<&GetPageRequest> for proto::GetPageRequest {
    fn from(value: &GetPageRequest) -> proto::GetPageRequest {
        proto::GetPageRequest {
            id: value.id,
            common: Some((&value.common).into()),
            rel: Some((&value.rel).into()),
            block_number: value.block_number,
        }
    }
}
impl TryFrom<&proto::GetPageRequest> for GetPageRequest {
    type Error = ProtocolError;

    fn try_from(value: &proto::GetPageRequest) -> Result<GetPageRequest, ProtocolError> {
        Ok(GetPageRequest {
            id: value.id,
            common: (&value.common.ok_or(ProtocolError::Missing("common"))?).into(),
            rel: (&value.rel.ok_or(ProtocolError::Missing("rel"))?).try_into()?,
            block_number: value.block_number,
        })
    }
}

impl From<&DbSizeRequest> for proto::DbSizeRequest {
    fn from(value: &DbSizeRequest) -> proto::DbSizeRequest {
        proto::DbSizeRequest {
            common: Some((&value.common).into()),
            db_oid: value.db_oid,
        }
    }
}

impl TryFrom<&proto::DbSizeRequest> for DbSizeRequest {
    type Error = ProtocolError;

    fn try_from(value: &proto::DbSizeRequest) -> Result<DbSizeRequest, ProtocolError> {
        Ok(DbSizeRequest {
            common: (&value.common.ok_or(ProtocolError::Missing("common"))?).into(),
            db_oid: value.db_oid,
        })
    }
}

impl From<&GetBaseBackupRequest> for proto::GetBaseBackupRequest {
    fn from(value: &GetBaseBackupRequest) -> proto::GetBaseBackupRequest {
        proto::GetBaseBackupRequest {
            common: Some((&value.common).into()),
            replica: value.replica,
        }
    }
}

impl TryFrom<&proto::GetBaseBackupRequest> for GetBaseBackupRequest {
    type Error = ProtocolError;

    fn try_from(
        value: &proto::GetBaseBackupRequest,
    ) -> Result<GetBaseBackupRequest, ProtocolError> {
        Ok(GetBaseBackupRequest {
            common: (&value.common.ok_or(ProtocolError::Missing("common"))?).into(),
            replica: value.replica,
        })
    }
}

impl TryFrom<&proto::GetSlruSegmentRequest> for GetSlruSegmentRequest {
    type Error = ProtocolError;

    fn try_from(value: &proto::GetSlruSegmentRequest) -> Result<Self, Self::Error> {
        Ok(GetSlruSegmentRequest {
            common: (&value.common.ok_or(ProtocolError::Missing("common"))?).into(),
            kind: value
                .kind
                .try_into()
                .or(Err(ProtocolError::InvalidValue("kind")))?,
            segno: value.segno,
        })
    }
}
