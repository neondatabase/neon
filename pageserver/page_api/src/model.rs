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

use bytes::Bytes;
use utils::lsn::Lsn;

use crate::proto;

#[derive(Clone, Debug)]
pub struct ReadLsn {
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
pub struct CheckRelExistsRequest {
    pub read_lsn: ReadLsn,
    pub rel: RelTag,
}

#[derive(Clone, Debug)]
pub struct GetRelSizeRequest {
    pub read_lsn: ReadLsn,
    pub rel: RelTag,
}

#[derive(Clone, Debug)]
pub struct GetRelSizeResponse {
    pub num_blocks: u32,
}

#[derive(Clone, Debug)]
pub struct GetPageRequest {
    pub request_id: u64,
    pub request_class: GetPageClass,
    pub read_lsn: ReadLsn,
    pub rel: RelTag,
    pub block_number: Vec<u32>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum GetPageClass {
    Normal,
    Prefetch,
    Background,
}

#[derive(Clone, Debug)]
pub struct GetPageResponse {
    pub request_id: u64,
    pub status: GetPageStatus,
    pub reason: String,
    pub page_image: Vec<Bytes>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum GetPageStatus {
    Ok,
    NotFound,
    Invalid,
    SlowDown,
}

#[derive(Clone, Debug)]
pub struct GetDbSizeRequest {
    pub read_lsn: ReadLsn,
    pub db_oid: u32,
}

#[derive(Clone, Debug)]
pub struct GetDbSizeResponse {
    pub num_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct GetBaseBackupRequest {
    pub read_lsn: ReadLsn,
    pub replica: bool,
}

#[derive(Clone, Debug)]
pub struct GetSlruSegmentRequest {
    pub read_lsn: ReadLsn,
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

impl From<&ReadLsn> for proto::ReadLsn {
    fn from(value: &ReadLsn) -> proto::ReadLsn {
        proto::ReadLsn {
            request_lsn: value.request_lsn.into(),
            not_modified_since_lsn: value.not_modified_since_lsn.into(),
        }
    }
}
impl From<&proto::ReadLsn> for ReadLsn {
    fn from(value: &proto::ReadLsn) -> ReadLsn {
        ReadLsn {
            request_lsn: value.request_lsn.into(),
            not_modified_since_lsn: value.not_modified_since_lsn.into(),
        }
    }
}

impl From<&CheckRelExistsRequest> for proto::CheckRelExistsRequest {
    fn from(value: &CheckRelExistsRequest) -> proto::CheckRelExistsRequest {
        proto::CheckRelExistsRequest {
            read_lsn: Some((&value.read_lsn).into()),
            rel: Some((&value.rel).into()),
        }
    }
}
impl TryFrom<&proto::CheckRelExistsRequest> for CheckRelExistsRequest {
    type Error = ProtocolError;

    fn try_from(
        value: &proto::CheckRelExistsRequest,
    ) -> Result<CheckRelExistsRequest, ProtocolError> {
        Ok(CheckRelExistsRequest {
            read_lsn: (&value.read_lsn.ok_or(ProtocolError::Missing("read_lsn"))?).into(),
            rel: (&value.rel.ok_or(ProtocolError::Missing("rel"))?).try_into()?,
        })
    }
}

impl From<&GetRelSizeRequest> for proto::GetRelSizeRequest {
    fn from(value: &GetRelSizeRequest) -> proto::GetRelSizeRequest {
        proto::GetRelSizeRequest {
            read_lsn: Some((&value.read_lsn).into()),
            rel: Some((&value.rel).into()),
        }
    }
}
impl TryFrom<&proto::GetRelSizeRequest> for GetRelSizeRequest {
    type Error = ProtocolError;

    fn try_from(value: &proto::GetRelSizeRequest) -> Result<GetRelSizeRequest, ProtocolError> {
        Ok(GetRelSizeRequest {
            read_lsn: (&value.read_lsn.ok_or(ProtocolError::Missing("read_lsn"))?).into(),
            rel: (&value.rel.ok_or(ProtocolError::Missing("rel"))?).try_into()?,
        })
    }
}

impl From<&GetPageRequest> for proto::GetPageRequest {
    fn from(value: &GetPageRequest) -> proto::GetPageRequest {
        proto::GetPageRequest {
            request_id: value.request_id,
            request_class: match value.request_class {
                GetPageClass::Normal => proto::GetPageClass::Normal as i32,
                GetPageClass::Prefetch => proto::GetPageClass::Prefetch as i32,
                GetPageClass::Background => proto::GetPageClass::Background as i32,
            },
            read_lsn: Some((&value.read_lsn).into()),
            rel: Some((&value.rel).into()),
            block_number: value.block_number.clone(),
        }
    }
}
impl TryFrom<&proto::GetPageRequest> for GetPageRequest {
    type Error = ProtocolError;

    fn try_from(value: &proto::GetPageRequest) -> Result<GetPageRequest, ProtocolError> {
        Ok(GetPageRequest {
            request_id: value.request_id,
            read_lsn: (&value.read_lsn.ok_or(ProtocolError::Missing("read_lsn"))?).into(),
            rel: (&value.rel.ok_or(ProtocolError::Missing("rel"))?).try_into()?,
            block_number: value.block_number.clone(),
            request_class: proto::GetPageClass::try_from(value.request_class)
                .unwrap_or(proto::GetPageClass::Unknown)
                .try_into()?,
        })
    }
}

impl TryFrom<proto::GetPageClass> for GetPageClass {
    type Error = ProtocolError;

    fn try_from(value: proto::GetPageClass) -> Result<GetPageClass, ProtocolError> {
        match value {
            proto::GetPageClass::Unknown => Err(ProtocolError::InvalidValue("class")),
            proto::GetPageClass::Normal => Ok(GetPageClass::Normal),
            proto::GetPageClass::Prefetch => Ok(GetPageClass::Prefetch),
            proto::GetPageClass::Background => Ok(GetPageClass::Background),
        }
    }
}

impl From<GetPageClass> for proto::GetPageClass {
    fn from(value: GetPageClass) -> proto::GetPageClass {
        match value {
            GetPageClass::Normal => proto::GetPageClass::Normal,
            GetPageClass::Prefetch => proto::GetPageClass::Prefetch,
            GetPageClass::Background => proto::GetPageClass::Background,
        }
    }
}

impl TryFrom<proto::GetPageResponse> for GetPageResponse {
    type Error = ProtocolError;

    fn try_from(value: proto::GetPageResponse) -> Result<GetPageResponse, ProtocolError> {
        Ok(GetPageResponse {
            request_id: value.request_id,
            status: proto::GetPageStatus::try_from(value.status)
                .unwrap_or(proto::GetPageStatus::Unknown)
                .try_into()?,
            reason: value.reason,
            page_image: value.page_image,
        })
    }
}

impl TryFrom<proto::GetPageStatus> for GetPageStatus {
    type Error = ProtocolError;

    fn try_from(value: proto::GetPageStatus) -> Result<GetPageStatus, ProtocolError> {
        match value {
            // Error on unknknown status -- we don't want to make any assumptions here.
            //
            // NB: this means that new statuses can only be used after all computes
            // have been updated to understand them. Do something else instead?
            proto::GetPageStatus::Unknown => Err(ProtocolError::InvalidValue("status")),
            proto::GetPageStatus::Ok => Ok(GetPageStatus::Ok),
            proto::GetPageStatus::NotFound => Ok(GetPageStatus::NotFound),
            proto::GetPageStatus::Invalid => Ok(GetPageStatus::Invalid),
            proto::GetPageStatus::SlowDown => Ok(GetPageStatus::SlowDown),
        }
    }
}

impl From<GetPageStatus> for proto::GetPageStatus {
    fn from(value: GetPageStatus) -> proto::GetPageStatus {
        match value {
            GetPageStatus::Ok => proto::GetPageStatus::Ok,
            GetPageStatus::NotFound => proto::GetPageStatus::NotFound,
            GetPageStatus::Invalid => proto::GetPageStatus::Invalid,
            GetPageStatus::SlowDown => proto::GetPageStatus::SlowDown,
        }
    }
}

impl From<&GetDbSizeRequest> for proto::GetDbSizeRequest {
    fn from(value: &GetDbSizeRequest) -> proto::GetDbSizeRequest {
        proto::GetDbSizeRequest {
            read_lsn: Some((&value.read_lsn).into()),
            db_oid: value.db_oid,
        }
    }
}

impl TryFrom<&proto::GetDbSizeRequest> for GetDbSizeRequest {
    type Error = ProtocolError;

    fn try_from(value: &proto::GetDbSizeRequest) -> Result<GetDbSizeRequest, ProtocolError> {
        Ok(GetDbSizeRequest {
            read_lsn: (&value.read_lsn.ok_or(ProtocolError::Missing("read_lsn"))?).into(),
            db_oid: value.db_oid,
        })
    }
}

impl From<&GetBaseBackupRequest> for proto::GetBaseBackupRequest {
    fn from(value: &GetBaseBackupRequest) -> proto::GetBaseBackupRequest {
        proto::GetBaseBackupRequest {
            read_lsn: Some((&value.read_lsn).into()),
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
            read_lsn: (&value.read_lsn.ok_or(ProtocolError::Missing("read_lsn"))?).into(),
            replica: value.replica,
        })
    }
}

impl TryFrom<&proto::GetSlruSegmentRequest> for GetSlruSegmentRequest {
    type Error = ProtocolError;

    fn try_from(value: &proto::GetSlruSegmentRequest) -> Result<Self, Self::Error> {
        Ok(GetSlruSegmentRequest {
            read_lsn: (&value.read_lsn.ok_or(ProtocolError::Missing("read_lsn"))?).into(),
            kind: value
                .kind
                .try_into()
                .or(Err(ProtocolError::InvalidValue("kind")))?,
            segno: value.segno,
        })
    }
}
