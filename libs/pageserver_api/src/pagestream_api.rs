//! Rust definitions of the libpq-based pagestream API
//!
//! See also the C implementation of the same API in pgxn/neon/pagestore_client.h

use std::io::{BufRead, Read};

use crate::reltag::RelTag;

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use utils::lsn::Lsn;

/// Block size.
///
/// XXX: We assume 8k block size in the SLRU fetch API. It's not great to hardcode
/// that in the protocol, because Postgres supports different block sizes as a compile
/// time option.
const BLCKSZ: usize = 8192;

// Wrapped in libpq CopyData
#[derive(PartialEq, Eq, Debug)]
pub enum PagestreamFeMessage {
    Exists(PagestreamExistsRequest),
    Nblocks(PagestreamNblocksRequest),
    GetPage(PagestreamGetPageRequest),
    DbSize(PagestreamDbSizeRequest),
    GetSlruSegment(PagestreamGetSlruSegmentRequest),
    #[cfg(feature = "testing")]
    Test(PagestreamTestRequest),
}

// Wrapped in libpq CopyData
#[derive(Debug, strum_macros::EnumProperty)]
pub enum PagestreamBeMessage {
    Exists(PagestreamExistsResponse),
    Nblocks(PagestreamNblocksResponse),
    GetPage(PagestreamGetPageResponse),
    Error(PagestreamErrorResponse),
    DbSize(PagestreamDbSizeResponse),
    GetSlruSegment(PagestreamGetSlruSegmentResponse),
    #[cfg(feature = "testing")]
    Test(PagestreamTestResponse),
}

// Keep in sync with `pagestore_client.h`
#[repr(u8)]
enum PagestreamFeMessageTag {
    Exists = 0,
    Nblocks = 1,
    GetPage = 2,
    DbSize = 3,
    GetSlruSegment = 4,
    /* future tags above this line */
    /// For testing purposes, not available in production.
    #[cfg(feature = "testing")]
    Test = 99,
}

// Keep in sync with `pagestore_client.h`
#[repr(u8)]
enum PagestreamBeMessageTag {
    Exists = 100,
    Nblocks = 101,
    GetPage = 102,
    Error = 103,
    DbSize = 104,
    GetSlruSegment = 105,
    /* future tags above this line */
    /// For testing purposes, not available in production.
    #[cfg(feature = "testing")]
    Test = 199,
}

impl TryFrom<u8> for PagestreamFeMessageTag {
    type Error = u8;
    fn try_from(value: u8) -> Result<Self, u8> {
        match value {
            0 => Ok(PagestreamFeMessageTag::Exists),
            1 => Ok(PagestreamFeMessageTag::Nblocks),
            2 => Ok(PagestreamFeMessageTag::GetPage),
            3 => Ok(PagestreamFeMessageTag::DbSize),
            4 => Ok(PagestreamFeMessageTag::GetSlruSegment),
            #[cfg(feature = "testing")]
            99 => Ok(PagestreamFeMessageTag::Test),
            _ => Err(value),
        }
    }
}

impl TryFrom<u8> for PagestreamBeMessageTag {
    type Error = u8;
    fn try_from(value: u8) -> Result<Self, u8> {
        match value {
            100 => Ok(PagestreamBeMessageTag::Exists),
            101 => Ok(PagestreamBeMessageTag::Nblocks),
            102 => Ok(PagestreamBeMessageTag::GetPage),
            103 => Ok(PagestreamBeMessageTag::Error),
            104 => Ok(PagestreamBeMessageTag::DbSize),
            105 => Ok(PagestreamBeMessageTag::GetSlruSegment),
            #[cfg(feature = "testing")]
            199 => Ok(PagestreamBeMessageTag::Test),
            _ => Err(value),
        }
    }
}

// A GetPage request contains two LSN values:
//
// request_lsn: Get the page version at this point in time.  Lsn::Max is a special value that means
// "get the latest version present". It's used by the primary server, which knows that no one else
// is writing WAL. 'not_modified_since' must be set to a proper value even if request_lsn is
// Lsn::Max. Standby servers use the current replay LSN as the request LSN.
//
// not_modified_since: Hint to the pageserver that the client knows that the page has not been
// modified between 'not_modified_since' and the request LSN. It's always correct to set
// 'not_modified_since equal' to 'request_lsn' (unless Lsn::Max is used as the 'request_lsn'), but
// passing an earlier LSN can speed up the request, by allowing the pageserver to process the
// request without waiting for 'request_lsn' to arrive.
//
// The now-defunct V1 interface contained only one LSN, and a boolean 'latest' flag. The V1 interface was
// sufficient for the primary; the 'lsn' was equivalent to the 'not_modified_since' value, and
// 'latest' was set to true. The V2 interface was added because there was no correct way for a
// standby to request a page at a particular non-latest LSN, and also include the
// 'not_modified_since' hint. That led to an awkward choice of either using an old LSN in the
// request, if the standby knows that the page hasn't been modified since, and risk getting an error
// if that LSN has fallen behind the GC horizon, or requesting the current replay LSN, which could
// require the pageserver unnecessarily to wait for the WAL to arrive up to that point. The new V2
// interface allows sending both LSNs, and let the pageserver do the right thing. There was no
// difference in the responses between V1 and V2.
//
// V3 version of protocol adds request ID to all requests. This request ID is also included in response
// as well as other fields from requests, which allows to verify that we receive response for our request.
// We copy fields from request to response to make checking more reliable: request ID is formed from process ID
// and local counter, so in principle there can be duplicated requests IDs if process PID is reused.
//
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum PagestreamProtocolVersion {
    V2,
    V3,
}

pub type RequestId = u64;

#[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
pub struct PagestreamRequest {
    pub reqid: RequestId,
    pub request_lsn: Lsn,
    pub not_modified_since: Lsn,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct PagestreamExistsRequest {
    pub hdr: PagestreamRequest,
    pub rel: RelTag,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct PagestreamNblocksRequest {
    pub hdr: PagestreamRequest,
    pub rel: RelTag,
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
pub struct PagestreamGetPageRequest {
    pub hdr: PagestreamRequest,
    pub rel: RelTag,
    pub blkno: u32,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct PagestreamDbSizeRequest {
    pub hdr: PagestreamRequest,
    pub dbnode: u32,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct PagestreamGetSlruSegmentRequest {
    pub hdr: PagestreamRequest,
    pub kind: u8,
    pub segno: u32,
}

#[derive(Debug)]
pub struct PagestreamExistsResponse {
    pub req: PagestreamExistsRequest,
    pub exists: bool,
}

#[derive(Debug)]
pub struct PagestreamNblocksResponse {
    pub req: PagestreamNblocksRequest,
    pub n_blocks: u32,
}

#[derive(Debug)]
pub struct PagestreamGetPageResponse {
    pub req: PagestreamGetPageRequest,
    pub page: Bytes,
}

#[derive(Debug)]
pub struct PagestreamGetSlruSegmentResponse {
    pub req: PagestreamGetSlruSegmentRequest,
    pub segment: Bytes,
}

#[derive(Debug)]
pub struct PagestreamErrorResponse {
    pub req: PagestreamRequest,
    pub message: String,
}

#[derive(Debug)]
pub struct PagestreamDbSizeResponse {
    pub req: PagestreamDbSizeRequest,
    pub db_size: i64,
}

#[cfg(feature = "testing")]
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PagestreamTestRequest {
    pub hdr: PagestreamRequest,
    pub batch_key: u64,
    pub message: String,
}

#[cfg(feature = "testing")]
#[derive(Debug)]
pub struct PagestreamTestResponse {
    pub req: PagestreamTestRequest,
}

impl PagestreamFeMessage {
    /// Serialize a compute -> pageserver message. This is currently only used in testing
    /// tools. Always uses protocol version 3.
    pub fn serialize(&self) -> Bytes {
        let mut bytes = BytesMut::new();

        match self {
            Self::Exists(req) => {
                bytes.put_u8(PagestreamFeMessageTag::Exists as u8);
                bytes.put_u64(req.hdr.reqid);
                bytes.put_u64(req.hdr.request_lsn.0);
                bytes.put_u64(req.hdr.not_modified_since.0);
                bytes.put_u32(req.rel.spcnode);
                bytes.put_u32(req.rel.dbnode);
                bytes.put_u32(req.rel.relnode);
                bytes.put_u8(req.rel.forknum);
            }

            Self::Nblocks(req) => {
                bytes.put_u8(PagestreamFeMessageTag::Nblocks as u8);
                bytes.put_u64(req.hdr.reqid);
                bytes.put_u64(req.hdr.request_lsn.0);
                bytes.put_u64(req.hdr.not_modified_since.0);
                bytes.put_u32(req.rel.spcnode);
                bytes.put_u32(req.rel.dbnode);
                bytes.put_u32(req.rel.relnode);
                bytes.put_u8(req.rel.forknum);
            }

            Self::GetPage(req) => {
                bytes.put_u8(PagestreamFeMessageTag::GetPage as u8);
                bytes.put_u64(req.hdr.reqid);
                bytes.put_u64(req.hdr.request_lsn.0);
                bytes.put_u64(req.hdr.not_modified_since.0);
                bytes.put_u32(req.rel.spcnode);
                bytes.put_u32(req.rel.dbnode);
                bytes.put_u32(req.rel.relnode);
                bytes.put_u8(req.rel.forknum);
                bytes.put_u32(req.blkno);
            }

            Self::DbSize(req) => {
                bytes.put_u8(PagestreamFeMessageTag::DbSize as u8);
                bytes.put_u64(req.hdr.reqid);
                bytes.put_u64(req.hdr.request_lsn.0);
                bytes.put_u64(req.hdr.not_modified_since.0);
                bytes.put_u32(req.dbnode);
            }

            Self::GetSlruSegment(req) => {
                bytes.put_u8(PagestreamFeMessageTag::GetSlruSegment as u8);
                bytes.put_u64(req.hdr.reqid);
                bytes.put_u64(req.hdr.request_lsn.0);
                bytes.put_u64(req.hdr.not_modified_since.0);
                bytes.put_u8(req.kind);
                bytes.put_u32(req.segno);
            }
            #[cfg(feature = "testing")]
            Self::Test(req) => {
                bytes.put_u8(PagestreamFeMessageTag::Test as u8);
                bytes.put_u64(req.hdr.reqid);
                bytes.put_u64(req.hdr.request_lsn.0);
                bytes.put_u64(req.hdr.not_modified_since.0);
                bytes.put_u64(req.batch_key);
                let message = req.message.as_bytes();
                bytes.put_u64(message.len() as u64);
                bytes.put_slice(message);
            }
        }

        bytes.into()
    }

    pub fn parse<R: std::io::Read>(
        body: &mut R,
        protocol_version: PagestreamProtocolVersion,
    ) -> anyhow::Result<PagestreamFeMessage> {
        // these correspond to the NeonMessageTag enum in pagestore_client.h
        //
        // TODO: consider using protobuf or serde bincode for less error prone
        // serialization.
        let msg_tag = body.read_u8()?;
        let (reqid, request_lsn, not_modified_since) = match protocol_version {
            PagestreamProtocolVersion::V2 => (
                0,
                Lsn::from(body.read_u64::<BigEndian>()?),
                Lsn::from(body.read_u64::<BigEndian>()?),
            ),
            PagestreamProtocolVersion::V3 => (
                body.read_u64::<BigEndian>()?,
                Lsn::from(body.read_u64::<BigEndian>()?),
                Lsn::from(body.read_u64::<BigEndian>()?),
            ),
        };

        match PagestreamFeMessageTag::try_from(msg_tag)
            .map_err(|tag: u8| anyhow::anyhow!("invalid tag {tag}"))?
        {
            PagestreamFeMessageTag::Exists => {
                Ok(PagestreamFeMessage::Exists(PagestreamExistsRequest {
                    hdr: PagestreamRequest {
                        reqid,
                        request_lsn,
                        not_modified_since,
                    },
                    rel: RelTag {
                        spcnode: body.read_u32::<BigEndian>()?,
                        dbnode: body.read_u32::<BigEndian>()?,
                        relnode: body.read_u32::<BigEndian>()?,
                        forknum: body.read_u8()?,
                    },
                }))
            }
            PagestreamFeMessageTag::Nblocks => {
                Ok(PagestreamFeMessage::Nblocks(PagestreamNblocksRequest {
                    hdr: PagestreamRequest {
                        reqid,
                        request_lsn,
                        not_modified_since,
                    },
                    rel: RelTag {
                        spcnode: body.read_u32::<BigEndian>()?,
                        dbnode: body.read_u32::<BigEndian>()?,
                        relnode: body.read_u32::<BigEndian>()?,
                        forknum: body.read_u8()?,
                    },
                }))
            }
            PagestreamFeMessageTag::GetPage => {
                Ok(PagestreamFeMessage::GetPage(PagestreamGetPageRequest {
                    hdr: PagestreamRequest {
                        reqid,
                        request_lsn,
                        not_modified_since,
                    },
                    rel: RelTag {
                        spcnode: body.read_u32::<BigEndian>()?,
                        dbnode: body.read_u32::<BigEndian>()?,
                        relnode: body.read_u32::<BigEndian>()?,
                        forknum: body.read_u8()?,
                    },
                    blkno: body.read_u32::<BigEndian>()?,
                }))
            }
            PagestreamFeMessageTag::DbSize => {
                Ok(PagestreamFeMessage::DbSize(PagestreamDbSizeRequest {
                    hdr: PagestreamRequest {
                        reqid,
                        request_lsn,
                        not_modified_since,
                    },
                    dbnode: body.read_u32::<BigEndian>()?,
                }))
            }
            PagestreamFeMessageTag::GetSlruSegment => Ok(PagestreamFeMessage::GetSlruSegment(
                PagestreamGetSlruSegmentRequest {
                    hdr: PagestreamRequest {
                        reqid,
                        request_lsn,
                        not_modified_since,
                    },
                    kind: body.read_u8()?,
                    segno: body.read_u32::<BigEndian>()?,
                },
            )),
            #[cfg(feature = "testing")]
            PagestreamFeMessageTag::Test => Ok(PagestreamFeMessage::Test(PagestreamTestRequest {
                hdr: PagestreamRequest {
                    reqid,
                    request_lsn,
                    not_modified_since,
                },
                batch_key: body.read_u64::<BigEndian>()?,
                message: {
                    let len = body.read_u64::<BigEndian>()?;
                    let mut buf = vec![0; len as usize];
                    body.read_exact(&mut buf)?;
                    String::from_utf8(buf)?
                },
            })),
        }
    }
}

impl PagestreamBeMessage {
    pub fn serialize(&self, protocol_version: PagestreamProtocolVersion) -> Bytes {
        let mut bytes = BytesMut::new();

        use PagestreamBeMessageTag as Tag;
        match protocol_version {
            PagestreamProtocolVersion::V2 => {
                match self {
                    Self::Exists(resp) => {
                        bytes.put_u8(Tag::Exists as u8);
                        bytes.put_u8(resp.exists as u8);
                    }

                    Self::Nblocks(resp) => {
                        bytes.put_u8(Tag::Nblocks as u8);
                        bytes.put_u32(resp.n_blocks);
                    }

                    Self::GetPage(resp) => {
                        bytes.put_u8(Tag::GetPage as u8);
                        bytes.put(&resp.page[..])
                    }

                    Self::Error(resp) => {
                        bytes.put_u8(Tag::Error as u8);
                        bytes.put(resp.message.as_bytes());
                        bytes.put_u8(0); // null terminator
                    }
                    Self::DbSize(resp) => {
                        bytes.put_u8(Tag::DbSize as u8);
                        bytes.put_i64(resp.db_size);
                    }

                    Self::GetSlruSegment(resp) => {
                        bytes.put_u8(Tag::GetSlruSegment as u8);
                        bytes.put_u32((resp.segment.len() / BLCKSZ) as u32);
                        bytes.put(&resp.segment[..]);
                    }

                    #[cfg(feature = "testing")]
                    Self::Test(resp) => {
                        bytes.put_u8(Tag::Test as u8);
                        bytes.put_u64(resp.req.batch_key);
                        let message = resp.req.message.as_bytes();
                        bytes.put_u64(message.len() as u64);
                        bytes.put_slice(message);
                    }
                }
            }
            PagestreamProtocolVersion::V3 => {
                match self {
                    Self::Exists(resp) => {
                        bytes.put_u8(Tag::Exists as u8);
                        bytes.put_u64(resp.req.hdr.reqid);
                        bytes.put_u64(resp.req.hdr.request_lsn.0);
                        bytes.put_u64(resp.req.hdr.not_modified_since.0);
                        bytes.put_u32(resp.req.rel.spcnode);
                        bytes.put_u32(resp.req.rel.dbnode);
                        bytes.put_u32(resp.req.rel.relnode);
                        bytes.put_u8(resp.req.rel.forknum);
                        bytes.put_u8(resp.exists as u8);
                    }

                    Self::Nblocks(resp) => {
                        bytes.put_u8(Tag::Nblocks as u8);
                        bytes.put_u64(resp.req.hdr.reqid);
                        bytes.put_u64(resp.req.hdr.request_lsn.0);
                        bytes.put_u64(resp.req.hdr.not_modified_since.0);
                        bytes.put_u32(resp.req.rel.spcnode);
                        bytes.put_u32(resp.req.rel.dbnode);
                        bytes.put_u32(resp.req.rel.relnode);
                        bytes.put_u8(resp.req.rel.forknum);
                        bytes.put_u32(resp.n_blocks);
                    }

                    Self::GetPage(resp) => {
                        bytes.put_u8(Tag::GetPage as u8);
                        bytes.put_u64(resp.req.hdr.reqid);
                        bytes.put_u64(resp.req.hdr.request_lsn.0);
                        bytes.put_u64(resp.req.hdr.not_modified_since.0);
                        bytes.put_u32(resp.req.rel.spcnode);
                        bytes.put_u32(resp.req.rel.dbnode);
                        bytes.put_u32(resp.req.rel.relnode);
                        bytes.put_u8(resp.req.rel.forknum);
                        bytes.put_u32(resp.req.blkno);
                        bytes.put(&resp.page[..])
                    }

                    Self::Error(resp) => {
                        bytes.put_u8(Tag::Error as u8);
                        bytes.put_u64(resp.req.reqid);
                        bytes.put_u64(resp.req.request_lsn.0);
                        bytes.put_u64(resp.req.not_modified_since.0);
                        bytes.put(resp.message.as_bytes());
                        bytes.put_u8(0); // null terminator
                    }
                    Self::DbSize(resp) => {
                        bytes.put_u8(Tag::DbSize as u8);
                        bytes.put_u64(resp.req.hdr.reqid);
                        bytes.put_u64(resp.req.hdr.request_lsn.0);
                        bytes.put_u64(resp.req.hdr.not_modified_since.0);
                        bytes.put_u32(resp.req.dbnode);
                        bytes.put_i64(resp.db_size);
                    }

                    Self::GetSlruSegment(resp) => {
                        bytes.put_u8(Tag::GetSlruSegment as u8);
                        bytes.put_u64(resp.req.hdr.reqid);
                        bytes.put_u64(resp.req.hdr.request_lsn.0);
                        bytes.put_u64(resp.req.hdr.not_modified_since.0);
                        bytes.put_u8(resp.req.kind);
                        bytes.put_u32(resp.req.segno);
                        bytes.put_u32((resp.segment.len() / BLCKSZ) as u32);
                        bytes.put(&resp.segment[..]);
                    }

                    #[cfg(feature = "testing")]
                    Self::Test(resp) => {
                        bytes.put_u8(Tag::Test as u8);
                        bytes.put_u64(resp.req.hdr.reqid);
                        bytes.put_u64(resp.req.hdr.request_lsn.0);
                        bytes.put_u64(resp.req.hdr.not_modified_since.0);
                        bytes.put_u64(resp.req.batch_key);
                        let message = resp.req.message.as_bytes();
                        bytes.put_u64(message.len() as u64);
                        bytes.put_slice(message);
                    }
                }
            }
        }
        bytes.into()
    }

    pub fn deserialize(buf: Bytes) -> anyhow::Result<Self> {
        let mut buf = buf.reader();
        let msg_tag = buf.read_u8()?;

        use PagestreamBeMessageTag as Tag;
        let ok =
            match Tag::try_from(msg_tag).map_err(|tag: u8| anyhow::anyhow!("invalid tag {tag}"))? {
                Tag::Exists => {
                    let reqid = buf.read_u64::<BigEndian>()?;
                    let request_lsn = Lsn(buf.read_u64::<BigEndian>()?);
                    let not_modified_since = Lsn(buf.read_u64::<BigEndian>()?);
                    let rel = RelTag {
                        spcnode: buf.read_u32::<BigEndian>()?,
                        dbnode: buf.read_u32::<BigEndian>()?,
                        relnode: buf.read_u32::<BigEndian>()?,
                        forknum: buf.read_u8()?,
                    };
                    let exists = buf.read_u8()? != 0;
                    Self::Exists(PagestreamExistsResponse {
                        req: PagestreamExistsRequest {
                            hdr: PagestreamRequest {
                                reqid,
                                request_lsn,
                                not_modified_since,
                            },
                            rel,
                        },
                        exists,
                    })
                }
                Tag::Nblocks => {
                    let reqid = buf.read_u64::<BigEndian>()?;
                    let request_lsn = Lsn(buf.read_u64::<BigEndian>()?);
                    let not_modified_since = Lsn(buf.read_u64::<BigEndian>()?);
                    let rel = RelTag {
                        spcnode: buf.read_u32::<BigEndian>()?,
                        dbnode: buf.read_u32::<BigEndian>()?,
                        relnode: buf.read_u32::<BigEndian>()?,
                        forknum: buf.read_u8()?,
                    };
                    let n_blocks = buf.read_u32::<BigEndian>()?;
                    Self::Nblocks(PagestreamNblocksResponse {
                        req: PagestreamNblocksRequest {
                            hdr: PagestreamRequest {
                                reqid,
                                request_lsn,
                                not_modified_since,
                            },
                            rel,
                        },
                        n_blocks,
                    })
                }
                Tag::GetPage => {
                    let reqid = buf.read_u64::<BigEndian>()?;
                    let request_lsn = Lsn(buf.read_u64::<BigEndian>()?);
                    let not_modified_since = Lsn(buf.read_u64::<BigEndian>()?);
                    let rel = RelTag {
                        spcnode: buf.read_u32::<BigEndian>()?,
                        dbnode: buf.read_u32::<BigEndian>()?,
                        relnode: buf.read_u32::<BigEndian>()?,
                        forknum: buf.read_u8()?,
                    };
                    let blkno = buf.read_u32::<BigEndian>()?;
                    let mut page = vec![0; 8192]; // TODO: use MaybeUninit
                    buf.read_exact(&mut page)?;
                    Self::GetPage(PagestreamGetPageResponse {
                        req: PagestreamGetPageRequest {
                            hdr: PagestreamRequest {
                                reqid,
                                request_lsn,
                                not_modified_since,
                            },
                            rel,
                            blkno,
                        },
                        page: page.into(),
                    })
                }
                Tag::Error => {
                    let reqid = buf.read_u64::<BigEndian>()?;
                    let request_lsn = Lsn(buf.read_u64::<BigEndian>()?);
                    let not_modified_since = Lsn(buf.read_u64::<BigEndian>()?);
                    let mut msg = Vec::new();
                    buf.read_until(0, &mut msg)?;
                    let cstring = std::ffi::CString::from_vec_with_nul(msg)?;
                    let rust_str = cstring.to_str()?;
                    Self::Error(PagestreamErrorResponse {
                        req: PagestreamRequest {
                            reqid,
                            request_lsn,
                            not_modified_since,
                        },
                        message: rust_str.to_owned(),
                    })
                }
                Tag::DbSize => {
                    let reqid = buf.read_u64::<BigEndian>()?;
                    let request_lsn = Lsn(buf.read_u64::<BigEndian>()?);
                    let not_modified_since = Lsn(buf.read_u64::<BigEndian>()?);
                    let dbnode = buf.read_u32::<BigEndian>()?;
                    let db_size = buf.read_i64::<BigEndian>()?;
                    Self::DbSize(PagestreamDbSizeResponse {
                        req: PagestreamDbSizeRequest {
                            hdr: PagestreamRequest {
                                reqid,
                                request_lsn,
                                not_modified_since,
                            },
                            dbnode,
                        },
                        db_size,
                    })
                }
                Tag::GetSlruSegment => {
                    let reqid = buf.read_u64::<BigEndian>()?;
                    let request_lsn = Lsn(buf.read_u64::<BigEndian>()?);
                    let not_modified_since = Lsn(buf.read_u64::<BigEndian>()?);
                    let kind = buf.read_u8()?;
                    let segno = buf.read_u32::<BigEndian>()?;
                    let n_blocks = buf.read_u32::<BigEndian>()?;
                    let mut segment = vec![0; n_blocks as usize * BLCKSZ];
                    buf.read_exact(&mut segment)?;
                    Self::GetSlruSegment(PagestreamGetSlruSegmentResponse {
                        req: PagestreamGetSlruSegmentRequest {
                            hdr: PagestreamRequest {
                                reqid,
                                request_lsn,
                                not_modified_since,
                            },
                            kind,
                            segno,
                        },
                        segment: segment.into(),
                    })
                }
                #[cfg(feature = "testing")]
                Tag::Test => {
                    let reqid = buf.read_u64::<BigEndian>()?;
                    let request_lsn = Lsn(buf.read_u64::<BigEndian>()?);
                    let not_modified_since = Lsn(buf.read_u64::<BigEndian>()?);
                    let batch_key = buf.read_u64::<BigEndian>()?;
                    let len = buf.read_u64::<BigEndian>()?;
                    let mut msg = vec![0; len as usize];
                    buf.read_exact(&mut msg)?;
                    let message = String::from_utf8(msg)?;
                    Self::Test(PagestreamTestResponse {
                        req: PagestreamTestRequest {
                            hdr: PagestreamRequest {
                                reqid,
                                request_lsn,
                                not_modified_since,
                            },
                            batch_key,
                            message,
                        },
                    })
                }
            };
        let remaining = buf.into_inner();
        if !remaining.is_empty() {
            anyhow::bail!(
                "remaining bytes in msg with tag={msg_tag}: {}",
                remaining.len()
            );
        }
        Ok(ok)
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Exists(_) => "Exists",
            Self::Nblocks(_) => "Nblocks",
            Self::GetPage(_) => "GetPage",
            Self::Error(_) => "Error",
            Self::DbSize(_) => "DbSize",
            Self::GetSlruSegment(_) => "GetSlruSegment",
            #[cfg(feature = "testing")]
            Self::Test(_) => "Test",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pagestream() {
        // Test serialization/deserialization of PagestreamFeMessage
        let messages = vec![
            PagestreamFeMessage::Exists(PagestreamExistsRequest {
                hdr: PagestreamRequest {
                    reqid: 0,
                    request_lsn: Lsn(4),
                    not_modified_since: Lsn(3),
                },
                rel: RelTag {
                    forknum: 1,
                    spcnode: 2,
                    dbnode: 3,
                    relnode: 4,
                },
            }),
            PagestreamFeMessage::Nblocks(PagestreamNblocksRequest {
                hdr: PagestreamRequest {
                    reqid: 0,
                    request_lsn: Lsn(4),
                    not_modified_since: Lsn(4),
                },
                rel: RelTag {
                    forknum: 1,
                    spcnode: 2,
                    dbnode: 3,
                    relnode: 4,
                },
            }),
            PagestreamFeMessage::GetPage(PagestreamGetPageRequest {
                hdr: PagestreamRequest {
                    reqid: 0,
                    request_lsn: Lsn(4),
                    not_modified_since: Lsn(3),
                },
                rel: RelTag {
                    forknum: 1,
                    spcnode: 2,
                    dbnode: 3,
                    relnode: 4,
                },
                blkno: 7,
            }),
            PagestreamFeMessage::DbSize(PagestreamDbSizeRequest {
                hdr: PagestreamRequest {
                    reqid: 0,
                    request_lsn: Lsn(4),
                    not_modified_since: Lsn(3),
                },
                dbnode: 7,
            }),
        ];
        for msg in messages {
            let bytes = msg.serialize();
            let reconstructed =
                PagestreamFeMessage::parse(&mut bytes.reader(), PagestreamProtocolVersion::V3)
                    .unwrap();
            assert!(msg == reconstructed);
        }
    }
}
