use std::convert::TryInto;
use std::pin::Pin;
use std::io::Error;
use std::io::ErrorKind;

use bytes::Bytes;
use futures_core::Stream;
use tonic::transport::Channel;
use tonic::{Request, Status, Streaming};
use tonic::metadata::AsciiMetadataValue;
use tonic::metadata::errors::InvalidMetadataValue;
use futures::StreamExt;

use utils::id::TenantId;
use utils::id::TimelineId;
use utils::shard::ShardIndex;

use crate::proto;
use crate::model;

#[derive(Clone)]
pub struct Client {
    client: proto::PageServiceClient<Channel>,
    tenant_id: String,
    timeline_id: String,
    shard_id: String,
    auth_header: Option<String>,
}

fn convert_metadata_err(e: InvalidMetadataValue, s: String) -> Error {
    let io_err = Error::new(
        ErrorKind::InvalidInput,
        format!("{} header was invalid: {}", s, e),
    );
    io_err
}

impl Client {

    pub async fn new(connstring: String,
                    tenant_id: TenantId,
                    timeline_id: TimelineId,
                    shard_id: ShardIndex,
                    auth_header: Option<String>) -> Result<Self, Error> {

        let mut client = proto::PageServiceClient::connect(connstring).await
            .map_err(|e| {
                Error::new(ErrorKind::Other, format!("Failed to connect to page service: {}", e))
            })?;

        // Attempt to parse headers, so that parsing fails early. Later, we can assume
        // that the headers are valid ASCII strings.
        let _tenant_ascii : AsciiMetadataValue = tenant_id.to_string().try_into()
            .map_err(|e: InvalidMetadataValue| {
                convert_metadata_err(InvalidMetadataValue::from(e), "tenant-id".to_string())
            })?;

        let _timeline_ascii : AsciiMetadataValue = timeline_id.to_string().try_into()
            .map_err(|e: InvalidMetadataValue| {
                convert_metadata_err(InvalidMetadataValue::from(e), "timeline-id".to_string())
            })?;

        let _shard_ascii : AsciiMetadataValue = shard_id.clone().to_string().try_into()
            .map_err(|e: InvalidMetadataValue| {
                convert_metadata_err(InvalidMetadataValue::from(e), "shard-id".to_string())
            })?;

        Ok(Self {
            client,
            tenant_id: tenant_id.to_string(),
            timeline_id: timeline_id.to_string(),
            shard_id: shard_id.to_string(),
            auth_header,
        })
    }

    // helper to insert metadata headers
    fn insert_metadata<T>(self, mut req: tonic::Request<T>) -> (tonic::Request<T>, Self) {
        let metadata = req.metadata_mut();

        // Assume that the headers are valid Ascii strings, since we already checked them in the constructor.
        let tenant_ascii : AsciiMetadataValue = self.tenant_id.clone().try_into().unwrap();
        let timeline_ascii : AsciiMetadataValue = self.timeline_id.clone().try_into().unwrap();
        let shard_ascii : AsciiMetadataValue = self.shard_id.clone().try_into().unwrap();

        metadata.insert("neon-tenant-id", tenant_ascii);
        metadata.insert("neon-timeline-id", timeline_ascii);
        metadata.insert("neon-shard-id", shard_ascii);

        return (req, self);
    }

    pub async fn check_rel_exists(
        mut self,
        req: model::CheckRelExistsRequest,
    ) -> Result<model::CheckRelExistsResponse, tonic::Status> {

        let proto_req = proto::CheckRelExistsRequest {
            read_lsn: Some(req.read_lsn.try_into()?),
            rel: Some(req.rel.into()),
        };

        let mut req_new = Request::new(proto_req);
        (req_new, self) = self.insert_metadata::<proto::CheckRelExistsRequest>(req_new);

        let response = self
            .client
            .check_rel_exists(req_new)
            .await?;

        let proto_resp = response.into_inner();
        Ok(proto_resp.into())
    }

    pub async fn get_base_backup(
        mut self,
        req: &model::GetBaseBackupRequest,
    ) -> Result<impl Stream<Item = Result<Bytes, tonic::Status>>, tonic::Status> {

        let proto_req = proto::GetBaseBackupRequest {
            read_lsn: Some(req.read_lsn.try_into()?),
            replica: req.replica,
        };

        let mut req_new = Request::new(proto_req);
        (req_new, self) = self.insert_metadata::<proto::GetBaseBackupRequest>(req_new);

        let response_stream: Streaming<proto::GetBaseBackupResponseChunk> = self
            .client
            .get_base_backup(req_new)
            .await?
            .into_inner();

        let domain_stream = response_stream.map(|chunk_res| {
            chunk_res.and_then(|proto_chunk| {
                let b: Bytes = proto_chunk.try_into().unwrap();
                Ok(b)
            })
        });

        Ok(domain_stream)
    }

    pub async fn get_db_size(
        mut self,
        req: model::GetDbSizeRequest,
    ) -> Result<u64, tonic::Status> {

        let proto_req = proto::GetDbSizeRequest {
            read_lsn: Some(req.read_lsn.try_into()?),
            db_oid: req.db_oid,
        };

        let mut req_new = Request::new(proto_req);
        (req_new, self) = self.insert_metadata::<proto::GetDbSizeRequest>(req_new);

        let response = self.client.get_db_size(req_new).await?;
        Ok(response.into_inner().into())
    }

    pub async fn get_pages<ReqSt>(
        mut self,
        inbound: ReqSt,
    ) -> Result<impl Stream<Item = Result<model::GetPageResponse, tonic::Status>> + Send + 'static, tonic::Status>
    where
        ReqSt: Stream<Item = model::GetPageRequest> + Send + 'static,
    {
        let outbound_proto: Pin<Box<dyn Stream<Item = proto::GetPageRequest> + Send>> =
            Box::pin(
                inbound.map(|domain_req| {
                    domain_req.try_into().unwrap()
                }),
            );

        let mut req_new = Request::new(outbound_proto);
        (req_new, self) = self.insert_metadata::<Pin<Box<dyn Stream<Item = proto::GetPageRequest> + Send>>>(req_new);

        let request_stream: Request<Pin<Box<dyn Stream<Item = proto::GetPageRequest> + Send>>> =
            req_new;

        let response_stream: Streaming<proto::GetPageResponse> =
            self.client.get_pages(request_stream).await?.into_inner();

        let domain_stream = response_stream.map(|proto| {
            match proto {
                Ok(proto) => {
                    model::GetPageResponse::try_from(proto)
                        .map_err(|e| Status::internal(e.to_string()))
                }
                Err(status) => {
                    Err(Status::from(status))
                }
            }
        });

        Ok(domain_stream)
    }

    pub async fn get_rel_size(
        mut self,
        req: model::GetRelSizeRequest,
    ) -> Result<model::GetRelSizeResponse, tonic::Status> {

        let proto_req = proto::GetRelSizeRequest {
            read_lsn: Some(req.read_lsn.try_into()?),
            rel: Some(req.rel.into()),
        };

        let mut req_new = Request::new(proto_req);
        (req_new, self) = self.insert_metadata::<proto::GetRelSizeRequest>(req_new);

        let response = self.client.get_rel_size(req_new).await?;
        let proto_resp = response.into_inner();
        Ok(proto_resp.into())
    }

    pub async fn get_slru_segment(
        mut self,
        req: model::GetSlruSegmentRequest,
    ) -> Result<model::GetSlruSegmentResponse, tonic::Status> {

        let proto_req = proto::GetSlruSegmentRequest {
            read_lsn: Some(req.read_lsn.try_into()?),
            kind: req.kind as u32,
            segno: req.segno,
        };

        let mut req_new = Request::new(proto_req);
        (req_new, self) = self.insert_metadata::<proto::GetSlruSegmentRequest>(req_new);

        let response = self.client.get_slru_segment(req_new).await?;
        Ok(response.into_inner().segment as Bytes)
    }
}
