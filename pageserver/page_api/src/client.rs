use std::convert::TryInto;
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
struct AuthInterceptor {
    tenant_id: AsciiMetadataValue,
    shard_id: Option<AsciiMetadataValue>,
    timeline_id: AsciiMetadataValue,
    auth_header: Option<AsciiMetadataValue>, // including "Bearer " prefix
}

impl AuthInterceptor {
    fn new(tenant_id: AsciiMetadataValue,
           timeline_id: AsciiMetadataValue,
           auth_token: Option<String>) -> Self {

        Self {
            tenant_id: tenant_id,
            shard_id: None,
            timeline_id: timeline_id,
            auth_header: auth_token
                .map(|t| format!("Bearer {t}"))
                .map(|t| t.parse()
                    .expect("could not parse auth header as AsciiMetadataValue")),
        }
    }

    fn for_shard(&self, shard_id: ShardIndex) -> Self {
        let mut with_shard = self.clone();
        with_shard.shard_id = Some(
            shard_id
                .to_string()
                .parse()
                .expect("could not parse shard id"),
        );
        with_shard
    }
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        req.metadata_mut()
            .insert("neon-tenant-id", self.tenant_id.clone());
        if let Some(shard_id) = &self.shard_id {
            req.metadata_mut().insert("neon-shard-id", shard_id.clone());
        }
        req.metadata_mut()
            .insert("neon-timeline-id", self.timeline_id.clone());
        if let Some(auth_header) = &self.auth_header {
            req.metadata_mut()
                .insert("authorization", auth_header.clone());
        }
        Ok(req)
    }
}
#[derive(Clone)]
pub struct Client {
    client: proto::PageServiceClient<tonic::service::interceptor::InterceptedService<Channel, AuthInterceptor>>,
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

        let endpoint = tonic::transport::Endpoint::from_shared(connstring)
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e.to_string()))?;

        let channel = endpoint.connect().await
            .map_err(|e| Error::new(ErrorKind::ConnectionRefused, e.to_string()))?;

        let tenant_ascii : AsciiMetadataValue = tenant_id.to_string().try_into()
            .map_err(|e: InvalidMetadataValue| {
                convert_metadata_err(InvalidMetadataValue::from(e), "tenant-id".to_string())
            })?;

        let timeline_ascii : AsciiMetadataValue = timeline_id.to_string().try_into()
            .map_err(|e: InvalidMetadataValue| {
                convert_metadata_err(InvalidMetadataValue::from(e), "timeline-id".to_string())
            })?;

        let _shard_ascii : AsciiMetadataValue = shard_id.clone().to_string().try_into()
            .map_err(|e: InvalidMetadataValue| {
                convert_metadata_err(InvalidMetadataValue::from(e), "shard-id".to_string())
            })?;

        let auth = AuthInterceptor::new(tenant_ascii, timeline_ascii, auth_header);

        let client = proto::PageServiceClient::with_interceptor(
            channel,
            auth.for_shard(shard_id.clone()),
        );
        Ok(Self {
            client,
        })
    }

    pub async fn check_rel_exists(
        &mut self,
        req: model::CheckRelExistsRequest,
    ) -> Result<model::CheckRelExistsResponse, tonic::Status> {

        let proto_req = proto::CheckRelExistsRequest::try_from(req)
            .map_err(|e| Status::internal(format!("Failed to convert request: {}", e)))?;

        let response = self
            .client
            .check_rel_exists(proto_req)
            .await?;

        let proto_resp = response.into_inner();
        Ok(proto_resp.into())
    }

    pub async fn get_base_backup(
        mut self,
        req: model::GetBaseBackupRequest,
    ) -> Result<impl Stream<Item = Result<Bytes, tonic::Status>>, tonic::Status> {

        let proto_req = proto::GetBaseBackupRequest::try_from(req)
            .map_err(|e| Status::internal(format!("Failed to convert request: {}", e)))?;

        let response_stream: Streaming<proto::GetBaseBackupResponseChunk> = self
            .client
            .get_base_backup(proto_req)
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

        let proto_req = proto::GetDbSizeRequest::try_from(req)
            .map_err(|e| Status::internal(format!("Failed to convert request: {}", e)))?;

        let response = self.client.get_db_size(proto_req).await?;
        Ok(response.into_inner().into())
    }

    pub async fn get_pages<ReqSt>(
        &mut self,
        inbound: ReqSt,
    ) -> Result<impl Stream<Item = Result<model::GetPageResponse, tonic::Status>> + Send + 'static, tonic::Status>
    where
        ReqSt: Stream<Item = model::GetPageRequest> + Send + 'static,
    {

        let outbound_proto =
                inbound.map(|domain_req| {
                    domain_req.into()
                });

        let req_new = Request::new(outbound_proto);

        let response_stream: Streaming<proto::GetPageResponse> =
            self.client.get_pages(req_new).await?.into_inner();

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

        let proto_req = proto::GetRelSizeRequest::try_from(req)
            .map_err(|e| Status::internal(format!("Failed to convert request: {}", e)))?;
        let response = self.client.get_rel_size(proto_req).await?;
        let proto_resp = response.into_inner();
        Ok(proto_resp.into())
    }

    pub async fn get_slru_segment(
        mut self,
        req: model::GetSlruSegmentRequest,
    ) -> Result<model::GetSlruSegmentResponse, tonic::Status> {

        let proto_req = proto::GetSlruSegmentRequest::try_from(req)
            .map_err(|e| Status::internal(format!("Failed to convert request: {}", e)))?;

        let response = self.client.get_slru_segment(proto_req).await?;
        Ok(response.into_inner().segment as Bytes)
    }
}
