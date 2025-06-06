use std::convert::TryInto;
use std::io::Error;
use std::io::ErrorKind;

use bytes::Bytes;
use futures::StreamExt;
use futures_core::Stream;
use tonic::metadata::AsciiMetadataValue;
use tonic::metadata::errors::InvalidMetadataValue;
use tonic::transport::Channel;
use tonic::{Request, Streaming};

use utils::id::TenantId;
use utils::id::TimelineId;
use utils::shard::ShardIndex;

use anyhow::Result;

use crate::model;
use crate::proto;
#[derive(Clone)]
struct AuthInterceptor {
    tenant_id: AsciiMetadataValue,
    shard_id: Option<AsciiMetadataValue>,
    timeline_id: AsciiMetadataValue,
    auth_header: Option<AsciiMetadataValue>, // including "Bearer " prefix
}

impl AuthInterceptor {
    fn new(
        tenant_id: TenantId,
        timeline_id: TimelineId,
        auth_token: Option<String>,
        shard_id: ShardIndex,
    ) -> Result<Self, InvalidMetadataValue> {
        let tenant_ascii: AsciiMetadataValue = tenant_id.to_string().try_into()?;
        let timeline_ascii: AsciiMetadataValue = timeline_id.to_string().try_into()?;
        let shard_ascii: AsciiMetadataValue = shard_id.clone().to_string().try_into()?;

        let auth_header: Option<AsciiMetadataValue> = match auth_token {
            Some(token) => Some(format!("Bearer {token}").try_into()?),
            None => None,
        };

        Ok(Self {
            tenant_id: tenant_ascii,
            shard_id: Some(shard_ascii),
            timeline_id: timeline_ascii,
            auth_header,
        })
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
    client: proto::PageServiceClient<
        tonic::service::interceptor::InterceptedService<Channel, AuthInterceptor>,
    >,
}

impl Client {
    pub async fn new<T: TryInto<tonic::transport::Endpoint> + Send + Sync + 'static>(
        into_endpoint: T,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        shard_id: ShardIndex,
        auth_header: Option<String>,
    ) -> anyhow::Result<Self> {
        let endpoint: tonic::transport::Endpoint = into_endpoint.try_into().map_err(|_e| {
            Error::new(ErrorKind::InvalidInput, "Unable to convert into endpoint.")
        })?;
        let channel = endpoint.connect().await?;
        let auth = AuthInterceptor::new(tenant_id, timeline_id, auth_header, shard_id)
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e.to_string()))?;
        let client = proto::PageServiceClient::with_interceptor(channel, auth);

        Ok(Self { client })
    }

    // Returns whether a relation exists.
    pub async fn check_rel_exists(
        &mut self,
        req: model::CheckRelExistsRequest,
    ) -> Result<model::CheckRelExistsResponse, tonic::Status> {
        let proto_req = proto::CheckRelExistsRequest::from(req);

        let response = self.client.check_rel_exists(proto_req).await?;

        let proto_resp = response.into_inner();
        Ok(proto_resp.into())
    }

    // Fetches a base backup.
    pub async fn get_base_backup(
        mut self,
        req: model::GetBaseBackupRequest,
    ) -> Result<impl Stream<Item = Result<Bytes, tonic::Status>>, tonic::Status> {
        let proto_req = proto::GetBaseBackupRequest::from(req);

        let response_stream: Streaming<proto::GetBaseBackupResponseChunk> =
            self.client.get_base_backup(proto_req).await?.into_inner();

        // TODO: Consider dechunking internally
        let domain_stream = response_stream.map(|chunk_res| {
            chunk_res.map(|proto_chunk| {
                let b: Bytes = proto_chunk.try_into().unwrap();
                b
            })
        });

        Ok(domain_stream)
    }

    // Returns the total size of a database, as # of bytes.
    pub async fn get_db_size(mut self, req: model::GetDbSizeRequest) -> Result<u64, tonic::Status> {
        let proto_req = proto::GetDbSizeRequest::from(req);

        let response = self.client.get_db_size(proto_req).await?;
        Ok(response.into_inner().into())
    }

    // Fetches pages.
    //
    // This is implemented as a bidirectional streaming RPC for performance.
    pub async fn get_pages<ReqSt>(
        &mut self,
        inbound: ReqSt,
    ) -> Result<
        impl Stream<Item = Result<model::GetPageResponse, tonic::Status>> + Send + 'static,
        tonic::Status,
    >
    where
        ReqSt: Stream<Item = model::GetPageRequest> + Send + 'static,
    {
        let outbound_proto = inbound.map(|domain_req| domain_req.into());

        let req_new = Request::new(outbound_proto);

        let response_stream: Streaming<proto::GetPageResponse> =
            self.client.get_pages(req_new).await?.into_inner();

        let domain_stream = response_stream.map(|proto| match proto {
            Ok(proto) => Ok(model::GetPageResponse::from(proto)),
            Err(status) => Err(status),
        });

        Ok(domain_stream)
    }

    // Returns the size of a relation, as # of blocks.
    pub async fn get_rel_size(
        mut self,
        req: model::GetRelSizeRequest,
    ) -> Result<model::GetRelSizeResponse, tonic::Status> {
        let proto_req = proto::GetRelSizeRequest::from(req);
        let response = self.client.get_rel_size(proto_req).await?;
        let proto_resp = response.into_inner();
        Ok(proto_resp.into())
    }

    // Fetches an SLRU segment.
    pub async fn get_slru_segment(
        mut self,
        req: model::GetSlruSegmentRequest,
    ) -> Result<model::GetSlruSegmentResponse, tonic::Status> {
        let proto_req = proto::GetSlruSegmentRequest::from(req);
        let response = self.client.get_slru_segment(proto_req).await?;
        Ok(response.into_inner().segment as Bytes)
    }
}
