use std::convert::TryInto;

use bytes::Bytes;
use futures::TryStreamExt;
use futures::{Stream, StreamExt};
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

///
/// AuthInterceptor adds tenant, timeline, and auth header to the channel. These
/// headers are required at the pageserver.
///
#[derive(Clone)]
struct AuthInterceptor {
    tenant_id: AsciiMetadataValue,
    timeline_id: AsciiMetadataValue,
    shard_id: AsciiMetadataValue,
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
        let shard_ascii: AsciiMetadataValue = shard_id.to_string().try_into()?;

        let auth_header: Option<AsciiMetadataValue> = match auth_token {
            Some(token) => Some(format!("Bearer {token}").try_into()?),
            None => None,
        };

        Ok(Self {
            tenant_id: tenant_ascii,
            shard_id: shard_ascii,
            timeline_id: timeline_ascii,
            auth_header,
        })
    }
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        req.metadata_mut()
            .insert("neon-tenant-id", self.tenant_id.clone());
        req.metadata_mut()
            .insert("neon-shard-id", self.shard_id.clone());
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
        compression: Option<tonic::codec::CompressionEncoding>,
    ) -> anyhow::Result<Self> {
        let endpoint: tonic::transport::Endpoint = into_endpoint
            .try_into()
            .map_err(|_e| anyhow::anyhow!("failed to convert endpoint"))?;
        let channel = endpoint.connect().await?;
        let auth = AuthInterceptor::new(tenant_id, timeline_id, auth_header, shard_id)
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let mut client = proto::PageServiceClient::with_interceptor(channel, auth);

        if let Some(compression) = compression {
            // TODO: benchmark this (including network latency).
            // TODO: consider enabling compression by default.
            client = client
                .accept_compressed(compression)
                .send_compressed(compression);
        }

        Ok(Self { client })
    }

    /// Returns whether a relation exists.
    pub async fn check_rel_exists(
        &mut self,
        req: model::CheckRelExistsRequest,
    ) -> Result<model::CheckRelExistsResponse, tonic::Status> {
        let proto_req = proto::CheckRelExistsRequest::from(req);

        let response = self.client.check_rel_exists(proto_req).await?;

        let proto_resp = response.into_inner();
        Ok(proto_resp.into())
    }

    /// Fetches a base backup.
    pub async fn get_base_backup(
        &mut self,
        req: model::GetBaseBackupRequest,
    ) -> Result<impl Stream<Item = Result<Bytes, tonic::Status>> + 'static, tonic::Status> {
        let proto_req = proto::GetBaseBackupRequest::from(req);

        let response_stream: Streaming<proto::GetBaseBackupResponseChunk> =
            self.client.get_base_backup(proto_req).await?.into_inner();

        // TODO: Consider dechunking internally
        let domain_stream = response_stream.map(|chunk_res| {
            chunk_res.and_then(|proto_chunk| {
                proto_chunk.try_into().map_err(|e| {
                    tonic::Status::internal(format!("Failed to convert response chunk: {e}"))
                })
            })
        });

        Ok(domain_stream)
    }

    /// Returns the total size of a database, as # of bytes.
    pub async fn get_db_size(
        &mut self,
        req: model::GetDbSizeRequest,
    ) -> Result<u64, tonic::Status> {
        let proto_req = proto::GetDbSizeRequest::from(req);

        let response = self.client.get_db_size(proto_req).await?;
        Ok(response.into_inner().into())
    }

    /// Fetches pages.
    ///
    /// This is implemented as a bidirectional streaming RPC for performance.
    /// Per-request errors are often returned as status_code instead of errors,
    /// to avoid tearing down the entire stream via tonic::Status.
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

        let domain_stream = response_stream.map_ok(model::GetPageResponse::from);

        Ok(domain_stream)
    }

    /// Returns the size of a relation, as # of blocks.
    pub async fn get_rel_size(
        &mut self,
        req: model::GetRelSizeRequest,
    ) -> Result<model::GetRelSizeResponse, tonic::Status> {
        let proto_req = proto::GetRelSizeRequest::from(req);
        let response = self.client.get_rel_size(proto_req).await?;
        let proto_resp = response.into_inner();
        Ok(proto_resp.into())
    }

    /// Fetches an SLRU segment.
    pub async fn get_slru_segment(
        &mut self,
        req: model::GetSlruSegmentRequest,
    ) -> Result<model::GetSlruSegmentResponse, tonic::Status> {
        let proto_req = proto::GetSlruSegmentRequest::from(req);
        let response = self.client.get_slru_segment(proto_req).await?;
        Ok(response.into_inner().try_into()?)
    }
}
