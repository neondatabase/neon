use anyhow::Context as _;
use futures::{Stream, StreamExt as _, TryStreamExt as _};
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;
use tonic::codec::CompressionEncoding;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};

use utils::id::{TenantId, TimelineId};
use utils::shard::ShardIndex;

use crate::model::*;
use crate::proto;

/// A basic Pageserver gRPC client, for a single tenant shard. This API uses native Rust domain
/// types from `model` rather than generated Protobuf types.
pub struct Client {
    inner: proto::PageServiceClient<InterceptedService<Channel, AuthInterceptor>>,
}

impl Client {
    /// Connects to the given gRPC endpoint.
    pub async fn connect<E>(
        endpoint: E,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        shard_id: ShardIndex,
        auth_token: Option<String>,
        compression: Option<CompressionEncoding>,
    ) -> anyhow::Result<Self>
    where
        E: TryInto<Endpoint> + Send + Sync + 'static,
        <E as TryInto<Endpoint>>::Error: std::error::Error + Send + Sync,
    {
        let endpoint: Endpoint = endpoint.try_into().context("invalid endpoint")?;
        let channel = endpoint.connect().await?;
        Self::new(
            channel,
            tenant_id,
            timeline_id,
            shard_id,
            auth_token,
            compression,
        )
    }

    /// Creates a new client using the given gRPC channel.
    pub fn new(
        channel: Channel,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        shard_id: ShardIndex,
        auth_token: Option<String>,
        compression: Option<CompressionEncoding>,
    ) -> anyhow::Result<Self> {
        let auth = AuthInterceptor::new(tenant_id, timeline_id, shard_id, auth_token)?;
        let mut inner = proto::PageServiceClient::with_interceptor(channel, auth);

        if let Some(compression) = compression {
            // TODO: benchmark this (including network latency).
            inner = inner
                .accept_compressed(compression)
                .send_compressed(compression);
        }

        Ok(Self { inner })
    }

    /// Returns whether a relation exists.
    pub async fn check_rel_exists(
        &mut self,
        req: CheckRelExistsRequest,
    ) -> tonic::Result<CheckRelExistsResponse> {
        let req = proto::CheckRelExistsRequest::from(req);
        let resp = self.inner.check_rel_exists(req).await?.into_inner();
        Ok(resp.into())
    }

    /// Fetches a base backup.
    pub async fn get_base_backup(
        &mut self,
        req: GetBaseBackupRequest,
    ) -> tonic::Result<impl AsyncRead + use<>> {
        let req = proto::GetBaseBackupRequest::from(req);
        let chunks = self.inner.get_base_backup(req).await?.into_inner();
        Ok(StreamReader::new(
            chunks
                .map_ok(|resp| resp.chunk)
                .map_err(std::io::Error::other),
        ))
    }

    /// Returns the total size of a database, as # of bytes.
    pub async fn get_db_size(&mut self, req: GetDbSizeRequest) -> tonic::Result<GetDbSizeResponse> {
        let req = proto::GetDbSizeRequest::from(req);
        let resp = self.inner.get_db_size(req).await?.into_inner();
        Ok(resp.into())
    }

    /// Fetches pages.
    ///
    /// This is implemented as a bidirectional streaming RPC for performance. Per-request errors are
    /// typically returned as status_code instead of errors, to avoid tearing down the entire stream
    /// via a tonic::Status error.
    pub async fn get_pages(
        &mut self,
        reqs: impl Stream<Item = GetPageRequest> + Send + 'static,
    ) -> tonic::Result<impl Stream<Item = tonic::Result<GetPageResponse>> + Send + 'static> {
        let reqs = reqs.map(proto::GetPageRequest::from);
        let resps = self.inner.get_pages(reqs).await?.into_inner();
        Ok(resps.map_ok(GetPageResponse::from))
    }

    /// Returns the size of a relation, as # of blocks.
    pub async fn get_rel_size(
        &mut self,
        req: GetRelSizeRequest,
    ) -> tonic::Result<GetRelSizeResponse> {
        let req = proto::GetRelSizeRequest::from(req);
        let resp = self.inner.get_rel_size(req).await?.into_inner();
        Ok(resp.into())
    }

    /// Fetches an SLRU segment.
    pub async fn get_slru_segment(
        &mut self,
        req: GetSlruSegmentRequest,
    ) -> tonic::Result<GetSlruSegmentResponse> {
        let req = proto::GetSlruSegmentRequest::from(req);
        let resp = self.inner.get_slru_segment(req).await?.into_inner();
        Ok(resp.try_into()?)
    }

    /// Acquires or extends a lease on the given LSN. This guarantees that the Pageserver won't
    /// garbage collect the LSN until the lease expires. Must be acquired on all relevant shards.
    ///
    /// Returns the lease expiration time, or a FailedPrecondition status if the lease could not be
    /// acquired because the LSN has already been garbage collected.
    pub async fn lease_lsn(&mut self, req: LeaseLsnRequest) -> tonic::Result<LeaseLsnResponse> {
        let req = proto::LeaseLsnRequest::from(req);
        let resp = self.inner.lease_lsn(req).await?.into_inner();
        Ok(resp.try_into()?)
    }
}

/// Adds authentication metadata to gRPC requests.
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
        shard_id: ShardIndex,
        auth_token: Option<String>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            tenant_id: tenant_id.to_string().try_into()?,
            timeline_id: timeline_id.to_string().try_into()?,
            shard_id: shard_id.to_string().try_into()?,
            auth_header: auth_token
                .map(|token| format!("Bearer {token}").try_into())
                .transpose()?,
        })
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> tonic::Result<tonic::Request<()>> {
        let metadata = req.metadata_mut();
        metadata.insert("neon-tenant-id", self.tenant_id.clone());
        metadata.insert("neon-timeline-id", self.timeline_id.clone());
        metadata.insert("neon-shard-id", self.shard_id.clone());
        if let Some(ref auth_header) = self.auth_header {
            metadata.insert("authorization", auth_header.clone());
        }
        Ok(req)
    }
}
