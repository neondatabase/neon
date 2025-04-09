//! Ground rules
//! ------------
//!
//! This module is indendend of any of the C code, and doesn't rely on
//! any PostgreSQL facilities. You are free to use any Rust crates,
//! Tokio, threads, you name it. If you wanted to build another client
//! application, separate from Postgres, you should be able to take
//! this module and embed it in standalone Rust program.
//!
//! The interface to this module is public CommunicatorProcess::process_*()
//! functions. They are async, and may be called from multiple threads.

use std::collections::HashMap;
use std::sync::RwLock;

use http;
use thiserror::Error;
use tonic;
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::Channel;

use crate::neon_request::{DbSizeRequest, GetPageRequest, RelExistsRequest, RelSizeRequest};

type Shardno = u16;

mod page_service {
    tonic::include_proto!("page_service");
}

use page_service::page_service_client::PageServiceClient;

type MyPageServiceClient = PageServiceClient<
    tonic::service::interceptor::InterceptedService<tonic::transport::Channel, AuthInterceptor>,
>;

#[derive(Error, Debug)]
pub enum CommunicatorError {
    #[error("could not connect to service: {0}")]
    ConnectError(#[from] tonic::transport::Error),
    #[error("could not perform request: {0}`")]
    RequestError(#[from] tonic::Status),

    #[error("could not perform request: {0}`")]
    InvalidUri(#[from] http::uri::InvalidUri),
}

pub struct CommunicatorProcessor {
    _tenant_id: String,
    _timeline_id: String,

    _auth_token: Option<String>,

    shard_map: HashMap<Shardno, String>,

    channels: RwLock<HashMap<Shardno, Channel>>,

    auth_interceptor: AuthInterceptor,
}

impl CommunicatorProcessor {
    /// TODO: this doesn't currently react to changes in the shard map.
    pub fn new(
        tenant_id: &str,
        timeline_id: &str,
        auth_token: &Option<String>,
        shard_map: HashMap<Shardno, String>,
    ) -> Self {
        Self {
            _tenant_id: tenant_id.to_string(),
            _timeline_id: timeline_id.to_string(),
            _auth_token: auth_token.clone(),
            shard_map,
            channels: RwLock::new(HashMap::new()),
            auth_interceptor: AuthInterceptor::new(tenant_id, timeline_id, auth_token.as_ref()),
        }
    }

    /// Process a request to get a database size.
    pub async fn process_dbsize_request(
        &self,
        request: &DbSizeRequest,
    ) -> Result<u64, CommunicatorError> {
        // Current sharding model assumes that all metadata is present only at shard 0.
        let shard_no = 0;

        let mut client = self.get_client(shard_no).await?;

        let request = tonic::Request::new(page_service::DbSizeRequest {
            common: Some(page_service::RequestCommon {
                request_lsn: request.request_lsn,
                not_modified_since_lsn: request.not_modified_since,
            }),
            db_oid: request.db_oid,
        });

        let response = client.db_size(request).await?;

        Ok(response.get_ref().num_bytes)
    }

    pub async fn process_rel_exists_request(
        &self,
        request: &RelExistsRequest,
    ) -> Result<bool, CommunicatorError> {
        // Current sharding model assumes that all metadata is present only at shard 0.
        let shard_no = 0;

        let mut client = self.get_client(shard_no).await?;

        let request = tonic::Request::new(page_service::RelExistsRequest {
            common: Some(page_service::RequestCommon {
                request_lsn: request.request_lsn,
                not_modified_since_lsn: request.not_modified_since,
            }),
            rel: Some(page_service::RelTag {
                spc_oid: request.spc_oid,
                db_oid: request.db_oid,
                rel_number: request.rel_number,
                fork_number: request.fork_number as u32,
            }),
        });

        let response = client.rel_exists(request).await?;

        Ok(response.get_ref().exists)
    }

    pub async fn process_rel_size_request(
        &self,
        request: &RelSizeRequest,
    ) -> Result<u32, CommunicatorError> {
        // Current sharding model assumes that all metadata is present only at shard 0.
        let shard_no = 0;

        let mut client = self.get_client(shard_no).await?;

        let request = tonic::Request::new(page_service::RelSizeRequest {
            common: Some(page_service::RequestCommon {
                request_lsn: request.request_lsn,
                not_modified_since_lsn: request.not_modified_since,
            }),
            rel: Some(page_service::RelTag {
                spc_oid: request.spc_oid,
                db_oid: request.db_oid,
                rel_number: request.rel_number,
                fork_number: request.fork_number as u32,
            }),
        });

        let response = client.rel_size(request).await?;

        Ok(response.get_ref().num_blocks)
    }

    pub async fn process_get_page_request(
        &self,
        request: &GetPageRequest,
    ) -> Result<Vec<u8>, CommunicatorError> {
        // FIXME: calculate the shard number correctly
        let shard_no = 0;

        let mut client = self.get_client(shard_no).await?;

        let request = tonic::Request::new(page_service::GetPageRequest {
            common: Some(page_service::RequestCommon {
                request_lsn: request.request_lsn,
                not_modified_since_lsn: request.not_modified_since,
            }),
            rel: Some(page_service::RelTag {
                spc_oid: request.spc_oid,
                db_oid: request.db_oid,
                rel_number: request.rel_number,
                fork_number: request.fork_number as u32,
            }),
            block_number: request.block_number,
        });

        let response = client.get_page(request).await?;

        Ok(response.into_inner().page_image)
    }

    /// Get a client for given shard
    ///
    /// This implements very basic caching. If we already have a client for the given shard,
    /// reuse it. If not, create a new client and put it to the cache.
    async fn get_client(&self, shard_no: u16) -> Result<MyPageServiceClient, CommunicatorError> {
        let reused_channel: Option<Channel> = {
            let channels = self.channels.read().unwrap();

            channels.get(&shard_no).cloned()
        };

        let channel = if let Some(reused_channel) = reused_channel {
            reused_channel
        } else {
            let endpoint: tonic::transport::Endpoint = self
                .shard_map
                .get(&shard_no)
                .expect("no url for shard {shard_no}")
                .parse()?;
            let channel = endpoint.connect().await?;

            // Insert it to the cache so that it can be reused on subsequent calls. It's possible
            // that another thread did the same concurrently, in which case we will overwrite the
            // client in the cache.
            {
                let mut channels = self.channels.write().unwrap();
                channels.insert(shard_no, channel.clone());
            }
            channel
        };

        let client = PageServiceClient::with_interceptor(channel, self.auth_interceptor.clone());
        Ok(client)
    }
}

/// Inject tenant_id, timeline_id and authentication token to all pageserver requests.
#[derive(Clone)]
struct AuthInterceptor {
    tenant_id: AsciiMetadataValue,
    timeline_id: AsciiMetadataValue,

    auth_token: Option<AsciiMetadataValue>,
}

impl AuthInterceptor {
    fn new(tenant_id: &str, timeline_id: &str, auth_token: Option<&String>) -> Self {
        Self {
            tenant_id: tenant_id.parse().expect("could not parse tenant id"),
            timeline_id: timeline_id.parse().expect("could not parse timeline id"),
            auth_token: auth_token.map(|x| x.parse().expect("could not parse auth token")),
        }
    }
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        req.metadata_mut()
            .insert("neon-tenant-id", self.tenant_id.clone());
        req.metadata_mut()
            .insert("neon-timeline-id", self.timeline_id.clone());
        if let Some(auth_token) = &self.auth_token {
            req.metadata_mut()
                .insert("neon-auth-token", auth_token.clone());
        }

        Ok(req)
    }
}
