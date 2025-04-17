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

use http;
use thiserror::Error;
use tonic;
use tonic::metadata::AsciiMetadataValue;

use crate::neon_request::DbSizeRequest;

type Shardno = u16;

mod page_service {
    tonic::include_proto!("page_service");
}

use page_service::page_service_client::PageServiceClient;

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
    tenant_id: String,
    timeline_id: String,

    auth_token: Option<String>,

    shard_map: HashMap<Shardno, String>,
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
            tenant_id: tenant_id.to_string(),
            timeline_id: timeline_id.to_string(),
            auth_token: auth_token.clone(),
            shard_map,
        }
    }

    /// Process a request to get a database size.
    pub async fn process_dbsize_request(
        &self,
        request: &DbSizeRequest,
    ) -> Result<u64, CommunicatorError> {
        // Current sharding model assumes that all metadata is present only at shard 0.
        let shard_no = 0;

        // FIXME: we create a new channel for every request. Inefficient

        let endpoint: tonic::transport::Endpoint = self
            .shard_map
            .get(&shard_no)
            .expect("no url for shard {shard_no}")
            .parse()?;
        let channel = endpoint.connect().await?;
        let mut client = PageServiceClient::with_interceptor(
            channel,
            AuthInterceptor::new(&self.tenant_id, &self.timeline_id, self.auth_token.as_ref()),
        );

        let request = tonic::Request::new(page_service::DbSizeRequest {
            request_lsn: request.request_lsn,
            not_modified_since_lsn: request.not_modified_since,
            db_oid: request.db_oid,
        });

        let response = client.db_size(request).await?;

        println!("RESPONSE={:?}", response);

        // TODO
        Ok(response.get_ref().db_size_bytes)
    }
}

/// Inject tenant_id, timeline_id and authentication token to all pageserver requests.
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
