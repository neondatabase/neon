use std::{str::FromStr, time::Duration};

use pageserver_api::controller_api::{SafekeeperDescribeResponse, SkSchedulingPolicy};
use reqwest::StatusCode;
use safekeeper_client::mgmt_api;
use tokio_util::sync::CancellationToken;
use utils::{backoff, id::NodeId, logging::SecretString};

use crate::{
    heartbeater::SafekeeperState,
    persistence::{DatabaseError, SafekeeperPersistence},
    safekeeper_client::SafekeeperClient,
};

#[derive(Clone)]
pub struct Safekeeper {
    pub(crate) skp: SafekeeperPersistence,
    cancel: CancellationToken,
    listen_http_addr: String,
    listen_http_port: u16,
    id: NodeId,
    availability: SafekeeperState,
}

impl Safekeeper {
    pub(crate) fn from_persistence(skp: SafekeeperPersistence, cancel: CancellationToken) -> Self {
        Self {
            cancel,
            listen_http_addr: skp.host.clone(),
            listen_http_port: skp.http_port as u16,
            id: NodeId(skp.id as u64),
            skp,
            availability: SafekeeperState::Offline,
        }
    }
    pub(crate) fn base_url(&self) -> String {
        format!("http://{}:{}", self.listen_http_addr, self.listen_http_port)
    }

    pub(crate) fn get_id(&self) -> NodeId {
        self.id
    }
    pub(crate) fn describe_response(&self) -> Result<SafekeeperDescribeResponse, DatabaseError> {
        self.skp.as_describe_response()
    }
    pub(crate) fn set_availability(&mut self, availability: SafekeeperState) {
        self.availability = availability;
    }
    /// Perform an operation (which is given a [`SafekeeperClient`]) with retries
    pub(crate) async fn with_client_retries<T, O, F>(
        &self,
        mut op: O,
        jwt: &Option<SecretString>,
        warn_threshold: u32,
        max_retries: u32,
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> mgmt_api::Result<T>
    where
        O: FnMut(SafekeeperClient) -> F,
        F: std::future::Future<Output = mgmt_api::Result<T>>,
    {
        fn is_fatal(e: &mgmt_api::Error) -> bool {
            use mgmt_api::Error::*;
            match e {
                ReceiveBody(_) | ReceiveErrorBody(_) => false,
                ApiError(StatusCode::SERVICE_UNAVAILABLE, _)
                | ApiError(StatusCode::GATEWAY_TIMEOUT, _)
                | ApiError(StatusCode::REQUEST_TIMEOUT, _) => false,
                ApiError(_, _) => true,
                Cancelled => true,
            }
        }

        backoff::retry(
            || {
                let http_client = reqwest::ClientBuilder::new()
                    .timeout(timeout)
                    .build()
                    .expect("Failed to construct HTTP client");

                let client = SafekeeperClient::from_client(
                    self.get_id(),
                    http_client,
                    self.base_url(),
                    jwt.clone(),
                );

                let node_cancel_fut = self.cancel.cancelled();

                let op_fut = op(client);

                async {
                    tokio::select! {
                        r = op_fut=> {r},
                        _ = node_cancel_fut => {
                        Err(mgmt_api::Error::Cancelled)
                    }}
                }
            },
            is_fatal,
            warn_threshold,
            max_retries,
            &format!(
                "Call to node {} ({}:{}) management API",
                self.id, self.listen_http_addr, self.listen_http_port
            ),
            cancel,
        )
        .await
        .unwrap_or(Err(mgmt_api::Error::Cancelled))
    }

    pub(crate) fn update_from_record(&mut self, record: crate::persistence::SafekeeperUpsert) {
        let crate::persistence::SafekeeperUpsert {
            active: _,
            availability_zone_id: _,
            host,
            http_port,
            id,
            port: _,
            region_id: _,
            version: _,
        } = record.clone();
        if id != self.id.0 as i64 {
            // The way the function is called ensures this. If we regress on that, it's a bug.
            panic!(
                "id can't be changed via update_from_record function: {id} != {}",
                self.id.0
            );
        }
        self.skp = crate::persistence::SafekeeperPersistence::from_upsert(
            record,
            SkSchedulingPolicy::from_str(&self.skp.scheduling_policy).unwrap(),
        );
        self.listen_http_port = http_port as u16;
        self.listen_http_addr = host;
    }
}
