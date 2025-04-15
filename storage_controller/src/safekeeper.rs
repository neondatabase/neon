use std::time::Duration;

use pageserver_api::controller_api::{SafekeeperDescribeResponse, SkSchedulingPolicy};
use reqwest::StatusCode;
use safekeeper_client::mgmt_api;
use tokio_util::sync::CancellationToken;
use utils::backoff;
use utils::id::NodeId;
use utils::logging::SecretString;

use crate::heartbeater::SafekeeperState;
use crate::persistence::{DatabaseError, SafekeeperPersistence};
use crate::safekeeper_client::SafekeeperClient;

#[derive(Clone)]
pub struct Safekeeper {
    pub(crate) skp: SafekeeperPersistence,
    cancel: CancellationToken,
    listen_http_addr: String,
    listen_http_port: u16,
    listen_https_port: Option<u16>,
    scheduling_policy: SkSchedulingPolicy,
    id: NodeId,
    /// Heartbeating result.
    availability: SafekeeperState,

    // Flag from storcon's config to use https for safekeeper API.
    // Invariant: if |true|, listen_https_port should contain a value.
    use_https: bool,
}

impl Safekeeper {
    pub(crate) fn from_persistence(
        skp: SafekeeperPersistence,
        cancel: CancellationToken,
        use_https: bool,
    ) -> anyhow::Result<Self> {
        if use_https && skp.https_port.is_none() {
            anyhow::bail!(
                "cannot load safekeeper {} from persistence: \
                https is enabled, but https port is not specified",
                skp.id,
            );
        }

        let scheduling_policy = skp.scheduling_policy.0;
        Ok(Self {
            cancel,
            listen_http_addr: skp.host.clone(),
            listen_http_port: skp.http_port as u16,
            listen_https_port: skp.https_port.map(|x| x as u16),
            id: NodeId(skp.id as u64),
            skp,
            availability: SafekeeperState::Offline,
            scheduling_policy,
            use_https,
        })
    }

    pub(crate) fn base_url(&self) -> String {
        if self.use_https {
            format!(
                "https://{}:{}",
                self.listen_http_addr,
                self.listen_https_port
                    .expect("https port should be specified if use_https is on"),
            )
        } else {
            format!("http://{}:{}", self.listen_http_addr, self.listen_http_port)
        }
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
    pub(crate) fn scheduling_policy(&self) -> SkSchedulingPolicy {
        self.scheduling_policy
    }
    pub(crate) fn set_scheduling_policy(&mut self, scheduling_policy: SkSchedulingPolicy) {
        self.scheduling_policy = scheduling_policy;
        self.skp.scheduling_policy = scheduling_policy.into();
    }
    pub(crate) fn availability(&self) -> SafekeeperState {
        self.availability.clone()
    }
    pub(crate) fn has_https_port(&self) -> bool {
        self.listen_https_port.is_some()
    }
    /// Perform an operation (which is given a [`SafekeeperClient`]) with retries
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn with_client_retries<T, O, F>(
        &self,
        mut op: O,
        http_client: &reqwest::Client,
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
                Timeout(_) => false,
            }
        }

        backoff::retry(
            || {
                let client = SafekeeperClient::new(
                    self.get_id(),
                    http_client.clone(),
                    self.base_url(),
                    jwt.clone(),
                );

                let node_cancel_fut = self.cancel.cancelled();

                let op_fut = tokio::time::timeout(timeout, op(client));

                async {
                    tokio::select! {
                        r = op_fut => match r {
                            Ok(r) => r,
                            Err(e) => Err(mgmt_api::Error::Timeout(format!("{e}"))),
                        },
                        _ = node_cancel_fut => {
                        Err(mgmt_api::Error::Cancelled)
                    }}
                }
            },
            is_fatal,
            warn_threshold,
            max_retries,
            &format!(
                "Call to safekeeper {} ({}) management API",
                self.id,
                self.base_url(),
            ),
            cancel,
        )
        .await
        .unwrap_or(Err(mgmt_api::Error::Cancelled))
    }

    pub(crate) fn update_from_record(
        &mut self,
        record: crate::persistence::SafekeeperUpsert,
    ) -> anyhow::Result<()> {
        let crate::persistence::SafekeeperUpsert {
            active: _,
            availability_zone_id: _,
            host,
            http_port,
            https_port,
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
        if self.use_https && https_port.is_none() {
            anyhow::bail!(
                "cannot update safekeeper {id}: \
                https is enabled, but https port is not specified"
            );
        }
        self.skp =
            crate::persistence::SafekeeperPersistence::from_upsert(record, self.scheduling_policy);
        self.listen_http_port = http_port as u16;
        self.listen_https_port = https_port.map(|x| x as u16);
        self.listen_http_addr = host;
        Ok(())
    }
}
