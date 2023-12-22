//! Connection request contexts

use std::net::IpAddr;

use chrono::Utc;
use serde::{Serialize, Serializer};
use smol_str::SmolStr;
use tracing::info;
use uuid::Uuid;

use crate::{error::ErrorKind, metrics::LatencyTimer};

#[derive(serde::Serialize)]
/// Context data for a single request to connect to a database.
///
/// This data should **not** be used for connection logic, only for observability and limiting purposes.
/// All connection logic should instead use strongly typed state machines, not a bunch of Options.
pub struct RequestContext {
    pub peer_addr: IpAddr,
    pub session_id: Uuid,
    #[serde(skip)]
    pub first_packet: chrono::DateTime<Utc>,
    pub protocol: &'static str,
    pub project: Option<SmolStr>,
    pub branch: Option<SmolStr>,
    pub endpoint_id: Option<SmolStr>,
    pub user: Option<SmolStr>,
    pub application: Option<SmolStr>,
    pub region: &'static str,
    pub error_kind: Option<ErrorKind>,
    #[serde(rename = "request_latency", serialize_with = "latency_timer")]
    pub latency_timer: LatencyTimer,
    #[serde(skip)]
    logged: bool,
}

fn latency_timer<S: Serializer>(timer: &LatencyTimer, s: S) -> Result<S::Ok, S::Error> {
    timer.accumulated.serialize(s)
}

impl RequestContext {
    pub fn new(
        session_id: Uuid,
        peer_addr: IpAddr,
        protocol: &'static str,
        region: &'static str,
    ) -> Self {
        Self {
            peer_addr,
            session_id,
            protocol,
            latency_timer: LatencyTimer::new(protocol),
            first_packet: Utc::now(),
            project: None,
            branch: None,
            endpoint_id: None,
            user: None,
            application: None,
            region,
            error_kind: None,
            logged: false,
        }
    }

    pub fn log(&mut self) {
        if !self.logged {
            self.logged = true;
            info!("{}", serde_json::to_string(self).unwrap());
        }
    }
}
