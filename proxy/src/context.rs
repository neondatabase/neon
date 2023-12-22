//! Connection request contexts

use std::net::IpAddr;

use smol_str::SmolStr;
use tokio::time::Instant;
use uuid::Uuid;

use crate::{error::ErrorKind, metrics::LatencyTimer};

/// Context data for a single request to connect to a database.
///
/// This data should **not** be used for connection logic, only for observability and limiting purposes.
/// All connection logic should instead use strongly typed state machines, not a bunch of Options.
pub struct RequestContext {
    pub peer_addr: IpAddr,
    pub session_id: Uuid,
    pub first_packet: Instant,
    pub protocol: &'static str,
    pub project: Option<SmolStr>,
    pub branch: Option<SmolStr>,
    pub endpoint_id: Option<SmolStr>,
    pub user: Option<SmolStr>,
    pub application: Option<SmolStr>,
    pub cluster: &'static str,
    pub error_kind: Option<ErrorKind>,
    pub latency_timer: LatencyTimer,
}

impl RequestContext {
    pub fn new(
        session_id: Uuid,
        peer_addr: IpAddr,
        protocol: &'static str,
        cluster: &'static str,
    ) -> Self {
        Self {
            peer_addr,
            session_id,
            protocol,
            latency_timer: LatencyTimer::new(protocol),
            first_packet: tokio::time::Instant::now(),
            project: None,
            branch: None,
            endpoint_id: None,
            user: None,
            application: None,
            cluster,
            error_kind: None,
        }
    }
}
