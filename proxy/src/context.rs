//! Connection request monitoring contexts

use chrono::Utc;
use once_cell::sync::OnceCell;
use pq_proto::StartupMessageParams;
use smol_str::SmolStr;
use std::net::IpAddr;
use tokio::sync::mpsc;
use tracing::{field::display, info, info_span, Span};
use uuid::Uuid;

use crate::{
    console::messages::{ColdStartInfo, MetricsAuxInfo},
    error::ErrorKind,
    intern::{BranchIdInt, ProjectIdInt},
    metrics::{ConnectOutcome, InvalidEndpointsGroup, LatencyTimer, Metrics, Protocol},
    DbName, EndpointId, RoleName,
};

use self::parquet::RequestData;

pub mod parquet;

pub static LOG_CHAN: OnceCell<mpsc::WeakUnboundedSender<RequestData>> = OnceCell::new();
pub static LOG_CHAN_DISCONNECT: OnceCell<mpsc::WeakUnboundedSender<RequestData>> = OnceCell::new();

/// Context data for a single request to connect to a database.
///
/// This data should **not** be used for connection logic, only for observability and limiting purposes.
/// All connection logic should instead use strongly typed state machines, not a bunch of Options.
pub struct RequestMonitoring {
    pub peer_addr: IpAddr,
    pub session_id: Uuid,
    pub protocol: Protocol,
    first_packet: chrono::DateTime<Utc>,
    region: &'static str,
    pub span: Span,

    // filled in as they are discovered
    project: Option<ProjectIdInt>,
    branch: Option<BranchIdInt>,
    endpoint_id: Option<EndpointId>,
    dbname: Option<DbName>,
    user: Option<RoleName>,
    application: Option<SmolStr>,
    error_kind: Option<ErrorKind>,
    pub(crate) auth_method: Option<AuthMethod>,
    success: bool,
    pub(crate) cold_start_info: ColdStartInfo,
    pg_options: Option<StartupMessageParams>,

    // extra
    // This sender is here to keep the request monitoring channel open while requests are taking place.
    sender: Option<mpsc::UnboundedSender<RequestData>>,
    // This sender is only used to log the length of session in case of success.
    disconnect_sender: Option<mpsc::UnboundedSender<RequestData>>,
    pub latency_timer: LatencyTimer,
    // Whether proxy decided that it's not a valid endpoint end rejected it before going to cplane.
    rejected: Option<bool>,
    disconnect_timestamp: Option<chrono::DateTime<Utc>>,
}

#[derive(Clone, Debug)]
pub enum AuthMethod {
    // aka link aka passwordless
    Web,
    ScramSha256,
    ScramSha256Plus,
    Cleartext,
}

impl RequestMonitoring {
    pub fn new(
        session_id: Uuid,
        peer_addr: IpAddr,
        protocol: Protocol,
        region: &'static str,
    ) -> Self {
        let span = info_span!(
            "connect_request",
            %protocol,
            ?session_id,
            %peer_addr,
            ep = tracing::field::Empty,
            role = tracing::field::Empty,
        );

        Self {
            peer_addr,
            session_id,
            protocol,
            first_packet: Utc::now(),
            region,
            span,

            project: None,
            branch: None,
            endpoint_id: None,
            dbname: None,
            user: None,
            application: None,
            error_kind: None,
            auth_method: None,
            success: false,
            rejected: None,
            cold_start_info: ColdStartInfo::Unknown,
            pg_options: None,

            sender: LOG_CHAN.get().and_then(|tx| tx.upgrade()),
            disconnect_sender: LOG_CHAN_DISCONNECT.get().and_then(|tx| tx.upgrade()),
            latency_timer: LatencyTimer::new(protocol),
            disconnect_timestamp: None,
        }
    }

    #[cfg(test)]
    pub fn test() -> Self {
        RequestMonitoring::new(Uuid::now_v7(), [127, 0, 0, 1].into(), Protocol::Tcp, "test")
    }

    pub fn console_application_name(&self) -> String {
        format!(
            "{}/{}",
            self.application.as_deref().unwrap_or_default(),
            self.protocol
        )
    }

    pub fn set_rejected(&mut self, rejected: bool) {
        self.rejected = Some(rejected);
    }

    pub fn set_cold_start_info(&mut self, info: ColdStartInfo) {
        self.cold_start_info = info;
        self.latency_timer.cold_start_info(info);
    }

    pub fn set_db_options(&mut self, options: StartupMessageParams) {
        self.set_application(options.get("application_name").map(SmolStr::from));
        if let Some(user) = options.get("user") {
            self.set_user(user.into());
        }
        if let Some(dbname) = options.get("database") {
            self.set_dbname(dbname.into());
        }

        self.pg_options = Some(options);
    }

    pub fn set_project(&mut self, x: MetricsAuxInfo) {
        if self.endpoint_id.is_none() {
            self.set_endpoint_id(x.endpoint_id.as_str().into())
        }
        self.branch = Some(x.branch_id);
        self.project = Some(x.project_id);
        self.set_cold_start_info(x.cold_start_info);
    }

    pub fn set_project_id(&mut self, project_id: ProjectIdInt) {
        self.project = Some(project_id);
    }

    pub fn set_endpoint_id(&mut self, endpoint_id: EndpointId) {
        if self.endpoint_id.is_none() {
            self.span.record("ep", display(&endpoint_id));
            let metric = &Metrics::get().proxy.connecting_endpoints;
            let label = metric.with_labels(self.protocol);
            metric.get_metric(label).measure(&endpoint_id);
            self.endpoint_id = Some(endpoint_id);
        }
    }

    fn set_application(&mut self, app: Option<SmolStr>) {
        if let Some(app) = app {
            self.application = Some(app);
        }
    }

    pub fn set_dbname(&mut self, dbname: DbName) {
        self.dbname = Some(dbname);
    }

    pub fn set_user(&mut self, user: RoleName) {
        self.span.record("role", display(&user));
        self.user = Some(user);
    }

    pub fn set_auth_method(&mut self, auth_method: AuthMethod) {
        self.auth_method = Some(auth_method);
    }

    pub fn has_private_peer_addr(&self) -> bool {
        match self.peer_addr {
            IpAddr::V4(ip) => ip.is_private(),
            _ => false,
        }
    }

    pub fn set_error_kind(&mut self, kind: ErrorKind) {
        // Do not record errors from the private address to metrics.
        if !self.has_private_peer_addr() {
            Metrics::get().proxy.errors_total.inc(kind);
        }
        if let Some(ep) = &self.endpoint_id {
            let metric = &Metrics::get().proxy.endpoints_affected_by_errors;
            let label = metric.with_labels(kind);
            metric.get_metric(label).measure(ep);
        }
        self.error_kind = Some(kind);
    }

    pub fn set_success(&mut self) {
        self.success = true;
    }

    pub fn log_connect(&mut self) {
        let outcome = if self.success {
            ConnectOutcome::Success
        } else {
            ConnectOutcome::Failed
        };
        if let Some(rejected) = self.rejected {
            let ep = self
                .endpoint_id
                .as_ref()
                .map(|x| x.as_str())
                .unwrap_or_default();
            // This makes sense only if cache is disabled
            info!(
                ?outcome,
                ?rejected,
                ?ep,
                "check endpoint is valid with outcome"
            );
            Metrics::get()
                .proxy
                .invalid_endpoints_total
                .inc(InvalidEndpointsGroup {
                    protocol: self.protocol,
                    rejected: rejected.into(),
                    outcome,
                });
        }
        if let Some(tx) = self.sender.take() {
            let _: Result<(), _> = tx.send(RequestData::from(&*self));
        }
    }

    fn log_disconnect(&mut self) {
        // If we are here, it's guaranteed that the user successfully connected to the endpoint.
        // Here we log the length of the session.
        self.disconnect_timestamp = Some(Utc::now());
        if let Some(tx) = self.disconnect_sender.take() {
            let _: Result<(), _> = tx.send(RequestData::from(&*self));
        }
    }
}

impl Drop for RequestMonitoring {
    fn drop(&mut self) {
        if self.sender.is_some() {
            self.log_connect();
        } else {
            self.log_disconnect();
        }
    }
}
