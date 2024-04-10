//! Connection request monitoring contexts

use chrono::Utc;
use once_cell::sync::OnceCell;
use smol_str::SmolStr;
use std::net::IpAddr;
use tokio::sync::mpsc;
use tracing::{field::display, info_span, Span};
use uuid::Uuid;

use crate::{
    console::messages::{ColdStartInfo, MetricsAuxInfo},
    error::ErrorKind,
    intern::{BranchIdInt, ProjectIdInt},
    metrics::{
        bool_to_str, LatencyTimer, ENDPOINT_ERRORS_BY_KIND, ERROR_BY_KIND, NUM_INVALID_ENDPOINTS,
    },
    DbName, EndpointId, RoleName,
};

use self::parquet::RequestData;

pub mod parquet;

static LOG_CHAN: OnceCell<mpsc::WeakUnboundedSender<RequestData>> = OnceCell::new();

/// Context data for a single request to connect to a database.
///
/// This data should **not** be used for connection logic, only for observability and limiting purposes.
/// All connection logic should instead use strongly typed state machines, not a bunch of Options.
pub struct RequestMonitoring {
    pub peer_addr: IpAddr,
    pub session_id: Uuid,
    pub protocol: &'static str,
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

    // extra
    // This sender is here to keep the request monitoring channel open while requests are taking place.
    sender: Option<mpsc::UnboundedSender<RequestData>>,
    pub latency_timer: LatencyTimer,
    // Whether proxy decided that it's not a valid endpoint end rejected it before going to cplane.
    rejected: bool,
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
        protocol: &'static str,
        region: &'static str,
    ) -> Self {
        let span = info_span!(
            "connect_request",
            %protocol,
            ?session_id,
            %peer_addr,
            ep = tracing::field::Empty,
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
            rejected: false,
            cold_start_info: ColdStartInfo::Unknown,

            sender: LOG_CHAN.get().and_then(|tx| tx.upgrade()),
            latency_timer: LatencyTimer::new(protocol),
        }
    }

    #[cfg(test)]
    pub fn test() -> Self {
        RequestMonitoring::new(Uuid::now_v7(), [127, 0, 0, 1].into(), "test", "test")
    }

    pub fn console_application_name(&self) -> String {
        format!(
            "{}/{}",
            self.application.as_deref().unwrap_or_default(),
            self.protocol
        )
    }

    pub fn set_rejected(&mut self, rejected: bool) {
        self.rejected = rejected;
    }

    pub fn set_cold_start_info(&mut self, info: ColdStartInfo) {
        self.cold_start_info = info;
        self.latency_timer.cold_start_info(info);
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
            crate::metrics::CONNECTING_ENDPOINTS
                .with_label_values(&[self.protocol])
                .measure(&endpoint_id);
            self.endpoint_id = Some(endpoint_id);
        }
    }

    pub fn set_application(&mut self, app: Option<SmolStr>) {
        self.application = app.or_else(|| self.application.clone());
    }

    pub fn set_dbname(&mut self, dbname: DbName) {
        self.dbname = Some(dbname);
    }

    pub fn set_user(&mut self, user: RoleName) {
        self.user = Some(user);
    }

    pub fn set_auth_method(&mut self, auth_method: AuthMethod) {
        self.auth_method = Some(auth_method);
    }

    pub fn set_error_kind(&mut self, kind: ErrorKind) {
        ERROR_BY_KIND
            .with_label_values(&[kind.to_metric_label()])
            .inc();
        if let Some(ep) = &self.endpoint_id {
            ENDPOINT_ERRORS_BY_KIND
                .with_label_values(&[kind.to_metric_label()])
                .measure(ep);
        }
        self.error_kind = Some(kind);
    }

    pub fn set_success(&mut self) {
        self.success = true;
    }

    pub fn log(self) {}
}

impl Drop for RequestMonitoring {
    fn drop(&mut self) {
        let outcome = if self.success { "success" } else { "failure" };
        NUM_INVALID_ENDPOINTS
            .with_label_values(&[self.protocol, bool_to_str(self.rejected), outcome])
            .inc();
        if let Some(tx) = self.sender.take() {
            let _: Result<(), _> = tx.send(RequestData::from(&*self));
        }
    }
}
