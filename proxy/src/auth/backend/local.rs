use std::net::SocketAddr;

use arc_swap::ArcSwapOption;

use super::jwt::{AuthRule, FetchAuthRules};
use crate::auth::backend::jwt::FetchAuthRulesError;
use crate::compute::ConnCfg;
use crate::context::RequestMonitoring;
use crate::control_plane::messages::{ColdStartInfo, EndpointJwksResponse, MetricsAuxInfo};
use crate::control_plane::NodeInfo;
use crate::intern::{BranchIdTag, EndpointIdTag, InternId, ProjectIdTag};
use crate::EndpointId;

pub struct LocalBackend {
    pub(crate) node_info: NodeInfo,
}

impl LocalBackend {
    pub fn new(postgres_addr: SocketAddr) -> Self {
        LocalBackend {
            node_info: NodeInfo {
                config: {
                    let mut cfg = ConnCfg::new();
                    cfg.host(&postgres_addr.ip().to_string());
                    cfg.port(postgres_addr.port());
                    cfg
                },
                // TODO(conrad): make this better reflect compute info rather than endpoint info.
                aux: MetricsAuxInfo {
                    endpoint_id: EndpointIdTag::get_interner().get_or_intern("local"),
                    project_id: ProjectIdTag::get_interner().get_or_intern("local"),
                    branch_id: BranchIdTag::get_interner().get_or_intern("local"),
                    cold_start_info: ColdStartInfo::WarmCached,
                },
                allow_self_signed_compute: false,
            },
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct StaticAuthRules;

pub static JWKS_ROLE_MAP: ArcSwapOption<EndpointJwksResponse> = ArcSwapOption::const_empty();

impl FetchAuthRules for StaticAuthRules {
    async fn fetch_auth_rules(
        &self,
        _ctx: &RequestMonitoring,
        _endpoint: EndpointId,
    ) -> Result<Vec<AuthRule>, FetchAuthRulesError> {
        let mappings = JWKS_ROLE_MAP.load();
        let role_mappings = mappings
            .as_deref()
            .ok_or(FetchAuthRulesError::RoleJwksNotConfigured)?;
        let mut rules = vec![];
        for setting in &role_mappings.jwks {
            rules.push(AuthRule {
                id: setting.id.clone(),
                jwks_url: setting.jwks_url.clone(),
                audience: setting.jwt_audience.clone(),
                role_names: setting.role_names.clone(),
            });
        }

        Ok(rules)
    }
}
