use std::{collections::HashMap, net::SocketAddr};

use anyhow::Context;
use arc_swap::ArcSwapOption;

use crate::{
    compute::ConnCfg,
    console::{
        messages::{ColdStartInfo, EndpointJwksResponse, MetricsAuxInfo},
        NodeInfo,
    },
    context::RequestMonitoring,
    intern::{BranchIdInt, BranchIdTag, EndpointIdTag, InternId, ProjectIdInt, ProjectIdTag},
    EndpointId, RoleName,
};

use super::jwt::{AuthRule, FetchAuthRules, JwkCache};

pub struct LocalBackend {
    pub(crate) jwks_cache: JwkCache,
    pub(crate) node_info: NodeInfo,
}

impl LocalBackend {
    pub fn new(postgres_addr: SocketAddr) -> Self {
        LocalBackend {
            jwks_cache: JwkCache::default(),
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

pub static JWKS_ROLE_MAP: ArcSwapOption<JwksRoleSettings> = ArcSwapOption::const_empty();

#[derive(Debug, Clone)]
pub struct JwksRoleSettings {
    pub roles: HashMap<RoleName, EndpointJwksResponse>,
    pub project_id: ProjectIdInt,
    pub branch_id: BranchIdInt,
}

impl FetchAuthRules for StaticAuthRules {
    async fn fetch_auth_rules(
        &self,
        _ctx: &RequestMonitoring,
        _endpoint: EndpointId,
        role_name: RoleName,
    ) -> anyhow::Result<Vec<AuthRule>> {
        let mappings = JWKS_ROLE_MAP.load();
        let role_mappings = mappings
            .as_deref()
            .and_then(|m| m.roles.get(&role_name))
            .context("JWKs settings for this role were not configured")?;
        let mut rules = vec![];
        for setting in &role_mappings.jwks {
            rules.push(AuthRule {
                id: setting.id.clone(),
                jwks_url: setting.jwks_url.clone(),
                audience: setting.jwt_audience.clone(),
            });
        }

        Ok(rules)
    }
}
