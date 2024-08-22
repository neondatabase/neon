use std::{collections::HashMap, net::SocketAddr};

use anyhow::{bail, Context};
use arc_swap::ArcSwapOption;

use crate::{
    console::messages::EndpointJwksResponse,
    intern::{BranchIdInt, ProjectIdInt},
    RoleName,
};

use super::jwt::{AuthRule, FetchAuthRules, JwkCache};

pub struct LocalBackend {
    pub jwks_cache: JwkCache,
    pub postgres: SocketAddr,
}

#[derive(Clone, Copy)]
pub struct StaticAuthRules;

pub static JWKS_ROLE_MAP: ArcSwapOption<JwksRoleSettings> = ArcSwapOption::const_empty();

#[derive(Debug, Clone)]
pub struct JwksRoleSettings {
    pub roles: HashMap<RoleName, EndpointJwksResponse>,
    pub project_id: ProjectIdInt,
    pub branch_id: BranchIdInt,
}

impl FetchAuthRules for StaticAuthRules {
    async fn fetch_auth_rules(&self, role_name: RoleName) -> anyhow::Result<Vec<AuthRule>> {
        let mappings = JWKS_ROLE_MAP.load();
        let Some(mappings) = &*mappings else {
            bail!("missing")
        };
        let role_mappings = mappings.roles.get(&role_name).context("missing")?;
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
