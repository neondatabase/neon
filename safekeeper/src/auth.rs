use anyhow::{bail, Result};
use utils::auth::{Claims, Scope};
use utils::id::TenantId;

pub fn check_permission(claims: &Claims, tenant_id: Option<TenantId>) -> Result<()> {
    match (&claims.scope, tenant_id) {
        (Scope::Tenant, None) => {
            bail!("Attempt to access management api with tenant scope. Permission denied")
        }
        (Scope::Tenant, Some(tenant_id)) => {
            if claims.tenant_id.unwrap() != tenant_id {
                bail!("Tenant id mismatch. Permission denied")
            }
            Ok(())
        }
        (Scope::PageServerApi, _) => bail!("PageServerApi scope makes no sense for Safekeeper"),
        (Scope::SafekeeperData, _) => Ok(()),
    }
}
