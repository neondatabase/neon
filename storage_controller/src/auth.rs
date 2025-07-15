use utils::auth::{AuthError, Claims, Scope};
use uuid::Uuid;

pub fn check_permission(claims: &Claims, required_scope: Scope) -> Result<(), AuthError> {
    if claims.scope != required_scope {
        return Err(AuthError("Scope mismatch. Permission denied".into()));
    }

    Ok(())
}

#[allow(dead_code)]
pub fn check_endpoint_permission(claims: &Claims, endpoint_id: Uuid) -> Result<(), AuthError> {
    if claims.scope != Scope::TenantEndpoint {
        return Err(AuthError("Scope mismatch. Permission denied".into()));
    }
    if claims.endpoint_id != Some(endpoint_id) {
        return Err(AuthError("Endpoint id mismatch. Permission denied".into()));
    }
    Ok(())
}
