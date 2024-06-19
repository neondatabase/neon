use utils::auth::{AuthError, Claims, Scope};

pub fn check_permission(claims: &Claims, required_scope: Scope) -> Result<(), AuthError> {
    if claims.scope != required_scope {
        return Err(AuthError("Scope mismatch. Permission denied".into()));
    }

    Ok(())
}
