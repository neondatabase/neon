//! Most async functions throughout the pageserver take a `ctx: &RequestContext`
//! argument. Currently, it's just a placeholder, but in upcoming commit, it
//! will be used for cancellation, and to ensure that a Tenant or Timeline isn't
//! removed while there are still tasks operating on it.

pub struct RequestContext {}

impl RequestContext {
    pub fn new() -> Self {
        RequestContext {}
    }
}
