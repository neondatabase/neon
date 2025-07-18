//! Various ID types used by proxy.

use type_safe_id::{StaticType, TypeSafeId};

/// The ID used for the client connection
pub type ClientConnId = TypeSafeId<ClientConn>;

#[derive(Copy, Clone, Default, Hash, PartialEq, Eq)]
pub struct ClientConn;

impl StaticType for ClientConn {
    // This is visible by customers, so we use 'neon' here instead of 'client'.
    const TYPE: &'static str = "neon_conn";
}

/// The ID used for the compute connection
pub type ComputeConnId = TypeSafeId<ComputeConn>;

#[derive(Copy, Clone, Default, Hash, PartialEq, Eq)]
pub struct ComputeConn;

impl StaticType for ComputeConn {
    const TYPE: &'static str = "compute_conn";
}

/// The ID used for the request to authenticate
pub type RequestId = TypeSafeId<Request>;
#[derive(Copy, Clone, Default, Hash, PartialEq, Eq)]
pub struct Request;

impl StaticType for Request {
    const TYPE: &'static str = "request";
}
