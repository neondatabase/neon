//! Structs representing the JSON formats used in the compute_ctl's HTTP API.

use crate::{privilege::Privilege, spec::ComputeSpec};
use serde::Deserialize;

/// Request of the /configure API
///
/// We now pass only `spec` in the configuration request, but later we can
/// extend it and something like `restart: bool` or something else. So put
/// `spec` into a struct initially to be more flexible in the future.
#[derive(Deserialize, Debug)]
pub struct ConfigurationRequest {
    pub spec: ComputeSpec,
}

#[derive(Deserialize, Debug)]
pub struct SetRoleGrantsRequest {
    pub database: String,
    pub schema: String,
    pub privileges: Vec<Privilege>,
    pub role: String,
}
