//! Structs representing the JSON formats used in the compute_ctl's HTTP API.
use serde::{Deserialize, Serialize};

use crate::privilege::Privilege;
use crate::responses::ComputeCtlConfig;
use crate::spec::{ComputeSpec, ExtVersion, PgIdent};

/// When making requests to the `compute_ctl` external HTTP server, the client
/// must specify a set of claims in `Authorization` header JWTs such that
/// `compute_ctl` can authorize the request.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ComputeClaims {
    pub compute_id: String,
}

/// Request of the /configure API
///
/// We now pass only `spec` in the configuration request, but later we can
/// extend it and something like `restart: bool` or something else. So put
/// `spec` into a struct initially to be more flexible in the future.
#[derive(Debug, Deserialize, Serialize)]
pub struct ConfigurationRequest {
    pub spec: ComputeSpec,
    pub compute_ctl_config: ComputeCtlConfig,
}

#[derive(Deserialize, Debug)]
pub struct ExtensionInstallRequest {
    pub extension: PgIdent,
    pub database: PgIdent,
    pub version: ExtVersion,
}

#[derive(Deserialize, Debug)]
pub struct SetRoleGrantsRequest {
    pub database: PgIdent,
    pub schema: PgIdent,
    pub privileges: Vec<Privilege>,
    pub role: PgIdent,
}
