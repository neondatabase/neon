//! Structs representing the JSON formats used in the compute_ctl's HTTP API.
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::privilege::Privilege;
use crate::responses::ComputeCtlConfig;
use crate::spec::{ComputeSpec, ExtVersion, PgIdent};

/// The value to place in the [`ComputeClaims::audience`] claim.
pub static COMPUTE_AUDIENCE: &str = "compute";

/// Available scopes for a compute's JWT.
#[derive(Copy, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ComputeClaimsScope {
    /// An admin-scoped token allows access to all of `compute_ctl`'s authorized
    /// facilities.
    Admin,
}

impl FromStr for ComputeClaimsScope {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "admin" => Ok(ComputeClaimsScope::Admin),
            _ => Err(anyhow::anyhow!("invalid compute claims scope \"{s}\"")),
        }
    }
}

/// When making requests to the `compute_ctl` external HTTP server, the client
/// must specify a set of claims in `Authorization` header JWTs such that
/// `compute_ctl` can authorize the request.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename = "snake_case")]
pub struct ComputeClaims {
    /// The compute ID that will validate the token. The only case in which this
    /// can be [`None`] is if [`Self::scope`] is
    /// [`ComputeClaimsScope::Admin`].
    pub compute_id: Option<String>,

    /// The scope of what the token authorizes.
    pub scope: Option<ComputeClaimsScope>,

    /// The recipient the token is intended for.
    ///
    /// See [RFC 7519](https://www.rfc-editor.org/rfc/rfc7519#section-4.1.3) for
    /// more information.
    ///
    /// TODO: Remove the [`Option`] wrapper when control plane learns to send
    /// the claim.
    #[serde(rename = "aud")]
    pub audience: Option<Vec<String>>,
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
