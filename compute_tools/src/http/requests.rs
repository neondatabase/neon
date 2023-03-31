use serde::Deserialize;

use crate::spec::ComputeSpec;

/// We now pass only `spec` in the configuration request, but later we can
/// extend it and something like `restart: bool` or something else. So put
/// `spec` into a struct initially to be more flexible in the future.
#[derive(Deserialize, Debug)]
pub struct ConfigurationRequest {
    pub spec: ComputeSpec,
}
