#[cfg(test)]
mod tests;

pub(crate) mod connect_compute;
pub(crate) mod retry;
pub(crate) mod wake_compute;

use itertools::Itertools;
use once_cell::sync::OnceCell;
use regex::Regex;
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, format_smolstr};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::compute;
pub use crate::pglb::copy_bidirectional::{ErrorSource, copy_bidirectional_client_compute};
use crate::pqproto::{BeMessage, CancelKeyData, StartupMessageParams};
use crate::stream::PqStream;
use crate::types::EndpointCacheKey;

/// Finish client connection initialization: confirm auth success, send params, etc.
pub(crate) fn prepare_client_connection(
    node: &compute::PostgresConnection,
    cancel_key_data: CancelKeyData,
    stream: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) {
    // Forward all deferred notices to the client.
    for notice in &node.delayed_notice {
        stream.write_raw(notice.as_bytes().len(), b'N', |buf| {
            buf.extend_from_slice(notice.as_bytes());
        });
    }

    // Forward all postgres connection params to the client.
    for (name, value) in &node.params {
        stream.write_message(BeMessage::ParameterStatus {
            name: name.as_bytes(),
            value: value.as_bytes(),
        });
    }

    stream.write_message(BeMessage::BackendKeyData(cancel_key_data));
    stream.write_message(BeMessage::ReadyForQuery);
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub(crate) struct NeonOptions(Vec<(SmolStr, SmolStr)>);

impl NeonOptions {
    // proxy options:

    /// `PARAMS_COMPAT` allows opting in to forwarding all startup parameters from client to compute.
    pub const PARAMS_COMPAT: &str = "proxy_params_compat";

    // cplane options:

    /// `LSN` allows provisioning an ephemeral compute with time-travel to the provided LSN.
    const LSN: &str = "lsn";

    /// `ENDPOINT_TYPE` allows configuring an ephemeral compute to be read_only or read_write.
    const ENDPOINT_TYPE: &str = "endpoint_type";

    pub(crate) fn parse_params(params: &StartupMessageParams) -> Self {
        params
            .options_raw()
            .map(Self::parse_from_iter)
            .unwrap_or_default()
    }

    pub(crate) fn parse_options_raw(options: &str) -> Self {
        Self::parse_from_iter(StartupMessageParams::parse_options_raw(options))
    }

    pub(crate) fn get(&self, key: &str) -> Option<SmolStr> {
        self.0
            .iter()
            .find_map(|(k, v)| (k == key).then_some(v))
            .cloned()
    }

    pub(crate) fn is_ephemeral(&self) -> bool {
        self.0.iter().any(|(k, _)| match &**k {
            // This is not a cplane option, we know it does not create ephemeral computes.
            Self::PARAMS_COMPAT => false,
            Self::LSN => true,
            Self::ENDPOINT_TYPE => true,
            // err on the side of caution. any cplane options we don't know about
            // might lead to ephemeral computes.
            _ => true,
        })
    }

    fn parse_from_iter<'a>(options: impl Iterator<Item = &'a str>) -> Self {
        let mut options = options
            .filter_map(neon_option)
            .map(|(k, v)| (k.into(), v.into()))
            .collect_vec();
        options.sort();
        Self(options)
    }

    pub(crate) fn get_cache_key(&self, prefix: &str) -> EndpointCacheKey {
        // prefix + format!(" {k}:{v}")
        // kinda jank because SmolStr is immutable
        std::iter::once(prefix)
            .chain(self.0.iter().flat_map(|(k, v)| [" ", &**k, ":", &**v]))
            .collect::<SmolStr>()
            .into()
    }

    /// <https://swagger.io/docs/specification/serialization/> DeepObject format
    /// `paramName[prop1]=value1&paramName[prop2]=value2&...`
    pub(crate) fn to_deep_object(&self) -> Vec<(SmolStr, SmolStr)> {
        self.0
            .iter()
            .map(|(k, v)| (format_smolstr!("options[{}]", k), v.clone()))
            .collect()
    }
}

pub(crate) fn neon_option(bytes: &str) -> Option<(&str, &str)> {
    static RE: OnceCell<Regex> = OnceCell::new();
    let re = RE.get_or_init(|| Regex::new(r"^neon_(\w+):(.+)").expect("regex should be correct"));

    let cap = re.captures(bytes)?;
    let (_, [k, v]) = cap.extract();
    Some((k, v))
}
