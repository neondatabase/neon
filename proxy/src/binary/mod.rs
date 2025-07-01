//! All binaries have the body of their main() defined here, so that the code
//! is also covered by code style configs in lib.rs and the unused-code check is
//! more effective when practically all modules are private to the lib.

pub mod local_proxy;
pub mod pg_sni_router;
pub mod proxy;

use crate::auth::backend::local::JWKS_ROLE_MAP;
use crate::config::ProxyConfig;
use crate::control_plane::messages::{EndpointJwksResponse, JwksSettings};
use crate::ext::TaskExt;
use crate::intern::RoleNameInt;
use crate::types::RoleName;
use anyhow::{Context, ensure};
use camino::{Utf8Path, Utf8PathBuf};
use compute_api::spec::LocalProxySpec;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

#[derive(Error, Debug)]
pub(crate) enum RefreshConfigError {
    #[error(transparent)]
    Read(#[from] std::io::Error),
    #[error(transparent)]
    Parse(#[from] serde_json::Error),
    #[error(transparent)]
    Validate(anyhow::Error),
    #[error(transparent)]
    Tls(anyhow::Error),
}

pub(crate) async fn refresh_config_loop(config: &ProxyConfig, path: Utf8PathBuf, rx: Arc<Notify>) {
    let mut init = true;
    loop {
        rx.notified().await;

        match refresh_config_inner(config, &path).await {
            Ok(()) => {}
            // don't log for file not found errors if this is the first time we are checking
            // for computes that don't use local_proxy, this is not an error.
            Err(RefreshConfigError::Read(e))
                if init && e.kind() == std::io::ErrorKind::NotFound =>
            {
                debug!(error=?e, ?path, "could not read config file");
            }
            Err(RefreshConfigError::Tls(e)) => {
                error!(error=?e, ?path, "could not read TLS certificates");
            }
            Err(e) => {
                error!(error=?e, ?path, "could not read config file");
            }
        }

        init = false;
    }
}

pub(crate) async fn refresh_config_inner(
    config: &ProxyConfig,
    path: &Utf8Path,
) -> Result<(), RefreshConfigError> {
    let bytes = tokio::fs::read(&path).await?;
    let data: LocalProxySpec = serde_json::from_slice(&bytes)?;

    let mut jwks_set = vec![];

    fn parse_jwks_settings(jwks: compute_api::spec::JwksSettings) -> anyhow::Result<JwksSettings> {
        let mut jwks_url = url::Url::from_str(&jwks.jwks_url).context("parsing JWKS url")?;

        ensure!(
            jwks_url.has_authority()
                && (jwks_url.scheme() == "http" || jwks_url.scheme() == "https"),
            "Invalid JWKS url. Must be HTTP",
        );

        ensure!(
            jwks_url.host().is_some_and(|h| h != url::Host::Domain("")),
            "Invalid JWKS url. No domain listed",
        );

        // clear username, password and ports
        jwks_url
            .set_username("")
            .expect("url can be a base and has a valid host and is not a file. should not error");
        jwks_url
            .set_password(None)
            .expect("url can be a base and has a valid host and is not a file. should not error");
        // local testing is hard if we need to have a specific restricted port
        if cfg!(not(feature = "testing")) {
            jwks_url.set_port(None).expect(
                "url can be a base and has a valid host and is not a file. should not error",
            );
        }

        // clear query params
        jwks_url.set_fragment(None);
        jwks_url.query_pairs_mut().clear().finish();

        if jwks_url.scheme() != "https" {
            // local testing is hard if we need to set up https support.
            if cfg!(not(feature = "testing")) {
                jwks_url
                    .set_scheme("https")
                    .expect("should not error to set the scheme to https if it was http");
            } else {
                warn!(scheme = jwks_url.scheme(), "JWKS url is not HTTPS");
            }
        }

        Ok(JwksSettings {
            id: jwks.id,
            jwks_url,
            _provider_name: jwks.provider_name,
            jwt_audience: jwks.jwt_audience,
            role_names: jwks
                .role_names
                .into_iter()
                .map(RoleName::from)
                .map(|s| RoleNameInt::from(&s))
                .collect(),
        })
    }

    for jwks in data.jwks.into_iter().flatten() {
        jwks_set.push(parse_jwks_settings(jwks).map_err(RefreshConfigError::Validate)?);
    }

    info!("successfully loaded new config");
    JWKS_ROLE_MAP.store(Some(Arc::new(EndpointJwksResponse { jwks: jwks_set })));

    if let Some(tls_config) = data.tls {
        let tls_config = tokio::task::spawn_blocking(move || {
            crate::tls::server_config::configure_tls(
                tls_config.key_path.as_ref(),
                tls_config.cert_path.as_ref(),
                None,
                false,
            )
        })
        .await
        .propagate_task_panic()
        .map_err(RefreshConfigError::Tls)?;
        config.tls_config.store(Some(Arc::new(tls_config)));
    }

    Ok(())
}
