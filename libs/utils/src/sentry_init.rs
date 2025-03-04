use std::borrow::Cow;
use std::env;

use sentry::ClientInitGuard;
pub use sentry::release_name;
use tracing::{error, info};

#[must_use]
pub fn init_sentry(
    release_name: Option<Cow<'static, str>>,
    extra_options: &[(&str, &str)],
) -> Option<ClientInitGuard> {
    let Ok(dsn) = env::var("SENTRY_DSN") else {
        info!("not initializing Sentry, no SENTRY_DSN given");
        return None;
    };
    let environment = env::var("SENTRY_ENVIRONMENT").unwrap_or_else(|_| "development".into());

    let guard = sentry::init((
        dsn,
        sentry::ClientOptions {
            release: release_name.clone(),
            environment: Some(environment.clone().into()),
            ..Default::default()
        },
    ));
    sentry::configure_scope(|scope| {
        for &(key, value) in extra_options {
            scope.set_extra(key, value.into());
        }
    });

    if let Some(dsn) = guard.dsn() {
        info!(
            "initialized Sentry for project {} environment {} release {} (using API {})",
            dsn.project_id(),
            environment,
            release_name.unwrap_or(Cow::Borrowed("None")),
            dsn.envelope_api_url(),
        );
    } else {
        // This should panic during sentry::init(), but we may as well cover it.
        error!("failed to initialize Sentry, invalid DSN");
    }

    Some(guard)
}
