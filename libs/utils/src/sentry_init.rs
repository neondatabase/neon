use sentry::ClientInitGuard;
use std::env;

#[must_use]
pub fn init_sentry(extra_options: &[(&str, &str)]) -> Option<ClientInitGuard> {
    let dsn = env::var("SENTRY_DSN").ok()?;

    let guard = sentry::init((
        dsn,
        sentry::ClientOptions {
            release: sentry::release_name!(),
            ..Default::default()
        },
    ));
    sentry::configure_scope(|scope| {
        for option in extra_options {
            scope.set_extra(option.0, option.1.into());
        }
    });
    Some(guard)
}
