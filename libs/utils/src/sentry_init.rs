use sentry::ClientInitGuard;
use std::env;

#[must_use]
pub fn init_sentry(process_name: &str) -> Option<ClientInitGuard> {
    let dsn = env::var("SENTRY_DSN").ok()?;

    let guard = sentry::init((
        dsn,
        sentry::ClientOptions {
            release: sentry::release_name!(),
            ..Default::default()
        },
    ));
    sentry::configure_scope(|scope| {
        scope.set_tag("process", process_name);
    });
    Some(guard)
}
