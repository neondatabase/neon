use sentry::ClientInitGuard;
use std::borrow::Cow;
use std::env;

pub use sentry::release_name;

#[must_use]
pub fn init_sentry(
    release_name: Option<Cow<'static, str>>,
    extra_options: &[(&str, &str)],
) -> Option<ClientInitGuard> {
    let dsn = env::var("SENTRY_DSN").ok()?;

    let guard = sentry::init((
        dsn,
        sentry::ClientOptions {
            release: release_name,
            ..Default::default()
        },
    ));
    sentry::configure_scope(|scope| {
        for &(key, value) in extra_options {
            scope.set_extra(key, value.into());
        }
    });
    Some(guard)
}
