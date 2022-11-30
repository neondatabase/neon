use sentry::ClientInitGuard;

pub fn init_sentry(
    maybe_sentry_url: Option<&String>,
    process_name: &str,
) -> Option<ClientInitGuard> {
    return match maybe_sentry_url {
        Some(sentry_url) => {
            let _guard = sentry::init((
                sentry_url.to_string(),
                sentry::ClientOptions {
                    release: sentry::release_name!(),
                    ..Default::default()
                },
            ));
            sentry::configure_scope(|scope| {
                scope.set_tag("process", process_name);
            });
            Some(_guard)
        }
        None => None,
    };
}
