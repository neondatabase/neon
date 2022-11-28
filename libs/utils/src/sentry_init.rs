pub fn init_sentry(sentry_url: &str, process_name: &str) {
    let _guard = sentry::init((
        sentry_url,
        sentry::ClientOptions {
            release: sentry::release_name!(),
            ..Default::default()
        },
    ));

    sentry::configure_scope(|scope| {
        scope.set_tag("process", process_name);
    });
}
