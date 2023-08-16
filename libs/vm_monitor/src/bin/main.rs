#[cfg(target_os = "linux")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use clap::Parser;
    use tracing_subscriber::EnvFilter;
    use vm_monitor::Args;

    let subscriber = tracing_subscriber::fmt::Subscriber::builder()
        .json()
        .with_file(true)
        .with_line_number(true)
        .with_span_list(true)
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args: &'static Args = Box::leak(Box::new(Args::parse()));
    vm_monitor::start(args).await
}

#[cfg(not(target_os = "linux"))]
fn main() {
    panic!("the monitor requires cgroups, which are only available on linux")
}
