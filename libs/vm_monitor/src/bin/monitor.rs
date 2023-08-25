// We expose a standalone binary _and_ start the monitor in `compute_ctl` so that
// we can test the monitor as part of the entire autoscaling system in
// neondatabase/autoscaling.
//
// The monitor was previously started by vm-builder, and for testing purposes,
// we can mimic that setup with this binary.

#[cfg(target_os = "linux")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use clap::Parser;
    use tokio_util::sync::CancellationToken;
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
    let token = CancellationToken::new();
    vm_monitor::start(args, token).await
}

#[cfg(not(target_os = "linux"))]
fn main() {
    panic!("the monitor requires cgroups, which are only available on linux")
}
