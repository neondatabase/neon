use camino::Utf8PathBuf;
use clap::Parser;
use pageserver::{
    pg_import,
    virtual_file::{self, api::IoEngineKind},
};
use utils::id::{TenantId, TimelineId};
use utils::logging::{self, LogFormat, TracingErrorLayerEnablement};

use std::str::FromStr;

//project_git_version!(GIT_VERSION);

#[derive(Parser)]
#[command(
    //version = GIT_VERSION,
    about = "Utility to import a Postgres data directory directly into image layers",
    //long_about = "..."
)]
struct CliOpts {
    /// Input Postgres data directory
    pgdata: Utf8PathBuf,

    /// Path to local dir where the layer files will be stored
    dest_path: Utf8PathBuf,

    #[arg(long, default_value_t = TenantId::from_str("42424242424242424242424242424242").unwrap())]
    tenant_id: TenantId,
    #[arg(long, default_value_t = TimelineId::from_str("42424242424242424242424242424242").unwrap())]
    timeline_id: TimelineId,
}

fn main() -> anyhow::Result<()> {
    logging::init(
        LogFormat::Plain,
        TracingErrorLayerEnablement::EnableWithRustLogFilter,
        logging::Output::Stdout,
    )?;

    virtual_file::init(100, IoEngineKind::StdFs, 512);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let cli = CliOpts::parse();

    rt.block_on(async_main(cli))?;

    Ok(())
}

async fn async_main(cli: CliOpts) -> anyhow::Result<()> {
    let mut import =
        pg_import::PgImportEnv::init(&cli.dest_path, cli.tenant_id, cli.timeline_id).await?;

    import.import_datadir(&cli.pgdata).await?;

    Ok(())
}
