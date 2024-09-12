use anyhow;
use camino::Utf8PathBuf;
use clap::Parser;
use pageserver::{pg_import, virtual_file::{self, api::IoEngineKind}};
use utils::logging::{self, LogFormat, TracingErrorLayerEnablement};

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
}

fn main() -> anyhow::Result<()> {
    logging::init(
        LogFormat::Plain,
        TracingErrorLayerEnablement::EnableWithRustLogFilter,
        logging::Output::Stdout,
    )?;

    virtual_file::init(
        100,
        IoEngineKind::StdFs,
        512,
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let cli = CliOpts::parse();

    rt.block_on(async_main(cli))?;

    Ok(())
}

async fn async_main(cli: CliOpts) -> anyhow::Result<()> {
    let mut import = pg_import::PgImportEnv::init().await?;
    import.import_datadir(&cli.pgdata, &cli.dest_path).await?;
    Ok(())
}
