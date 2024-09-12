use anyhow;
use pageserver::{pg_import, virtual_file::{self, api::IoEngineKind}};
use utils::logging::{self, LogFormat, TracingErrorLayerEnablement};

fn main() -> anyhow::Result<()> {
    println!("Hey!");

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

    rt.block_on(pg_import::do_import())?;
    
    Ok(())
}
