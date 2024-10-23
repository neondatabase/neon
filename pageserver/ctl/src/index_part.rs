use anyhow::Context;
use camino::Utf8PathBuf;
use pageserver::tenant::IndexPart;

#[derive(clap::Subcommand)]
pub(crate) enum IndexPartCmd {
    Dump { path: Utf8PathBuf },
}

pub(crate) async fn main(cmd: &IndexPartCmd) -> anyhow::Result<()> {
    match cmd {
        IndexPartCmd::Dump { path } => {
            let bytes = tokio::fs::read(path).await.context("read file")?;
            let des: IndexPart = IndexPart::from_json_bytes(&bytes).context("deserialize")?;
            let output = serde_json::to_string_pretty(&des).context("serialize output")?;
            println!("{output}");
            Ok(())
        }
    }
}
