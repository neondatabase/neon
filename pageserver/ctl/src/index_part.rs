use std::collections::HashMap;

use anyhow::Context;
use camino::Utf8PathBuf;
use pageserver::tenant::remote_timeline_client::index::LayerFileMetadata;
use pageserver::tenant::storage_layer::LayerName;
use pageserver::tenant::{metadata::TimelineMetadata, IndexPart};
use utils::lsn::Lsn;

#[derive(clap::Subcommand)]
pub(crate) enum IndexPartCmd {
    Dump {
        path: Utf8PathBuf,
        #[clap(long, value_enum, default_value = "all-json")]
        what: What,
    },
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
pub(crate) enum What {
    AllJson,
    TimelineMetadataDebugString,
}

pub(crate) async fn main(cmd: &IndexPartCmd) -> anyhow::Result<()> {
    match cmd {
        IndexPartCmd::Dump { path, what } => {
            let bytes = tokio::fs::read(path).await.context("read file")?;
            let des: IndexPart = IndexPart::from_s3_bytes(&bytes).context("deserialize")?;
            #[derive(serde::Serialize)]
            struct Output<'a> {
                layer_metadata: &'a HashMap<LayerName, LayerFileMetadata>,
                disk_consistent_lsn: Lsn,
                timeline_metadata: &'a TimelineMetadata,
            }

            match what {
                What::AllJson => {
                    let output = Output {
                        layer_metadata: &des.layer_metadata,
                        disk_consistent_lsn: des.metadata.disk_consistent_lsn(),
                        timeline_metadata: &des.metadata,
                    };

                    let output =
                        serde_json::to_string_pretty(&output).context("serialize output")?;
                    println!("{output}");
                }
                What::TimelineMetadataDebugString => {
                    println!("{:#?}", des.metadata);
                }
            }

            Ok(())
        }
    }
}
