use anyhow::Context;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::NamedTempFile;

use super::RawMetric;

pub(super) async fn read_metrics_from_disk(path: Arc<PathBuf>) -> anyhow::Result<Vec<RawMetric>> {
    // do not add context to each error, callsite will log with full path
    let span = tracing::Span::current();
    tokio::task::spawn_blocking(move || {
        let _e = span.entered();
        let mut file = std::fs::File::open(&*path)?;
        let reader = std::io::BufReader::new(&mut file);
        anyhow::Ok(serde_json::from_reader::<_, Vec<RawMetric>>(reader)?)
    })
    .await
    .context("read metrics join error")
    .and_then(|x| x)
}

pub(super) async fn flush_metrics_to_disk(
    current_metrics: &Arc<Vec<RawMetric>>,
    path: &Arc<PathBuf>,
) -> anyhow::Result<()> {
    use std::io::Write;

    anyhow::ensure!(path.parent().is_some(), "path must have parent: {path:?}");

    let span = tracing::Span::current();
    tokio::task::spawn_blocking({
        let current_metrics = current_metrics.clone();
        let path = path.clone();
        move || {
            let _e = span.entered();

            let parent = path.parent().expect("existence checked");
            let mut tempfile = NamedTempFile::new_in(parent)?;

            {
                let mut writer = std::io::BufWriter::new(&mut tempfile);
                serde_json::to_writer(&mut writer, &*current_metrics)
                    .context("serialize metrics")?;
                writer
                    .into_inner()
                    .map_err(|_| anyhow::anyhow!("flushing metrics failed"))?;
            }

            tempfile.flush()?;
            tempfile.as_file().sync_all()?;

            drop(tempfile.persist(&*path)?);

            let f = std::fs::File::open(path.parent().unwrap())?;
            f.sync_all()?;

            anyhow::Ok(())
        }
    })
    .await
    .with_context(|| format!("write metrics to {path:?} join error"))
    .and_then(|x| x.with_context(|| format!("write metrics to {path:?}")))
}
