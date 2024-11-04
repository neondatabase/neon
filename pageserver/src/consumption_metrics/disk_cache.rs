use anyhow::Context;
use camino::{Utf8Path, Utf8PathBuf};
use std::sync::Arc;

use crate::consumption_metrics::NewMetricsRefRoot;

use super::{NewMetricsRoot, NewRawMetric, RawMetric};

pub(super) fn read_metrics_from_serde_value(
    json_value: serde_json::Value,
) -> anyhow::Result<Vec<NewRawMetric>> {
    if NewMetricsRoot::is_v2_metrics(&json_value) {
        let root = serde_json::from_value::<NewMetricsRoot>(json_value)?;
        Ok(root.metrics)
    } else {
        let all_metrics = serde_json::from_value::<Vec<RawMetric>>(json_value)?;
        let all_metrics = all_metrics
            .into_iter()
            .map(|(key, (event_type, value))| NewRawMetric {
                key,
                kind: event_type,
                value,
            })
            .collect();
        Ok(all_metrics)
    }
}

pub(super) async fn read_metrics_from_disk(
    path: Arc<Utf8PathBuf>,
) -> anyhow::Result<Vec<NewRawMetric>> {
    // do not add context to each error, callsite will log with full path
    let span = tracing::Span::current();
    tokio::task::spawn_blocking(move || {
        let _e = span.entered();

        if let Some(parent) = path.parent() {
            if let Err(e) = scan_and_delete_with_same_prefix(&path) {
                tracing::info!("failed to cleanup temporary files in {parent:?}: {e:#}");
            }
        }

        let mut file = std::fs::File::open(&*path)?;
        let reader = std::io::BufReader::new(&mut file);
        let json_value = serde_json::from_reader::<_, serde_json::Value>(reader)?;
        read_metrics_from_serde_value(json_value)
    })
    .await
    .context("read metrics join error")
    .and_then(|x| x)
}

fn scan_and_delete_with_same_prefix(path: &Utf8Path) -> std::io::Result<()> {
    let it = std::fs::read_dir(path.parent().expect("caller checked"))?;

    let prefix = path.file_name().expect("caller checked").to_string();

    for entry in it {
        let entry = entry?;
        if !entry.metadata()?.is_file() {
            continue;
        }
        let file_name = entry.file_name();

        if path.file_name().unwrap() == file_name {
            // do not remove our actual file
            continue;
        }

        let file_name = file_name.to_string_lossy();

        if !file_name.starts_with(&*prefix) {
            continue;
        }

        let path = entry.path();

        if let Err(e) = std::fs::remove_file(&path) {
            tracing::warn!("cleaning up old tempfile {file_name:?} failed: {e:#}");
        } else {
            tracing::info!("cleaned up old tempfile {file_name:?}");
        }
    }

    Ok(())
}

pub(super) async fn flush_metrics_to_disk(
    current_metrics: &Arc<Vec<NewRawMetric>>,
    path: &Arc<Utf8PathBuf>,
) -> anyhow::Result<()> {
    use std::io::Write;

    anyhow::ensure!(path.parent().is_some(), "path must have parent: {path:?}");
    anyhow::ensure!(
        path.file_name().is_some(),
        "path must have filename: {path:?}"
    );

    let span = tracing::Span::current();
    tokio::task::spawn_blocking({
        let current_metrics = current_metrics.clone();
        let path = path.clone();
        move || {
            let _e = span.entered();

            let parent = path.parent().expect("existence checked");
            let file_name = path.file_name().expect("existence checked");
            let mut tempfile = camino_tempfile::Builder::new()
                .prefix(file_name)
                .suffix(".tmp")
                .tempfile_in(parent)?;

            tracing::debug!("using tempfile {:?}", tempfile.path());

            // write out all of the raw metrics, to be read out later on restart as cached values
            {
                let mut writer = std::io::BufWriter::new(&mut tempfile);
                serde_json::to_writer(
                    &mut writer,
                    &NewMetricsRefRoot::new(current_metrics.as_ref()),
                )
                .context("serialize metrics")?;
                writer
                    .into_inner()
                    .map_err(|_| anyhow::anyhow!("flushing metrics failed"))?;
            }

            tempfile.flush()?;
            tempfile.as_file().sync_all()?;

            fail::fail_point!("before-persist-last-metrics-collected");

            drop(tempfile.persist(&*path).map_err(|e| e.error)?);

            let f = std::fs::File::open(path.parent().unwrap())?;
            f.sync_all()?;

            anyhow::Ok(())
        }
    })
    .await
    .with_context(|| format!("write metrics to {path:?} join error"))
    .and_then(|x| x.with_context(|| format!("write metrics to {path:?}")))
}
