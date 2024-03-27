use std::io::SeekFrom;

use anyhow::{Context, Result};
use async_compression::{
    tokio::{bufread::ZstdDecoder, write::ZstdEncoder},
    zstd::CParameter,
    Level,
};
use camino::Utf8Path;
use nix::NixPath;
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncBufRead,
    io::AsyncSeekExt,
    io::AsyncWriteExt,
};
use tokio_tar::{Archive, Builder, HeaderMode};
use walkdir::WalkDir;

/// Creates a Zstandard tarball.
pub async fn create_zst_tarball(path: &Utf8Path, tarball: &Utf8Path) -> Result<(File, u64)> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&tarball)
        .await
        .with_context(|| format!("tempfile creation {tarball}"))?;

    let mut paths = Vec::new();
    for entry in WalkDir::new(path) {
        let entry = entry?;
        let metadata = entry.metadata().expect("error getting dir entry metadata");
        // Also allow directories so that we also get empty directories
        if !(metadata.is_file() || metadata.is_dir()) {
            continue;
        }
        let path = entry.into_path();
        paths.push(path);
    }
    // Do a sort to get a more consistent listing
    paths.sort_unstable();
    let zstd = ZstdEncoder::with_quality_and_params(
        file,
        Level::Default,
        &[CParameter::enable_long_distance_matching(true)],
    );
    let mut builder = Builder::new(zstd);
    // Use reproducible header mode
    builder.mode(HeaderMode::Deterministic);
    for p in paths {
        let rel_path = p.strip_prefix(path)?;
        if rel_path.is_empty() {
            // The top directory should not be compressed,
            // the tar crate doesn't like that
            continue;
        }
        builder.append_path_with_name(&p, rel_path).await?;
    }
    let mut zstd = builder.into_inner().await?;
    zstd.shutdown().await?;
    let mut compressed = zstd.into_inner();
    let compressed_len = compressed.metadata().await?.len();
    compressed.seek(SeekFrom::Start(0)).await?;
    Ok((compressed, compressed_len))
}

/// Creates a Zstandard tarball.
pub async fn extract_zst_tarball(
    path: &Utf8Path,
    tarball: impl AsyncBufRead + Unpin,
) -> Result<()> {
    let decoder = Box::pin(ZstdDecoder::new(tarball));
    let mut archive = Archive::new(decoder);
    archive.unpack(path).await?;
    Ok(())
}
