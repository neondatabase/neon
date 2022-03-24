//! A set of structs to represent a compressed part of the timeline, and methods to asynchronously compress and uncompress a stream of data,
//! without holding the entire data in memory.
//! For the latter, both compress and uncompress functions operate buffered streams (currently hardcoded size of [`ARCHIVE_STREAM_BUFFER_SIZE_BYTES`]),
//! not attempting to hold the entire archive in memory.
//!
//! The compression is done with <a href="https://datatracker.ietf.org/doc/html/rfc8878">zstd</a> streaming algorithm via the `async-compression` crate.
//! The crate does not contain any knobs to tweak the compression, but otherwise is one of the only ones that's both async and has an API to manage the part of an archive.
//! Zstd was picked as the best algorithm among the ones available in the crate, after testing the initial timeline file compression.
//!
//! Archiving is almost agnostic to timeline file types, with an exception of the metadata file, that's currently distinguished in the [un]compression code.
//! The metadata file is treated separately when [de]compression is involved, to reduce the risk of corrupting the metadata file.
//! When compressed, the metadata file is always required and stored as the last file in the archive stream.
//! When uncompressed, the metadata file gets naturally uncompressed last, to ensure that all other layer files are decompressed successfully first.
//!
//! Archive structure:
//! +----------------------------------------+
//! | header | file_1, ..., file_k, metadata |
//! +----------------------------------------+
//!
//! The archive consists of two separate zstd archives:
//! * header archive, that contains all files names and their sizes and relative paths in the timeline directory
//! Header is a Rust structure, serialized into bytes and compressed with zstd.
//! * files archive, that has metadata file as the last one, all compressed with zstd into a single binary blob
//!
//! Header offset is stored in the file name, along with the `disk_consistent_lsn` from the metadata file.
//! See [`parse_archive_name`] and [`ARCHIVE_EXTENSION`] for the name details, example: `00000000016B9150-.zst_9732`.
//! This way, the header could be retrieved without reading an entire archive file.

use std::{
    collections::BTreeSet,
    future::Future,
    io::Cursor,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{bail, ensure, Context};
use async_compression::tokio::bufread::{ZstdDecoder, ZstdEncoder};
use serde::{Deserialize, Serialize};
use tokio::{
    fs,
    io::{self, AsyncReadExt, AsyncWriteExt},
};
use tracing::*;
use zenith_utils::{bin_ser::BeSer, lsn::Lsn};

use crate::layered_repository::metadata::{TimelineMetadata, METADATA_FILE_NAME};

use super::index::RelativePath;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveHeader {
    /// All regular timeline files, excluding the metadata file.
    pub files: Vec<FileEntry>,
    // Metadata file name is known to the system, as its location relative to the timeline dir,
    // so no need to store anything but its size in bytes.
    pub metadata_file_size: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FileEntry {
    /// Uncompressed file size, bytes.
    pub size: u64,
    /// A path, relative to the directory root, used when compressing the directory contents.
    pub subpath: RelativePath,
}

const ARCHIVE_EXTENSION: &str = "-.zst_";
const ARCHIVE_STREAM_BUFFER_SIZE_BYTES: usize = 4 * 1024 * 1024;

/// Streams an archive of files given into a stream target, defined by the closure.
///
/// The closure approach is picked for cases like S3, where we would need a name of the file before we can get a stream to write the bytes into.
/// Current idea is to place the header size in the name of the file, to enable the fast partial remote file index restoration without actually reading remote storage file contents.
///
/// Performs the compression in multiple steps:
/// * prepares an archive header, stripping the `source_dir` prefix from the `files`
/// * generates the name of the archive
/// * prepares archive producer future, knowing the header and the file list
/// An `impl AsyncRead` and `impl AsyncWrite` pair of connected streams is created to implement the partial contents streaming.
/// The writer end gets into the archive producer future, to put the header and a stream of compressed files.
/// * prepares archive consumer future, by executing the provided closure
/// The closure gets the reader end stream and the name of the file to create a future that would stream the file contents elsewhere.
/// * runs and waits for both futures to complete
/// * on a successful completion of both futures, header, its size and the user-defined consumer future return data is returned
/// Due to the design above, the archive name and related data is visible inside the consumer future only, so it's possible to return the data,
/// needed for future processing.
pub async fn archive_files_as_stream<Cons, ConsRet, Fut>(
    source_dir: &Path,
    files: impl Iterator<Item = &PathBuf>,
    metadata: &TimelineMetadata,
    create_archive_consumer: Cons,
) -> anyhow::Result<(ArchiveHeader, u64, ConsRet)>
where
    Cons: FnOnce(Box<dyn io::AsyncRead + Unpin + Send + Sync + 'static>, String) -> Fut
        + Send
        + 'static,
    Fut: Future<Output = anyhow::Result<ConsRet>> + Send + 'static,
    ConsRet: Send + Sync + 'static,
{
    let metadata_bytes = metadata
        .to_bytes()
        .context("Failed to create metadata bytes")?;
    let (archive_header, compressed_header_bytes) =
        prepare_header(source_dir, files, &metadata_bytes)
            .await
            .context("Failed to prepare file for archivation")?;

    let header_size = compressed_header_bytes.len() as u64;
    let (write, read) = io::duplex(ARCHIVE_STREAM_BUFFER_SIZE_BYTES);
    let archive_filler = write_archive_contents(
        source_dir.to_path_buf(),
        archive_header.clone(),
        metadata_bytes,
        write,
    );
    let archive_name = archive_name(metadata.disk_consistent_lsn(), header_size);
    let archive_stream =
        Cursor::new(compressed_header_bytes).chain(ZstdEncoder::new(io::BufReader::new(read)));

    let (archive_creation_result, archive_upload_result) = tokio::join!(
        tokio::spawn(archive_filler),
        tokio::spawn(async move {
            create_archive_consumer(Box::new(archive_stream), archive_name).await
        })
    );
    archive_creation_result
        .context("Failed to spawn archive creation future")?
        .context("Failed to create an archive")?;
    let upload_return_value = archive_upload_result
        .context("Failed to spawn archive upload future")?
        .context("Failed to upload the archive")?;

    Ok((archive_header, header_size, upload_return_value))
}

/// Similar to [`archive_files_as_stream`], creates a pair of streams to uncompress the 2nd part of the archive,
/// that contains files and is located after the header.
/// S3 allows downloading partial file contents for a given file key (i.e. name), to accommodate this retrieval,
/// a closure is used.
/// Same concepts with two concurrent futures, user-defined closure, future and return value apply here, but the
/// consumer and the receiver ends are swapped, since the uncompression happens.
pub async fn uncompress_file_stream_with_index<Prod, ProdRet, Fut>(
    destination_dir: PathBuf,
    files_to_skip: Arc<BTreeSet<PathBuf>>,
    disk_consistent_lsn: Lsn,
    header: ArchiveHeader,
    header_size: u64,
    create_archive_file_part: Prod,
) -> anyhow::Result<ProdRet>
where
    Prod: FnOnce(Box<dyn io::AsyncWrite + Unpin + Send + Sync + 'static>, String) -> Fut
        + Send
        + 'static,
    Fut: Future<Output = anyhow::Result<ProdRet>> + Send + 'static,
    ProdRet: Send + Sync + 'static,
{
    let (write, mut read) = io::duplex(ARCHIVE_STREAM_BUFFER_SIZE_BYTES);
    let archive_name = archive_name(disk_consistent_lsn, header_size);

    let (archive_download_result, archive_uncompress_result) = tokio::join!(
        tokio::spawn(async move { create_archive_file_part(Box::new(write), archive_name).await }),
        tokio::spawn(async move {
            uncompress_with_header(&files_to_skip, &destination_dir, header, &mut read).await
        })
    );

    let download_value = archive_download_result
        .context("Failed to spawn archive download future")?
        .context("Failed to download an archive")?;
    archive_uncompress_result
        .context("Failed to spawn archive uncompress future")?
        .context("Failed to uncompress the archive")?;

    Ok(download_value)
}

/// Reads archive header from the stream given:
/// * parses the file name to get the header size
/// * reads the exact amount of bytes
/// * uncompresses and deserializes those
pub async fn read_archive_header<A: io::AsyncRead + Send + Sync + Unpin>(
    archive_name: &str,
    from: &mut A,
) -> anyhow::Result<ArchiveHeader> {
    let (_, header_size) = parse_archive_name(Path::new(archive_name))?;

    let mut compressed_header_bytes = vec![0; header_size as usize];
    from.read_exact(&mut compressed_header_bytes)
        .await
        .with_context(|| {
            format!(
                "Failed to read header header from the archive {}",
                archive_name
            )
        })?;

    let mut header_bytes = Vec::new();
    ZstdDecoder::new(io::BufReader::new(compressed_header_bytes.as_slice()))
        .read_to_end(&mut header_bytes)
        .await
        .context("Failed to decompress a header from the archive")?;

    Ok(ArchiveHeader::des(&header_bytes)
        .context("Failed to deserialize a header from the archive")?)
}

/// Reads the archive metadata out of the archive name:
/// * `disk_consistent_lsn` of the checkpoint that was archived
/// * size of the archive header
pub fn parse_archive_name(archive_path: &Path) -> anyhow::Result<(Lsn, u64)> {
    let archive_name = archive_path
        .file_name()
        .with_context(|| format!("Archive '{}' has no file name", archive_path.display()))?
        .to_string_lossy();
    let (lsn_str, header_size_str) =
        archive_name
            .rsplit_once(ARCHIVE_EXTENSION)
            .with_context(|| {
                format!(
                    "Archive '{}' has incorrect extension, expected to contain '{}'",
                    archive_path.display(),
                    ARCHIVE_EXTENSION
                )
            })?;
    let disk_consistent_lsn = Lsn::from_hex(lsn_str).with_context(|| {
        format!(
            "Archive '{}' has an invalid disk consistent lsn in its extension",
            archive_path.display(),
        )
    })?;
    let header_size = header_size_str.parse::<u64>().with_context(|| {
        format!(
            "Archive '{}' has an invalid a header offset number in its extension",
            archive_path.display(),
        )
    })?;
    Ok((disk_consistent_lsn, header_size))
}

fn archive_name(disk_consistent_lsn: Lsn, header_size: u64) -> String {
    let archive_name = format!(
        "{:016X}{ARCHIVE_EXTENSION}{}",
        u64::from(disk_consistent_lsn),
        header_size,
        ARCHIVE_EXTENSION = ARCHIVE_EXTENSION,
    );
    archive_name
}

pub async fn uncompress_with_header(
    files_to_skip: &BTreeSet<PathBuf>,
    destination_dir: &Path,
    header: ArchiveHeader,
    archive_after_header: impl io::AsyncRead + Send + Sync + Unpin,
) -> anyhow::Result<()> {
    debug!("Uncompressing archive into {}", destination_dir.display());
    let mut archive = ZstdDecoder::new(io::BufReader::new(archive_after_header));

    if !destination_dir.exists() {
        fs::create_dir_all(&destination_dir)
            .await
            .with_context(|| {
                format!(
                    "Failed to create target directory at {}",
                    destination_dir.display()
                )
            })?;
    } else if !destination_dir.is_dir() {
        bail!(
            "Destination path '{}' is not a valid directory",
            destination_dir.display()
        );
    }
    debug!("Will extract {} files from the archive", header.files.len());
    for entry in header.files {
        uncompress_entry(
            &mut archive,
            &entry.subpath.as_path(destination_dir),
            entry.size,
            files_to_skip,
        )
        .await
        .with_context(|| format!("Failed to uncompress archive entry {:?}", entry))?;
    }
    uncompress_entry(
        &mut archive,
        &destination_dir.join(METADATA_FILE_NAME),
        header.metadata_file_size,
        files_to_skip,
    )
    .await
    .context("Failed to uncompress the metadata entry")?;
    Ok(())
}

async fn uncompress_entry(
    archive: &mut ZstdDecoder<io::BufReader<impl io::AsyncRead + Send + Sync + Unpin>>,
    destination_path: &Path,
    entry_size: u64,
    files_to_skip: &BTreeSet<PathBuf>,
) -> anyhow::Result<()> {
    if let Some(parent) = destination_path.parent() {
        fs::create_dir_all(parent).await.with_context(|| {
            format!(
                "Failed to create parent directory for {}",
                destination_path.display()
            )
        })?;
    };

    if files_to_skip.contains(destination_path) {
        debug!("Skipping {}", destination_path.display());
        copy_n_bytes(entry_size, archive, &mut io::sink())
            .await
            .context("Failed to skip bytes in the archive")?;
        return Ok(());
    }

    let mut destination =
        io::BufWriter::new(fs::File::create(&destination_path).await.with_context(|| {
            format!(
                "Failed to open file {} for extraction",
                destination_path.display()
            )
        })?);
    copy_n_bytes(entry_size, archive, &mut destination)
        .await
        .with_context(|| {
            format!(
                "Failed to write extracted archive contents into file {}",
                destination_path.display()
            )
        })?;
    destination
        .flush()
        .await
        .context("Failed to flush the streaming archive bytes")?;
    Ok(())
}

async fn write_archive_contents(
    source_dir: PathBuf,
    header: ArchiveHeader,
    metadata_bytes: Vec<u8>,
    mut archive_input: io::DuplexStream,
) -> anyhow::Result<()> {
    debug!("Starting writing files into archive");
    for file_entry in header.files {
        let path = file_entry.subpath.as_path(&source_dir);
        let mut source_file =
            io::BufReader::new(fs::File::open(&path).await.with_context(|| {
                format!(
                    "Failed to open file for archiving to path {}",
                    path.display()
                )
            })?);
        let bytes_written = io::copy(&mut source_file, &mut archive_input)
            .await
            .with_context(|| {
                format!(
                    "Failed to open add a file into archive, file path {}",
                    path.display()
                )
            })?;
        ensure!(
            file_entry.size == bytes_written,
            "File {} was written to the archive incompletely",
            path.display()
        );
        trace!(
            "Added file '{}' ({} bytes) into the archive",
            path.display(),
            bytes_written
        );
    }
    let metadata_bytes_written = io::copy(&mut metadata_bytes.as_slice(), &mut archive_input)
        .await
        .context("Failed to add metadata into the archive")?;
    ensure!(
        header.metadata_file_size == metadata_bytes_written,
        "Metadata file was written to the archive incompletely",
    );

    archive_input
        .shutdown()
        .await
        .context("Failed to finalize the archive")?;
    debug!("Successfully streamed all files into the archive");
    Ok(())
}

async fn prepare_header(
    source_dir: &Path,
    files: impl Iterator<Item = &PathBuf>,
    metadata_bytes: &[u8],
) -> anyhow::Result<(ArchiveHeader, Vec<u8>)> {
    let mut archive_files = Vec::new();
    for file_path in files {
        let file_metadata = fs::metadata(file_path).await.with_context(|| {
            format!(
                "Failed to read metadata during archive indexing for {}",
                file_path.display()
            )
        })?;
        ensure!(
            file_metadata.is_file(),
            "Archive indexed path {} is not a file",
            file_path.display()
        );

        if file_path.file_name().and_then(|name| name.to_str()) != Some(METADATA_FILE_NAME) {
            let entry = FileEntry {
                subpath: RelativePath::new(source_dir, file_path).with_context(|| {
                    format!(
                        "File '{}' does not belong to pageserver workspace",
                        file_path.display()
                    )
                })?,
                size: file_metadata.len(),
            };
            archive_files.push(entry);
        }
    }

    let header = ArchiveHeader {
        files: archive_files,
        metadata_file_size: metadata_bytes.len() as u64,
    };

    debug!("Appending a header for {} files", header.files.len());
    let header_bytes = header.ser().context("Failed to serialize a header")?;
    debug!("Header bytes len {}", header_bytes.len());
    let mut compressed_header_bytes = Vec::new();
    ZstdEncoder::new(io::BufReader::new(header_bytes.as_slice()))
        .read_to_end(&mut compressed_header_bytes)
        .await
        .context("Failed to compress header bytes")?;
    debug!(
        "Compressed header bytes len {}",
        compressed_header_bytes.len()
    );
    Ok((header, compressed_header_bytes))
}

async fn copy_n_bytes(
    n: u64,
    from: &mut (impl io::AsyncRead + Send + Sync + Unpin),
    into: &mut (impl io::AsyncWrite + Send + Sync + Unpin),
) -> anyhow::Result<()> {
    let bytes_written = io::copy(&mut from.take(n), into).await?;
    ensure!(
        bytes_written == n,
        "Failed to read exactly {} bytes from the input, bytes written: {}",
        n,
        bytes_written,
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use tokio::{fs, io::AsyncSeekExt};

    use crate::repository::repo_harness::{RepoHarness, TIMELINE_ID};

    use super::*;

    #[tokio::test]
    async fn compress_and_uncompress() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("compress_and_uncompress")?;
        let timeline_dir = repo_harness.timeline_path(&TIMELINE_ID);
        init_directory(
            &timeline_dir,
            vec![
                ("first", "first_contents"),
                ("second", "second_contents"),
                (METADATA_FILE_NAME, "wrong_metadata"),
            ],
        )
        .await?;
        let timeline_files = list_file_paths_with_contents(&timeline_dir).await?;
        assert_eq!(
            timeline_files,
            vec![
                (
                    timeline_dir.join("first"),
                    FileContents::Text("first_contents".to_string())
                ),
                (
                    timeline_dir.join(METADATA_FILE_NAME),
                    FileContents::Text("wrong_metadata".to_string())
                ),
                (
                    timeline_dir.join("second"),
                    FileContents::Text("second_contents".to_string())
                ),
            ],
            "Initial timeline contents should contain two normal files and a wrong metadata file"
        );

        let metadata = TimelineMetadata::new(Lsn(0x30), None, None, Lsn(0), Lsn(0), Lsn(0));
        let paths_to_archive = timeline_files
            .into_iter()
            .map(|(path, _)| path)
            .collect::<Vec<_>>();

        let tempdir = tempfile::tempdir()?;
        let base_path = tempdir.path().to_path_buf();
        let (header, header_size, archive_target) = archive_files_as_stream(
            &timeline_dir,
            paths_to_archive.iter(),
            &metadata,
            move |mut archive_streamer, archive_name| async move {
                let archive_target = base_path.join(&archive_name);
                let mut archive_file = fs::File::create(&archive_target).await?;
                io::copy(&mut archive_streamer, &mut archive_file).await?;
                Ok(archive_target)
            },
        )
        .await?;

        let mut file = fs::File::open(&archive_target).await?;
        file.seek(io::SeekFrom::Start(header_size)).await?;
        let target_dir = tempdir.path().join("extracted");
        uncompress_with_header(&BTreeSet::new(), &target_dir, header, file).await?;

        let extracted_files = list_file_paths_with_contents(&target_dir).await?;

        assert_eq!(
            extracted_files,
            vec![
                (
                    target_dir.join("first"),
                    FileContents::Text("first_contents".to_string())
                ),
                (
                    target_dir.join(METADATA_FILE_NAME),
                    FileContents::Binary(metadata.to_bytes()?)
                ),
                (
                    target_dir.join("second"),
                    FileContents::Text("second_contents".to_string())
                ),
            ],
            "Extracted files should contain all local timeline files besides its metadata, which should be taken from the arguments"
        );

        Ok(())
    }

    async fn init_directory(
        root: &Path,
        files_with_contents: Vec<(&str, &str)>,
    ) -> anyhow::Result<()> {
        fs::create_dir_all(root).await?;
        for (file_name, contents) in files_with_contents {
            fs::File::create(root.join(file_name))
                .await?
                .write_all(contents.as_bytes())
                .await?;
        }
        Ok(())
    }

    #[derive(PartialEq, Eq, PartialOrd, Ord)]
    enum FileContents {
        Text(String),
        Binary(Vec<u8>),
    }

    impl std::fmt::Debug for FileContents {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Text(text) => f.debug_tuple("Text").field(text).finish(),
                Self::Binary(bytes) => f
                    .debug_tuple("Binary")
                    .field(&format!("{} bytes", bytes.len()))
                    .finish(),
            }
        }
    }

    async fn list_file_paths_with_contents(
        root: &Path,
    ) -> anyhow::Result<Vec<(PathBuf, FileContents)>> {
        let mut file_paths = Vec::new();

        let mut dir_listings = vec![fs::read_dir(root).await?];
        while let Some(mut dir_listing) = dir_listings.pop() {
            while let Some(entry) = dir_listing.next_entry().await? {
                let entry_path = entry.path();
                if entry_path.is_file() {
                    let contents = match String::from_utf8(fs::read(&entry_path).await?) {
                        Ok(text) => FileContents::Text(text),
                        Err(e) => FileContents::Binary(e.into_bytes()),
                    };
                    file_paths.push((entry_path, contents));
                } else if entry_path.is_dir() {
                    dir_listings.push(fs::read_dir(entry_path).await?);
                } else {
                    info!(
                        "Skipping path '{}' as it's not a file or a directory",
                        entry_path.display()
                    );
                }
            }
        }

        file_paths.sort();
        Ok(file_paths)
    }
}
