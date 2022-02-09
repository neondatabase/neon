//! A CLI helper to deal with remote storage (S3, usually) blobs as archives.
//! See [`compression`] for more details about the archives.

use std::{collections::BTreeSet, path::Path};

use anyhow::{bail, ensure, Context};
use clap::{App, Arg};
use pageserver::{
    layered_repository::metadata::{TimelineMetadata, METADATA_FILE_NAME},
    remote_storage::compression,
};
use tokio::{fs, io};
use zenith_utils::GIT_VERSION;

const LIST_SUBCOMMAND: &str = "list";
const ARCHIVE_ARG_NAME: &str = "archive";

const EXTRACT_SUBCOMMAND: &str = "extract";
const TARGET_DIRECTORY_ARG_NAME: &str = "target_directory";

const CREATE_SUBCOMMAND: &str = "create";
const SOURCE_DIRECTORY_ARG_NAME: &str = "source_directory";

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let arg_matches = App::new("pageserver zst blob [un]compressor utility")
        .version(GIT_VERSION)
        .subcommands(vec![
            App::new(LIST_SUBCOMMAND)
                .about("List the archive contents")
                .arg(
                    Arg::new(ARCHIVE_ARG_NAME)
                        .required(true)
                        .takes_value(true)
                        .help("An archive to list the contents of"),
                ),
            App::new(EXTRACT_SUBCOMMAND)
                .about("Extracts the archive into the directory")
                .arg(
                    Arg::new(ARCHIVE_ARG_NAME)
                        .required(true)
                        .takes_value(true)
                        .help("An archive to extract"),
                )
                .arg(
                    Arg::new(TARGET_DIRECTORY_ARG_NAME)
                        .required(false)
                        .takes_value(true)
                        .help("A directory to extract the archive into. Optional, will use the current directory if not specified"),
                ),
            App::new(CREATE_SUBCOMMAND)
                .about("Creates an archive with the contents of a directory (only the first level files are taken, metadata file has to be present in the same directory)")
                .arg(
                    Arg::new(SOURCE_DIRECTORY_ARG_NAME)
                        .required(true)
                        .takes_value(true)
                        .help("A directory to use for creating the archive"),
                )
                .arg(
                    Arg::new(TARGET_DIRECTORY_ARG_NAME)
                        .required(false)
                        .takes_value(true)
                        .help("A directory to create the archive in. Optional, will use the current directory if not specified"),
                ),
        ])
        .get_matches();

    let subcommand_name = match arg_matches.subcommand_name() {
        Some(name) => name,
        None => bail!("No subcommand specified"),
    };

    let subcommand_matches = match arg_matches.subcommand_matches(subcommand_name) {
        Some(matches) => matches,
        None => bail!(
            "No subcommand arguments were recognized for subcommand '{}'",
            subcommand_name
        ),
    };

    let target_dir = Path::new(
        subcommand_matches
            .value_of(TARGET_DIRECTORY_ARG_NAME)
            .unwrap_or("./"),
    );

    match subcommand_name {
        LIST_SUBCOMMAND => {
            let archive = match subcommand_matches.value_of(ARCHIVE_ARG_NAME) {
                Some(archive) => Path::new(archive),
                None => bail!("No '{}' argument is specified", ARCHIVE_ARG_NAME),
            };
            list_archive(archive).await
        }
        EXTRACT_SUBCOMMAND => {
            let archive = match subcommand_matches.value_of(ARCHIVE_ARG_NAME) {
                Some(archive) => Path::new(archive),
                None => bail!("No '{}' argument is specified", ARCHIVE_ARG_NAME),
            };
            extract_archive(archive, target_dir).await
        }
        CREATE_SUBCOMMAND => {
            let source_dir = match subcommand_matches.value_of(SOURCE_DIRECTORY_ARG_NAME) {
                Some(source) => Path::new(source),
                None => bail!("No '{}' argument is specified", SOURCE_DIRECTORY_ARG_NAME),
            };
            create_archive(source_dir, target_dir).await
        }
        unknown => bail!("Unknown subcommand {}", unknown),
    }
}

async fn list_archive(archive: &Path) -> anyhow::Result<()> {
    let archive = archive.canonicalize().with_context(|| {
        format!(
            "Failed to get the absolute path for the archive path '{}'",
            archive.display()
        )
    })?;
    ensure!(
        archive.is_file(),
        "Path '{}' is not an archive file",
        archive.display()
    );
    println!("Listing an archive at path '{}'", archive.display());
    let archive_name = match archive.file_name().and_then(|name| name.to_str()) {
        Some(name) => name,
        None => bail!(
            "Failed to get the archive name from the path '{}'",
            archive.display()
        ),
    };

    let archive_bytes = fs::read(&archive)
        .await
        .context("Failed to read the archive bytes")?;

    let header = compression::read_archive_header(archive_name, &mut archive_bytes.as_slice())
        .await
        .context("Failed to read the archive header")?;

    let empty_path = Path::new("");
    println!("-------------------------------");

    let longest_path_in_archive = header
        .files
        .iter()
        .filter_map(|file| Some(file.subpath.as_path(empty_path).to_str()?.len()))
        .max()
        .unwrap_or_default()
        .max(METADATA_FILE_NAME.len());

    for regular_file in &header.files {
        println!(
            "File: {:width$} uncompressed size: {} bytes",
            regular_file.subpath.as_path(empty_path).display(),
            regular_file.size,
            width = longest_path_in_archive,
        )
    }
    println!(
        "File: {:width$} uncompressed size: {} bytes",
        METADATA_FILE_NAME,
        header.metadata_file_size,
        width = longest_path_in_archive,
    );
    println!("-------------------------------");

    Ok(())
}

async fn extract_archive(archive: &Path, target_dir: &Path) -> anyhow::Result<()> {
    let archive = archive.canonicalize().with_context(|| {
        format!(
            "Failed to get the absolute path for the archive path '{}'",
            archive.display()
        )
    })?;
    ensure!(
        archive.is_file(),
        "Path '{}' is not an archive file",
        archive.display()
    );
    let archive_name = match archive.file_name().and_then(|name| name.to_str()) {
        Some(name) => name,
        None => bail!(
            "Failed to get the archive name from the path '{}'",
            archive.display()
        ),
    };

    if !target_dir.exists() {
        fs::create_dir_all(target_dir).await.with_context(|| {
            format!(
                "Failed to create the target dir at path '{}'",
                target_dir.display()
            )
        })?;
    }
    let target_dir = target_dir.canonicalize().with_context(|| {
        format!(
            "Failed to get the absolute path for the target dir path '{}'",
            target_dir.display()
        )
    })?;
    ensure!(
        target_dir.is_dir(),
        "Path '{}' is not a directory",
        target_dir.display()
    );
    let mut dir_contents = fs::read_dir(&target_dir)
        .await
        .context("Failed to list the target directory contents")?;
    let dir_entry = dir_contents
        .next_entry()
        .await
        .context("Failed to list the target directory contents")?;
    ensure!(
        dir_entry.is_none(),
        "Target directory '{}' is not empty",
        target_dir.display()
    );

    println!(
        "Extracting an archive at path '{}' into directory '{}'",
        archive.display(),
        target_dir.display()
    );

    let mut archive_file = fs::File::open(&archive).await.with_context(|| {
        format!(
            "Failed to get the archive name from the path '{}'",
            archive.display()
        )
    })?;
    let header = compression::read_archive_header(archive_name, &mut archive_file)
        .await
        .context("Failed to read the archive header")?;
    compression::uncompress_with_header(&BTreeSet::new(), &target_dir, header, &mut archive_file)
        .await
        .context("Failed to extract the archive")
}

async fn create_archive(source_dir: &Path, target_dir: &Path) -> anyhow::Result<()> {
    let source_dir = source_dir.canonicalize().with_context(|| {
        format!(
            "Failed to get the absolute path for the source dir path '{}'",
            source_dir.display()
        )
    })?;
    ensure!(
        source_dir.is_dir(),
        "Path '{}' is not a directory",
        source_dir.display()
    );

    if !target_dir.exists() {
        fs::create_dir_all(target_dir).await.with_context(|| {
            format!(
                "Failed to create the target dir at path '{}'",
                target_dir.display()
            )
        })?;
    }
    let target_dir = target_dir.canonicalize().with_context(|| {
        format!(
            "Failed to get the absolute path for the target dir path '{}'",
            target_dir.display()
        )
    })?;
    ensure!(
        target_dir.is_dir(),
        "Path '{}' is not a directory",
        target_dir.display()
    );

    println!(
        "Compressing directory '{}' and creating resulting archive in directory '{}'",
        source_dir.display(),
        target_dir.display()
    );

    let mut metadata_file_contents = None;
    let mut files_co_archive = Vec::new();

    let mut source_dir_contents = fs::read_dir(&source_dir)
        .await
        .context("Failed to read the source directory contents")?;

    while let Some(source_dir_entry) = source_dir_contents
        .next_entry()
        .await
        .context("Failed to read a source dir entry")?
    {
        let entry_path = source_dir_entry.path();
        if entry_path.is_file() {
            if entry_path.file_name().and_then(|name| name.to_str()) == Some(METADATA_FILE_NAME) {
                let metadata_bytes = fs::read(entry_path)
                    .await
                    .context("Failed to read metata file bytes in the source dir")?;
                metadata_file_contents = Some(
                    TimelineMetadata::from_bytes(&metadata_bytes)
                        .context("Failed to parse metata file contents in the source dir")?,
                );
            } else {
                files_co_archive.push(entry_path);
            }
        }
    }

    let metadata = match metadata_file_contents {
        Some(metadata) => metadata,
        None => bail!(
            "No metadata file found in the source dir '{}', cannot create the archive",
            source_dir.display()
        ),
    };

    let _ = compression::archive_files_as_stream(
        &source_dir,
        files_co_archive.iter(),
        &metadata,
        move |mut archive_streamer, archive_name| async move {
            let archive_target = target_dir.join(&archive_name);
            let mut archive_file = fs::File::create(&archive_target).await?;
            io::copy(&mut archive_streamer, &mut archive_file).await?;
            Ok(archive_target)
        },
    )
    .await
    .context("Failed to create an archive")?;

    Ok(())
}
