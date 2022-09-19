use std::{
    borrow::Cow,
    ffi::OsStr,
    fs::{self, File},
    future::Future,
    io,
    path::{Path, PathBuf},
};

use anyhow::Context;

pub async fn init_via_temporary_directory<C, I, F, O>(
    target_directory: &Path,
    create_tmp_directory: C,
    init_in_directory: I,
) -> anyhow::Result<O>
where
    C: Future<Output = anyhow::Result<PathBuf>>,
    I: FnOnce(PathBuf) -> F,
    F: Future<Output = anyhow::Result<O>>,
{
    let tmp_directory = create_tmp_directory
        .await
        .context("Failed to create temporary directory")?;
    anyhow::ensure!(
        tmp_directory.exists(),
        "Tmp directory {} was not created",
        tmp_directory.display()
    );

    let init_result = init_in_directory(tmp_directory.clone())
        .await
        .with_context(|| {
            format!(
                "Failed to fill temporary directory {} with data",
                tmp_directory.display()
            )
        })
        .and_then(|init_result| {
            fs::rename(&tmp_directory, &target_directory).with_context(|| {
                format!(
                    "failed to move temporary directory {} into the permanent one {}",
                    tmp_directory.display(),
                    target_directory.display()
                )
            })?;
            Ok(init_result)
        });

    let init_result = match init_result {
        Ok(init_result) => init_result,
        Err(init_error) => {
            if tmp_directory.exists() {
                if let Err(removal_error) = fs::remove_dir_all(&tmp_directory) {
                    anyhow::bail!(
                    "Failed to initialize directory {} and remove its temporary directory {}: {:?} and {:?}",
                    target_directory.display(),
                    tmp_directory.display(),
                    init_error,
                    removal_error,
                )
                }
            }
            anyhow::bail!(
                "Failed to initialize directory {}: {:?}",
                target_directory.display(),
                init_error
            )
        }
    };

    let target_dir_parent = target_directory.parent().with_context(|| {
        format!(
            "Failed to get directory parent for {}",
            target_directory.display()
        )
    })?;
    fsync(target_dir_parent).context("Failed to synchronize target directory updates")?;

    Ok(init_result)
}

/// Adds a suffix to the file(directory) name, either appending the suffux to the end of its extension,
/// or if there's no extension, creates one and puts a suffix there.
pub fn path_with_suffix_extension(original_path: impl AsRef<Path>, suffix: &str) -> PathBuf {
    let new_extension = match original_path
        .as_ref()
        .extension()
        .map(OsStr::to_string_lossy)
    {
        Some(extension) => Cow::Owned(format!("{extension}.{suffix}")),
        None => Cow::Borrowed(suffix),
    };
    original_path
        .as_ref()
        .with_extension(new_extension.as_ref())
}

pub fn fsync_file_and_parent(file_path: &Path) -> anyhow::Result<()> {
    let parent = file_path
        .parent()
        .with_context(|| format!("File {} has no parent", file_path.display()))?;

    fsync(file_path)?;
    fsync(parent)?;
    Ok(())
}

pub fn fsync(path: &Path) -> anyhow::Result<()> {
    File::open(path)
        .context("Failed to open the file")
        .and_then(|file| file.sync_all().context("Failed to sync file metadata"))
        .with_context(|| format!("Failed to fsync file {}", path.display()))
}

/// Similar to [`std::fs::create_dir`], except we fsync the
/// created directory and its parent.
pub fn create_dir(path: impl AsRef<Path>) -> anyhow::Result<()> {
    let directory_path = path.as_ref();

    fs::create_dir(directory_path)
        .with_context(|| format!("Failed to create directory {}", directory_path.display()))?;
    fsync_file_and_parent(directory_path).context("failed to fsync created directory")?;
    Ok(())
}

/// Similar to [`std::fs::create_dir_all`], except we fsync all
/// newly created directories and the pre-existing parent.
pub fn create_dir_all(path: impl AsRef<Path>) -> anyhow::Result<()> {
    let mut path = path.as_ref();

    let mut dirs_to_create = Vec::new();

    // Figure out which directories we need to create.
    loop {
        match path.metadata() {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => anyhow::bail!("non-directory found in path: {}", path.display()),
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => anyhow::bail!(
                "Error during path {} metadata retrieval: {}",
                path.display(),
                e
            ),
        }

        dirs_to_create.push(path);

        match path.parent() {
            Some(parent) => path = parent,
            None => anyhow::bail!("can't find parent of path '{}'", path.display()),
        }
    }

    // Create directories from parent to child.
    for &path in dirs_to_create.iter().rev() {
        fs::create_dir(path)?;
    }

    // Fsync the created directories from child to parent.
    for &path in dirs_to_create.iter() {
        fsync(path)?;
    }

    // If we created any new directories, fsync the parent.
    if !dirs_to_create.is_empty() {
        fsync(path)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_path_with_suffix_extension() {
        let p = PathBuf::from("/foo/bar");
        assert_eq!(
            &path_with_suffix_extension(&p, "temp").to_string_lossy(),
            "/foo/bar.temp"
        );
        let p = PathBuf::from("/foo/bar");
        assert_eq!(
            &path_with_suffix_extension(&p, "temp.temp").to_string_lossy(),
            "/foo/bar.temp.temp"
        );
        let p = PathBuf::from("/foo/bar.baz");
        assert_eq!(
            &path_with_suffix_extension(&p, "temp.temp").to_string_lossy(),
            "/foo/bar.baz.temp.temp"
        );
        let p = PathBuf::from("/foo/bar.baz");
        assert_eq!(
            &path_with_suffix_extension(&p, ".temp").to_string_lossy(),
            "/foo/bar.baz..temp"
        );
        let p = PathBuf::from("/foo/bar/dir/");
        assert_eq!(
            &path_with_suffix_extension(&p, ".temp").to_string_lossy(),
            "/foo/bar/dir..temp"
        );
    }

    #[test]
    fn test_create_dir_fsyncd() {
        let dir = tempdir().unwrap();

        let existing_dir_path = dir.path();
        let err = create_dir(existing_dir_path).unwrap_err();
        let error_message = format!("{:#}", err);
        assert!(
            error_message.contains("File exists"),
            "Unexpected error message: {error_message}"
        );

        let child_dir = existing_dir_path.join("child");
        create_dir(child_dir).unwrap();

        let nested_child_dir = existing_dir_path.join("child1").join("child2");
        let err = create_dir(nested_child_dir).unwrap_err();
        let error_message = format!("{:#}", err);
        assert!(
            error_message.contains("No such file or directory"),
            "Unexpected error message: {error_message}"
        );
    }

    #[test]
    fn test_create_dir_all_fsyncd() {
        let dir = tempdir().unwrap();

        let existing_dir_path = dir.path();
        create_dir_all(existing_dir_path).unwrap();

        let child_dir = existing_dir_path.join("child");
        assert!(!child_dir.exists());
        create_dir_all(&child_dir).unwrap();
        assert!(child_dir.exists());

        let nested_child_dir = existing_dir_path.join("child1").join("child2");
        assert!(!nested_child_dir.exists());
        create_dir_all(&nested_child_dir).unwrap();
        assert!(nested_child_dir.exists());

        let file_path = existing_dir_path.join("file");
        std::fs::write(&file_path, b"").unwrap();

        let err = create_dir_all(&file_path).unwrap_err();
        let error_message = format!("{:#}", err);
        assert!(
            error_message.contains("non-directory found in path"),
            "Unexpected error message: {error_message}"
        );

        let invalid_dir_path = file_path.join("folder");
        create_dir_all(&invalid_dir_path).unwrap_err();
    }
}
