use std::{
    borrow::Cow,
    ffi::OsStr,
    fs::{self, File},
    io,
    path::{Path, PathBuf},
};

/// Similar to [`std::fs::create_dir`], except we fsync the
/// created directory and its parent.
pub fn create_dir(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();

    fs::create_dir(path)?;
    fsync_file_and_parent(path)?;
    Ok(())
}

/// Similar to [`std::fs::create_dir_all`], except we fsync all
/// newly created directories and the pre-existing parent.
pub fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let mut path = path.as_ref();

    let mut dirs_to_create = Vec::new();

    // Figure out which directories we need to create.
    loop {
        match path.metadata() {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("non-directory found in path: {}", path.display()),
                ));
            }
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }

        dirs_to_create.push(path);

        match path.parent() {
            Some(parent) => path = parent,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("can't find parent of path '{}'", path.display()).as_str(),
                ));
            }
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

/// Adds a suffix to the file(directory) name, either appending the suffix to the end of its extension,
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

pub fn fsync_file_and_parent(file_path: &Path) -> io::Result<()> {
    let parent = file_path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("File {file_path:?} has no parent"),
        )
    })?;

    fsync(file_path)?;
    fsync(parent)?;
    Ok(())
}

pub fn fsync(path: &Path) -> io::Result<()> {
    File::open(path)
        .map_err(|e| io::Error::new(e.kind(), format!("Failed to open the file {path:?}: {e}")))
        .and_then(|file| {
            file.sync_all().map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("Failed to sync file {path:?} data and metadata: {e}"),
                )
            })
        })
        .map_err(|e| io::Error::new(e.kind(), format!("Failed to fsync file {path:?}: {e}")))
}

pub async fn fsync_async(path: impl AsRef<std::path::Path>) -> Result<(), std::io::Error> {
    tokio::fs::File::open(path).await?.sync_all().await
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_create_dir_fsyncd() {
        let dir = tempdir().unwrap();

        let existing_dir_path = dir.path();
        let err = create_dir(existing_dir_path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);

        let child_dir = existing_dir_path.join("child");
        create_dir(child_dir).unwrap();

        let nested_child_dir = existing_dir_path.join("child1").join("child2");
        let err = create_dir(nested_child_dir).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
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
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);

        let invalid_dir_path = file_path.join("folder");
        create_dir_all(invalid_dir_path).unwrap_err();
    }

    #[test]
    fn test_path_with_suffix_extension() {
        let p = PathBuf::from("/foo/bar");
        assert_eq!(
            &path_with_suffix_extension(p, "temp").to_string_lossy(),
            "/foo/bar.temp"
        );
        let p = PathBuf::from("/foo/bar");
        assert_eq!(
            &path_with_suffix_extension(p, "temp.temp").to_string_lossy(),
            "/foo/bar.temp.temp"
        );
        let p = PathBuf::from("/foo/bar.baz");
        assert_eq!(
            &path_with_suffix_extension(p, "temp.temp").to_string_lossy(),
            "/foo/bar.baz.temp.temp"
        );
        let p = PathBuf::from("/foo/bar.baz");
        assert_eq!(
            &path_with_suffix_extension(p, ".temp").to_string_lossy(),
            "/foo/bar.baz..temp"
        );
        let p = PathBuf::from("/foo/bar/dir/");
        assert_eq!(
            &path_with_suffix_extension(p, ".temp").to_string_lossy(),
            "/foo/bar/dir..temp"
        );
    }
}
