/// Extensions to `std::fs` types.
use std::{fs, io, path::Path};

use anyhow::Context;

mod rename_noreplace;
pub use rename_noreplace::rename_noreplace;

pub trait PathExt {
    /// Returns an error if `self` is not a directory.
    fn is_empty_dir(&self) -> io::Result<bool>;
}

impl<P> PathExt for P
where
    P: AsRef<Path>,
{
    fn is_empty_dir(&self) -> io::Result<bool> {
        Ok(fs::read_dir(self)?.next().is_none())
    }
}

pub async fn is_directory_empty(path: impl AsRef<Path>) -> anyhow::Result<bool> {
    let mut dir = tokio::fs::read_dir(&path)
        .await
        .context(format!("read_dir({})", path.as_ref().display()))?;
    Ok(dir.next_entry().await?.is_none())
}

pub async fn list_dir(path: impl AsRef<Path>) -> anyhow::Result<Vec<String>> {
    let mut dir = tokio::fs::read_dir(&path)
        .await
        .context(format!("read_dir({})", path.as_ref().display()))?;

    let mut content = vec![];
    while let Some(next) = dir.next_entry().await? {
        let file_name = next.file_name();
        content.push(file_name.to_string_lossy().to_string());
    }

    Ok(content)
}

pub fn ignore_not_found(e: io::Error) -> io::Result<()> {
    if e.kind() == io::ErrorKind::NotFound {
        Ok(())
    } else {
        Err(e)
    }
}

pub fn ignore_absent_files<F>(fs_operation: F) -> io::Result<()>
where
    F: Fn() -> io::Result<()>,
{
    fs_operation().or_else(ignore_not_found)
}

#[cfg(test)]
mod test {
    use crate::fs_ext::{is_directory_empty, list_dir};

    use super::ignore_absent_files;

    #[test]
    fn is_empty_dir() {
        use super::PathExt;

        let dir = camino_tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // test positive case
        assert!(
            dir_path.is_empty_dir().expect("test failure"),
            "new tempdir should be empty"
        );

        // invoke on a file to ensure it returns an error
        let file_path = dir_path.join("testfile");
        let f = std::fs::File::create(&file_path).unwrap();
        drop(f);
        assert!(file_path.is_empty_dir().is_err());

        // do it again on a path, we know to be nonexistent
        std::fs::remove_file(&file_path).unwrap();
        assert!(file_path.is_empty_dir().is_err());
    }

    #[tokio::test]
    async fn is_empty_dir_async() {
        let dir = camino_tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // test positive case
        assert!(
            is_directory_empty(dir_path).await.expect("test failure"),
            "new tempdir should be empty"
        );

        // invoke on a file to ensure it returns an error
        let file_path = dir_path.join("testfile");
        let f = std::fs::File::create(&file_path).unwrap();
        drop(f);
        assert!(is_directory_empty(&file_path).await.is_err());

        // do it again on a path, we know to be nonexistent
        std::fs::remove_file(&file_path).unwrap();
        assert!(is_directory_empty(file_path).await.is_err());
    }

    #[test]
    fn ignore_absent_files_works() {
        let dir = camino_tempfile::tempdir().unwrap();

        let file_path = dir.path().join("testfile");

        ignore_absent_files(|| std::fs::remove_file(&file_path)).expect("should execute normally");

        let f = std::fs::File::create(&file_path).unwrap();
        drop(f);

        ignore_absent_files(|| std::fs::remove_file(&file_path)).expect("should execute normally");

        assert!(!file_path.exists());
    }

    #[tokio::test]
    async fn list_dir_works() {
        let dir = camino_tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        assert!(list_dir(dir_path).await.unwrap().is_empty());

        let file_path = dir_path.join("testfile");
        let _ = std::fs::File::create(&file_path).unwrap();

        assert_eq!(&list_dir(dir_path).await.unwrap(), &["testfile"]);

        let another_dir_path = dir_path.join("testdir");
        std::fs::create_dir(another_dir_path).unwrap();

        let expected = &["testdir", "testfile"];
        let mut actual = list_dir(dir_path).await.unwrap();
        actual.sort();
        assert_eq!(actual, expected);
    }
}
