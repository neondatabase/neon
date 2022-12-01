/// Extensions to `std::fs` types.
use std::{fs, io, path::Path};

pub trait PathExt {
    /// Returns an error if `self` is not a directory.
    fn is_empty_dir(&self) -> io::Result<bool>;
}

impl<P> PathExt for P
where
    P: AsRef<Path>,
{
    fn is_empty_dir(&self) -> io::Result<bool> {
        Ok(fs::read_dir(self)?.into_iter().next().is_none())
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    #[test]
    fn is_empty_dir() {
        use super::PathExt;

        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        // test positive case
        assert!(
            dir_path.is_empty_dir().expect("test failure"),
            "new tempdir should be empty"
        );

        // invoke on a file to ensure it returns an error
        let file_path: PathBuf = dir_path.join("testfile");
        let f = std::fs::File::create(&file_path).unwrap();
        drop(f);
        assert!(file_path.is_empty_dir().is_err());

        // do it again on a path, we know to be nonexistent
        std::fs::remove_file(&file_path).unwrap();
        assert!(file_path.is_empty_dir().is_err());
    }
}
